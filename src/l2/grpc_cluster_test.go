package l2

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	kvraft "6.5840/kvraft1"
	"6.5840/persist"
	"6.5840/proto"
	raft "6.5840/raft1"
	"6.5840/transport"
	grpcx "6.5840/transport/grpc"

	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const l2MaxRaftState = -1 // L2 暂不快照

// grpcNode 是集群里的一个进程内节点（真 gRPC，但同进程）。
type grpcNode struct {
	id      string
	addr    string
	dataDir string
	nodeIDs []string
	peers   map[string]string // NodeID → addr

	mu     sync.Mutex
	alive  bool
	srv    *grpclib.Server
	rf     *raft.Raft
	ends   []*grpcx.ClientEnd
	kvConn *grpclib.ClientConn
	kvCli  proto.KVClient
}

// grpcCluster 是 Cluster 的 gRPC 实现：N 个进程内节点，彼此用真 gRPC 互连。
type grpcCluster struct {
	t       *testing.T
	nodeIDs []string
	peers   map[string]string
	nodes   map[string]*grpcNode
	fault   *FaultPolicy
}

var _ Cluster = (*grpcCluster)(nil)

// newGrpcCluster 起 n 个进程内 gRPC 节点并互连。
func newGrpcCluster(t *testing.T, n int) *grpcCluster {
	nodeIDs := make([]string, 0, n)
	peers := make(map[string]string, n)
	listeners := make(map[string]net.Listener, n)
	base := t.TempDir()

	for i := 0; i < n; i++ {
		id := fmt.Sprintf("n%d", i)
		lis, err := net.Listen("tcp", "127.0.0.1:0") // 预留端口拿地址
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		nodeIDs = append(nodeIDs, id)
		peers[id] = lis.Addr().String()
		listeners[id] = lis
	}
	sort.Strings(nodeIDs)

	c := &grpcCluster{t: t, nodeIDs: nodeIDs, peers: peers, nodes: map[string]*grpcNode{}, fault: NewFaultPolicy()}
	for _, id := range nodeIDs {
		nd := &grpcNode{
			id:      id,
			addr:    peers[id],
			dataDir: filepath.Join(base, id),
			nodeIDs: nodeIDs,
			peers:   peers,
		}
		c.nodes[id] = nd
		c.startNode(nd, listeners[id])
	}
	return c
}

// startNode 用给定 listener（首次）或同 addr 重新监听（重启）启动一个节点。
func (c *grpcCluster) startNode(nd *grpcNode, lis net.Listener) {
	t := c.t
	if lis == nil {
		var err error
		lis, err = net.Listen("tcp", nd.addr) // 重启：复用同 addr（SO_REUSEADDR）
		if err != nil {
			t.Fatalf("relisten %s: %v", nd.addr, err)
		}
	}

	ends := make([]transport.ClientEnd, len(nd.nodeIDs))
	grpcEnds := make([]*grpcx.ClientEnd, len(nd.nodeIDs))
	for i, pid := range nd.nodeIDs {
		// 每条 peer 连接装一个知道 source→target 的 client 拦截器，用于分区注入。
		ce, err := grpcx.NewClientEnd(nd.peers[pid],
			grpclib.WithUnaryInterceptor(c.fault.ClientInterceptor(nd.id, pid)))
		if err != nil {
			t.Fatalf("dial peer %s: %v", pid, err)
		}
		ends[i] = ce
		grpcEnds[i] = ce
	}

	persister, err := persist.OpenFilePersister(nd.dataDir)
	if err != nil {
		t.Fatalf("persister %s: %v", nd.id, err)
	}
	kv, rfi := kvraft.StartKVServerGrpc(ends, transport.NodeID(nd.id), toNodeIDs(nd.nodeIDs), persister, l2MaxRaftState)
	rf := rfi.(*raft.Raft)

	srv := grpclib.NewServer(grpclib.UnaryInterceptor(c.fault.ServerInterceptor(nd.id)))
	proto.RegisterRaftServer(srv, grpcx.NewRaftService(rf))
	proto.RegisterKVServer(srv, grpcx.NewKVService(kv))
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpclib.NewClient(nd.addr, grpclib.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("kv self-dial %s: %v", nd.id, err)
	}

	nd.mu.Lock()
	nd.alive = true
	nd.srv = srv
	nd.rf = rf
	nd.ends = grpcEnds
	nd.kvConn = conn
	nd.kvCli = proto.NewKVClient(conn)
	nd.mu.Unlock()
}

func (c *grpcCluster) NodeIDs() []string { return c.nodeIDs }

// Leader 直接读进程内每个节点的 raft.GetState（无需 RPC），返回 term 最高的 leader。
func (c *grpcCluster) Leader() (string, bool) {
	best, bestTerm := "", -1
	for _, id := range c.nodeIDs {
		nd := c.nodes[id]
		nd.mu.Lock()
		alive, rf := nd.alive, nd.rf
		nd.mu.Unlock()
		if !alive || rf == nil {
			continue
		}
		if term, isLeader := rf.GetState(); isLeader && term > bestTerm {
			best, bestTerm = id, term
		}
	}
	return best, best != ""
}

func (c *grpcCluster) Put(key, value string, version uint64) Result {
	return c.sweep(func(cli proto.KVClient, ctx context.Context) (proto.Err, string, uint64, error) {
		rep, err := cli.Put(ctx, &proto.PutArgs{Key: key, Value: value, Version: version})
		if err != nil {
			return 0, "", 0, err
		}
		return rep.Err, "", 0, nil
	})
}

func (c *grpcCluster) Get(key string) Result {
	return c.sweep(func(cli proto.KVClient, ctx context.Context) (proto.Err, string, uint64, error) {
		rep, err := cli.Get(ctx, &proto.GetArgs{Key: key})
		if err != nil {
			return 0, "", 0, err
		}
		return rep.Err, rep.Value, rep.Version, nil
	})
}

// sweep 对存活节点依次尝试一次 KV 调用，遇 ErrWrongLeader/连接失败换下一个，
// 整轮失败则退避重试，直到拿到定性回复或超时。
func (c *grpcCluster) sweep(call func(proto.KVClient, context.Context) (proto.Err, string, uint64, error)) Result {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range c.nodeIDs {
			cli := c.kvClient(id)
			if cli == nil {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			e, val, ver, err := call(cli, ctx)
			cancel()
			if err != nil || e == proto.Err_ERR_WRONG_LEADER {
				continue
			}
			return Result{Value: val, Version: ver, Err: errString(e)}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return Result{} // Err="" 超时未找到 leader
}

func (c *grpcCluster) Crash(id string) {
	nd := c.nodes[id]
	nd.mu.Lock()
	defer nd.mu.Unlock()
	if !nd.alive {
		return
	}
	nd.srv.Stop() // 立即停服，peer 连接被拒
	nd.rf.Kill()  // 停 raft goroutine，销毁内存驱动（只剩落盘状态）
	for _, ce := range nd.ends {
		_ = ce.Close()
	}
	_ = nd.kvConn.Close()
	nd.alive = false
	nd.srv, nd.rf, nd.ends, nd.kvConn, nd.kvCli = nil, nil, nil, nil, nil
}

func (c *grpcCluster) Restart(id string) {
	nd := c.nodes[id]
	nd.mu.Lock()
	alive := nd.alive
	nd.mu.Unlock()
	if alive {
		return
	}
	c.startNode(nd, nil) // 同 addr、同 dataDir → FilePersister 恢复；peers 自动重连
}

// Disconnect/Connect 把节点从 peer 平面隔离/恢复（经 fault interceptor）。
func (c *grpcCluster) Disconnect(id string) { c.fault.Partition(id) }
func (c *grpcCluster) Connect(id string)    { c.fault.Heal(id) }

func (c *grpcCluster) Cleanup() {
	for _, id := range c.nodeIDs {
		c.Crash(id)
	}
}

func (c *grpcCluster) kvClient(id string) proto.KVClient {
	nd := c.nodes[id]
	nd.mu.Lock()
	defer nd.mu.Unlock()
	if !nd.alive {
		return nil
	}
	return nd.kvCli
}

func toNodeIDs(ids []string) []transport.NodeID {
	out := make([]transport.NodeID, len(ids))
	for i, id := range ids {
		out[i] = transport.NodeID(id)
	}
	return out
}

func errString(e proto.Err) string {
	switch e {
	case proto.Err_OK:
		return "OK"
	case proto.Err_ERR_NO_KEY:
		return "ErrNoKey"
	case proto.Err_ERR_VERSION:
		return "ErrVersion"
	case proto.Err_ERR_WRONG_LEADER:
		return "ErrWrongLeader"
	case proto.Err_ERR_MAYBE:
		return "ErrMaybe"
	default:
		return "ErrUnspecified"
	}
}
