// Command raftkvd 是一个 Raft 节点的宿主进程：起一个 gRPC server，
// 挂上 Raft peer 平面服务（RaftService），N 个进程组成一个集群。
//
// Phase 1 里程碑 ②：先只跑 Raft（选主 + 复制），暂不挂 KV service。
// applyCh 目前被排空丢弃；里程碑 ③ 会换成真正的 KV 状态机。
//
// 用法示例（本地 3 节点）：
//
//	raftkvd --node-id n1 --listen 127.0.0.1:5001 --data-dir ./data/n1 \
//	        --peers n1=127.0.0.1:5001,n2=127.0.0.1:5002,n3=127.0.0.1:5003
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"

	"6.5840/filewal"
	kvraft "6.5840/kvraft1"
	"6.5840/proto"
	raft "6.5840/raft1"
	"6.5840/transport"
	grpcx "6.5840/transport/grpc"
	"6.5840/wal"

	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type config struct {
	nodeID  transport.NodeID
	listen  string
	dataDir string
	peers   map[transport.NodeID]string // NodeID → host:port
}

func main() {
	cfg, err := parseFlags()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config error:", err)
		flag.Usage()
		os.Exit(2)
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})).
		With("node_id", string(cfg.nodeID))
	slog.SetDefault(log)

	if err := run(cfg, log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(cfg config, log *slog.Logger) error {
	// --- 稳定顺序的 nodeIDs，peers[i] 对应 nodeIDs[i] ---
	nodeIDs := make([]transport.NodeID, 0, len(cfg.peers))
	for id := range cfg.peers {
		nodeIDs = append(nodeIDs, id)
	}
	sort.Slice(nodeIDs, func(i, j int) bool { return nodeIDs[i] < nodeIDs[j] })

	// --- 每个 peer 一条 gRPC ClientEnd（惰性连接，peer 未就绪也没关系）---
	ends := make([]transport.ClientEnd, len(nodeIDs))
	grpcEnds := make([]*grpcx.ClientEnd, len(nodeIDs))
	for i, id := range nodeIDs {
		ce, err := grpcx.NewClientEnd(cfg.peers[id])
		if err != nil {
			return fmt.Errorf("dial peer %s (%s): %w", id, cfg.peers[id], err)
		}
		ends[i] = ce
		grpcEnds[i] = ce
	}
	defer func() {
		for _, ce := range grpcEnds {
			_ = ce.Close()
		}
	}()

	// --- 落盘 WAL（Phase 2：真 append-only fileWAL，替代 Phase 1 的全量 blob Persister）---
	if err := os.MkdirAll(cfg.dataDir, 0o755); err != nil {
		return fmt.Errorf("mkdir data-dir: %w", err)
	}
	fw, err := filewal.Open(wal.OSFS(), cfg.dataDir)
	if err != nil {
		return fmt.Errorf("open filewal: %w", err)
	}

	// --- 启动 KV 节点（内部经 rsm 创建 Raft，applyCh 喂 KV 状态机）---
	// maxraftstate=-1：暂不快照（4a 上 pebble 后再开，测 SaveSnapshot 路径）。
	kv, rfi := kvraft.StartKVServerGrpc(ends, cfg.nodeID, nodeIDs, fw, -1)
	rf, ok := rfi.(*raft.Raft)
	if !ok {
		return fmt.Errorf("raft 返回类型非 *raft.Raft，无法挂 RaftService")
	}

	// --- gRPC server 同时挂 RaftService(peer 平面) + KVService(client 平面) ---
	// Phase 1 两平面 co-locate 在一个端口；Phase 3 再按 etcd 式拆分。
	lis, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.listen, err)
	}
	srv := grpclib.NewServer()
	proto.RegisterRaftServer(srv, grpcx.NewRaftService(rf))
	proto.RegisterKVServer(srv, grpcx.NewKVService(kv))
	reflection.Register(srv) // 便于 grpcurl 等工具免 .proto 直接调用

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()
	log.Info("raftkvd started",
		"listen", cfg.listen, "data_dir", cfg.dataDir, "peers", len(nodeIDs))

	// --- 等 SIGTERM/SIGINT 优雅停机 ---
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case s := <-sig:
		log.Info("shutting down", "signal", s.String())
		srv.GracefulStop() // leadership transfer 留到 Phase 3
		return nil
	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("grpc serve: %w", err)
		}
		return nil
	}
}

// --- flag 解析 ---

func parseFlags() (config, error) {
	var (
		nodeID  = flag.String("node-id", "", "本节点的稳定 NodeID（如 n1），必须出现在 --peers 中")
		listen  = flag.String("listen", "", "gRPC 监听地址 host:port（如 127.0.0.1:5001）")
		dataDir = flag.String("data-dir", "", "持久化数据目录")
		peers   = flag.String("peers", "", "集群成员表：NodeID=host:port，逗号分隔（含本节点）")
	)
	flag.Parse()

	cfg := config{
		nodeID:  transport.NodeID(*nodeID),
		listen:  *listen,
		dataDir: *dataDir,
	}
	if cfg.nodeID == "" {
		return cfg, fmt.Errorf("--node-id 必填")
	}
	if cfg.listen == "" {
		return cfg, fmt.Errorf("--listen 必填")
	}
	if cfg.dataDir == "" {
		return cfg, fmt.Errorf("--data-dir 必填")
	}
	m, err := parsePeers(*peers)
	if err != nil {
		return cfg, err
	}
	if _, ok := m[cfg.nodeID]; !ok {
		return cfg, fmt.Errorf("--node-id %q 不在 --peers 中", cfg.nodeID)
	}
	cfg.peers = m
	return cfg, nil
}

// parsePeers 解析 "n1=host1:port1,n2=host2:port2" 成 NodeID→addr。
func parsePeers(s string) (map[transport.NodeID]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, fmt.Errorf("--peers 必填")
	}
	m := make(map[transport.NodeID]string)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		id, addr = strings.TrimSpace(id), strings.TrimSpace(addr)
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("--peers 条目格式应为 NodeID=host:port，得到 %q", part)
		}
		if _, dup := m[transport.NodeID(id)]; dup {
			return nil, fmt.Errorf("--peers 中 NodeID %q 重复", id)
		}
		m[transport.NodeID(id)] = addr
	}
	if len(m) == 0 {
		return nil, fmt.Errorf("--peers 为空")
	}
	return m, nil
}
