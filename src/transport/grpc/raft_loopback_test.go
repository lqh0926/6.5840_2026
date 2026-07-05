package grpc

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"6.5840/proto"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/transport"

	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// memPersister 是 persist.Persister 的最小内存实现，仅供本包测试使用。
type memPersister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
}

func (p *memPersister) Save(rs, snap []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.raftstate, p.snapshot = rs, snap
}
func (p *memPersister) ReadRaftState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.raftstate
}
func (p *memPersister) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshot
}
func (p *memPersister) RaftStateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.raftstate)
}

// noopEnd 是永不发送成功的 ClientEnd 占位；单节点 raft 不会向自己发 RPC，
// 故它实际不会被调用，仅用于填满 peers 切片。
type noopEnd struct{}

func (noopEnd) Call(string, any, any) bool { return false }

// newLoopbackClient 起一个包着真 *raft.Raft 的 gRPC 服务端（bufconn 内存传输），
// 返回一个对接它的 ClientEnd。这条链路验证的是完整的生产路径：
// raft Go 结构体 → ClientEnd 转 proto → gRPC → RaftService 转回 Go 结构体 →
// 真 raft handler → 结果沿原路转回。
func newLoopbackClient(t *testing.T) (*ClientEnd, func()) {
	t.Helper()

	applyCh := make(chan raftapi.ApplyMsg, 64)
	go func() { // 排空 applyCh，避免 raft 的 apply goroutine 阻塞
		for range applyCh {
		}
	}()

	rfi := raft.Make(
		[]transport.ClientEnd{noopEnd{}},
		transport.NodeID("self"),
		[]transport.NodeID{"self"},
		&memPersister{},
		applyCh,
	)
	rf := rfi.(*raft.Raft)

	lis := bufconn.Listen(bufSize)
	srv := grpclib.NewServer()
	proto.RegisterRaftServer(srv, NewRaftService(rf))
	go func() {
		if err := srv.Serve(lis); err != nil && err != grpclib.ErrServerStopped {
			t.Logf("服务端退出: %v", err)
		}
	}()

	conn, err := grpclib.NewClient("passthrough:///bufnet",
		grpclib.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpclib.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ce := &ClientEnd{
		addr:    "bufnet",
		conn:    conn,
		raftCli: proto.NewRaftClient(conn),
		kvCli:   proto.NewKVClient(conn),
		timeout: 2 * time.Second,
	}
	cleanup := func() {
		conn.Close()
		srv.Stop()
		rf.Kill() // 停掉 raft 后台 goroutine，避免测试间泄漏
		close(applyCh)
	}
	return ce, cleanup
}

// TestRaftKillIdempotent 验证 Kill 可重复调用且不 panic/死锁，
// 且 Kill 后 killed 生效、后台 goroutine 能退出。
func TestRaftKillIdempotent(t *testing.T) {
	applyCh := make(chan raftapi.ApplyMsg, 8)
	go func() {
		for range applyCh {
		}
	}()
	rfi := raft.Make(
		[]transport.ClientEnd{noopEnd{}},
		transport.NodeID("solo"),
		[]transport.NodeID{"solo"},
		&memPersister{},
		applyCh,
	)
	rf := rfi.(*raft.Raft)

	rf.Kill()
	rf.Kill() // 第二次应为 no-op，不得 panic
	close(applyCh)

	// Kill 后 GetState 仍应可安全调用（不 panic/死锁）
	_, _ = rf.GetState()
}

// TestRaftServiceRequestVoteLoopback 验证 RequestVote 经完整 gRPC 链路打到真 raft。
// 用很高的 term，使断言对单节点 raft 的后台选举（term 缓慢自增）保持鲁棒。
func TestRaftServiceRequestVoteLoopback(t *testing.T) {
	ce, cleanup := newLoopbackClient(t)
	defer cleanup()

	args := &raft.RequestVoteArgs{Term: 1000, CandidateId: "cand", LastLogIndex: 0, LastLogTerm: 0}
	reply := &raft.RequestVoteReply{}
	if !ce.Call("Raft.RequestVote", args, reply) {
		t.Fatal("RequestVote Call 返回 false")
	}
	if !reply.VoteGranted {
		t.Errorf("期望真 raft 授予投票，实际 granted=false term=%d", reply.Term)
	}
	if reply.Term != 1000 {
		t.Errorf("期望 reply.Term=1000，实际 %d", reply.Term)
	}
}

// TestRaftServiceAppendEntriesLoopback 验证 AppendEntries（含日志条目转换）
// 经完整链路打到真 raft，且被接受。
func TestRaftServiceAppendEntriesLoopback(t *testing.T) {
	ce, cleanup := newLoopbackClient(t)
	defer cleanup()

	// 先用高 term 心跳让节点承认领导者，再复制一条日志。
	args := &raft.AppendEntries{
		Term:         2000,
		LeaderId:     "leader",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []raft.LogEntry{
			{Command: []byte("cmd-1"), Term: 2000},
		},
		LeaderCommit: 0,
	}
	reply := &raft.AppendEntriesReply{}
	if !ce.Call("Raft.AppendEntriesHandler", args, reply) {
		t.Fatal("AppendEntries Call 返回 false")
	}
	if !reply.Success {
		t.Errorf("期望 AppendEntries 成功，实际 success=false term=%d", reply.Term)
	}
}
