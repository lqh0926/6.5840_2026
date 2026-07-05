package grpc

import (
	"context"
	"net"
	"testing"
	"time"

	"6.5840/proto"
	raft "6.5840/raft1"
	"6.5840/transport"

	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

// stubRaftServer 实现 proto.RaftServer，用于测试。
type stubRaftServer struct {
	proto.UnimplementedRaftServer
	term        int64
	voteGranted bool
}

func (s *stubRaftServer) RequestVote(_ context.Context, args *proto.RequestVoteArgs) (*proto.RequestVoteReply, error) {
	if args.Term > s.term {
		s.term = args.Term
		s.voteGranted = true
		return &proto.RequestVoteReply{Term: s.term, VoteGranted: true}, nil
	}
	return &proto.RequestVoteReply{Term: s.term, VoteGranted: false}, nil
}

func (s *stubRaftServer) AppendEntries(_ context.Context, args *proto.AppendEntriesArgs) (*proto.AppendEntriesReply, error) {
	if args.Term >= s.term {
		s.term = args.Term
		return &proto.AppendEntriesReply{Term: s.term, Success: true, LastMatchIndex: args.PrevLogIndex + int64(len(args.Entries))}, nil
	}
	return &proto.AppendEntriesReply{Term: s.term, Success: false}, nil
}

// stubKVServer 实现 proto.KVServer，用于测试。
type stubKVServer struct {
	proto.UnimplementedKVServer
	data    map[string]string
	version map[string]uint64
}

func (s *stubKVServer) Get(_ context.Context, args *proto.GetArgs) (*proto.GetReply, error) {
	v, ok := s.data[args.Key]
	if !ok {
		return &proto.GetReply{Err: proto.Err_ERR_NO_KEY}, nil
	}
	return &proto.GetReply{Value: v, Version: s.version[args.Key], Err: proto.Err_OK}, nil
}

func (s *stubKVServer) Put(_ context.Context, args *proto.PutArgs) (*proto.PutReply, error) {
	if args.Version != 0 && s.version[args.Key] != args.Version {
		return &proto.PutReply{Err: proto.Err_ERR_VERSION}, nil
	}
	s.data[args.Key] = args.Value
	s.version[args.Key]++
	return &proto.PutReply{Err: proto.Err_OK}, nil
}

// newBufconnServer 创建一个基于 bufconn 的内存 gRPC 服务端，
// 并返回通过 bufconn 与之通信的 ClientEnd（不走真实 TCP）。
func newBufconnServer(t *testing.T) (*ClientEnd, func()) {
	t.Helper()

	lis := bufconn.Listen(bufSize)
	srv := grpclib.NewServer()

	proto.RegisterRaftServer(srv, &stubRaftServer{term: 1})
	proto.RegisterKVServer(srv, &stubKVServer{
		data:    map[string]string{},
		version: map[string]uint64{},
	})

	go func() {
		if err := srv.Serve(lis); err != nil && err != grpclib.ErrServerStopped {
			t.Logf("服务端异常退出: %v", err)
		}
	}()

	// 通过 bufconn dialer 创建 ClientEnd
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
	}

	return ce, cleanup
}

// ── 接口合规 ────────────────────────────────────────────────────

func TestImplementsClientEnd(t *testing.T) {
	var ce transport.ClientEnd
	ce2, cleanup := newBufconnServer(t)
	defer cleanup()
	ce = ce2 // 编译期接口检查；赋值避免 unused 警告
	_ = ce
}

// ── Raft RPC 测试 ───────────────────────────────────────────────

func TestCallRequestVote(t *testing.T) {
	ce, cleanup := newBufconnServer(t)
	defer cleanup()

	// 调用方（raft）传自己的 Go 结构体；ClientEnd 内部转 proto。
	args := &raft.RequestVoteArgs{
		Term:         2,
		CandidateId:  "n1",
		LastLogIndex: 5,
		LastLogTerm:  1,
	}
	reply := &raft.RequestVoteReply{}

	ok := ce.Call("Raft.RequestVote", args, reply)
	if !ok {
		t.Fatal("Call 返回 false")
	}
	if !reply.VoteGranted {
		t.Error("期望 VoteGranted=true，实际为 false")
	}
	if reply.Term != 2 {
		t.Errorf("期望 Term=2，实际为 %d", reply.Term)
	}
}

func TestCallRequestVoteRejected(t *testing.T) {
	ce, cleanup := newBufconnServer(t)
	defer cleanup()

	// 第一次投票：term=2，服务端 term 推进到 2
	args1 := &raft.RequestVoteArgs{Term: 2, CandidateId: "n1", LastLogIndex: 5, LastLogTerm: 1}
	reply1 := &raft.RequestVoteReply{}
	ce.Call("Raft.RequestVote", args1, reply1)

	// 过期的 term=1 投票应被拒绝
	args2 := &raft.RequestVoteArgs{Term: 1, CandidateId: "n2", LastLogIndex: 3, LastLogTerm: 0}
	reply2 := &raft.RequestVoteReply{}

	ok := ce.Call("Raft.RequestVote", args2, reply2)
	if !ok {
		t.Fatal("Call 返回 false")
	}
	if reply2.VoteGranted {
		t.Error("过期的 term 应返回 VoteGranted=false")
	}
}

// ── KV RPC 测试 ─────────────────────────────────────────────────

func TestCallKVPutGet(t *testing.T) {
	ce, cleanup := newBufconnServer(t)
	defer cleanup()

	// Put
	putArgs := &proto.PutArgs{Key: "hello", Value: "world", Version: 0}
	putReply := &proto.PutReply{}
	ok := ce.Call("KVServer.Put", putArgs, putReply)
	if !ok {
		t.Fatal("Put Call 返回 false")
	}
	if putReply.Err != proto.Err_OK {
		t.Errorf("Put: 期望 OK，实际为 %v", putReply.Err)
	}

	// Get
	getArgs := &proto.GetArgs{Key: "hello"}
	getReply := &proto.GetReply{}
	ok = ce.Call("KVServer.Get", getArgs, getReply)
	if !ok {
		t.Fatal("Get Call 返回 false")
	}
	if getReply.Err != proto.Err_OK {
		t.Errorf("Get: 期望 OK，实际为 %v", getReply.Err)
	}
	if getReply.Value != "world" {
		t.Errorf("Get: 期望 'world'，实际为 %q", getReply.Value)
	}
}

func TestCallKVPutVersionConflict(t *testing.T) {
	ce, cleanup := newBufconnServer(t)
	defer cleanup()

	// version=0 创建
	ce.Call("KVServer.Put", &proto.PutArgs{Key: "k", Value: "v1", Version: 0}, &proto.PutReply{})

	// 错误的 version 写入
	putArgs := &proto.PutArgs{Key: "k", Value: "v2", Version: 99}
	putReply := &proto.PutReply{}
	ok := ce.Call("KVServer.Put", putArgs, putReply)
	if !ok {
		t.Fatal("Call 返回 false")
	}
	if putReply.Err != proto.Err_ERR_VERSION {
		t.Errorf("期望 ERR_VERSION，实际为 %v", putReply.Err)
	}
}

// ── 错误场景 ────────────────────────────────────────────────────

func TestCallUnknownMethod(t *testing.T) {
	ce, cleanup := newBufconnServer(t)
	defer cleanup()

	ok := ce.Call("NoSuchService.Method", &proto.GetArgs{}, &proto.GetReply{})
	if ok {
		t.Error("未知方法应返回 false")
	}
}

func TestCallWrongArgsType(t *testing.T) {
	ce, cleanup := newBufconnServer(t)
	defer cleanup()

	// 把 GetArgs 传给 Raft 方法 —— 类型断言失败，应优雅返回 false
	ok := ce.Call("Raft.RequestVote", &proto.GetArgs{Key: "x"}, &proto.RequestVoteReply{})
	if ok {
		t.Error("错误 args 类型应返回 false")
	}
}

func TestCallWrongReplyType(t *testing.T) {
	ce, cleanup := newBufconnServer(t)
	defer cleanup()

	args := &raft.RequestVoteArgs{Term: 2, CandidateId: "n1"} // 正确 args 类型
	var reply proto.GetReply                                 // 错误 reply 类型
	ok := ce.Call("Raft.RequestVote", args, &reply)
	if ok {
		t.Error("错误 reply 类型应返回 false")
	}
}
