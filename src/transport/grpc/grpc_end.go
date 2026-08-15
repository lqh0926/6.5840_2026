// Package grpc 提供基于 gRPC 的 transport.ClientEnd 实现。
// 每个 ClientEnd 封装一条到指定 peer 的 gRPC 连接，
// 通过方法名字符串派发 Call(svcMeth, args, reply) 到对应的强类型 gRPC 方法。
// 这个 method-name switch 是有意集中在此处的（参见 ROADMAP.md 设计决策）——
// 它避免了类型化接口导致的 import 循环，并保持传输层不依赖 Raft/KV 语义。
package grpc

import (
	"context"
	"time"

	"6.5840/proto"
	raft "6.5840/raft1"
	"6.5840/transport"

	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// 编译期接口检查
var _ transport.ClientEnd = (*ClientEnd)(nil)

// DefaultTimeout 是每个 RPC 的默认超时时间。
const DefaultTimeout = 5 * time.Second

// ClientEnd 是 gRPC 实现的 transport.ClientEnd。
// 它持有到一个 peer 的单条 gRPC 连接，并在这条连接上复用
// Raft（peer 平面）和 KV（client 平面）两种 RPC。
type ClientEnd struct {
	addr    string
	conn    *grpclib.ClientConn
	raftCli proto.RaftClient
	kvCli   proto.KVClient
	timeout time.Duration
}

// NewClientEnd 创建一个连接到 addr 的 ClientEnd。
// 底层 gRPC 连接采用惰性建立（首次 RPC 调用时才真正建连）。
// 用完后应调用 Close() 释放资源。
// 额外的 opts 会追加到默认拨号选项之后（L2 测试用来注入 fault interceptor）。
func NewClientEnd(addr string, opts ...grpclib.DialOption) (*ClientEnd, error) {
	dialOpts := append([]grpclib.DialOption{
		grpclib.WithTransportCredentials(insecure.NewCredentials()),
	}, opts...)
	conn, err := grpclib.NewClient(addr, dialOpts...)
	if err != nil {
		return nil, err
	}
	return &ClientEnd{
		addr:    addr,
		conn:    conn,
		raftCli: proto.NewRaftClient(conn),
		kvCli:   proto.NewKVClient(conn),
		timeout: DefaultTimeout,
	}, nil
}

// Addr 返回本 ClientEnd 所连接的 peer 地址。
func (c *ClientEnd) Addr() string { return c.addr }

// SetTimeout 覆盖每次 RPC 的超时时间。
func (c *ClientEnd) SetTimeout(d time.Duration) { c.timeout = d }

// Close 关闭底层 gRPC 连接。
func (c *ClientEnd) Close() error {
	return c.conn.Close()
}

// Call 按 svcMeth 派发到对应的 gRPC 方法。
//
// svcMeth 沿用 labrpc 的命名约定："Service.Method"。
// 支持的方法及其 args/reply 类型（各服务用自己的原生 Go 类型，转换收在内部）：
//
//	Raft.RequestVote           → *raft.RequestVoteArgs / *raft.RequestVoteReply
//	Raft.AppendEntriesHandler  → *raft.AppendEntries   / *raft.AppendEntriesReply
//	                             （Snapshot!=nil 时内部路由到 gRPC InstallSnapshot）
//	Raft.TimeoutNow           → *raft.TimeoutNowArgs  / *raft.TimeoutNowReply
//	KVServer.Get               → *proto.GetArgs   / *proto.GetReply
//	KVServer.Put               → *proto.PutArgs   / *proto.PutReply
//	KVServer.Append            → *proto.AppendArgs / *proto.AppendReply
//
// 注：Raft 平面收 raft 的 Go 结构体（raft 不依赖 proto）；KV 平面暂用 proto
// 消息（KV clerk 的 gRPC 迁移是独立一步）。
// 成功返回 true；任何错误（连接失败、超时、类型不匹配、未知方法）均返回 false。
func (c *ClientEnd) Call(svcMeth string, args any, reply any) bool {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	switch svcMeth {
	// ── Raft peer 平面 ──────────────────────────────────────────
	// args/reply 是 raft 自己的 Go 结构体（raft 不认识 proto）；
	// 转换与快照路由都在这里完成，raft 只管调 Call。
	case "Raft.RequestVote":
		a, ok := args.(*raft.RequestVoteArgs)
		if !ok {
			return false
		}
		r, ok := reply.(*raft.RequestVoteReply)
		if !ok {
			return false
		}
		resp, err := c.raftCli.RequestVote(ctx, requestVoteToPB(a))
		if err != nil {
			return false
		}
		requestVoteReplyFromPB(resp, r)
		return true

	case "Raft.AppendEntriesHandler":
		a, ok := args.(*raft.AppendEntries)
		if !ok {
			return false
		}
		r, ok := reply.(*raft.AppendEntriesReply)
		if !ok {
			return false
		}
		// raft 把快照折叠进 AppendEntries；这里按 Snapshot!=nil 路由到
		// proto 的独立 InstallSnapshot 方法，并合成标准 Raft 的成功语义
		// （proto.InstallSnapshotReply 只回 Term，matchIndex 由 leader 用
		// SnapshotIndex 直接推断，见 sendAppendEntries 的快照分支）。
		if a.Snapshot != nil {
			resp, err := c.raftCli.InstallSnapshot(ctx, installSnapshotToPB(a))
			if err != nil {
				return false
			}
			r.Term = int(resp.Term)
			r.Success = resp.Term <= int64(a.Term)
			if r.Success {
				r.LastMatchIndex = a.SnapshotIndex
			}
			return true
		}
		resp, err := c.raftCli.AppendEntries(ctx, appendEntriesToPB(a))
		if err != nil {
			return false
		}
		appendEntriesReplyFromPB(resp, r)
		return true

	case "Raft.TimeoutNow":
		a, ok := args.(*raft.TimeoutNowArgs)
		if !ok {
			return false
		}
		r, ok := reply.(*raft.TimeoutNowReply)
		if !ok {
			return false
		}
		resp, err := c.raftCli.TimeoutNow(ctx, timeoutNowToPB(a))
		if err != nil {
			return false
		}
		timeoutNowReplyFromPB(resp, r)
		return true

	// ── KV client 平面 ──────────────────────────────────────────
	case "KVServer.Get":
		req, ok := args.(*proto.GetArgs)
		if !ok {
			return false
		}
		resp, err := c.kvCli.Get(ctx, req)
		if err != nil {
			return false
		}
		r, ok := reply.(*proto.GetReply)
		if !ok {
			return false
		}
		*r = *resp
		return true

	case "KVServer.Put":
		req, ok := args.(*proto.PutArgs)
		if !ok {
			return false
		}
		resp, err := c.kvCli.Put(ctx, req)
		if err != nil {
			return false
		}
		r, ok := reply.(*proto.PutReply)
		if !ok {
			return false
		}
		*r = *resp
		return true

	case "KVServer.Append":
		req, ok := args.(*proto.AppendArgs)
		if !ok {
			return false
		}
		resp, err := c.kvCli.Append(ctx, req)
		if err != nil {
			return false
		}
		r, ok := reply.(*proto.AppendReply)
		if !ok {
			return false
		}
		*r = *resp
		return true

	// ── 未知方法 ───────────────────────────────────────────────
	default:
		return false
	}
}
