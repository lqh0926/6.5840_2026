package grpc

import (
	"context"

	"6.5840/proto"
	raft "6.5840/raft1"
)

// RaftService 把一个 *raft.Raft 适配成生成的 proto.RaftServer，
// 使 Raft 节点能通过 gRPC 对外提供 peer 平面服务。
//
// 它只做「翻译」：proto → raft Go 结构体 → 调用【原样未改】的 raft handler →
// 结果转回 proto。raft 的算法代码一行不动。
//
// 注意：raft 的三个 handler（RequestVote / AppendEntriesHandler /
// 折叠在其中的快照分支）是 *raft.Raft 的导出方法，不在 raftapi.Raft 接口里，
// 因此这里持有的是具体类型 *raft.Raft（由调用方对 raft.Make 的返回值断言得到）。
type RaftService struct {
	proto.UnimplementedRaftServer
	rf *raft.Raft
}

// NewRaftService 用一个具体的 *raft.Raft 构造服务端适配器。
func NewRaftService(rf *raft.Raft) *RaftService {
	return &RaftService{rf: rf}
}

func (s *RaftService) RequestVote(_ context.Context, pb *proto.RequestVoteArgs) (*proto.RequestVoteReply, error) {
	args := requestVoteFromPB(pb)
	var reply raft.RequestVoteReply
	s.rf.RequestVote(args, &reply)
	return requestVoteReplyToPB(&reply), nil
}

func (s *RaftService) AppendEntries(_ context.Context, pb *proto.AppendEntriesArgs) (*proto.AppendEntriesReply, error) {
	args := appendEntriesFromPB(pb)
	var reply raft.AppendEntriesReply
	s.rf.AppendEntriesHandler(args, &reply)
	return appendEntriesReplyToPB(&reply), nil
}

// InstallSnapshot 把 proto 的独立快照请求转成「带快照的 AppendEntries」，
// 交给同一个 AppendEntriesHandler 的快照分支处理，再把结果压回 proto。
// proto.InstallSnapshotReply 只带 Term（标准 Raft 语义，leader 靠 term 判定；
// matchIndex 由 leader 用 LastIncludedIndex 直接推断，见客户端 Call 的合成逻辑）。
func (s *RaftService) InstallSnapshot(_ context.Context, pb *proto.InstallSnapshotArgs) (*proto.InstallSnapshotReply, error) {
	args := installSnapshotFromPB(pb)
	var reply raft.AppendEntriesReply
	s.rf.AppendEntriesHandler(args, &reply)
	return &proto.InstallSnapshotReply{Term: int64(reply.Term)}, nil
}
