// 本文件是 Raft 的 Go 结构体 ↔ proto 消息的转换层。
//
// 设计（Plan B）：Raft 核心完全不认识 proto —— raft1 只用自己的 Go 结构体，
// 且 L1（labrpc）继续用 labgob 跑原结构体（labgob 无法往返 proto 结构体，
// 其 checkDefault 会在 proto 的未导出字段上 panic，故 proto 只能出现在这里）。
// 所有 Go↔proto 的转换都收在 transport/grpc 内部，用 proto 原生 marshal。
//
// 命令字段的约定：gRPC 路径上，LogEntry.Command 恒为 RSM 产出的 []byte，
// 对应 proto 的 bytes；raft 单测里的 int 命令只跑 L1，永不到达这里。
package grpc

import (
	"6.5840/proto"
	raft "6.5840/raft1"
	"6.5840/transport"
)

// ---- RequestVote ----

func requestVoteToPB(a *raft.RequestVoteArgs) *proto.RequestVoteArgs {
	return &proto.RequestVoteArgs{
		Term:         int64(a.Term),
		CandidateId:  string(a.CandidateId),
		LastLogIndex: int64(a.LastLogIndex),
		LastLogTerm:  int64(a.LastLogTerm),
	}
}

func requestVoteFromPB(pb *proto.RequestVoteArgs) *raft.RequestVoteArgs {
	return &raft.RequestVoteArgs{
		Term:         int(pb.Term),
		CandidateId:  transport.NodeID(pb.CandidateId),
		LastLogIndex: int(pb.LastLogIndex),
		LastLogTerm:  int(pb.LastLogTerm),
	}
}

func requestVoteReplyToPB(r *raft.RequestVoteReply) *proto.RequestVoteReply {
	return &proto.RequestVoteReply{Term: int64(r.Term), VoteGranted: r.VoteGranted}
}

func requestVoteReplyFromPB(pb *proto.RequestVoteReply, r *raft.RequestVoteReply) {
	r.Term = int(pb.Term)
	r.VoteGranted = pb.VoteGranted
}

// ---- TimeoutNow（显式 leadership transfer）----

func timeoutNowToPB(a *raft.TimeoutNowArgs) *proto.TimeoutNowArgs {
	return &proto.TimeoutNowArgs{
		Term:         int64(a.Term),
		LeaderId:     string(a.LeaderId),
		LastLogIndex: int64(a.LastLogIndex),
		LastLogTerm:  int64(a.LastLogTerm),
	}
}

func timeoutNowFromPB(pb *proto.TimeoutNowArgs) *raft.TimeoutNowArgs {
	return &raft.TimeoutNowArgs{
		Term:         int(pb.Term),
		LeaderId:     transport.NodeID(pb.LeaderId),
		LastLogIndex: int(pb.LastLogIndex),
		LastLogTerm:  int(pb.LastLogTerm),
	}
}

func timeoutNowReplyToPB(r *raft.TimeoutNowReply) *proto.TimeoutNowReply {
	return &proto.TimeoutNowReply{Term: int64(r.Term), Accepted: r.Accepted}
}

func timeoutNowReplyFromPB(pb *proto.TimeoutNowReply, r *raft.TimeoutNowReply) {
	r.Term = int(pb.Term)
	r.Accepted = pb.Accepted
}

// ---- LogEntry ----

func logEntryToPB(e raft.LogEntry) *proto.LogEntry {
	var cmd []byte
	// gRPC 路径命令恒为 []byte（RSM 产出）；用 comma-ok 避免非 []byte 时 panic。
	if b, ok := e.Command.([]byte); ok {
		cmd = b
	}
	return &proto.LogEntry{Command: cmd, Term: int64(e.Term)}
}

func logEntryFromPB(pb *proto.LogEntry) raft.LogEntry {
	var cmd interface{}
	if pb.Command != nil {
		cmd = pb.Command
	}
	return raft.LogEntry{Command: cmd, Term: int(pb.Term)}
}

// ---- AppendEntries（普通复制/心跳）----

func appendEntriesToPB(a *raft.AppendEntries) *proto.AppendEntriesArgs {
	entries := make([]*proto.LogEntry, len(a.Entries))
	for i := range a.Entries {
		entries[i] = logEntryToPB(a.Entries[i])
	}
	return &proto.AppendEntriesArgs{
		Term:         int64(a.Term),
		LeaderId:     string(a.LeaderId),
		PrevLogIndex: int64(a.PrevLogIndex),
		PrevLogTerm:  int64(a.PrevLogTerm),
		Entries:      entries,
		LeaderCommit: int64(a.LeaderCommit),
	}
}

func appendEntriesFromPB(pb *proto.AppendEntriesArgs) *raft.AppendEntries {
	entries := make([]raft.LogEntry, len(pb.Entries))
	for i := range pb.Entries {
		entries[i] = logEntryFromPB(pb.Entries[i])
	}
	return &raft.AppendEntries{
		Term:         int(pb.Term),
		LeaderId:     transport.NodeID(pb.LeaderId),
		PrevLogIndex: int(pb.PrevLogIndex),
		PrevLogTerm:  int(pb.PrevLogTerm),
		Entries:      entries,
		LeaderCommit: int(pb.LeaderCommit),
	}
}

func appendEntriesReplyToPB(r *raft.AppendEntriesReply) *proto.AppendEntriesReply {
	return &proto.AppendEntriesReply{
		Term:           int64(r.Term),
		Success:        r.Success,
		LastMatchIndex: int64(r.LastMatchIndex),
	}
}

func appendEntriesReplyFromPB(pb *proto.AppendEntriesReply, r *raft.AppendEntriesReply) {
	r.Term = int(pb.Term)
	r.Success = pb.Success
	r.LastMatchIndex = int(pb.LastMatchIndex)
}

// ---- InstallSnapshot ----
//
// raft 把 InstallSnapshot 折叠进了 AppendEntries（AppendEntries 结构体带
// Snapshot 字段，AppendEntriesHandler 里判 Snapshot!=nil 走快照分支）。
// proto 则把两者拆成独立方法/消息。故这里在「带快照的 AppendEntries」与
// proto.InstallSnapshotArgs 之间转换，路由由调用方按 Snapshot!=nil 决定。

func installSnapshotToPB(a *raft.AppendEntries) *proto.InstallSnapshotArgs {
	return &proto.InstallSnapshotArgs{
		Term:              int64(a.Term),
		LeaderId:          string(a.LeaderId),
		LastIncludedIndex: int64(a.SnapshotIndex),
		LastIncludedTerm:  int64(a.SnapshotTerm),
		Data:              a.Snapshot,
	}
}

func installSnapshotFromPB(pb *proto.InstallSnapshotArgs) *raft.AppendEntries {
	return &raft.AppendEntries{
		Term:          int(pb.Term),
		LeaderId:      transport.NodeID(pb.LeaderId),
		Snapshot:      pb.Data,
		SnapshotIndex: int(pb.LastIncludedIndex),
		SnapshotTerm:  int(pb.LastIncludedTerm),
	}
}
