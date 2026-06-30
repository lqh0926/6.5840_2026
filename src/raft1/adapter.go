package raft

import (
	"fmt"

	"6.5840/labrpc"
	"6.5840/persist"
	"6.5840/raftapi"
	"6.5840/transport"
)

// MakeFromLabrpc 是 labrpc 边界适配器：把 tester 提供的、按 int 下标排列的
// []*labrpc.ClientEnd 转成 Raft 需要的 transport.ClientEnd + 稳定 NodeID，
// 再调用 Make。下标 → NodeID 的约定（"n{i}"）只存在于这一处，避免散落到多个
// 调用点后悄悄漂移、导致集群身份错配。
func MakeFromLabrpc(ends []*labrpc.ClientEnd, me int,
	persister persist.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	peers := make([]transport.ClientEnd, len(ends))
	nodeIDs := make([]transport.NodeID, len(ends))
	for i, e := range ends {
		peers[i] = e
		nodeIDs[i] = transport.NodeID(fmt.Sprintf("n%d", i))
	}
	return Make(peers, nodeIDs[me], nodeIDs, persister, applyCh)
}
