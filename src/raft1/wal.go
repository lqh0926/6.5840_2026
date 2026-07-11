package raft

import (
	"bytes"
	"log"

	"6.5840/labgob"
	"6.5840/persist"
	"6.5840/transport"
)

// WAL 是 raft 持久化的语义化契约（Phase 2 Step 1 引入）。
//
// 相比底层的 persist.Persister（只会「存全量 blob」），WAL 让 raft 在每个持久化
// 点显式表达「我这次到底改了什么」：改元数据 / 追加日志 / 冲突截尾 / 打快照。只有
// 表达出「增量」，底层实现才可能只 append 而非每次全量重写——这是 Phase 2 真 WAL
// 的前提。
//
// Step 1 只提供 persisterWAL 适配器：所有方法都落到「更新内存全量模型 → 调
// persist.Persister.Save(整 blob)」，行为逐字节等于改造前的 rf.persist()。真正
// append-only + fsync 的 fileWAL 在 Step 3 落地（它是本接口的另一份实现）。
type WAL interface {
	// SaveMeta 持久化元数据（term / votedFor）。对应 stepDown / 授票 / 发起选举。
	SaveMeta(term int, vote transport.NodeID)
	// AppendLog 在日志尾部追加 entries。对应 Start / AppendEntries 接受新条目。
	AppendLog(entries []LogEntry)
	// TruncateSuffix 丢弃绝对索引 >= fromIndex 的所有日志条目（AppendEntries 冲突截尾）。
	TruncateSuffix(fromIndex int)
	// SaveSnapshot 打快照：日志变为 [哨兵(sentinelTerm)] + suffix，snapshotIndex =
	// snapshotIndex，同时持久化 snapshot。服务层 Snapshot 与 follower 安装 leader
	// 快照两处都走它——二者结果形态一致（只是 sentinelTerm/suffix 由调用点算好传入），
	// 故用一个方法统一，而非在持久化层重新推导两套不同的日志变换。
	SaveSnapshot(snapshotIndex, sentinelTerm int, snapshot []byte, suffix []LogEntry)
	// Load 读取已持久化状态；ok=false 表示全新节点（无持久化 raftstate）。无论 ok，
	// 返回的 Snapshot 字段都反映底层快照（快照可独立于 raftstate 存在）。
	Load() (st PersistState, ok bool)
	// Size 返回已持久化 raftstate（log/元数据，不含快照）的字节数，用于 maxraftstate
	// 触发压缩。语义等于旧 persist.Persister.RaftStateSize()。
	Size() int
}

// PersistState 是 Load 返回的全量持久化状态。
type PersistState struct {
	Term          int
	Vote          transport.NodeID
	Logs          []LogEntry
	SnapshotIndex int
	Snapshot      []byte
}

// --- raftstate blob 编解码（与改造前 rf.persist 逐字段对齐）---
//
// 字段顺序固定为 term, logs, voteFor, snapshotIndex。任何改动都会破坏与已落盘数据
// 的兼容 → 崩溃恢复读到错数据，故这是全项目唯一的编码真源。

func encodeRaftState(term int, logs []LogEntry, vote transport.NodeID, snapshotIndex int) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(term)
	e.Encode(logs)
	e.Encode(vote)
	e.Encode(snapshotIndex)
	return w.Bytes()
}

func decodeRaftState(data []byte) (term int, logs []LogEntry, vote transport.NodeID, snapshotIndex int, ok bool) {
	if len(data) < 1 {
		return 0, nil, "", 0, false
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&term) != nil || d.Decode(&logs) != nil ||
		d.Decode(&vote) != nil || d.Decode(&snapshotIndex) != nil {
		log.Fatalf("Failed to read persisted state")
	}
	return term, logs, vote, snapshotIndex, true
}

// persisterWAL 是 WAL 的 Step-1 适配器：内部维护一份全量模型（与 rf 的持久化字段
// 并行演进），每次写操作更新模型后，把整块 raftstate blob + snapshot 交给底层
// persist.Persister.Save。行为逐字节等于改造前的 rf.persist()。
//
// 关键：模型必须在【构造时】从底层已持久化数据 seed，否则首次写会用空 logs 覆盖磁盘
// 里的日志。全新节点 seed 为哨兵 [{Term:0}]，与 Make 里 rf 的初始 logs 字面量一致。
type persisterWAL struct {
	p persist.Persister

	// 内存全量模型
	term          int
	vote          transport.NodeID
	logs          []LogEntry
	snapshotIndex int
	snapshot      []byte
}

func newPersisterWAL(p persist.Persister) *persisterWAL {
	w := &persisterWAL{p: p}
	term, logs, vote, snapshotIndex, ok := decodeRaftState(p.ReadRaftState())
	if ok {
		w.term, w.logs, w.vote, w.snapshotIndex = term, logs, vote, snapshotIndex
	} else {
		w.logs = []LogEntry{{Term: 0}} // 与 Make 的初始哨兵一致
	}
	w.snapshot = p.ReadSnapshot()
	return w
}

// flush 把当前全量模型编码成整 blob 落盘。Step-1 适配器每次写都全量重写（与改造前
// 语义相同）；真 fileWAL 在这里才走增量 append。
func (w *persisterWAL) flush() {
	w.p.Save(encodeRaftState(w.term, w.logs, w.vote, w.snapshotIndex), w.snapshot)
}

func (w *persisterWAL) SaveMeta(term int, vote transport.NodeID) {
	w.term, w.vote = term, vote
	w.flush()
}

func (w *persisterWAL) AppendLog(entries []LogEntry) {
	w.logs = append(w.logs, entries...)
	w.flush()
}

func (w *persisterWAL) TruncateSuffix(fromIndex int) {
	// fromIndex - snapshotIndex 是绝对索引 → w.logs 下标的映射（与 rf.relIdx 同一
	// 公式，这是适配器模型与 raft 之间唯一的索引耦合点）。
	w.logs = w.logs[:fromIndex-w.snapshotIndex]
	w.flush()
}

func (w *persisterWAL) SaveSnapshot(snapshotIndex, sentinelTerm int, snapshot []byte, suffix []LogEntry) {
	// 复制 suffix：调用方传入的是 rf.logs[1:]，与 rf 共享底层数组；不复制会在 rf 后续
	// 改动日志时污染适配器模型。
	logs := make([]LogEntry, 0, 1+len(suffix))
	logs = append(logs, LogEntry{Term: sentinelTerm})
	logs = append(logs, suffix...)
	w.logs = logs
	w.snapshotIndex = snapshotIndex
	w.snapshot = snapshot
	w.flush()
}

func (w *persisterWAL) Size() int { return w.p.RaftStateSize() }

func (w *persisterWAL) Load() (PersistState, bool) {
	term, logs, vote, snapshotIndex, ok := decodeRaftState(w.p.ReadRaftState())
	st := PersistState{Snapshot: w.p.ReadSnapshot()}
	if ok {
		st.Term, st.Logs, st.Vote, st.SnapshotIndex = term, logs, vote, snapshotIndex
	}
	return st, ok
}
