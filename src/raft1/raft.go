package raft

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/persist"
	"6.5840/raftapi"
	"6.5840/transport"
)

const (
	Leader = iota
	Follower
	Candidate
)

type LogEntry struct {
	Command interface{}
	Term    int
}

type AppendEntries struct {
	Term          int
	LeaderId      transport.NodeID
	PrevLogIndex  int
	PrevLogTerm   int
	Entries       []LogEntry
	LeaderCommit  int
	Snapshot      []byte // non-empty if this is an InstallSnapshot RPC
	SnapshotIndex int    // absolute index of last entry included in snapshot (only meaningful if Snapshot is non-empty)
	SnapshotTerm  int    // term of last entry included in snapshot (only meaningful if Snapshot is non-empty)
}

type AppendEntriesReply struct {
	Term           int
	Success        bool
	LastMatchIndex int
}

type Raft struct {
	mu    sync.Mutex
	peers []transport.ClientEnd
	wal   WAL // 语义化持久化契约；Make 走 persisterWAL 适配器，binary 走 filewal.FileWAL
	me    transport.NodeID

	// Persistent state
	term    int
	voteFor transport.NodeID
	logs    []LogEntry // logs[0] is sentinel; absolute index i → logs[i - snapshotIndex]

	// Volatile state
	customerId   int
	leaderId     transport.NodeID
	voteCount    int
	commitIndex  int
	appliedIndex int

	// Snapshot state
	snapshotIndex int // absolute index of last entry included in snapshot (= sentinel position)
	snapshot      []byte

	// Per-peer replication state (only meaningful when leader)
	matchIndex map[transport.NodeID]int // highest log index confirmed replicated on peer (only increases)
	nextIndex  map[transport.NodeID]int // next log index to send to peer (can decrease on failure)

	// Peer identity mapping
	nodeIDs   []transport.NodeID       // 全部节点 NodeID 的有序列表（与 peers 切片平行）
	nodeIndex map[transport.NodeID]int // 反向索引：NodeID → peers 切片下标

	resetCh    chan struct{}
	newEntryCh chan struct{} // signal from Start() to heartbeat goroutine: replicate immediately
	applyCh    chan raftapi.ApplyMsg
	applyCond  *sync.Cond
	// electionGeneration changes whenever a valid leader/vote RPC resets the
	// election timer or a new election begins. It closes the race where timer.C
	// wins select just as resetCh becomes ready.
	electionGeneration uint64

	// Shutdown（Kill 用；L1 从不调用，故 done 永不关闭、行为不变）
	dead int32         // 由 Kill() 置位，killed() 读取
	done chan struct{} // 由 Kill() 关闭，唤醒 select-based 循环与阻塞的 applyCh 发送
}

type TimeoutNowArgs struct {
	Term         int
	LeaderId     transport.NodeID
	LastLogIndex int
	LastLogTerm  int
}

type TimeoutNowReply struct {
	Term     int
	Accepted bool
}

// electionRound is the immutable state captured while becoming a candidate.
// RequestVote RPCs use this snapshot after rf.mu is released.
type electionRound struct {
	term         int
	lastLogIndex int
	lastLogTerm  int
}

// --- Log index helpers ---

// relIdx converts an absolute Raft log index to a rf.logs slice index.
func (rf *Raft) relIdx(absIdx int) int {
	return absIdx - rf.snapshotIndex
}

// lastLogIndex returns the absolute index of the last log entry.
func (rf *Raft) lastLogIndex() int {
	return len(rf.logs) - 1 + rf.snapshotIndex
}

// logAt returns the log entry at absolute index absIdx.
func (rf *Raft) logAt(absIdx int) LogEntry {
	return rf.logs[rf.relIdx(absIdx)]
}

// --- Persistence ---
//
// 持久化不再走单一 rf.persist()（全量重写），而是通过 rf.wal 的语义化方法表达增量：
// 元数据用 SaveMeta、追加用 AppendLog、冲突截尾用 TruncateSuffix、快照用 SaveSnapshot。
// 编解码与适配器实现见 wal.go。

func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.wal.Size()
}

// --- Snapshot ---

// Snapshot is called by the service after it has serialized state up to and
// including index.  Raft may discard all log entries up through that index.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.snapshotIndex {
		return
	}
	if index > rf.lastLogIndex() {
		log.Fatalf("Snapshot index %d out of bounds (last log index %d)", index, rf.lastLogIndex())
	}
	// Keep logs[relIdx(index):] so logs[0] becomes the new sentinel (term preserved).
	logs := []LogEntry{{Term: rf.logAt(index).Term}}
	rf.logs = append(logs, rf.logs[rf.relIdx(index)+1:]...)
	rf.snapshotIndex = index
	rf.snapshot = snapshot
	rf.wal.SaveSnapshot(rf.snapshotIndex, rf.logs[0].Term, rf.snapshot, rf.logs[1:])
}

// --- RPC types ---

type RequestVoteArgs struct {
	Term         int
	CandidateId  transport.NodeID
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// --- Internal helpers ---

func (rf *Raft) stepDown(newTerm int) {
	rf.term = newTerm
	rf.customerId = Follower
	rf.leaderId = ""
	rf.voteFor = ""
	rf.voteCount = 0
	rf.wal.SaveMeta(rf.term, rf.voteFor)
}

// resetElectionTimerLocked invalidates any timer created from an older
// generation and wakes ticker so it can start a fresh randomized timeout.
// rf.mu must be held.
func (rf *Raft) resetElectionTimerLocked() {
	rf.electionGeneration++
	select {
	case rf.resetCh <- struct{}{}:
	default:
	}
}

// beginElectionLocked performs the complete persistent/volatile transition to
// Candidate atomically with the caller's trigger validation. Network RPCs are
// intentionally sent later, after rf.mu is released.
// rf.mu must be held.
func (rf *Raft) beginElectionLocked() electionRound {
	rf.term++
	rf.customerId = Candidate
	rf.leaderId = ""
	rf.voteFor = rf.me
	rf.voteCount = 1
	rf.wal.SaveMeta(rf.term, rf.voteFor)
	rf.resetElectionTimerLocked()
	return electionRound{
		term:         rf.term,
		lastLogIndex: rf.lastLogIndex(),
		lastLogTerm:  rf.logs[len(rf.logs)-1].Term,
	}
}

// sendRequestVotes starts the network portion of an election from a state
// snapshot captured by beginElectionLocked.
func (rf *Raft) sendRequestVotes(round electionRound) {
	for _, id := range rf.nodeIDs {
		if id == rf.me {
			continue
		}
		args := &RequestVoteArgs{
			Term:         round.term,
			CandidateId:  rf.me,
			LastLogIndex: round.lastLogIndex,
			LastLogTerm:  round.lastLogTerm,
		}
		go rf.sendRequestVote(id, args, &RequestVoteReply{})
	}
}

// --- RPC handlers ---

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.term {
		reply.Term = rf.term
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.term {
		rf.stepDown(args.Term)
	}

	lastIdx := rf.lastLogIndex()
	lastTerm := rf.logs[len(rf.logs)-1].Term
	logOk := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)

	if (rf.voteFor == "" || rf.voteFor == args.CandidateId) && logOk {
		rf.voteFor = args.CandidateId
		rf.wal.SaveMeta(rf.term, rf.voteFor)
		rf.resetElectionTimerLocked()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.term
}

func (rf *Raft) AppendEntriesHandler(args *AppendEntries, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.term {
		reply.Term = rf.term
		reply.Success = false
		return
	}
	if args.Term > rf.term {
		rf.stepDown(args.Term)
	}
	rf.customerId = Follower
	rf.leaderId = args.LeaderId
	rf.resetElectionTimerLocked()

	// Stale RPC: leader is probing an index already covered by our snapshot.
	// Tell leader to resume from snapshotIndex so it can try a later probe.

	if args.Snapshot != nil {
		// InstallSnapshot RPC: replace state with snapshot and discard all logs.
		if args.SnapshotIndex <= rf.snapshotIndex {
			// Stale snapshot, ignore.
			reply.Term = rf.term
			reply.Success = true
			reply.LastMatchIndex = rf.snapshotIndex
			return
		}
		rf.snapshot = args.Snapshot

		logs := []LogEntry{{Term: args.SnapshotTerm}}
		if rf.relIdx(args.SnapshotIndex) < len(rf.logs) && rf.logAt(args.SnapshotIndex).Term == args.SnapshotTerm {
			rf.logs = append(logs, rf.logs[rf.relIdx(args.SnapshotIndex)+1:]...)
		} else {
			rf.logs = logs
		}
		rf.snapshotIndex = args.SnapshotIndex
		rf.commitIndex = max(rf.commitIndex, rf.snapshotIndex)
		rf.wal.SaveSnapshot(rf.snapshotIndex, rf.logs[0].Term, rf.snapshot, rf.logs[1:])

		reply.Term = rf.term
		reply.Success = true
		reply.LastMatchIndex = rf.snapshotIndex
		rf.applyCond.Signal()
		return
	}
	if args.PrevLogIndex < rf.snapshotIndex {
		reply.Term = rf.term
		reply.Success = false
		reply.LastMatchIndex = rf.snapshotIndex
		return
	}
	// Consistency check
	if args.PrevLogIndex > rf.lastLogIndex() || rf.logAt(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Term = rf.term
		reply.Success = false
		if args.PrevLogIndex > rf.lastLogIndex() {
			// Log too short: tell leader to resume from our real tail.
			reply.LastMatchIndex = rf.lastLogIndex()
		} else {
			// Term mismatch: skip the entire conflicting term block.
			conflictTerm := rf.logAt(args.PrevLogIndex).Term
			for i := args.PrevLogIndex; i > rf.snapshotIndex; i-- {
				if rf.logAt(i).Term != conflictTerm {
					reply.LastMatchIndex = i
					break
				}
			}
		}
		// Conflict spans all the way to the snapshot boundary → LastMatchIndex=0,
		// leader retries from the start.
		return
	}

	reply.Term = rf.term
	reply.Success = true
	if len(args.Entries) > 0 {
		// 只在 term 冲突处截断：找到第一个与本地不一致的条目再截断并追加。
		// 无条件截断会被 longreordering 下迟到的短 AE 抹掉已 committed 的尾部。
		i := 0
		for i < len(args.Entries) {
			logIdx := args.PrevLogIndex + 1 + i
			if logIdx > rf.lastLogIndex() {
				break // 超出范围，后面全是新的
			}
			if rf.logAt(logIdx).Term != args.Entries[i].Term {
				break // 找到冲突点
			}
			i++ // term 相同，已有且一致，跳过
		}
		if i < len(args.Entries) {
			logIdx := args.PrevLogIndex + 1 + i
			rf.logs = append(rf.logs[:rf.relIdx(logIdx)], args.Entries[i:]...)
			// 冲突：先截掉 logIdx 起的旧尾巴，再追加新条目（两步复现这一行 in-place 覆盖）。
			rf.wal.TruncateSuffix(logIdx)
			rf.wal.AppendLog(args.Entries[i:])
		}
		// i == len(args.Entries)：全部已存在且一致，无需操作
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
		rf.applyCond.Signal()
	}
}

// --- RPC senders ---

func (rf *Raft) sendAppendEntries(server transport.NodeID, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[rf.nodeIndex[server]].Call("Raft.AppendEntriesHandler", args, reply)
	if !ok {
		return ok
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.term {
		rf.stepDown(reply.Term)
		return ok
	}
	// Discard stale replies (term changed or no longer leader).
	if rf.term != args.Term || rf.customerId != Leader {
		return ok
	}

	if reply.Success {
		var newMatch int
		if args.Snapshot != nil {
			newMatch = args.SnapshotIndex
		} else {
			newMatch = args.PrevLogIndex + len(args.Entries)
		}
		if newMatch > rf.matchIndex[server] {
			rf.matchIndex[server] = newMatch
		}
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		// Advance commitIndex if a new majority exists for an entry in current term.
		prevCommit := rf.commitIndex
		for N := rf.commitIndex + 1; N <= rf.lastLogIndex(); N++ {
			if rf.logAt(N).Term != rf.term {
				continue
			}
			cnt := 1
			for _, id := range rf.nodeIDs {
				if id != rf.me && rf.matchIndex[id] >= N {
					cnt++
				}
			}
			if cnt > len(rf.nodeIDs)/2 {
				rf.commitIndex = N
			}
		}
		if rf.commitIndex > prevCommit {
			rf.applyCond.Signal()
		}
	} else {
		// Discard out-of-order stale failure: matchIndex already passed this probe.
		if args.PrevLogIndex < rf.matchIndex[server] {
			return ok
		}
		// Fast backtrack: jump nextIndex to where follower says it diverges.
		rf.nextIndex[server] = reply.LastMatchIndex + 1
		// nextIndex must stay at least 1 and not exceed lastLogIndex+1.
		if rf.nextIndex[server] < 1 {
			rf.nextIndex[server] = 1
		}
	}
	return ok
}

func (rf *Raft) sendRequestVote(server transport.NodeID, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[rf.nodeIndex[server]].Call("Raft.RequestVote", args, reply)
	if !ok {
		return ok
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.term {
		rf.stepDown(reply.Term)
		return ok
	}
	if !reply.VoteGranted || rf.customerId != Candidate || rf.term != args.Term {
		return ok
	}
	rf.voteCount++
	if rf.voteCount > len(rf.nodeIDs)/2 {
		rf.customerId = Leader
		rf.leaderId = rf.me
		lastIdx := rf.lastLogIndex()
		for _, id := range rf.nodeIDs {
			rf.matchIndex[id] = 0
			rf.nextIndex[id] = lastIdx + 1
		}
	}
	return ok
}

// --- Service API ---

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term, rf.leaderId == rf.me
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.customerId != Leader {
		return -1, rf.term, false
	}
	index := rf.lastLogIndex() + 1
	entry := LogEntry{Command: command, Term: rf.term}
	rf.logs = append(rf.logs, entry)
	rf.wal.AppendLog([]LogEntry{entry})
	// Signal heartbeat goroutine to replicate immediately instead of waiting
	// for the next 100ms tick.
	select {
	case rf.newEntryCh <- struct{}{}:
	default:
	}
	return index, rf.term, true
}

// --- Background goroutines ---

func (rf *Raft) randomTimeout() time.Duration {
	ms := 300 + (rand.Int63() % 200)
	return time.Duration(ms) * time.Millisecond
}

func (rf *Raft) startHeartbeat() {
	for {
		rf.mu.Lock()
		if rf.customerId == Leader {
			term := rf.term
			me := rf.me
			for _, id := range rf.nodeIDs {
				if id == rf.me {
					continue
				}
				prevLogIndex := rf.nextIndex[id] - 1
				if prevLogIndex > rf.lastLogIndex() {
					prevLogIndex = rf.lastLogIndex()
				}
				var prevLogTerm int
				var snapshot []byte
				if prevLogIndex < rf.snapshotIndex {
					prevLogIndex = 0
					snapshot = rf.snapshot
					prevLogTerm = 0
				} else {
					prevLogTerm = rf.logAt(prevLogIndex).Term
				}
				var entries []LogEntry
				if prevLogIndex < rf.lastLogIndex() && snapshot == nil {
					entries = append([]LogEntry{}, rf.logs[rf.relIdx(prevLogIndex)+1:]...)
				}
				leaderCommit := rf.commitIndex
				snapshotIndex := rf.snapshotIndex
				snapshotTerm := rf.logs[0].Term
				go func(server transport.NodeID) {
					rf.sendAppendEntries(server, &AppendEntries{
						Term:          term,
						LeaderId:      me,
						PrevLogIndex:  prevLogIndex,
						PrevLogTerm:   prevLogTerm,
						Entries:       entries,
						LeaderCommit:  leaderCommit,
						Snapshot:      snapshot,
						SnapshotIndex: snapshotIndex,
						SnapshotTerm:  snapshotTerm,
					}, &AppendEntriesReply{})
				}(id)
			}
		}
		rf.mu.Unlock()
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-rf.newEntryCh:
			timer.Stop()
		case <-timer.C:
		case <-rf.done:
			timer.Stop()
			return
		}
	}
}

func (rf *Raft) ticker() {
	for {
		rf.mu.Lock()
		generation := rf.electionGeneration
		rf.mu.Unlock()

		timer := time.NewTimer(rf.randomTimeout())
		select {
		case <-timer.C:
			rf.mu.Lock()
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
			// A heartbeat/vote grant may have reset the timer at the same
			// instant timer.C became ready. The generation check makes that
			// reset win deterministically instead of starting a stale election.
			if rf.customerId == Leader || rf.electionGeneration != generation {
				rf.mu.Unlock()
				continue
			}
			round := rf.beginElectionLocked()
			rf.mu.Unlock()
			rf.sendRequestVotes(round)
		case <-rf.resetCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-rf.done:
			timer.Stop()
			return
		}
	}
}

func (rf *Raft) sendApplyMsg() {
	for {
		rf.mu.Lock()
		for rf.commitIndex == rf.appliedIndex && !rf.killed() {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		startIdx := rf.appliedIndex + 1
		var snapshot []byte
		if startIdx <= rf.snapshotIndex {
			snapshot = rf.snapshot
			startIdx = rf.snapshotIndex + 1
		}
		snapshotTerm := rf.logs[0].Term
		snapshotIndex := rf.snapshotIndex
		entries := append([]LogEntry{}, rf.logs[rf.relIdx(startIdx):rf.relIdx(rf.commitIndex)+1]...)
		rf.appliedIndex = rf.commitIndex
		rf.mu.Unlock()
		if snapshot != nil {
			select {
			case rf.applyCh <- raftapi.ApplyMsg{
				CommandValid:  false,
				Snapshot:      snapshot,
				SnapshotTerm:  snapshotTerm,
				SnapshotIndex: snapshotIndex,
				SnapshotValid: true,
			}:
			case <-rf.done:
				return
			}
		}
		for i, entry := range entries {
			select {
			case rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: startIdx + i,
			}:
			case <-rf.done:
				return
			}
		}
	}
}

func (rf *Raft) TimeoutNow(args *TimeoutNowArgs, reply *TimeoutNowReply) {
	rf.mu.Lock()
	if args.Term < rf.term {
		reply.Term = rf.term
		reply.Accepted = false
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.term {
		rf.stepDown(args.Term)
	}
	if args.LeaderId != rf.leaderId {
		reply.Term = rf.term
		reply.Accepted = false
		rf.mu.Unlock()
		return
	}
	if args.LastLogIndex < rf.snapshotIndex || args.LastLogIndex != rf.lastLogIndex() ||
		rf.logAt(args.LastLogIndex).Term != args.LastLogTerm {
		reply.Term = rf.term
		reply.Accepted = false
		rf.mu.Unlock()
		return
	}
	round := rf.beginElectionLocked()
	reply.Term = round.term
	reply.Accepted = true
	rf.mu.Unlock()
	rf.sendRequestVotes(round)
}

func (rf *Raft) TransferLeadership(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	rf.mu.Lock()
	if rf.customerId != Leader {
		rf.mu.Unlock()
		return raftapi.ErrNotLeader
	}
	// Find the node with the highest matchIndex to transfer leadership to.
	var target transport.NodeID
	maxMatchIndex := -1
	for _, id := range rf.nodeIDs {
		if id == rf.me {
			continue
		}
		if rf.matchIndex[id] > maxMatchIndex {
			maxMatchIndex = rf.matchIndex[id]
			target = id
		}
	}
	if target == "" {
		rf.mu.Unlock()
		return raftapi.ErrNoOtherNodes
	}
	args := &TimeoutNowArgs{
		Term:         rf.term,
		LeaderId:     rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
	}
	peer := rf.peers[rf.nodeIndex[target]]
	rf.mu.Unlock()

	// transport.ClientEnd has no context-aware Call method. Run the RPC in a
	// separate goroutine so shutdown can stop waiting when ctx expires. The
	// buffered channel lets that goroutine finish even after this method has
	// returned; the underlying Call is still bounded by its own RPC timeout.
	type result struct {
		reply TimeoutNowReply
		ok    bool
	}
	resultCh := make(chan result, 1)
	go func() {
		var reply TimeoutNowReply
		ok := peer.Call("Raft.TimeoutNow", args, &reply)
		resultCh <- result{reply: reply, ok: ok}
	}()

	var rpcResult result
	select {
	case <-ctx.Done():
		return ctx.Err()
	case rpcResult = <-resultCh:
	}

	if !rpcResult.ok {
		return raftapi.ErrRPCFailed
	}

	rf.mu.Lock()
	if rpcResult.reply.Term > rf.term {
		rf.stepDown(rpcResult.reply.Term)
	}
	rf.mu.Unlock()

	if !rpcResult.reply.Accepted {
		return raftapi.ErrTransferRejected
	}
	return nil
}

// --- Make ---

// Make 用内存/文件 persister 引导 Raft —— L1 测试与旧路径走这条，内部包一层
// persisterWAL 适配器（行为逐字节等于改造前）。
func Make(peers []transport.ClientEnd, me transport.NodeID, nodeIDs []transport.NodeID,
	persister persist.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	return MakeWithWAL(peers, me, nodeIDs, newPersisterWAL(persister), applyCh)
}

// MakeWithWAL 直接注入一个 WAL 实现（binary 用 filewal.FileWAL 走这条）。
func MakeWithWAL(peers []transport.ClientEnd, me transport.NodeID, nodeIDs []transport.NodeID,
	w WAL, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	matchIndex := make(map[transport.NodeID]int, len(peers))
	nextIndex := make(map[transport.NodeID]int, len(peers))
	nodeIndex := make(map[transport.NodeID]int, len(peers))
	for i, id := range nodeIDs {
		nodeIndex[id] = i
		matchIndex[id] = 0
		nextIndex[id] = 0
	}

	rf := &Raft{
		peers:      peers,
		me:         me,
		term:       0,
		customerId: Follower,
		logs:       []LogEntry{{Term: 0}},
		leaderId:   "",
		voteFor:    "",
		applyCh:    applyCh,
		matchIndex: matchIndex,
		nextIndex:  nextIndex,
		nodeIDs:    nodeIDs,
		nodeIndex:  nodeIndex,
	}

	rf.wal = w
	if st, ok := rf.wal.Load(); ok {
		rf.term = st.Term
		rf.logs = st.Logs
		rf.voteFor = st.Vote
		rf.snapshotIndex = st.SnapshotIndex
		rf.snapshot = st.Snapshot
	} else {
		// 全新节点：raftstate 为空，但快照可能独立存在（与改造前无条件 ReadSnapshot 一致）。
		rf.snapshot = st.Snapshot
	}

	rf.resetCh = make(chan struct{}, 1)
	rf.newEntryCh = make(chan struct{}, 1)
	rf.done = make(chan struct{})
	rf.applyCond = sync.NewCond(&rf.mu)
	// After crash-recovery, appliedIndex and commitIndex start at snapshotIndex;
	// the service layer restores application state from the snapshot directly.
	rf.appliedIndex = rf.snapshotIndex
	rf.commitIndex = rf.snapshotIndex

	go rf.ticker()
	go rf.startHeartbeat()
	go rf.logStatus()
	go rf.sendApplyMsg()

	return rf
}

// Kill 停止本 Raft 节点的所有后台 goroutine 并使之可被 GC。
// 幂等（多次调用安全）。用于测试的 crash/重启，以及避免 goroutine 泄漏。
// Kill 不属于 raftapi.Raft 接口，仅在具体类型 *Raft 上可用。
func (rf *Raft) Kill() {
	if !atomic.CompareAndSwapInt32(&rf.dead, 0, 1) {
		return // 已 Kill
	}
	close(rf.done)
	// 在持锁下 Broadcast，避免与 sendApplyMsg 的「检查条件→Wait」窗口竞争导致漏唤醒。
	rf.mu.Lock()
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func (rf *Raft) logStatus() {
	roleNames := []string{"Leader", "Follower", "Candidate"}
	for {
		select {
		case <-rf.done:
			return
		case <-time.After(2000 * time.Millisecond):
		}
		rf.mu.Lock()
		log.Printf("[Raft %s] role=%s term=%d logLen=%d snapIdx=%d commit=%d applied=%d leader=%s",
			rf.me, roleNames[rf.customerId], rf.term, len(rf.logs),
			rf.snapshotIndex, rf.commitIndex, rf.appliedIndex, rf.leaderId)
		rf.mu.Unlock()
	}
}
