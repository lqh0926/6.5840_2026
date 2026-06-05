package raft

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
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
	LeaderId      int
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
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *tester.Persister
	me        int

	// Persistent state
	term    int
	voteFor int
	logs    []LogEntry // logs[0] is sentinel; absolute index i → logs[i - snapshotIndex]

	// Volatile state
	customerId   int
	leaderId     int
	voteCount    int
	commitIndex  int
	appliedIndex int

	// Snapshot state
	snapshotIndex int // absolute index of last entry included in snapshot (= sentinel position)
	snapshot      []byte

	// Per-peer replication state (only meaningful when leader)
	matchIndex []int // matchIndex[i]: highest log index confirmed replicated on peer i (only increases)
	nextIndex  []int // nextIndex[i]:  next log index to send to peer i (can decrease on failure)

	resetCh   chan struct{}
	applyCh   chan raftapi.ApplyMsg
	applyCond *sync.Cond
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

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.logs)
	e.Encode(rf.voteFor)
	e.Encode(rf.snapshotIndex)
	rf.persister.Save(w.Bytes(), rf.snapshot)
}

func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var logs []LogEntry
	var voteFor int
	var snapshotIndex int
	if d.Decode(&term) != nil || d.Decode(&logs) != nil ||
		d.Decode(&voteFor) != nil || d.Decode(&snapshotIndex) != nil {
		log.Fatalf("Failed to read persisted state")
	}
	rf.term = term
	rf.logs = logs
	rf.voteFor = voteFor
	rf.snapshotIndex = snapshotIndex
}

func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
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
	rf.persist()
}

// --- RPC types ---

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
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
	rf.leaderId = -1
	rf.voteFor = -1
	rf.voteCount = 0
	rf.persist()
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

	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && logOk {
		rf.voteFor = args.CandidateId
		rf.persist()
		select {
		case rf.resetCh <- struct{}{}:
		default:
		}
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
	select {
	case rf.resetCh <- struct{}{}:
	default:
	}

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
		rf.persist()

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
		rf.logs = append(rf.logs[:rf.relIdx(args.PrevLogIndex)+1], args.Entries...)
		rf.persist()
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
		rf.applyCond.Signal()
	}
}

// --- RPC senders ---

func (rf *Raft) sendAppendEntries(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
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
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= N {
					cnt++
				}
			}
			if cnt > len(rf.peers)/2 {
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

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
	if rf.voteCount > len(rf.peers)/2 {
		rf.customerId = Leader
		rf.leaderId = rf.me
		lastIdx := rf.lastLogIndex()
		for i := range rf.peers {
			rf.matchIndex[i] = 0
			rf.nextIndex[i] = lastIdx + 1
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
	rf.logs = append(rf.logs, LogEntry{Command: command, Term: rf.term})
	rf.persist()
	return index, rf.term, true
}

// --- Background goroutines ---

func (rf *Raft) randomTimeout() time.Duration {
	ms := 300 + (rand.Int63() % 200)
	return time.Duration(ms) * time.Millisecond
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.term++
	rf.customerId = Candidate
	rf.voteFor = rf.me
	rf.voteCount = 1
	rf.persist()
	lastIdx := rf.lastLogIndex()
	lastTerm := rf.logs[len(rf.logs)-1].Term
	term := rf.term
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			rf.sendRequestVote(server, args, &RequestVoteReply{})
		}(i)
	}
}

func (rf *Raft) startHeartbeat() {
	for {
		rf.mu.Lock()
		if rf.customerId == Leader {
			term := rf.term
			me := rf.me
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				prevLogIndex := rf.nextIndex[i] - 1
				if prevLogIndex > rf.lastLogIndex() {
					prevLogIndex = rf.lastLogIndex()
				}
				var prevLogTerm int
				var snapshot []byte
				// TODO(3D): if prevLogIndex < rf.snapshotIndex, send InstallSnapshot instead.
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
				go func(server int) {
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
				}(i)
			}
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) ticker() {
	for {
		timer := time.NewTimer(rf.randomTimeout())
		select {
		case <-timer.C:
			rf.mu.Lock()
			isLeader := rf.customerId == Leader
			rf.mu.Unlock()
			if !isLeader {
				rf.startElection()
			}
		case <-rf.resetCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
	}
}

func (rf *Raft) sendApplyMsg() {
	for {
		rf.mu.Lock()
		for rf.commitIndex == rf.appliedIndex {
			rf.applyCond.Wait()
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
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid:  false,
				Snapshot:      snapshot,
				SnapshotTerm:  snapshotTerm,
				SnapshotIndex: snapshotIndex,
				SnapshotValid: true,
			}
		}
		for i, entry := range entries {
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: startIdx + i,
			}
		}
	}
}

// --- Make ---

func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{
		peers:      peers,
		persister:  persister,
		me:         me,
		term:       0,
		customerId: Follower,
		logs:       []LogEntry{{Term: 0}},
		leaderId:   -1,
		voteFor:    -1,
		applyCh:    applyCh,
		matchIndex: make([]int, len(peers)),
		nextIndex:  make([]int, len(peers)),
	}

	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	rf.resetCh = make(chan struct{}, 1)
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

func (rf *Raft) logStatus() {
	roleNames := []string{"Leader", "Follower", "Candidate"}
	for {
		time.Sleep(2000 * time.Millisecond)
		rf.mu.Lock()
		log.Printf("[Raft %d] role=%s term=%d logLen=%d snapIdx=%d commit=%d applied=%d leader=%d",
			rf.me, roleNames[rf.customerId], rf.term, len(rf.logs),
			rf.snapshotIndex, rf.commitIndex, rf.appliedIndex, rf.leaderId)
		rf.mu.Unlock()
	}
}
