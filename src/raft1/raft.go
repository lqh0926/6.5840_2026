package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"bytes"
	"log"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
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
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term           int
	Success        bool
	LastMatchIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu            sync.Mutex            // Lock to protect shared access to this peer's state
	peers         []*labrpc.ClientEnd   // RPC end points of all peers
	persister     *tester.Persister     // Object to hold this peer's persisted state
	me            int                   // this peer's index into peers[]
	term          int                   // current term, increases monotonically
	customerId    int                   // id of the customer that this server is serving, -1 if none
	logs          []LogEntry            // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	leaderId      int                   // id of the current leader, -1 if none
	voteFor       int                   // candidateId that received vote in current term, -1 if none
	voteCount     int                   // number of votes received in current term
	resetCh       chan struct{}         // channel to reset election timer
	peerLastIndex []int                 // for each server, index of the last log entry known to be replicated on that server
	applyCh       chan raftapi.ApplyMsg // channel to send apply messages to the service (or tester)
	commitIndex   int                   // index of the highest log entry known to be committed
	appliedIndex  int                   // index of the highest log entry applied to state machine
	applyCond     *sync.Cond            // condition variable to signal when new entries are committed
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = rf.term
	isleader = rf.leaderId == rf.me
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.logs)
	e.Encode(rf.voteFor)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var logs []LogEntry
	var voteFor int
	if d.Decode(&term) != nil || d.Decode(&logs) != nil || d.Decode(&voteFor) != nil {
		log.Fatalf("Failed to read persisted state")
	} else {
		rf.term = term
		rf.logs = logs
		rf.voteFor = voteFor
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) stepDown(newTerm int) {
	rf.term = newTerm
	rf.customerId = Follower
	rf.leaderId = -1
	rf.voteFor = -1
	rf.voteCount = 0
	rf.persist()
}

// example RequestVote RPC handler.
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
	// If the candidate's log is at least as up-to-date as receiver's log, grant vote
	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := rf.logs[lastLogIndex].Term
	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		rf.persist()
		select {
		case rf.resetCh <- struct{}{}:
		default:
		}
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.term
}

func (rf *Raft) AppendEntriesHandler(args *AppendEntries, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Term < rf.term {
		reply.Term = rf.term
		reply.Success = false
		rf.mu.Unlock()
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
	if args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.term
		reply.Success = false
		if args.PrevLogIndex < len(rf.logs) {
			// term 不匹配：找冲突 term 区段的起始位置（冲突 term 必须取 follower 自身的）
			conflictTerm := rf.logs[args.PrevLogIndex].Term
			for i := args.PrevLogIndex; i > 0; i-- {
				if rf.logs[i].Term != conflictTerm {
					reply.LastMatchIndex = i
					break
				}
			}
		}
		// prevLogIndex 越界（log 太短）或冲突 term 贯穿全 log 时 LastMatchIndex=0，leader 从头重试
		rf.mu.Unlock()
		return
	}
	reply.Term = rf.term
	reply.Success = true
	// Append any new entries not already in the log
	if len(args.Entries) > 0 {
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		rf.logs = append(rf.logs, args.Entries...)
		rf.persist()
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
		// Apply committed entries to state machine
		rf.applyCond.Signal()
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.term {
			rf.stepDown(reply.Term)
			return ok
		}
		// 丢弃 stale 回复：term 已变或已不是 leader
		if rf.term != args.Term || rf.customerId != Leader {
			return ok
		}
		if reply.Success {
			rf.peerLastIndex[server] = max(rf.peerLastIndex[server], args.PrevLogIndex+len(args.Entries))
			prevCommitIndex := rf.commitIndex
			for N := rf.commitIndex + 1; N < len(rf.logs); N++ {
				matchCount := 1
				for i := range rf.peers {
					if i != rf.me && rf.peerLastIndex[i] >= N {
						matchCount++
					}
				}
				if matchCount > len(rf.peers)/2 && rf.logs[N].Term == rf.term {
					rf.commitIndex = N
				}
			}
			if rf.commitIndex > prevCommitIndex {
				rf.applyCond.Signal()
			}
		} else {
			// 丢弃乱序 failure 回复：已确认 peer 有更多 entry，这个回复是过期的
			if args.PrevLogIndex < rf.peerLastIndex[server] {
				return ok
			}
			rf.peerLastIndex[server] = reply.LastMatchIndex
		}
	}
	return ok
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// 读取状态后立即释放锁，不能持锁做 RPC

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.term {
			rf.stepDown(reply.Term)
		} else if reply.VoteGranted && rf.customerId == Candidate && rf.term == args.Term {
			rf.voteCount++
			if rf.voteCount > len(rf.peers)/2 {
				rf.customerId = Leader
				rf.leaderId = rf.me
				for i := range rf.peerLastIndex {
					rf.peerLastIndex[i] = 0
				}
			}
		}
	}
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.customerId != Leader {
		return -1, rf.term, false
	}
	index := len(rf.logs)
	term := rf.term
	rf.logs = append(rf.logs, LogEntry{Command: command, Term: term})
	rf.persist()
	return index, term, true
}

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
	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := rf.logs[lastLogIndex].Term
	term := rf.term
	rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				args := &RequestVoteArgs{
					Term:         term,
					CandidateId:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				reply := &RequestVoteReply{}
				rf.sendRequestVote(server, args, reply)
			}(i)
		}
	}

}

func (rf *Raft) startHeartbeat() {
	for {
		rf.mu.Lock()
		if rf.customerId == Leader {
			term := rf.term
			me := rf.me
			for i := range rf.peers {
				if i != rf.me {
					prevLogIndex := rf.peerLastIndex[i]
					if prevLogIndex >= len(rf.logs) {
						prevLogIndex = len(rf.logs) - 1
					}
					prevLogTerm := rf.logs[prevLogIndex].Term
					var entries []LogEntry
					if prevLogIndex < len(rf.logs)-1 {
						entries = append([]LogEntry{}, rf.logs[prevLogIndex+1:]...)
					}
					leaderCommit := rf.commitIndex
					go func(server int) {
						args := &AppendEntries{
							Term:         term,
							LeaderId:     me,
							PrevLogIndex: prevLogIndex,
							PrevLogTerm:  prevLogTerm,
							Entries:      entries,
							LeaderCommit: leaderCommit,
						}
						reply := &AppendEntriesReply{}
						rf.sendAppendEntries(server, args, reply)
					}(i)
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) ticker() {

	for true {
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
		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.

	}
}

func (rf *Raft) sendApplyMsg() {
	for {
		rf.mu.Lock()
		for rf.commitIndex == rf.appliedIndex {
			rf.applyCond.Wait()
		}
		startIndex := rf.appliedIndex + 1
		entriesToApply := append([]LogEntry{}, rf.logs[startIndex:rf.commitIndex+1]...)
		rf.appliedIndex = rf.commitIndex
		rf.mu.Unlock()
		for i, entry := range entriesToApply {
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: startIndex + i,
			}
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.term = 0
	rf.customerId = Follower
	rf.logs = make([]LogEntry, 1)
	rf.logs[0] = LogEntry{Term: 0}
	rf.leaderId = -1
	rf.voteFor = -1
	rf.voteCount = 0
	// Your initialization code here (3A, 3B, 3C).
	rf.applyCh = applyCh
	rf.peerLastIndex = make([]int, len(peers))
	for i := range rf.peerLastIndex {
		rf.peerLastIndex[i] = 0
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.resetCh = make(chan struct{}, 1)
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.appliedIndex = 0

	// start ticker goroutine to start elections
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
		role := roleNames[rf.customerId]
		term := rf.term
		logLen := len(rf.logs)
		leaderId := rf.leaderId
		commitIndex := rf.commitIndex
		appliedIndex := rf.appliedIndex
		rf.mu.Unlock()
		log.Printf("[Raft %d] role=%s term=%d logLen=%d commit=%d applied=%d leaderId=%d",
			rf.me, role, term, logLen, commitIndex, appliedIndex, leaderId)
	}
}
