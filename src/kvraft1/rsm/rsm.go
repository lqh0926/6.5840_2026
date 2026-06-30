package rsm

import (
	"math/rand"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/persist"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Op struct {
	Opcommad interface{}
	HashNum  int64
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	chMap        map[int]chan any
	hashNumMap   map[int]int64
	// Your definitions here.
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister persist.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		chMap:        make(map[int]chan any),
		hashNumMap:   make(map[int]int64),
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.MakeFromLabrpc(servers, me, persister, rsm.applyCh)
	}
	go rsm.readApplyCh()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	_, rfIsLeader := rsm.rf.GetState()
	if !rfIsLeader {
		return rpc.ErrWrongLeader, nil
	}
	rsm.mu.Lock()
	ch := make(chan any, 1)
	hashNum := rand.Int63()
	op := Op{Opcommad: req, HashNum: hashNum}
	logId, term, ok := rsm.rf.Start(op)
	if !ok {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	rsm.hashNumMap[logId] = hashNum
	oldCh, ok := rsm.chMap[logId]
	if ok {
		close(oldCh)
	}
	rsm.chMap[logId] = ch
	rsm.mu.Unlock()
	overallTimeout := time.After(1 * time.Second)
	for {
		select {
		case res, ok := <-ch:
			if !ok {
				return rpc.ErrWrongLeader, nil
			}
			return rpc.OK, res
		case <-time.After(20 * time.Millisecond):
			newTerm, rfIsLeader := rsm.rf.GetState()
			if !rfIsLeader || newTerm != term {
				rsm.mu.Lock()
				delete(rsm.chMap, logId)
				delete(rsm.hashNumMap, logId)
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
		case <-overallTimeout:
			rsm.mu.Lock()
			delete(rsm.chMap, logId)
			delete(rsm.hashNumMap, logId)
			rsm.mu.Unlock()
			return rpc.ErrWrongLeader, nil
		}
	}

}

func (rsm *RSM) readApplyCh() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)
			res := rsm.sm.DoOp(op.Opcommad)
			rsm.mu.Lock()
			hashNum, hasHash := rsm.hashNumMap[msg.CommandIndex]
			ch, hasCh := rsm.chMap[msg.CommandIndex]
			if hasCh {
				if hasHash && hashNum == op.HashNum {
					ch <- res
				} else {
					close(ch)
				}
			}
			delete(rsm.hashNumMap, msg.CommandIndex)
			delete(rsm.chMap, msg.CommandIndex)
			rsm.mu.Unlock()
			if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() > rsm.maxraftstate {
				snapshot := rsm.sm.Snapshot()
				rsm.rf.Snapshot(msg.CommandIndex, snapshot)
			}
		} else if msg.SnapshotValid {
			rsm.sm.Restore(msg.Snapshot)
		}
	}
}
