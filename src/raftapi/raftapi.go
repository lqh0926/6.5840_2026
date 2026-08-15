package raftapi

import "errors"

// Leadership-transfer errors are sentinel values so callers can classify the
// failure with errors.Is without depending on error strings.
var (
	ErrNotLeader        = errors.New("raft: not leader")
	ErrNoOtherNodes     = errors.New("raft: no other nodes")
	ErrRPCFailed        = errors.New("raft: leadership transfer RPC failed")
	ErrTransferRejected = errors.New("raft: leadership transfer rejected")
)

// The Raft interface
type Raft interface {
	// Start agreement on a new log entry, and return the log index
	// for that entry, the term, and whether the peer is the leader.
	Start(command interface{}) (int, int, bool)

	// Ask a Raft for its current term, and whether it thinks it is
	// leader
	GetState() (int, bool)

	// For Snaphots (3D)
	Snapshot(index int, snapshot []byte)
	PersistBytes() int
}

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the server (or
// tester), via the applyCh passed to Make(). Set CommandValid to true
// to indicate that the ApplyMsg contains a newly committed log entry.
//
// You'll find the Snapshot fields useful later in the lab.
// Exactly one of CommandValid and SnapshotValid should be true.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
