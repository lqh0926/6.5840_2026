// Package persist 定义 Raft 持久化状态的最小抽象。
// tester.Persister 自动满足该接口，同时也可以替换为内存实现或磁盘实现。
package persist

// Persister 是 Raft 持久化状态存取的最小接口。
// Save 将 raft 状态和快照原子性地一起写入，避免状态不一致。
type Persister interface {
	Save(raftstate []byte, snapshot []byte)
	ReadRaftState() []byte
	ReadSnapshot() []byte
	RaftStateSize() int
}
