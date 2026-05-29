# Bug Log

Valuable bugs encountered during 6.5840 lab implementation. Careless/trivial omissions are excluded.

---

## Lab 3 — Raft

### BUG-001 · voteFor 在同 term 内被错误重置导致 split-brain

**文件**: `src/raft1/raft.go` — `AppendEntriesHandler`

**错误代码**:
```go
rf.customerId = Follower
rf.leaderId = args.LeaderId
rf.voteFor = -1   // ❌ 无论 term 是否变化都重置
rf.voteCount = 0
```

**问题**: 每次收到心跳都把 `voteFor` 重置为 -1，即使 term 没有变化。
如果网络中有同 term 的滞留 RequestVote，follower 会再次投票，同一 term 可能产生两个 leader（split-brain）。

**正确做法**: 只在 term 升高时才重置 voteFor：
```go
if args.Term > rf.term {
    rf.term = args.Term
    rf.voteFor = -1   // ✅ 只在新 term 时重置
    rf.voteCount = 0
}
rf.customerId = Follower
rf.leaderId = args.LeaderId
```

**核心 Raft 不变式**: `voteFor` 记录的是"当前 term 我已经把票投给了谁"，只要 term 不变，这个记录就必须保持，防止同 term 重复投票。

---

### BUG-002 · startElection goroutine 在运行时才读 rf.term，可能发出错误 term 的 RequestVote

**文件**: `src/raft1/raft.go` — `startElection` / `sendRequestVote`

**错误代码**:
```go
func (rf *Raft) startElection() {
    rf.mu.Lock()
    rf.term++          // term = T
    ...
    rf.mu.Unlock()
    for i := range rf.peers {
        go func(server int) {
            args := &RequestVoteArgs{}
            rf.sendRequestVote(server, args, reply) // ❌ 内部才读 rf.term
        }(i)
    }
}

func (rf *Raft) sendRequestVote(...) {
    rf.mu.Lock()
    args.Term = rf.term  // ❌ 此时 rf.term 可能已被推高到 T+1
    ...
}
```

**问题**: goroutine 被 spawn 后不一定立刻执行。如果在它运行之前 `rf.term` 被其他事件推高（收到更高 term 的 RPC），goroutine 会用新 term 发出"上一次选举"的 RequestVote，导致选票被记入错误的选举轮次。

**正确做法**: 在 `startElection()` 持锁阶段就捕获所有需要的值，goroutine 用捕获值构造 args：
```go
func (rf *Raft) startElection() {
    rf.mu.Lock()
    rf.term++
    ...
    term := rf.term           // ✅ spawn 时捕获
    lastLogIndex := len(rf.logs) - 1
    lastLogTerm := rf.logs[lastLogIndex].Term
    rf.mu.Unlock()
    for i := range rf.peers {
        go func(server int) {
            args := &RequestVoteArgs{
                Term:         term,        // ✅ 用捕获值
                LastLogIndex: lastLogIndex,
                LastLogTerm:  lastLogTerm,
            }
            rf.sendRequestVote(server, args, reply)
        }(i)
    }
}
```

**核心原则**: goroutine 所需的共享状态快照必须在持锁时完成，不能依赖 goroutine 运行时再去读。

---
