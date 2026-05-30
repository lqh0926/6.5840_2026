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

### BUG-003 · AppendEntries reject 时未重置选举计时器，导致频繁重新选举

**文件**: `src/raft1/raft.go` — `AppendEntriesHandler`

**错误代码**:
```go
rf.customerId = Follower
rf.leaderId = args.LeaderId
if args.PrevLogIndex >= len(rf.logs) || ... {
    // ❌ 直接 return，没有重置计时器
    rf.mu.Unlock()
    return
}
// 只有成功时才重置计时器
select { case rf.resetCh <- struct{}{}: default: }
```

**问题**: follower 收到合法 leader（term 合法）的 AppendEntries，因日志不一致而 reject，但没有重置选举计时器。follower 误以为 leader 不存在，触发新选举，造成 term 不断上涨、leader 频繁更换，初始选举期间 RPC 数超过测试上限（>30）。

**正确做法**: 只要 term 合法，确认 leader 存在后立即重置计时器，不管日志一致性检查结果如何：
```go
rf.customerId = Follower
rf.leaderId = args.LeaderId
select { case rf.resetCh <- struct{}{}: default: }  // ✅ term 合法就重置
if args.PrevLogIndex >= len(rf.logs) || ... {
    // reject，但计时器已重置
    rf.mu.Unlock()
    return
}
```

**核心 Raft 不变式**: 选举计时器的重置条件是"收到合法 leader 的消息"，与日志是否一致无关。

---

### BUG-004 · RequestVote 授予投票后未重置选举计时器

**文件**: `src/raft1/raft.go` — `RequestVote`

**错误代码**:
```go
if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && ... {
    reply.VoteGranted = true
    rf.voteFor = args.CandidateId
    // ❌ 没有重置计时器
}
```

**问题**: follower 刚把票投给 candidate A，但自身计时器未重置，可能马上自己也发起选举（尤其在 timeout 快到期时），产生高 term 干扰 A 的选举。

**正确做法**: 投票成功时也重置计时器（论文 Figure 2 明确要求："election timeout elapses without ... granting vote to candidate"）：
```go
reply.VoteGranted = true
rf.voteFor = args.CandidateId
select { case rf.resetCh <- struct{}{}: default: }  // ✅
```

**核心 Raft 不变式**: 论文 §5.2：只要在 timeout 内"授予了选票"，就不应该发起新选举。

---

### BUG-005 · startHeartbeat 中 LeaderCommit 在 goroutine 内读取，存在数据竞争

**文件**: `src/raft1/raft.go` — `startHeartbeat`

**错误代码**:
```go
go func(server int) {
    args := &AppendEntries{
        LeaderCommit: rf.commitIndex,  // ❌ 锁已释放后才读
    }
    rf.sendAppendEntries(server, args, reply)
}(i)
```

**问题**: goroutine 在锁释放后运行，读取 `rf.commitIndex` 时没有持锁，是 Go 内存模型下的数据竞争。在某些调度时序下，follower 收到 `LeaderCommit=0` 的心跳，不更新 `commitIndex`，导致 `applyCh` 永远收不到消息，`one()` 超时失败。

同样的问题也存在于 `entries` 切片：直接取 `rf.logs` 子切片，goroutine 运行时底层数组可能已被修改。

**正确做法**: 在持锁阶段捕获所有值，`entries` 做深拷贝：
```go
entries = append([]LogEntry{}, rf.logs[prevLogIndex+1:]...)  // ✅ 深拷贝
leaderCommit := rf.commitIndex                               // ✅ 持锁时捕获
go func(server int) {
    args := &AppendEntries{
        Entries:      entries,
        LeaderCommit: leaderCommit,
    }
}(i)
```

**核心原则**: goroutine 闭包捕获的是引用，共享状态必须在持锁时以值的形式快照。

---


### BUG-006 · 当选 leader 时未重置 peerLastIndex，导致错误统计 majority

**文件**: `src/raft1/raft.go` — `sendRequestVote`

**错误代码**:
```go
if rf.voteCount > len(rf.peers)/2 {
    rf.customerId = Leader
    rf.leaderId = rf.me
    // ❌ 没有重置 peerLastIndex
}
```

**问题**: `peerLastIndex[i]` 记录的是"已确认复制到节点 i 的最高 index"（相当于论文的 matchIndex）。如果一台服务器上一个 term 当过 leader，`peerLastIndex[2] = 5`，然后下台，再次当选时该值仍是 5。新 leader 会误以为节点 2 已有前 5 条 entry，跳过复制，在统计 majority 时虚报，导致提前提交或错误的 commitIndex。

**正确做法**: 当选 leader 时重置所有 peerLastIndex 为 0（对应论文 Figure 2 "matchIndex[] = 0"）：
```go
if rf.voteCount > len(rf.peers)/2 {
    rf.customerId = Leader
    rf.leaderId = rf.me
    for i := range rf.peerLastIndex {
        rf.peerLastIndex[i] = 0  // ✅ 重置，强制重新确认
    }
}
```

**核心 Raft 不变式**: 论文 Figure 2：leader 当选后 matchIndex 必须初始化为 0，nextIndex 初始化为 lastLogIndex+1，不能沿用上一个 term 的值。

---

### BUG-008 · AppendEntries 快速回退：冲突 term 用了 leader 的期望值而非 follower 的实际值，导致 LastMatchIndex == prevLogIndex 死循环

**文件**: `src/raft1/raft.go` — `AppendEntriesHandler`

**错误代码**:
```go
prevTerm := args.PrevLogTerm   // ❌ leader 的期望 term，不是 follower 的实际 term
for i := len(rf.logs) - 1; i >= 0; i-- {
    if rf.logs[i].Term < prevTerm {
        reply.LastMatchIndex = i
        break
    }
}
```

**问题**: 两个独立的子问题：

1. **PrevLogIndex 越界时**（follower 日志比 leader 短）：follower 根本没有 prevLogIndex 处的 entry，无法知道冲突 term 是什么。仅靠 matchIndex 这一个字段，只能返回 `LastMatchIndex=0`，让 leader 从头重试；若想进一步加速，需要额外维护 `lastRequireId` 等字段，当前实现不支持，返回 0 是正确的兜底。

2. **PrevLogIndex 未越界但 term 不匹配时**：`prevTerm` 应该取 `rf.logs[args.PrevLogIndex].Term`（follower 自身在该位置的实际 term），而非 `args.PrevLogTerm`（leader 的期望 term）。两者不同时，搜索基准错误，可能找到 prevLogIndex 本身（其 term < args.PrevLogTerm），导致 `LastMatchIndex == prevLogIndex`，leader 将 `peerLastIndex` 设为同一值，下轮心跳 prevLogIndex 不变，follower 再次返回相同值，陷入死循环。

**正确做法**:
```go
if args.PrevLogIndex < len(rf.logs) {
    // term 不匹配：用 follower 自身的冲突 term，跳过整个冲突 term 区段
    conflictTerm := rf.logs[args.PrevLogIndex].Term  // ✅ follower 的实际 term
    for i := args.PrevLogIndex; i > 0; i-- {
        if rf.logs[i].Term != conflictTerm {
            reply.LastMatchIndex = i
            break
        }
    }
}
// PrevLogIndex 越界时 LastMatchIndex 保持 0，leader 从头重试
```

**核心不变式**: 快速回退的冲突 term 必须来自 follower 自身日志，不能用 leader 传来的期望值；当 prevLogIndex 越界时，若只维护 matchIndex 则无法加速，只能返回 0。

---
