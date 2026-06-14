# Bug Log

Valuable bugs encountered during 6.5840 lab implementation. Careless/trivial omissions are excluded.

---

## Lab 2 — Simple KV Server

### BUG-L2-001 · Put 第一次 RPC 收到 ErrVersion 误返回 ErrMaybe，破坏线性一致语义　`【Medium】`

**文件**: `src/kvsrv1/client.go` — `Clerk.Put`

**错误代码**:
```go
ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
if ok {
    if reply.Err == rpc.ErrVersion {
        return rpc.ErrMaybe   // ❌ 第一次 RPC 就返回 ErrMaybe
    }
    return reply.Err
}
```

**问题**: 关键区别在于 ErrVersion 是**第一次 RPC** 收到的，还是**重发（resend）** 收到的。

第一次 RPC `ok == true` 说明请求-响应完整往返，服务器一定**没有**执行这次 Put（版本号不匹配被拒）。此时是 100% 确定的"没生效"，应如实返回 `ErrVersion`。若返回 `ErrMaybe`，就把一个"确定失败"误报成"可能成功"，破坏测试对线性一致性的预期。

`ErrMaybe` 只能用于**重发路径**：第一次 RPC `ok == false`（请求或响应丢失），Clerk 无法区分两种情况——
1. 第一次请求没到服务器 → 没生效 → 重发会成功；
2. 第一次请求已到达并成功执行（版本号已 +1），只是响应丢了 → 重发因版本号已变而收到 `ErrVersion`，但 Put 其实**已经成功**。

重发收到 ErrVersion 时无法分辨是 1 还是 2，所以只能返回 `ErrMaybe`（可能生效）。

**正确做法**: 第一次 RPC 如实返回 `reply.Err`（含 ErrVersion）；只有进入重发循环后收到 ErrVersion 才转成 ErrMaybe：
```go
ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
if ok {
    return reply.Err                 // ✅ 第一次：ErrVersion 确定没生效，照实返回
}
for {
    ok = ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
    if ok {
        if reply.Err == rpc.ErrVersion {
            return rpc.ErrMaybe      // ✅ 重发：之前可能已生效
        }
        return reply.Err
    }
    time.Sleep(100 * time.Millisecond)
}
```

**核心不变式**: ErrVersion 来自第一次往返成功的 RPC → 确定没生效 → `ErrVersion`；来自重发 → 之前可能已生效 → `ErrMaybe`。区分依据是"是否为重发"，不是"是否收到 ErrVersion"。

---

## Lab 3 — Raft

### BUG-001 · voteFor 在同 term 内被错误重置导致 split-brain　`【High】`

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

### BUG-002 · startElection goroutine 在运行时才读 rf.term，可能发出错误 term 的 RequestVote　`【Medium】`

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

### BUG-003 · AppendEntries reject 时未重置选举计时器，导致频繁重新选举　`【High】`

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

### BUG-004 · RequestVote 授予投票后未重置选举计时器　`【Medium】`

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

### BUG-005 · startHeartbeat 中 LeaderCommit 在 goroutine 内读取，存在数据竞争　`【High】`

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


### BUG-006 · 当选 leader 时未重置 peerLastIndex，导致错误统计 majority　`【High】`

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

### BUG-008 · AppendEntries 快速回退：冲突 term 用了 leader 的期望值而非 follower 的实际值，导致 LastMatchIndex == prevLogIndex 死循环　`【Medium】`

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

### BUG-009 · sendAppendEntries failure 回复无 stale 保护，乱序回复导致 peerLastIndex 倒退　`【Medium】`

**文件**: `src/raft1/raft.go` — `sendAppendEntries`

**错误代码**:
```go
} else {
    if reply.LastMatchIndex > 0 {
        rf.peerLastIndex[server] = reply.LastMatchIndex  // ❌ 无条件写，可能倒退
    } else {
        rf.peerLastIndex[server] = 0
    }
}
```

**问题**: labrpc longreordering 模式下，RPC 回复最多延迟 2000ms，而心跳每 100ms 发一次。一个旧轮次的 failure 回复（`LastMatchIndex=0`）可能在 peerLastIndex 已经被后续 success 回复推进到 15 之后才到达，把 peerLastIndex 从 15 打回 0。

此外 `sendAppendEntries` 没有像 `sendRequestVote` 那样的 stale 保护（`rf.term == args.Term && rf.customerId == Leader`），stepDown 之后也会继续执行 failure 路径，写入无意义的 peerLastIndex。

两个问题叠加结果：leader 误以为 follower 什么都没有，下次心跳把整个日志重发一遍；如果两个 follower 都被打回 0，新 entry 的 majority 计数归零，commit 延迟一个心跳周期。

**危害程度说明**: follower 的 log 实际上没有丢失（log 持久化 + AppendEntriesHandler 幂等），所以正确性不受影响；纯粹是性能/活性问题，极端情况可能导致 TestCount3B（RPC 次数上限）超出。

**正确做法**:
```go
if reply.Term > rf.term {
    rf.stepDown(reply.Term)
    return ok  // ✅ stepDown 后直接返回
}
// ✅ 丢弃 stale 回复
if rf.term != args.Term || rf.customerId != Leader {
    return ok
}
if reply.Success {
    ...
} else {
    // ✅ args.PrevLogIndex 已落后于当前确认值，说明这是一个乱序的旧 failure 回复
    if args.PrevLogIndex < rf.peerLastIndex[server] {
        return ok
    }
    rf.peerLastIndex[server] = reply.LastMatchIndex
}
```

**核心原则**: success 路径用 `max()` 保证 peerLastIndex 单调递增；failure 路径同理需要保证不被乱序回复倒退，判断依据是 `args.PrevLogIndex` 是否已经落后于当前确认值。

---

### BUG-010 · peerLastIndex 同时充当 matchIndex 和 nextIndex，导致 quorum 统计被 failure 路径破坏　`【High】`

**文件**: `src/raft1/raft.go` — `Raft` struct、`sendAppendEntries`、`startHeartbeat`

**错误代码**:
```go
// 字段
peerLastIndex []int  // ❌ 同时用于 matchIndex（quorum）和 nextIndex-1（探测位置）

// sendAppendEntries failure 路径
rf.peerLastIndex[server] = reply.LastMatchIndex  // ❌ 降低了 matchIndex

// startHeartbeat
prevLogIndex := rf.peerLastIndex[i]  // ❌ 直接用 matchIndex 当 nextIndex-1
```

**问题**: Raft 论文 Figure 2 定义了两个独立的 per-peer 变量：

- `matchIndex[i]`：已确认复制到 peer i 的最高 index，**只增**，用于 quorum commit 判断
- `nextIndex[i]`：下次要发给 peer i 的 index，**可以降低**，用于快速回退

合并成一个 `peerLastIndex` 后，failure 路径（`peerLastIndex = reply.LastMatchIndex`，可能为 0）会降低 matchIndex，导致已 committed 的 entry 在 quorum 统计中丢失计数，进而阻塞后续 commit——在持续高频写入 + 网络乱序下，每次 failure 回复都会让 leader 误以为 majority 减少。

具体危害：
1. `matchIndex` 被打回 0 → quorum 扫描时 matchCount 不足 → 本该 commit 的 entry 卡住
2. 下轮心跳 `prevLogIndex = matchIndex = 0` → 整条 log 全部重发 → RPC 次数爆炸（TestCount3B 失败）
3. 正确性边界：若所有 success 回复已确认 commitIndex，暂不影响已 commit 内容；但若 failure 打回发生在首次 commit 之前，会造成无限阻塞

**正确做法**: 拆分为两个字段：
```go
matchIndex []int  // 已确认复制的最高 index，只增，用于 quorum
nextIndex  []int  // 下次发送的起点，可降，用于确定 prevLogIndex
```

当选 leader 时初始化：
```go
for i := range rf.peers {
    rf.matchIndex[i] = 0
    rf.nextIndex[i] = rf.lastLogIndex() + 1
}
```

success 路径（matchIndex 和 nextIndex 都向前推进）：
```go
rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
rf.nextIndex[server] = rf.matchIndex[server] + 1
```

failure 路径（只修改 nextIndex，matchIndex 不变）：
```go
if args.PrevLogIndex < rf.matchIndex[server] {
    return ok  // 乱序 stale failure，丢弃
}
rf.nextIndex[server] = reply.LastMatchIndex + 1
```

**核心 Raft 不变式**: matchIndex 单调递增，是已知事实；nextIndex 是乐观猜测，可以随 failure 回退。两者职责不同，不能共用一个变量。

---

### BUG-011 · AppendEntries 无条件截断日志，乱序 RPC 可抹掉已 committed 的 entry　`【High】` `【经典错误】`

**文件**: `src/raft1/raft.go` — `AppendEntriesHandler`

**错误代码**:
```go
if len(args.Entries) > 0 {
    rf.logs = rf.logs[:rf.relIdx(args.PrevLogIndex)+1]  // ❌ 无条件截断
    rf.logs = append(rf.logs, args.Entries...)
    rf.persist()
}
```

**问题**: labrpc longreordering 模式下，同一 term 内的 RPC 回复可延迟最多 2000ms，导致旧消息晚于新消息到达。典型时序：

```
Leader → Follower: AppendEntries(prevIdx=4, entries=[5,6])  ← 较新，先到
  Follower: log 变成 [0..6]，entry 6 append 成功
  Leader: majority 确认 entry 6，commitIndex = 6

Leader → Follower: AppendEntries(prevIdx=4, entries=[5])    ← 较旧，后到
  Follower: 无条件截断到 index 4，再 append [5]
  → entry 6 被抹掉，但 leader 认为已 committed！
```

**根本原因**: 论文 §5.3 明确规定——只有 **term 冲突**（相同 index 但 term 不同）才能截断，相同 term 的相同 entry 绝对不能删除。当前实现违反了这一原则：只要 entries 非空，不管 term 是否一致都无条件截断。

同样的问题出现在 InstallSnapshot 路径：如果 follower 在 snapshotIndex 处的 term 与 SnapshotTerm 一致，说明此后的 entry 也是一致的，不应全部丢弃。

**正确做法**: 找到第一个冲突点，只从冲突点开始截断：
```go
if len(args.Entries) > 0 {
    i := 0
    for i < len(args.Entries) {
        logIdx := args.PrevLogIndex + 1 + i
        if logIdx > rf.lastLogIndex() {
            break  // 超出范围，后面全是新的
        }
        if rf.logAt(logIdx).Term != args.Entries[i].Term {
            break  // 找到冲突点
        }
        i++  // term 相同，已有且一致，跳过
    }
    if i < len(args.Entries) {
        logIdx := args.PrevLogIndex + 1 + i
        rf.logs = append(rf.logs[:rf.relIdx(logIdx)], args.Entries[i:]...)
        rf.persist()
    }
    // i == len(args.Entries)：全部已存在且一致，无需操作
}
```

**核心 Raft 不变式**: "If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it."（论文 §5.3）冲突的定义是 **term 不同**，term 相同则不是冲突，不得截断。

---

### BUG-012 · InstallSnapshot 保留尾部日志时未校验 term，导致分叉条目残留、follower 永久卡死　`【High】`

**文件**: `src/raft1/raft.go` — `AppendEntriesHandler`（InstallSnapshot 分支）

**错误代码**:
```go
logs := []LogEntry{{Term: args.SnapshotTerm}}
if rf.relIdx(args.SnapshotIndex) < len(rf.logs) {
    // ❌ 只判断长度，没校验 SnapshotIndex 处 term 是否一致
    rf.logs = append(logs, rf.logs[rf.relIdx(args.SnapshotIndex)+1:]...)
}
rf.snapshotIndex = args.SnapshotIndex
```

**问题**: 保留 `SnapshotIndex` 之后的尾部条目时，只判断了 `relIdx < len`，没有检查 follower 在 `SnapshotIndex` 处的条目 term 是否等于 `SnapshotTerm`。若 follower 此处的日志来自**分叉历史**（同 index 不同 term），这些尾部条目本应丢弃却被保留，残留在快照之上，与 leader 的真实日志冲突。

**永久卡死时序**: follower 残留分叉尾部后，leader 发 `AppendEntries(prevIdx=SnapshotIndex, prevTerm=正确)` 时一致性检查在尾部反复失配；回退又退到 `SnapshotIndex`，触发的 InstallSnapshot 因 `SnapshotIndex <= rf.snapshotIndex` 被当作 stale 直接 return success（不重建日志），于是 nextIndex 在 `SnapshotIndex+1` 与回退之间死循环，follower commit 永远停在 snapshotIndex。

**正确做法**: 只有 follower 在 `SnapshotIndex` 处已有同 index 同 term 的条目时才保留尾部，否则丢弃整个 log：
```go
logs := []LogEntry{{Term: args.SnapshotTerm}}
if rf.relIdx(args.SnapshotIndex) < len(rf.logs) &&
    rf.logAt(args.SnapshotIndex).Term == args.SnapshotTerm {
    rf.logs = append(logs, rf.logs[rf.relIdx(args.SnapshotIndex)+1:]...)
} else {
    rf.logs = logs
}
rf.snapshotIndex = args.SnapshotIndex
```

**核心 Raft 不变式**: 论文 Figure 13 规则 6——"If existing log entry has same index and term as snapshot's last included entry, retain log entries following it and reply." 否则丢弃整个 log。term 是判定保留/丢弃的唯一依据，与 BUG-011（截断必须以 term 一致性为准）同源。

---

## Lab 4 — Fault-tolerant KV

> 本节多为**设计阶段讨论结论**与 **2026 版 vs 2024 版差异**梳理，非运行期崩溃型 bug，但都是极易绕进去的理解陷阱。

### 2026 版 vs 2024 版核心差异（必须先理解，否则后面全错）

| 维度 | 2024 版（旧 6.824） | 2026 版（本仓库） |
|---|---|---|
| 写语义 | 无条件 `Put` / `Append` | 带 `version` 的 **conditional Put（CAS）**：version 匹配才执行，执行后 version+1 |
| 重复写防护 | **clientId+seq 去重表**，apply 前查表跳过重复 | **version 天然幂等**：重发的 Put 因 version 已变而被 `ErrVersion` 挡住，不会执行第二次 |
| 去重表持久化 | 必须把 `clientId→seqMax` 一起写进快照 | **没有去重表**，无需持久化任何 client 状态 |
| 不确定结果 | 去重表实现 exactly-once，client 直接拿真实结果 | client 自己合成 `ErrMaybe`，把"可能执行了"暴露给应用层 |
| 快照内容 | KV 数据 + 去重表 | **仅 KV 数据（含 version）** |

**一句话**：2026 版用 **version（CAS）替代了 clientId+seq 去重表**。version 本身就存在 KV 数据里，会随快照一起持久化，所以幂等性"免费"获得，不再需要任何独立去重结构。

**但这套"免费"是有代价的**——version 只能为**幂等操作**提供 exactly-once *效果*，对**非幂等的 Append 无法归因**，这正是 `ErrMaybe` 存在的根本原因。这是理解整个 2026 设计取舍的核心，详见 BUG-L4-004。

---

### BUG-L4-001 · 把 rsm 的 HashNum 随机数误当成去重机制　`【概念】`

**文件**: `src/kvraft1/rsm/rsm.go` — `Submit` / `readApplyCh`

**误解**: 以为 `Op.HashNum`（`hashNumMap[logId]` + apply 时比对）是"客户端去重"，因此担心快照丢掉它会导致去重失效、重复执行。

**真相**: `HashNum` 与去重**毫无关系**，它解决的是 Raft KV 的经典问题——

- leader 调 `rf.Start(op)` 拿到 index i，在 `Submit` 里等 `chMap[i]`；
- 若该 leader 失去 leadership，index i 可能被**别人的命令**覆盖；
- apply 到 index i 时比对 `applied.HashNum == 我记录的 hashNum`，不等说明"这个槽位不是我的命令" → 返回 `ErrWrongLeader`。

它是 **leader 进程内、单次 RPC 生命周期**的"apply 匹配标记"，apply 完即 `delete`。**纯内存瞬态，不进快照、也不该进快照**：重启后没有任何在途 `Submit` 在等旧 channel，这个信息天生无意义。

| | HashNum（rsm 层） | clientId+seq 去重表（2024 版） |
|---|---|---|
| 解决 | "index i 回来的是不是我提交的 op" | "这个请求是否已执行过" |
| 存活 | 内存瞬态，用完即弃 | 需随状态机持久化/快照 |
| 2026 版 | 保留（仍需要） | **不存在** |

**核心**：apply 匹配（HashNum）与重复请求去重（version）是两个正交层面，别混为一谈。

---

### BUG-L4-002 · ErrWrongLeader 统一表示"未 commit"，client 重试安全　`【设计】`

**文件**: `src/kvraft1/rsm/rsm.go` — `Submit`

从 client 视角只有两类结果：

1. 命令**真正 commit + apply** → 拿到真实结果（`OK` / `ErrVersion` / `ErrNoKey`）；
2. 命令**没能 commit**（不是 leader / 失去 leadership / index 被占用 / 超时）→ 一律 `ErrWrongLeader`，让 client 换 server 重试。

`Submit` 把 not-leader、`Start` 失败、term 变化、index 被别人占用（`close(ch)`）全部归为 `ErrWrongLeader` 是正确的。重试安全因为：Get 幂等、Put 由 version 保证幂等。

**关键陷阱（时序题）**：A 收到 Put 后 `Start`、随即离线 → 这条 log 可能已被别的 leader commit 并快照。A 恢复后收到的是 InstallSnapshot 而非 CommandValid，等待的 channel 永远不来 signal。**但 `Submit` 不会卡死**——它的 `time.After` + `GetState()` 兜底发现 `term 变了 / 非 leader`，20ms 内返回 `ErrWrongLeader`。**这条路径根本不依赖 HashNum，靠的是 term 检查。**

**核心不变式**：「等待的 log 被快照跳过」与「我仍是原 term 的 leader」**互斥**——一个节点只会通过 InstallSnapshot 跳过它"已非 leader / 已非原 term"的 log；它作为 leader Start 且正在等的 log，要么本地正常 apply（log 一定先 apply 再被压进快照），要么早已 stepDown 被 term 检查兜底。所以 HashNum 被快照丢弃永不造成误判。

---

### BUG-L4-003 · ErrMaybe 是 Clerk 合成的；Clerk 不能把 ErrWrongLeader 当终态　`【High】`

**文件**: `src/kvraft1/client.go` — `Clerk.Put` / `Clerk.Get`

`rpc` 包注释明确：`ErrMaybe` 是 **"Err returned by Clerk only"**——server 永远不返回它，由 Clerk 在重试循环里把 `ErrVersion` 转成 `ErrMaybe`。

**错误代码**:
```go
ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply)
if ok {
    return reply.Err   // ❌ ok 但 reply.Err==ErrWrongLeader 时，把"未 commit"当终态抛给应用层
}
```

**问题**: `ErrWrongLeader` 不是终态，是"换 server 重试"的信号。直接返回它既错误，又破坏了 first/resend 语义。

**ErrVersion → ErrVersion 还是 ErrMaybe 的判据**：

- **第一个干净返回（非 WrongLeader、非超时）就拿到 ErrVersion** → 返回 `ErrVersion`（server 真做了版本检查，确定没改）；
- **经历过 WrongLeader / 超时之后再拿到 ErrVersion** → 返回 `ErrMaybe`。因为那个 WrongLeader 可能是"server 已 `Start`、log 其实已 commit、只是它失去了 leadership"，client 无从区分"立即拒绝（没执行）"和"已执行但响应丢失"，必须保守。

**正确范式**:
```go
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
    args := rpc.PutArgs{Key: key, Value: value, Version: version}
    first := true
    for {
        var reply rpc.PutReply
        ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply)
        if ok && reply.Err != rpc.ErrWrongLeader {     // ✅ 只有非 WrongLeader 才是真实结果
            if !first && reply.Err == rpc.ErrVersion {
                return rpc.ErrMaybe                     // ✅ resend 的 ErrVersion → 可能已执行
            }
            return reply.Err
        }
        first = false                                  // WrongLeader / 超时都消耗 first
        ck.leader = (ck.leader + 1) % len(ck.servers)  // ✅ 换 leader 重试
        if !ok {
            time.Sleep(100 * time.Millisecond)
        }
    }
}
```

**连带 bug**：① 原代码用 `ck.server`（字段不存在，编译不过，应为 `ck.servers[ck.leader]`）；② `Get` 用 `for !ok` 循环，WrongLeader 时 `ok==true` 会错误退出循环并返回假的 `ErrNoKey`，应改为 `for {}` 仅在拿到真实结果时 return。

**核心不变式**: 判定 ErrVersion vs ErrMaybe 的依据是「**这是不是对该 Put 的第一个、且干净返回的 RPC**」，不是「是否收到 ErrVersion」。一旦出现过任何"不确定"（WrongLeader / 超时），后续 ErrVersion 一律 ErrMaybe。与 [[BUG-L2-001]]（kvsrv 单机版的同一判据）同源，只是多了 WrongLeader 这个"不确定来源"。

---

### BUG-L4-004 · version 去重的本质局限：只能去重幂等操作，对非幂等 Append 无法归因（ErrMaybe 的根源）　`【核心】`

**这是 2026 vs 2024 在语义上的根本取舍，前面几条都是它的推论。**

**version 去重的粒度是"key 的状态版本"，而不是"哪个 client 的哪次请求"。** 它能保证的只有一件事：**对一个特定 `(value, version=N)` 的写，最多生效一次**——因为成功一次后 version 就变成 N+1，同一个写再来会被 `ErrVersion` 挡掉。

- **对幂等操作（覆盖式 Put）**：重复执行结果一样，所以"最多生效一次"等价于 exactly-once *效果*，version 机制够用。
- **对非幂等操作（Append = `Get` 拿 (v, ver) → `Put(v+delta, ver)` 的 CAS 循环）**：version 机制**做不到归因**。

**归因问题（你说的"确实做了但不知道谁做的"）**：

单个 client C 做 append：
```
C: Get → version=5
C: Put(newval, version=5) → server 执行成功，version 5→6，但响应丢失
C: 超时重试 Put(newval, version=5) → 现在 version=6，返回 ErrVersion
```

C 看到 ErrVersion，只知道"version 已经不是 5 了"，但**无法回答"5→6 这一步是不是我这次请求造成的"**：

- 情况 (a)：是我上次成功了 → 我**不能**再 append 一次（否则 append 两遍）；
- 情况 (b)：是别人在我俩之间改的，我上次根本没成功 → 我**应该**基于新 version 重新 append。

version 只记录"状态变了"，不记录"谁把它改成这样的"，所以 (a)(b) 无法区分。client 只能把不确定性甩给应用层 → 返回 **`ErrMaybe`**。**这就是 ErrMaybe 的根源，也是 version 方案对非幂等操作的语义降级：从 exactly-once 退化为 at-most-once + "可能做了"。**

**对比 2024 版为什么能做到（含 Append 的真 exactly-once）**：

clientId+seq 去重表是**按请求归因**的——它记录"client C 的 seq=N 这个请求执行过没有、结果是什么"。重发时直接查表**返回缓存结果**，于是：

- 即便是非幂等的 Append，也精确知道"这个请求执行过了，结果是 X"，不重复执行；
- 不需要 ErrMaybe，因为"谁的哪次请求做没做"被去重表精确记录了。

| | 2026 version（CAS） | 2024 clientId+seq 去重表 |
|---|---|---|
| 去重粒度 | key 的状态版本 | (clientId, seq) 请求 |
| 幂等操作 | exactly-once 效果 ✓ | exactly-once ✓ |
| 非幂等 Append | **无法归因 → at-most-once + ErrMaybe** | 缓存结果 → 真 exactly-once ✓ |
| 重发的处理 | 看 version 变没变（猜） | 查表返回缓存结果（确定） |
| 代价 | 无去重表、无需持久化 client 状态；语义弱 | 需维护+持久化去重表；语义强 |

**核心不变式**: version 去重的能力边界 = 操作是否幂等。它把"是否重复执行"判断**外包给了 key 的版本状态**，因此只能服务"重复无害"的幂等写；非幂等写的 exactly-once 必须由更高层（按请求归因的去重表，或应用层自己的 version+重试逻辑）承担，而 2026 版选择了后者并以 `ErrMaybe` 暴露不确定性。与 [[BUG-L4-003]]（ErrMaybe 何时返回）互为表里：L4-003 讲"何时合成 ErrMaybe"，L4-004 讲"为什么不得不有 ErrMaybe"。

---
