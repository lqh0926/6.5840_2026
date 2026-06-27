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

**落地备注 (2026-06-14)**: 当初只有 InstallSnapshot 路径（BUG-012）按此原则改了，普通 AppendEntries 路径（`raft.go` 第 297 行附近）仍是 `rf.logs = append(rf.logs[:relIdx(PrevLogIndex)+1], args.Entries...)` 的无条件截断 —— 文档写了正确做法但源码这一行漏改。lab4c 偶发的 `Snapshot index N out of bounds (last log index N-1)` 即由此引起：迟到的短 AE 抹掉已 committed 的尾部 entry，rsm 随后对该 index 调 `rf.Snapshot` 越界 Fatal。现已统一为逐条找冲突点截断，8 轮 `-race` 压测 TestSnapshotUnreliableRecoverConcurrentPartition* 全过。

**同根因的第二张脸 (2026-06-16)**: 回归压测期间还出现过 `commitIndex > lastLogIndex` 的 slice 越界 panic（`sendApplyMsg` 中 `rf.logs[relIdx(startIdx):relIdx(commitIndex)+1]`）。与 `Snapshot out of bounds` **同一个根因**：无条件截断把日志砍到 commit 水位线以下，而迟到那条短 AE 的 `LeaderCommit` 比当前 commit 小、进不了 commit 推进分支，于是 commitIndex 停在原处、日志却变短 → apply 时越界。修复（只在 term 冲突点截断、已匹配尾部一律保留）同时根除两个 panic。曾因 `go test` 不重建 daemon 子进程（见笔记 daemon_test_rebuild）反复复现失败、临时 DIAG 探针不触发，误判为缓存假象；改用 `make RUN=... raft1/kvraft1`（`.FORCE` 重建 daemon + `-race`）后，50 轮 + 1 轮共 51 轮 3A/3B/3C/3D/4B/4C 全过，DIAG 当门哨零触发，确认根除。临时 DIAG 插桩已于本日删除。

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

## Lab 5 — Sharded KV

> 本节为**实现前设计讨论**的总结，均为概念/架构层面的理解陷阱，非运行期崩溃。但每一条都直接决定代码怎么写，绕进去就会写错。

### 三层 Clerk 架构（理清谁打谁）`【架构】`

```
                    测试 ts.MakeClerk()
                            │
                            ▼
              ┌─────────────────────────────┐
              │  shardkv.Clerk (顶层路由)    │   ← src/shardkv1/client.go
              │  - Get/Put 实现              │
              │   - sck  : *ShardCtrler     │
              │   - rcks : map[gid]->grpCk   │
              │   - cfg  : *ShardConfig     │
              └─────────────┬───────────────┘
                            │
              ┌─────────────┴────────────────────────┐
              │ (1) sck.Query() 查配置                 │ (2) 找到 key 所属组的 gid
              ▼                                       ▼
   ┌──────────────────────────┐          ┌─────────────────────────────────┐
   │ ShardCtrler (分片控制器)  │          │  shardgrp.Clerk (per-group)      │ ← src/shardkv1/shardgrp/client.go
   │ - IKVClerk = kvsrv.Clerk │          │  - Get/Put 直接打到一个 Raft 组   │
   │ - 把 config 当字符串存    │          │  - Freeze/Install/Delete 分片迁移 │
   └────────────┬─────────────┘          └──────────────┬──────────────────┘
                │                                       │ RPC
                ▼                                       ▼
   ┌──────────────────────────┐          ┌─────────────────────────────────┐
   │ kvsrv (GRP0, 单机)       │          │  shardgrp KVServer (Raft 组)     │
   │ - 只存一份 config 字符串   │          │  - 真正存用户的 key/value         │
   │ - "first" -> config json │          │  - 用 Raft 复制 + RSM             │
   └──────────────────────────┘          └─────────────────────────────────┘
```

**容易混淆的点**：`shardctrler.go:27` 的 `sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)` 是**控制器**用来读写配置字符串的 client，**不是**用户数据路径。用户数据路径是 `shardkv.Clerk` → `shardgrp.Clerk` → shardgrp KVServer（Raft 组）。两条路径缺一不可，但分别服务不同目的。

**RSM 不复制，直接 import 复用**：`shardgrp/server.go` 已经 `import "6.5840/kvraft1/rsm"`，`KVServer` 结构里有 `rsm *rsm.RSM` 字段。RSM 通过 `StateMachine` 接口（`DoOp`/`Snapshot`/`Restore`）与具体业务解耦，kvraft 和 shardgrp 只是同一接口的两个实现。

---

### BUG-L5-001 · shardkv.Clerk 路由策略：缓存 cfg + ErrWrongGroup 刷新，不是每次 query　`【设计】`

**文件**: `src/shardkv1/client.go` — `Clerk.Get` / `Clerk.Put`

**关键发现**：
- `GetArgs`/`PutArgs` **不带 config num 字段** — server 无法从请求里知道 client 用的是哪份配置
- `ErrWrongGroup` 在整个代码库里**只有声明，从不被引用** — 它是留给 Lab 5 的占位符，要由我们实现去用
- `TestDeleteBasic5A`（shardkv_test.go:120-154）明确检查迁移后 gid1+gid2 的 snapshot 总大小，**迁移后 gid1 必须删掉迁出的 shard** — client 不能假设 "gid1 上还留着旧数据兜底"

**正确做法**：缓存 cfg，遇 `ErrWrongGroup` 刷新重试：
```go
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
    shard := shardcfg.Key2Shard(key)
    for {
        if ck.cfg == nil {
            ck.cfg = ck.sck.Query()
        }
        gid := ck.cfg.Shards[shard]
        rck := ck.getOrCreateGrpClerk(gid, ck.cfg.Groups[gid])
        v, ver, err := rck.Get(key)
        if err == rpc.ErrWrongGroup {
            ck.cfg = nil   // 缓存过期，刷新
            continue
        }
        return v, ver, err
    }
}
```

**为什么不是每次 query**：配置变更频率远低于 Get/Put 频率；`sck.Query()` 本身要走一次 kvsrv 的 RPC 往返。线性化测试（`TestManyConcurrentClerkReliable5A`，20 秒、10 个并发 clerk）每次 Get 都查配置会严重拖慢。

**为什么不能只靠 ErrNoKey 刷新**：ErrNoKey 是合法的业务结果（key 真的不存在），刷新配置再试还是 ErrNoKey，会陷入死循环。必须有独立的"配置过期"信号，这就是 `ErrWrongGroup` 的用途。

**核心不变式**: client 的 cfg 是"猜测"，server 跟踪的 config 才是"事实"。猜测错了会被 `ErrWrongGroup` 打回，刷新重试。系统对 client 之间 cfg 一致性要求最弱 — 每个最终看到正确 cfg 即可，快慢无所谓。

---

### BUG-L5-002 · shardgrp.Clerk 按 gid lazy 缓存，不每次 query 全 make　`【设计】`

**文件**: `src/shardkv1/client.go` — `Clerk.getOrCreateGrpClerk`

**骨架已经表态**：`rcks map[tester.Tgid]*shardgrp.Clerk` 字段 + `GetClerk(gid)` 接口 = 作者希望你按 gid 缓存。

**为什么不能每次 query 全 make**：
1. `shardgrp.Clerk` 有 `leader int` 字段 — 记着上次成功的 leader 索引，下次直接打那个 server，省掉一轮 ErrWrongLeader 往返。每次 make 新 clerk 就把 leader 缓存丢了
2. 一次 Get 只用一个 gid — `cfg.Groups` 可能返回 8 个组，但当前 key 只属于 1 个 shard → 1 个 gid。给其他 7 个组 make clerk 是纯浪费
3. 并发测试性能 — `TestManyConcurrentClerkReliable5A` 跑 20 秒、10 个 clerk 并发，每次 query 都重建 map 会显著拖慢

**正确做法**：
```go
func (ck *Clerk) getOrCreateGrpClerk(gid tester.Tgid, servers []string) *shardgrp.Clerk {
    if rck, ok := ck.rcks[gid]; ok {
        return rck                      // 缓存命中，复用 leader 信息
    }
    rck := shardgrp.MakeClerk(ck.clnt, servers)
    ck.rcks[gid] = rck
    return rck
}
```

**核心原则**: query 是按需（缓存过期/nil 时才查），make 也是按需（每个 gid 第一次见到才 make）。`*tester.Clnt` 是嵌入而不是字段，Clerk 调用方式是 `ck.Call(...)` 而非 `ck.clnt.Call(...)`。

---

### BUG-L5-003 · 配置变更用 push：controller 同步驱动迁移　`【设计】`

**文件**: `src/shardkv1/shardctrler/shardctrler.go`

**官方设计（lab5.md L17/122/128）就是 push**：controller 的 `ChangeConfigTo` 是同步阻塞调用，亲自驱动整个迁移——读当前配置 → 对每个换手的分片依次 `FreezeShard`(源) → `InstallShard`(目标) → `DeleteShard`(源) → 最后把新配置发布到 kvsrv。shardgrp 只被动响应这三个 RPC，**没有常驻轮询 goroutine**。

**曾考虑过 pull（让 gaining group 轮询配置自驱），当时列的五点顾虑其实 push 都能解决**：

1. **Controller 是临时的** — `ChangeConfigTo` 同步阻塞，迁移没做完不返回，期间实例一直存活；崩溃后的恢复由 Part B 的 current/next 双配置 + `InitController` 重做迁移完成（用 Num 幂等）。不存在"push 完进程就死"的问题。
2. **多 Controller 并发** — 用带版本的 Put 对 next 配置做 CAS，同一 Num 只有一个 controller 发布成功；所有 shardgrp RPC 用 `Num` fencing，落败/重复的 controller 发的旧 RPC 被 shardgrp 直接忽略。
3. **"等结果"不用轮询** — Freeze/Install/Delete 是同步 RPC，经 rsm 提交并返回 `OK` 就代表该步在目标组已落地，controller 直接知道完成，无需 poll 消费者状态。
4. **通讯录自带** — controller 从 `cfg.Groups[gid]` 拿到各组 server 列表，`shardgrp.MakeClerk(sck.clnt, servers)` 建 clerk 发 RPC，不需要预先配置地址。
5. **故障恢复集中在 controller** — controller 挂在迁移中途，新 controller 的 `InitController` 看到 next.Num > current.Num 就重跑该迁移；每步带 Num 幂等，已装的跳过、没装的补上，数据不丢。状态不必散在消费者本地。

**核心原则**: controller 是**唯一的同步驱动者**；shardgrp 用 `cfg.Num` 单调比较防过期 RPC + 三步各自走 rsm 保证组内 3 副本一致。换来无轮询、无消费者状态机、恢复逻辑集中——代价只是 `ChangeConfigTo` 在迁移期间阻塞。

---

### BUG-L5-004 · 迁移协议：controller 驱动 Freeze→Install→Delete　`【设计】`

**文件**: `src/shardkv1/shardctrler/shardctrler.go`、`src/shardkv1/shardgrp/server.go`、`src/shardkv1/shardgrp/shardrpc/shardrpc.go`

**方向**（controller 在 `ChangeConfigTo` 里同步驱动；S 从 gid1 迁到 gid2）：
```
controller (ChangeConfigTo 同步驱动)
  │ ① FreezeShard(S, num=2)  ─────→  gid1(源): 标记 S 冻结, 返回 S 的 KV 数据
  │ ←─────────── State []byte ──────
  │
  │ ② InstallShard(S, state, num=2) ─→ gid2(目标): 提交到 gid2 的 Raft, 装入并立即服务
  │
  │ ③ DeleteShard(S, num=2)  ─────→  gid1(源): 删 S 的 KV 数据
```

- **Freeze**：controller → gid1，返回**源组上属于 shard S 的 KV 数据**（不是整个 kvMap，只是 `Key2Shard(key) == S` 的那些 key）
- **Install**：controller → gid2，gid2 的 handler 走 `rsm.Submit` 提交到自己的 Raft
- **Delete**：controller → gid1

**为什么是 Freeze→Install→Delete 顺序**：删数据必须是最后一步，且必须在 Install 成功之后。否则源组先删、目标组还没装好就会丢数据。即使中途 RPC 丢或 controller 崩溃，重试（或 Part B 的新 controller）用 `num` 去重：已装过的 Install 跳过、直接补发 DeleteShard，数据不丢。

**controller 给各组发 RPC 的地址来自 `cfg.Groups[gid]`** — config 自带通讯录，controller 不需要预先"知道某组在哪"。

**单一驱动者，无"哪个副本发起"问题**：controller 是组外的唯一驱动者，不存在 3 副本各自发 RPC 的竞争。三个 handler 都走 `rsm.Submit`，由对应 group 的 leader 提交、3 副本经 Raft 一致 apply。

**核心不变式**: 每步都带 `cfg.Num` 做幂等，重复 RPC、controller 重试/换主都能安全处理。Freeze/Install/Delete 三步各自是 Raft op（提交到对应 group 的 Raft 日志），确保组内 3 副本对 shardState 一致。

---

### BUG-L5-005 · 谁负责服务：shardState 三态 + Install 立即服务（不死锁）　`【设计】`

**文件**: `src/shardkv1/shardgrp/server.go` — `DoOp` / `DoGet` / `DoPut`

**核心争议**：Install 之后是立即服务（`Normal`），还是等 Delete 成功后才服务？

**Delete 才服务的死锁风险**：
```
协议: Freeze → Install (不服务, 等 Delete) → Delete (controller 发给 gid1) → gid2 才服务
死锁场景: gid1 在 Install 之后挂了 → Delete RPC 永远打不通 → gid2 永远不 Activate → shard 永久不可用
```

**Install 立即服务避免死锁**：
```
gid1:  Normal → Frozen → Gone
gid2:  Absent → Normal   ← Install apply 后立即 Normal，不需中间态
```

gid2 激活只取决于自己 Raft 把 InstallOp apply 成功，**不依赖 Delete 这步**。gid1 挂了，controller 的 InstallShard 一旦在 gid2 落地，gid2 就开始服务。DeleteShard 是 best-effort，由 controller（或 Part B 新 controller）重试，不影响 gid2 服务。

| 协议 | gid2 激活依赖 | gid1 挂了会怎样 |
|---|---|---|
| Delete 才服务 | gid1 DeleteShard reply | ❌ gid2 永远不 Activate，死锁 |
| Install 立即服务 | 自己 Raft apply InstallOp | ✅ gid2 正常服务，DeleteShard 异步重试 |

**协议选择的核心原则：激活路径不能有外部依赖**。gid2 是迁移受益者，它激活必须自给自足。

**迁移窗口的 service gap**：
```
t0: gid1 FreezeOp apply → Frozen,拒 Get/Put
================= gap 开始 =================
t0 → t1: client Get/Put 到 gid1 → ErrWrongGroup
         client Get/Put 到 gid2 → ErrWrongGroup (Absent 状态)
         client 刷 cfg 也救不了(还是 cfg2,路由到 gid2 仍 Absent)
         client 在 sleep+retry 循环里等
t1: gid2 InstallOp apply → Normal,开始服务
================= gap 结束 =================
t1 之后: client 重试命中 gid2 → 成功
t2: controller → gid1 DeleteShard (best-effort,不影响 t1 后的服务)
```

**liveness 靠 gid2 最终 Install 成功打破**，不依赖 gid1。gap 典型几百 ms（一次 RPC 往返 + Raft 提交），测试 `checkShutdownSharding`（test.go:190）期望 `ndone < n` 验证 shard 真的不可用。

**核心不变式**: Frozen 和 Gone 在功能上行为一致 — 都返回 `ErrWrongGroup`。区别只在内部：`Frozen` 数据还在（打包给 FreezeShardReply 用），`Gone` 数据已删。对外可见的只有"服务"vs"不服务"两态。`DoGet`/`DoPut` 里判断 shardState 只要非 `Normal` 就一律 `ErrWrongGroup`，不用分 Frozen/Gone。

---

### BUG-L5-006 · Frozen 必须拒绝 Get：线性一致的可见性边界问题　`【概念】`

**文件**: `src/shardkv1/shardgrp/server.go` — `DoGet`

**常见误区**：Frozen 期间数据还在 gid1 上，且 t0→t1 窗口里 S 上零写入（gid1 frozen 拒写、gid2 Absent 拒写），那 Frozen 时允许 Get 继续返回数据应该是安全的吧？

**错误论证链（曾经绕进去的版本）**：
1. ❌ " gid1 frozen 后数据会被并发写" — **错**，t0→t1 窗口里 gid1 拒写、gid2 还没 Install 也拒写，S 上完全静止
2. ❌ "因为数据会变，所以 frozen 读会读到过时数据" — **前提错了**
3. ✅ **真正的理由**：t1 之后 gid2 激活开始服务，此后可能发生新写。如果 gid1 frozen 时允许读，client 不会收到 `ErrWrongGroup` → 不会刷 cfg → 不会路由到 gid2 → 会一直读到 gid1 的 t0 快照。t1 之后这些读违反 real-time order（t2 的 Put 完成后，任何 t2 之后的 Get 必须读到新值）。

**线性一致破坏时序**：
```
t0: gid1 freeze → frozen, 仍提供 Get (假设的错误设计)
t1: gid2 Install 完成, 开始服务
t2: client B Put("x", v1) → gid2 → 成功
t3: client A (cfg1 缓存未刷新) Get("x") → gid1 → 拿到 v0 (旧快照)
    ↑
    t2 < t3, B 的 Put 在前, A 的 Get 在后
    线性一致要求 A 读到 v1，但 A 读到 v0 → ❌
```

**核心不是数据竞争，是"可见性边界"**：Frozen 之后，gid1 必须停止数据可见性，强制 client 转移到 gid2，才能保证 client 后续读到的是 gid2 的最新数据。只要 frozen 拒绝读 → client 收到 `ErrWrongGroup` → 刷 cfg → 路由到 gid2 → 读到最新。

**与 Get 走不走 Raft 无关**：即使实现了快速读（ReadIndex / Lease / No-op），在 Frozen 态也必须拒绝。快速读只在 Normal 态有用（省 Raft 日志提交开销）。

**核心不变式**: `shardState[S] != Normal` 时 `DoGet`/`DoPut` 一律返回 `ErrWrongGroup`。Frozen 数据保留纯粹是为了等 FreezeShard RPC 时打包，不是为了读。对外可见性在 freeze 那一刻已经完全切断。

---

### BUG-L5-007 · 快速读与分片迁移不兼容的深层原因　`【概念】`

**常见疑问**：如果实现了快速读（ReadIndex / Lease / No-op+本地读），frozen 上读是不是天然安全？

**结论**：No-op 快速读在分片迁移场景下会挂 — 除非把 shardState 当作读屏障 + 迁移协议做得非常精密（双 read 路径 + 配置原子切换）。2026 版用"Get 也走 Raft + Frozen 时一律 ErrWrongGroup"绕开这个难题。

**No-op 方案的根本弱点**：leader 上任时插一条 no-op op 进 log，确保 commitIndex ≥ 这个 no-op 的 index。之后的"本地读"不进 log，直接读状态机。**但 no-op 本质上是把"当前状态机的快照版本"钉死在 no-op 提交那一刻**。

**在分片迁移下暴露的问题**：
```
gid2 上任时 (假设 cfg2 已经发布但 gid2 还没 Install S)
    client A (cfg2) Get("x") 路由到 gid2
    gid2 leader 走快速读：我是 leader, 状态机里查 x
      ① shardState[S] = Absent → 该返回什么?
         - 返回 ErrNoKey: 错! x 在 gid1 那边还有值,不是真的不存在
         - 返回 ErrWrongGroup: client 死循环(直到 Install op apply)
         - 返回旧数据: 状态机里根本没有 x,没东西可返回
```

**解法（如果一定要支持快速读）**：状态机里必须有 shardState 标记作为"读屏障"。`Absent`/`Waiting` 状态返回 `ErrWrongGroup`，只有 `Normal` 才允许快速读。但这又退回"client 死循环等迁移完成"，快速读**没有收益** — client 卡住的根本原因不是日志提交开销，是状态机里没有数据。

**工业级方案**：DynamoDB/Spanner/CockroachDB 用"双 read 路径 + 配置原子切换" — gid1 frozen 仍可读返回 freeze 时刻快照，gid2 Install 完成后开始服务。但要求**配置切换是"原子可见"的**：client 要么看到 cfg1 要么看到 cfg2，不能一会 cfg1 一会 cfg2。kvsrv 单机 CAS 满足这个，但 client 缓存 cfg 后**已经路由的 RPC 在途中怎么办**？这需要 "configuration change as consensus"（详见 Spanner/FaRM 论文），不是简单"加个状态"能解决的。

**2026 版选择**：不用快速读，用 ErrWrongGroup 收敛。Get 也走 `rsm.Submit`，迁移窗口期间任何 group 都不服务这个 shard。代价是 migration window 期间 shard 不可用，liveness 靠"迁移最终完成" + 测试给的超时足够长保证。

**核心原则**: 快速读只确认 leader 身份，不确认"读的数据仍是当前权威"。迁移期间数据可能已在新 owner 那边被写，old owner 的快速读会返回过时数据，破坏线性一致。2026 版用"避免快速读"换"正确性可证"，牺牲一点性能换工程简单度。

---

### BUG-L5-008 · shardgrp clerk 无限重试 → 组离开后 shardkv clerk 永不刷新配置　`【正确性】`

**文件**: `src/shardkv1/shardgrp/client.go` — `Get` / `Put`

**问题**：`TestJoinLeaveBasic5A` 在 `leave(Gid1)` + `Shutdown(Gid1)` 之后，`CheckGet` 永久卡死（goroutine 栈停在 `shardgrp/client.go` 的重试循环 → `shardkv/client.go:79`）。leave 的迁移其实已经完成、cfg 已更新为新配置，但客户端读不到数据。

**根因（两层 clerk 的失配）**：
- shardkv clerk 只在收到 **`ErrWrongGroup`** 时才刷新配置、重新路由。
- 但一个**已离开并下线**的组，shardgrp clerk 收到的是**网络失败（`ok==false`）**，不是 `ErrWrongGroup`。
- 旧代码里 shardgrp clerk 对网络失败/`ErrWrongLeader` **无限 `for {}` 重试**，永不返回 → shardkv clerk 永远等不到返回值 → 没机会刷新到新配置 → 一直把 key 路由到那个下线的旧组。

**错误代码**：
```go
func (ck *Clerk) Get(key string) (...) {
    for {                                   // ← 无限重试，组下线时永不返回
        ok := ck.Call(ck.servers[ck.leader], "KVServer.Get", &args, &reply)
        if ok && reply.Err != rpc.ErrWrongLeader { return ... }
        ck.leader = (ck.leader + 1) % len(ck.servers)
        time.Sleep(100 * time.Millisecond)
    }
}
```

**正确做法**：给 shardgrp clerk 的重试加**轮数上限**，超过就返回 `ErrWrongGroup`，把控制权交还上层让它重读配置：
```go
const maxRetries = 30
func (ck *Clerk) Get(key string) (...) {
    for tries := 0; tries < maxRetries; tries++ {
        ok := ck.Call(...)
        if ok && reply.Err != rpc.ErrWrongLeader { return ... }
        ck.leader = (ck.leader + 1) % len(ck.servers)
        time.Sleep(100 * time.Millisecond)
    }
    return "", 0, rpc.ErrWrongGroup   // 多轮联系不上 → 让 shardkv clerk 刷新配置重路由
}
```
shardkv clerk 收到 `ErrWrongGroup` 后 `cfg=nil` 重新 `Query()` → 读到新配置 → 路由到新 owner。只有第一个误路由的 key 付一次超时代价，刷新后 cfg 缓存为新配置，其余 key 直接命中。

**核心不变式**: 「整组下线/离开」和「分片不归本组」对客户端是**同一类需要重读配置的事件**，但底层信号不同（网络失败 vs `ErrWrongGroup`）。clerk 必须把「持续联系不上」也归一为「该刷新配置」，不能无限重试一个可能已经不存在的组——否则上层永远拿不到刷新配置的机会。

---

### BUG-L5-009 · 重启时 Restore 晚于 apply 协程启动，已提交日志落在空状态上被丢弃 → 误报 ErrNoKey　`【High】` `【经典错误】`

**文件**: `src/shardkv1/shardgrp/server.go` — `StartServerShardGrp`

**现象**：分区/重启相关的 5C 测试（`TestPartitionRecovery*5C` 等）**间歇性**失败，客户端对一个明明 Put 过的 key `Get` 误报 `ErrNoKey`；该组该分片是 `Ready`，但 kvMap 里就是缺这个键。可靠网络/无重启的 5A 用例从不复现。

**错误代码**：
```go
kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)  // 内部已 go readApplyCh()

if persister.SnapshotSize() > 0 {
    kv.Restore(persister.ReadSnapshot())   // ❌ 在 apply 协程启动之后才恢复
}
```

**根因**：raft 重启后 `appliedIndex = commitIndex = snapshotIndex`，**不会**用 applyCh 重发快照，而是约定**服务层自己**从快照恢复，raft 只通过 applyCh 投递 `snapshotIndex+1..` 的已提交日志增量。而 `MakeRSM` 内部 `go readApplyCh()` **立刻**启动了 apply 协程。若 leader 较积极地推进 commit，apply 协程会在 `kv.Restore()` 之前就把已提交日志喂给 `DoOp`，而此时 `kvMap` 是空的、`shardState` 是默认值（非 Gid1 全 `NotOwned`）：

1. 日志里的 `Put(k2∈s)` 先被 `DoPut` 处理 → 此刻 `shardState[s]==NotOwned` → 直接 `ErrWrongGroup`，**不写 kvMap**，这条已提交的 Put 被丢弃；
2. 紧接着 `kv.Restore(快照)` 把 `shardState[s]` 恢复成 `Ready`、`kvMap` 恢复成快照内容（**没有 k2**）；
3. raft 的 `appliedIndex` 已越过这条日志，不会再投递一次；
4. 客户端 `Get(k2)` → `shardState[s]==Ready` → 查 kvMap 缺失 → **误报 ErrNoKey**。

（Install 类日志若先于 Restore 被应用，会被快照覆盖丢数据或把分片卡在 `NotOwned`，同源问题。此外 `Restore` 与 `DoOp` 并发读写 `kvMap` 本身也是 data race。）

之所以是「间歇」：`StartServers` 后 follower 要先重新入群、收到 AppendEntries 推进 commit 才会触发 apply，通常 `Restore`（µs 级）抢先赢了；但不保证，调度/负载一变就翻车。

**正确做法**：把 Restore 提到 `MakeRSM` **之前**，保证 apply 协程拿到的永远是已恢复好的状态，日志增量叠加在快照之上：
```go
if persister.SnapshotSize() > 0 {
    kv.Restore(persister.ReadSnapshot())   // ✅ 先恢复
}
kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)  // 再启动 raft / apply
```

**核心不变式**: 服务层从快照恢复状态，必须在 RSM 的 apply 循环处理**任何** `snapshotIndex+1` 之后的日志**之前**完成——否则那些已提交日志会落在空/默认状态上被错误处理并永久丢失。「先 Restore 再 MakeRSM」是与 raft「appliedIndex 从 snapshotIndex 起、只投递增量」语义对齐的唯一安全顺序。

---

### BUG-L5-010 · `next` 的 CAS 在非 OK 时要「重读 next 确认归属」——并发同 num 不同配置下会误报 ErrNoKey　`【High】`

**文件**: `src/shardkv1/shardctrler/shardctrler.go` — `ChangeConfigTo`

`ChangeConfigTo` 第一步 `Put("next", new, Num-1)` 是 CAS 发布迁移意图。kvsrv clerk 在重发撞 `ErrVersion` 时会返回 `ErrMaybe`（写其实已成功、回复丢了）。两种极端处理都错：
- 一律 `err != OK { return }`：不可靠网络下把「自己已写成功的那轮」也放弃 → join/leave 不生效（`TestOneConcurrentClerkUnreliable5A` 报 `leaveGroups failed`、`TestJoinLeave5B` 失败）。
- 一律「ErrMaybe 就继续」：并发下多个 controller 对**同一 num 提议不同配置**时（`concurCtrler` 每个 worker join 不同 ngid），输家拿到 ErrMaybe 也继续，去迁移自己那份**没人认的配置** → 与真正赢家配置冲突 → 数据错乱、`Get` 误报 **ErrNoKey**（`TestAcquireLockConcurrentUnreliable5C` 实测 ~1/3 复现）。

**正解**：非 OK 时**重读 `next`**，只有它确实等于自己的配置才继续，否则放弃：
```go
if err := sck.IKVClerk.Put("next", new.String(), rpc.Tversion(new.Num-1)); err != rpc.OK {
    if got, _ := sck.readCfg("next"); got == nil || got.String() != new.String() {
        return // 没抢到 next（别的配置占了这一轮 / 我没写成功）→ 放弃
    }
    // next 确实是 new：我的写已生效（ErrMaybe 实为成功）→ 继续
}
```

**核心不变式**: `ErrMaybe` 是「可能已生效」的暧昧态——既不能当确定失败回退、也不能当确定成功盲目前进，要靠**重读权威状态**（这里是 `next`）定夺归属。

---

## 踩坑 / 决策记录（非最终代码，避免再走弯路）

### NOTE-L5-A · lease / 迁移加重试上限 / clerk 缓存 都试过、都回退了

调 `TestPartitionRecovery*5C` 在 `-race` 下偶发 DATA RACE 时绕了很大弯，结论先记下：

- **不要给迁移三方法加 `maxRetries` 上限 + 让 migrate 中止**：曾以为 concurCtrler 会卡死在「已 leave 下线的组」上，其实那次「死锁」是**同时跑 5 个 `-race` 进程压垮 CPU** 的假象，单进程无界 `for{}` 本就能过。加上限后不可靠网络的合法 join/leave 被误杀。
- **不要加 lease/epoch 让老控制器「被取代后退出」**：ShardCtrler 是**库**，生命周期归调用者；库不该自决谁失败。lease 只把 partition race 从 ~40% 降到 ~25%，没真解决，反而违背库语义。
- **DATA RACE 的真凶在 tester 框架内部、无锁**：`group.go` 的 `sg.srvs[]`(StartServer vs disconnect)、`sockrpc/rpcsrv.go` 的 `rpcs.l`(listen vs Close)。触发方是 `partitionCtrler` 后台 goroutine 的 join/leave churn 撞测试结尾 `Cleanup`。**严重度只随配置变更速度变**。
- **timing 是三方耦合**：5B 的 2s 死线要迁移**快**(caching / sleep 20~50ms)；partition cleanup race 要 churn **慢**；unreliable 的 raft 重启重连要 RPC 负载**低**。20/50ms 或 caching 会触发 cleanup race 或 unreliable 选举风暴(term→1000+，根因是 server 重启后 daemon socket 重连 + 丢包，纯框架/时序)。

**最终选择**：迁移退避保持 **100ms**（HEAD 的负载水平，partition/unreliable/concurrent 都稳），接受 `TestJoinLeave5B` 那条 2s 死线偶发 flaky（对任何实现都紧，属这类测试固有抖动）。只保留两个真正的正确性修复：[[BUG-L5-009]] 的 Restore 顺序、本条 BUG-L5-010 的 ErrMaybe 重读。实测 `make RUN='-run 5' shardkv` 一轮 23/23 全过。

---

## 测试通过记录（2026-06-27，`-race`，Apple M4）

各 lab 用 `make`（自带 `-v -race`，会重建 daemon 二进制）完整跑一轮：

| Lab | 命令 | 结果 |
|---|---|---|
| Lab 3（Raft） | `make raft1` | ✅ `ok 6.5840/raft1 415.851s` |
| Lab 4（Fault-tolerant KV） | `make kvraft1` | ✅ `ok 6.5840/kvraft1 249.143s` |
| Lab 5（Sharded KV） | `make RUN='-run 5' shardkv` | ✅ `ok 6.5840/shardkv1 587.177s`（23/23） |

Lab 5 全部 23 个用例（5A×14 + 5B×2 + 5C×7，含 4 个 partition recovery）单轮全过。

> 注：`TestJoinLeave5B`（2s 死线）与 `TestPartitionRecovery*5C`（`-race` 清理竞争 / 不可靠选举风暴）偶发 flaky，根因在 tester 框架的并发缺陷而非解题逻辑，详见上文 BUG-L5-010 的「踩坑 / 决策记录」。
