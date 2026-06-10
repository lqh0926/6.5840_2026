# 6.5840 - 2026 春季

# 6.5840 Lab 4: 容错键值服务

**协作策略** // **提交实验** // **配置 Go** // **指导** // **Piazza**

## 引言

在本实验中，你将使用 Lab 3 中构建的 Raft 库来构建一个容错的键值存储服务。对于客户端来说，该服务看起来与 Lab 2 中的服务器类似。但不同的是，该服务由一组服务器组成，它们使用 Raft 来维护相同的数据库。只要大多数服务器存活且可以通信，你的键值服务就应该能够继续处理客户端请求，无论出现其他故障还是网络分区。完成 Lab 4 后，你将实现 Raft 交互图中展示的所有部分（Clerk、Service 和 Raft）。

客户端将通过 Clerk 与你的键值服务交互，和 Lab 2 一样。Clerk 实现了 `Put` 和 `Get` 方法，语义与 Lab 2 相同：Put 是至多一次的（at-most-once），并且 Put/Get 必须形成线性一致性（linearizable）的历史记录。

对于单个服务器来说，提供线性一致性相对容易。但如果服务是复制的，就更困难了——所有服务器必须为并发请求选择相同的执行顺序，必须避免使用过时的状态回复客户端，并且必须在发生故障后恢复状态，同时保留所有已确认的客户端更新。

本实验包含三个部分。在 **Part A** 中，你将使用你的 Raft 实现来构建一个复制状态机（RSM）包；RSM 不关心它所复制的请求类型。在 **Part B** 中，你将使用 RSM 实现一个复制的键值服务，但不使用快照。在 **Part C** 中，你将使用 Lab 3D 的快照实现，使 Raft 能够丢弃旧的日志条目。请分别在各自的截止日期前提交每个部分。

你应该复习扩展 Raft 论文，特别是第 7 节（但不包括第 8 节）。如需更广泛的视角，可以查看 Chubby、Paxos Made Live、Spanner、Zookeeper、Harp、Viewstamped Replication 和 Bolosky 等人的工作。

**早点开始。**

## 入门指南

我们在 `src/kvraft1` 中提供了骨架代码和测试。骨架代码使用骨架包 `src/kvraft1/rsm` 来复制服务器。服务器必须实现 RSM 中定义的 `StateMachine` 接口，以便使用 RSM 进行复制。你的大部分工作将是实现 RSM 以提供与服务器无关的复制。你还需要修改 `kvraft1/client.go` 和 `kvraft1/server.go` 来实现服务器特定的部分。这种分离使你可以复用 RSM 于下一个实验。你可以复用 Lab 2 中的一些代码（例如，通过复制或导入 `src/kvsrv1` 包来复用服务器代码），但这不是必须的。

要开始运行，请执行以下命令。别忘了 `git pull` 获取最新软件。

```bash
$ cd ~/6.5840
$ git pull
..
```

## Part A: 复制状态机（RSM）（中等/较难）

```bash
$ cd src
$ make rsm1
=== RUN   TestBasic4A
Test RSM basic (reliable network)...
    rsm_test.go:28: expected 0 instead of 0
```

在客户端/服务器服务使用 Raft 进行复制的常见场景中，服务以两种方式与 Raft 交互：服务 leader 通过调用 `raft.Start()` 提交客户端操作，而所有服务副本通过 Raft 的 `applyCh` 接收已提交的操作并执行它们。在 leader 上，这两种活动是相互作用的。在任何给定时刻，一些服务器 goroutine 正在处理客户端请求，它们调用了 `raft.Start()`，并且每个都在等待其操作被提交，以获取执行该操作的结果。同时，当已提交的操作出现在 `applyCh` 上时，每个操作需要被服务执行，其结果需要交还给那个调用了 `raft.Start()` 的 goroutine，以便它可以将结果返回给客户端。

`rsm` 包封装了上述交互。它作为服务（例如键值数据库）和 Raft 之间的一个中间层。在 `rsm/rsm.go` 中，你将需要实现一个 "reader" goroutine 来读取 `applyCh`，以及一个 `rsm.Submit()` 函数，该函数为一个客户端操作调用 `raft.Start()`，然后等待 reader goroutine 将执行该操作的结果交还给它。

使用 RSM 的服务对 RSM reader goroutine 来说表现为一个 `StateMachine` 对象，该对象提供 `DoOp()` 方法。reader goroutine 应该将每个已提交的操作交给 `DoOp()`；`DoOp()` 的返回值应该交给对应的 `rsm.Submit()` 调用，以供其返回。`DoOp()` 的参数和返回值类型为 `any`；实际值的类型应分别与服务传递给 `rsm.Submit()` 的参数和返回值类型相同。

服务应将每个客户端操作传递给 `rsm.Submit()`。为了帮助 reader goroutine 将 `applyCh` 的消息与等待中的 `rsm.Submit()` 调用匹配起来，`Submit()` 应将每个客户端操作包装在一个 `Op` 结构中，并附带一个唯一标识符。然后 `Submit()` 应等待操作被提交并执行完毕，并返回执行结果（即 `DoOp()` 返回的值）。如果 `raft.Start()` 指示当前节点不是 Raft leader，`Submit()` 应返回 `rpc.ErrWrongLeader` 错误。`Submit()` 应检测并处理以下情况：在调用 `raft.Start()` 之后立即发生了领导权变更，导致操作丢失（永远未被提交）。

对于 Part A，RSM 测试器充当服务，提交其解释为对由一个整数组成的状态进行增量操作的操作。在 Part B 中，你将把 RSM 作为键值服务的一部分来使用，该服务实现 `StateMachine`（和 `DoOp()`），并调用 `rsm.Submit()`。

如果一切顺利，客户端请求的事件序列如下：

1. 客户端向服务 leader 发送请求。
2. 服务 leader 调用 `rsm.Submit()` 并传入该请求。
3. `rsm.Submit()` 调用 `raft.Start()` 并传入请求，然后等待。
4. Raft 提交该请求并将其发送到所有节点的 `applyCh` 上。
5. 每个节点上的 RSM reader goroutine 从 `applyCh` 读取请求，并将其传递给服务的 `DoOp()`。
6. 在 leader 上，RSM reader goroutine 将 `DoOp()` 的返回值交给最初提交该请求的 `Submit()` goroutine，然后 `Submit()` 返回该值。

实现 `rsm.go`：`Submit()` 方法和一个 reader goroutine。如果你通过了 RSM 4A 测试，则完成此任务：

```bash
$ cd src
$ make RUN="-run 4A" rsm1
go build -race -o main/rsm1d main/rsm1d.go
cd kvraft1/rsm; go test -v -race -run 4A
=== RUN   TestBasic4A
Test RSM basic (reliable network)...
  ... Passed --  time  4.2s #peers 3 #RPCs    50 #Ops   10
--- PASS: TestBasic4A (4.57s)
=== RUN   TestConcurrent4A
Test concurrent submit (reliable network)...
  ... Passed --  time  1.0s #peers 3 #RPCs    28 #Ops   50
--- PASS: TestConcurrent4A (1.39s)
=== RUN   TestLeaderFailure4A
Test Leader Failure (reliable network)...
  ... Passed --  time  2.9s #peers 3 #RPCs    32 #Ops    2
--- PASS: TestLeaderFailure4A (3.29s)
=== RUN   TestLeaderPartition4A
Test Leader Partition (reliable network)...
2026/03/11 10:43:46 partition leader 0
  ... Passed --  time  3.6s #peers 3 #RPCs    61 #Ops    2
--- PASS: TestLeaderPartition4A (4.04s)
=== RUN   TestRestartReplay4A
Test Restart (reliable network)...
  ... Passed --  time 28.4s #peers 3 #RPCs   467 #Ops  101
--- PASS: TestRestartReplay4A (28.79s)
=== RUN   TestShutdown4A
Test Shutdown (reliable network)...
  ... Passed --  time 10.0s #peers 3 #RPCs     0 #Ops    0
--- PASS: TestShutdown4A (10.38s)
=== RUN   TestRestartSubmit4A
Test Restart and submit (reliable network)...
  ... Passed --  time 39.8s #peers 3 #RPCs   463 #Ops  102
--- PASS: TestRestartSubmit4A (40.21s)
PASS
ok      6.5840/kvraft1/rsm     93.691s
```

你不应该需要向 Raft 的 `ApplyMsg` 或 Raft RPC（如 `AppendEntries`）添加任何字段，但是你允许这么做。

你的解决方案需要处理以下情况：RSM leader 已经通过 `Submit()` 为某个请求调用了 `Start()`，但在该请求被提交到日志之前失去了领导权。一种做法是让 RSM 检测到它已失去领导权——通过注意到 Raft 的 term 已改变，或者在 `Start()` 返回的索引处出现了不同的请求——然后从 `Submit()` 返回 `rpc.ErrWrongLeader`。如果前 leader 被单独分区，它将无法得知新的 leader；但在同一分区中的任何客户端也无法与新的 leader 通信，因此在这种情况下，服务器无限期等待直到分区恢复是可以接受的。

## Part B: 无快照的键值服务（中等）

```bash
$ cd src
$ make RUN="-run 4B" kvraft1
go build -race -o main/kvraft1d main/kvraft1d.go
cd kvraft1 && go test -v -race -run 4B
=== RUN   TestBasic4B
Test: one client (4B basic) (reliable network)...
Fatal: Wrong error
```

现在你将使用 `rsm` 包来复制一个键值服务器。每个服务器（"kvservers"）将有一个关联的 RSM/Raft 节点。Clerk 将 `Put()` 和 `Get()` RPC 发送到其关联的 Raft 为 leader 的 kvserver。kvserver 代码将 Put/Get 操作提交给 RSM，RSM 使用 Raft 复制它，并在每个节点上调用你服务器的 `DoOp`，该方法应将操作应用到该节点的键值数据库上；其目的是让服务器维护键值数据库的相同副本。

Clerk 有时不知道哪个 kvserver 是 Raft leader。如果 Clerk 将 RPC 发送到错误的 kvserver，或者无法连接到该 kvserver，Clerk 应通过发送到不同的 kvserver 来重试。如果键值服务将操作提交到其 Raft 日志（并因此将操作应用到键值状态机），leader 通过响应其 RPC 向 Clerk 报告结果。如果操作未能提交（例如，leader 被替换），服务器报告一个错误，Clerk 则用另一个服务器重试。

你的第一个任务是实现一个在网络无丢包、无服务器故障时能正常工作的解决方案。

你可以随意将 Lab 2 的客户端代码（`kvsrv1/client.go`）复制到 `kvraft1/client.go` 中。你需要添加决定将每个 RPC 发送到哪个 kvserver 的逻辑。

你还需要在 `server.go` 中实现 `Put()` 和 `Get()` RPC 处理函数。这些处理函数应使用 `rsm.Submit()` 将请求提交给 Raft。当 `rsm` 包从 `applyCh` 读取命令时，它应调用 `DoOp` 方法，你需要在 `server.go` 中实现该方法。

当你可靠地通过测试套件中的第一个测试时，即完成此任务：`make RUN="-run TestBasic4B" kvraft1`。

kvserver 如果不属于多数派（majority），则不应完成 `Get()` RPC（这样它就不会提供过时的数据）。一个简单的解决方案是使用 `Submit()` 将每个 `Get()`（以及每个 `Put()`）都写入 Raft 日志。你不必实现第 8 节中描述的只读操作优化。

最好从一开始就添加锁，因为避免死锁的需求有时会影响整体代码设计。测试器默认使用竞态检测器运行你的代码。

现在你应该修改你的解决方案，使其在网络和服务器故障的情况下继续工作。你将面临的一个问题是：Clerk 可能需要多次发送同一个 RPC，直到找到一个肯定回复的 kvserver。如果 leader 在将条目提交到 Raft 日志后立即故障，Clerk 可能收不到回复，因此可能会将请求重新发送给另一个 leader。每个 `Clerk.Put()` 调用对于特定的版本号应该只产生一次执行。

添加代码来处理故障。你的 Clerk 可以使用与 Lab 2 类似的重复计划，包括在重试的 Put RPC 响应丢失时返回 `ErrMaybe`。当你的代码可靠地通过所有 4B 测试（`make RUN="-run 4B" kvraft1`）时，即完成。

请记住，RSM leader 可能失去领导权并从 `Submit()` 返回 `rpc.ErrWrongLeader`。在这种情况下，你应该安排 Clerk 将请求重新发送给其他服务器，直到找到新的 leader。

你可能需要修改你的 Clerk，使其记住哪个服务器在上一次 RPC 中是 leader，并将下一个 RPC 首先发送到该服务器。这将避免在每次 RPC 时浪费时间搜索 leader，这可能帮助你足够快地通过某些测试。

你的代码现在应该通过 Lab 4B 测试，如下所示：

```bash
$ cd src
$ make RUN="-run 4B" kvraft1
go build -race -o main/kvraft1d main/kvraft1d.go
cd kvraft1 && go test -v -race -run 4B
=== RUN   TestBasic4B
Test: one client (4B basic) (reliable network)...
  ... Passed --  time  3.5s #peers 5 #RPCs   395 #Ops  122
--- PASS: TestBasic4B (4.11s)
=== RUN   TestSpeed4B
Test: one client (4B speed) (reliable network)...
  ... Passed --  time 33.4s #peers 3 #RPCs  3291 #Ops 1002
--- PASS: TestSpeed4B (33.80s)
=== RUN   TestConcurrent4B
Test: many clients (4B many clients) (reliable network)...
  ... Passed --  time  4.1s #peers 5 #RPCs   953 #Ops  558
--- PASS: TestConcurrent4B (4.69s)
=== RUN   TestUnreliable4B
Test: many clients (4B many clients) (unreliable network)...
  ... Passed --  time  4.6s #peers 5 #RPCs   685 #Ops  210
--- PASS: TestUnreliable4B (5.22s)
=== RUN   TestOnePartition4B
Test: one client (4B progress in majority) (unreliable network)...
  ... Passed --  time  4.9s #peers 5 #RPCs   231 #Ops    4
Test: no progress in minority (4B) (unreliable network)...
  ... Passed --  time  1.8s #peers 5 #RPCs   110 #Ops    7
Test: completion after heal (4B) (unreliable network)...
  ... Passed --  time  1.1s #peers 5 #RPCs    43 #Ops    4
--- PASS: TestOnePartition4B (8.36s)
=== RUN   TestManyPartitionsOneClient4B
Test: partitions, one client (4B partitions, one client) (reliable network)...
  ... Passed --  time  9.4s #peers 5 #RPCs   520 #Ops  114
--- PASS: TestManyPartitionsOneClient4B (10.08s)
=== RUN   TestManyPartitionsManyClients4B
Test: partitions, many clients (4B partitions, many clients (4B)) (reliable network)...
  ... Passed --  time 16.1s #peers 5 #RPCs  1271 #Ops  558
--- PASS: TestManyPartitionsManyClients4B (16.68s)
=== RUN   TestPersistOneClient4B
Test: restarts, one client (4B restarts, one client 4B ) (reliable network)...
  ... Passed --  time  8.4s #peers 5 #RPCs   311 #Ops   62
--- PASS: TestPersistOneClient4B (9.01s)
=== RUN   TestPersistConcurrent4B
Test: restarts, many clients (4B restarts, many clients) (reliable network)...
  ... Passed --  time  8.5s #peers 5 #RPCs   994 #Ops  350
--- PASS: TestPersistConcurrent4B (9.11s)
=== RUN   TestPersistConcurrentUnreliable4B
Test: restarts, many clients (4B restarts, many clients ) (unreliable network)...
  ... Passed --  time 10.3s #peers 5 #RPCs   672 #Ops  114
--- PASS: TestPersistConcurrentUnreliable4B (10.89s)
=== RUN   TestPersistPartition4B
Test: restarts, partitions, many clients (4B restarts, partitions, many clients) (reliable network)...
  ... Passed --  time 14.3s #peers 5 #RPCs   804 #Ops   94
--- PASS: TestPersistPartition4B (14.95s)
=== RUN   TestPersistPartitionUnreliable4B
Test: restarts, partitions, many clients (4B restarts, partitions, many clients) (unreliable network)...
  ... Passed --  time 22.0s #peers 5 #RPCs  1229 #Ops  102
--- PASS: TestPersistPartitionUnreliable4B (22.64s)
=== RUN   TestPersistPartitionUnreliableLinearizable4B
Test: restarts, partitions, random keys, many clients (4B restarts, partitions, random keys, many clients) (unreliable network)...
  ... Passed --  time 24.1s #peers 7 #RPCs  4464 #Ops  444
--- PASS: TestPersistPartitionUnreliableLinearizable4B (24.94s)
PASS
ok      6.5840/kvraft1    175.518s
```

每个 "Passed" 后面的数字分别是：实际时间（秒）、节点数、发送的 RPC 数量（包括客户端 RPC），以及执行的键值操作数（Clerk Get/Put 调用）。

## Part C: 带快照的键值服务（中等）

目前，你的键值服务器没有调用 Raft 库的 `Snapshot()` 方法，因此重启的服务器必须重放完整的持久化 Raft 日志才能恢复其状态。现在你将修改 kvserver 和 RSM，使其与 Raft 协作以节省日志空间并减少重启时间，使用 Lab 3D 中的 Raft `Snapshot()`。

测试器将 `maxraftstate` 传递给 `StartKVServer()`，后者将其传递给 RSM。`maxraftstate` 表示持久化 Raft 状态的最大允许大小（以字节为单位，包括日志但不包括快照）。你应该将 `maxraftstate` 与 `rf.PersistBytes()` 进行比较。每当你的 RSM 检测到 Raft 状态大小接近此阈值时，它应该通过调用 Raft 的 `Snapshot` 来保存快照。RSM 可以通过调用 `StateMachine` 接口的 `Snapshot` 方法来获得 kvserver 的快照以创建此快照。如果 `maxraftstate` 为 -1，则不需要快照。`maxraftstate` 限制适用于 Raft 作为第一个参数传递给 `persister.Save()` 的 GOB 编码字节。

你可以在 `tester1/persister.go` 中找到 persister 对象的源代码。

修改你的 RSM，使其检测持久化的 Raft 状态何时变得过大，然后将快照交给 Raft。当 RSM 服务器重启时，它应使用 `persister.ReadSnapshot()` 读取快照，如果快照的长度大于零，则将快照传递给 `StateMachine` 的 `Restore()` 方法。如果你通过 RSM 中的 `TestSnapshot4C`，则完成此任务。

```bash
$ cd src
$ make RUN="-run 4C" kvraft1
go build -race -o main/kvraft1d main/kvraft1d.go
cd kvraft1 && go test -v -race -run 4C
=== RUN   TestSnapshotRPC4C
Test: snapshots, one client (4C SnapshotsRPC) (reliable network)...
Test: InstallSnapshot RPC (4C) (reliable network)...
signal: killed
FAIL    6.5840/kvraft1    61.186s
```

思考 RSM 应该在什么时候对其状态进行快照，以及快照中除了服务器状态之外还应包含什么。Raft 使用 `Save()` 将每个快照以及相应的 Raft 状态存储在 persister 对象中。你可以使用 `ReadSnapshot()` 读取最新存储的快照。

快照中存储的结构体的所有字段必须大写开头（导出）。

实现 `kvraft1/server.go` 的 `Snapshot()` 和 `Restore()` 方法，由 RSM 调用。修改 RSM 以处理包含快照的 `applyCh` 消息。

你可能会在此任务中发现 Raft 和 RSM 库中的 bug。如果你对 Raft 实现做了修改，请确保它继续通过所有 Lab 3 测试。

Lab 4 测试的合理耗时约为 400 秒实际时间和 700 秒 CPU 时间。

你的代码应通过 4C 测试（如下方示例）以及 4A+B 测试（且你的 Raft 必须继续通过 Lab 3 测试）。

```bash
$ make RUN="-run 4C" kvraft1
go build -race -o main/kvraft1d main/kvraft1d.go
cd kvraft1 && go test -v -race -run 4C
=== RUN   TestSnapshotRPC4C
Test: snapshots, one client (4C SnapshotsRPC) (reliable network)...
Test: InstallSnapshot RPC (4C) (reliable network)...
  ... Passed --  time  4.8s #peers 3 #RPCs   248 #Ops   72
--- PASS: TestSnapshotRPC4C (5.18s)
=== RUN   TestSnapshotSize4C
Test: snapshots, one client (4C snapshot size is reasonable) (reliable network)...
  ... Passed --  time 21.0s #peers 3 #RPCs  2569 #Ops 1200
--- PASS: TestSnapshotSize4C (21.42s)
=== RUN   TestSpeed4C
Test: snapshots, one client (4C speed) (reliable network)...
  ... Passed --  time 24.9s #peers 3 #RPCs  3208 #Ops 1002
--- PASS: TestSpeed4C (25.32s)
=== RUN   TestSnapshotRecover4C
Test: restarts, snapshots, one client (4C restarts, snapshots, one client) (reliable network)...
  ... Passed --  time  8.2s #peers 5 #RPCs   273 #Ops   50
--- PASS: TestSnapshotRecover4C (8.78s)
=== RUN   TestSnapshotRecoverManyClients4C
Test: restarts, snapshots, many clients (4C restarts, snapshots, many clients ) (reliable network)...
info: linearizability check timed out, assuming history is ok
info: linearizability check timed out, assuming history is ok
info: linearizability check timed out, assuming history is ok
  ... Passed --  time 12.5s #peers 5 #RPCs  3525 #Ops 1670
--- PASS: TestSnapshotRecoverManyClients4C (13.15s)
=== RUN   TestSnapshotUnreliable4C
Test: snapshots, many clients (4C unreliable net, snapshots, many clients) (unreliable network)...
  ... Passed --  time  5.5s #peers 5 #RPCs   773 #Ops  230
--- PASS: TestSnapshotUnreliable4C (6.16s)
=== RUN   TestSnapshotUnreliableRecover4C
Test: restarts, snapshots, many clients (4C unreliable net, restarts, snapshots, many clients) (unreliable network)...
  ... Passed --  time 10.7s #peers 5 #RPCs   804 #Ops   78
--- PASS: TestSnapshotUnreliableRecover4C (11.28s)
=== RUN   TestSnapshotUnreliableRecoverConcurrentPartition4C
Test: restarts, partitions, snapshots, many clients (4C unreliable net, restarts, partitions, snapshots, many clients) (unreliable network)...
  ... Passed --  time 17.4s #peers 5 #RPCs   894 #Ops   94
--- PASS: TestSnapshotUnreliableRecoverConcurrentPartition4C (17.97s)
=== RUN   TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable4C
Test: restarts, partitions, snapshots, random keys, many clients (4C unreliable net, restarts, partitions, snapshots, random keys, many clients) (unreliable network)...
  ... Passed --  time 19.6s #peers 7 #RPCs  2957 #Ops  368
--- PASS: TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable4C (20.45s)
PASS
ok      6.5840/kvraft1    130.724s
```