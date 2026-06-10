6.5840 - Spring 2026
6.5840 Lab 3: Raft
协作政策 // 提交实验 // 配置 Go // 指南 // Piazza

# 介绍

这是一系列实验中的第一个，你将在其中构建一个容错的键/值存储系统。在本实验中，你将实现 Raft，一种复制状态机协议。在下一个实验中，你将在 Raft 之上构建一个键/值服务。然后，你将在多个复制状态机上"分片"你的服务以获得更高的性能。

复制服务通过在多个副本服务器上存储其状态（即数据）的完整副本来实现容错。复制允许服务在某些服务器发生故障（崩溃、网络中断或不稳定）时继续运行。挑战在于，故障可能导致副本之间持有不同的数据副本。

Raft 将客户端请求组织成一个序列，称为日志，并确保所有副本服务器看到相同的日志。每个副本按日志顺序执行客户端请求，将其应用到服务状态的本地副本。由于所有活跃的副本看到相同的日志内容，它们以相同的顺序执行相同的请求，从而继续保持相同的服务状态。如果某个服务器发生故障但后来恢复，Raft 会负责将其日志更新到最新状态。只要至少大多数服务器存活且能相互通信，Raft 就会继续运行。如果没有这样的多数派，Raft 将不会取得进展，但一旦多数派能够再次通信，它将从上次中断的地方继续。

在本实验中，你将把 Raft 实现为一个带有相关方法的 Go 对象类型，旨在作为更大服务中的模块使用。一组 Raft 实例通过 RPC 相互通信以维护复制日志。你的 Raft 接口将支持无限序列的编号命令，也称为日志条目。条目使用索引号进行编号。给定索引的日志条目最终将被提交。届时，你的 Raft 应该将日志条目发送给更大的服务以供其执行。

你应该遵循扩展 Raft 论文中的设计，特别注意 Figure 2。你将实现论文中的大部分内容，包括保存持久化状态以及在节点故障后重启时读取它。你不需要实现集群成员变更（第 6 节）。

本实验分四个部分提交。你必须在每个对应的截止日期前提交每个部分。

# 入门指南

如果你已经完成了实验 1，你已经拥有实验源代码的副本。如果没有，你可以在实验 1 的说明中找到通过 git 获取源代码的方法。

我们为你提供了骨架代码 `src/raft1/raft.go`。我们还提供了一组测试，你应该使用这些测试来驱动你的实现工作，我们将使用这些测试来评分你提交的实验。测试位于 `src/raft1/raft_test.go`。

我们评分你的提交时，将在没有 `-race` 标志的情况下运行测试。但是，你应该使用 `-race` 进行测试。

要启动并运行，请执行以下命令。不要忘记 `git pull` 以获取最新软件。

```
$ cd ~/6.5840
$ git pull
...
$ cd src
$ make raft1
go build -race -o main/raft1d main/raft1d.go
cd raft1 && go test -v -race
=== RUN   TestInitialElection3A
Test (3A): initial election (reliable network)...
Fatal: expected one leader, got none
        /Users/rtm/824-process-raft/src/raft1/test.go:151
        /Users/rtm/824-process-raft/src/raft1/raft_test.go:36
info: wrote visualization to /var/folders/x_/vk0xmxwn1sj91m89wsn5b1yh0000gr/T/porcupine-2242138501.html
--- FAIL: TestInitialElection3A (5.51s)
...
$
```

# 代码

通过向 `raft1/raft.go` 添加代码来实现 Raft。在该文件中，你将找到骨架代码，以及如何发送和接收 RPC 的示例。

你的实现必须支持以下接口，测试器以及（最终）你的键/值服务器将使用这些接口。你可以在 `raft.go` 和 `raftapi/raftapi.go` 的注释中找到更多细节。

```go
// 创建一个新的 Raft 服务器实例：
rf := Make(peers, me, persister, applyCh)

// 开始对一个新日志条目达成共识：
rf.Start(command interface{}) (index, term, isleader)

// 查询 Raft 的当前任期，以及它是否认为自己是领导者
rf.GetState() (term, isLeader)

// 每次有新的条目提交到日志时，每个 Raft 节点
// 都应该向服务（或测试器）发送一个 ApplyMsg。
type ApplyMsg
```

服务调用 `Make(peers, me, …)` 来创建一个 Raft 节点。`peers` 参数是 Raft 节点（包括此节点）的网络标识符数组，用于 RPC。`me` 参数是此节点在 `peers` 数组中的索引。`Start(command)` 要求 Raft 开始处理以将命令追加到复制日志中。`Start()` 应立即返回，无需等待日志追加完成。服务期望你的实现在每次有新的提交日志条目时，通过 `Make()` 的 `applyCh` channel 参数发送一个 `ApplyMsg`。

`raft.go` 包含发送 RPC（`sendRequestVote()`）和处理传入 RPC（`RequestVote()`）的示例代码。你的 Raft 节点应该使用 `labrpc` Go 包（源代码在 `src/labrpc`）交换 RPC。测试器可以指示 `labrpc` 延迟 RPC、重新排序它们以及丢弃它们以模拟各种网络故障。虽然你可以临时修改 `labrpc`，但请确保你的 Raft 能在原始 `labrpc` 上正常工作，因为我们将用它来测试和评分你的实验。你的 Raft 实例必须仅通过 RPC 交互；例如，它们不允许使用共享的 Go 变量或文件进行通信。

后续实验基于本实验，因此给自己足够的时间编写坚实的代码非常重要。

# 第 3A 部分：领导者选举（中等难度）

实现 Raft 领导者选举和心跳（不包含日志条目的 AppendEntries RPC）。第 3A 部分的目标是：选出一个单一的领导者，在没有故障的情况下领导者保持其领导地位，如果旧领导者发生故障或发往/来自旧领导者的数据包丢失，则由新的领导者接替。在 `src` 目录中运行 `make RUN="-run 3A" raft1` 来测试你的 3A 代码。

遵循论文的 Figure 2。此时你需要关注发送和接收 RequestVote RPC、与选举相关的服务器规则以及与领导者选举相关的状态。

将 Figure 2 中领导者选举的状态添加到 `raft.go` 的 Raft 结构体中。
填写 `RequestVoteArgs` 和 `RequestVoteReply` 结构体。修改 `Make()` 以创建一个后台 goroutine，当一段时间没有收到其他节点的消息时，通过定期发送 RequestVote RPC 来启动领导者选举。实现 `RequestVote()` RPC 处理器，使得服务器能够相互投票。
要实现心跳，定义一个 `AppendEntries` RPC 结构体（尽管你可能暂时不需要所有参数），并让领导者定期发送它们。编写一个 `AppendEntries` RPC 处理器方法。
测试器要求领导者每秒发送心跳 RPC 不超过十次。
测试器要求你的 Raft 在旧领导者故障后的五秒内选举出新的领导者（如果大多数节点仍能通信）。
论文的第 5.2 节提到了 150 到 300 毫秒范围内的选举超时。这样的范围只有在领导者发送心跳的频率远高于每 150 毫秒一次（例如每 10 毫秒一次）时才有意义。由于测试器将你限制在每秒几十次心跳，你将必须使用比论文中 150 到 300 毫秒更大的选举超时，但又不能太大，否则你可能无法在五秒内选举出领导者。

你可能会觉得 Go 的 `rand` 很有用。
你需要编写定期或在延迟后执行操作的代码。最简单的方法是创建一个带有调用 `time.Sleep()` 循环的 goroutine；参见 `Make()` 为此目的创建的 `ticker()` goroutine。不要使用 Go 的 `time.Timer` 或 `time.Ticker`，它们很难正确使用。
如果你的代码难以通过测试，请再次阅读论文的 Figure 2；领导者选举的完整逻辑分布在图中的多个部分。
不要忘记实现 `GetState()`。
Go RPC 仅发送名称以大写字母开头的结构体字段。子结构体也必须有大写字段名（例如数组中日志记录的字段）。`labgob` 包会对此发出警告；不要忽略这些警告。

本实验最具挑战性的部分可能是调试。请参考指南页面获取调试技巧。

如果你未能通过某个测试，测试器会生成一个文件，可视化时间线，上面标记了事件，包括网络分区、崩溃的服务器以及执行的检查。这是一个可视化示例。此外，你可以通过编写（例如）`tester.Annotate("Server 0", "简短描述", "详细信息")` 来添加自己的注释。

在提交第 3A 部分之前，确保你通过了 3A 测试，这样你会看到类似以下内容：

```
$ make RUN="-run 3A" raft1
go build -race -o main/raft1d main/raft1d.go
cd raft1 && go test -v -race -run 3A
=== RUN   TestInitialElection3A
Test (3A): initial election (reliable network)...
  ... Passed --  time  3.5s #peers 3 #RPCs    32 #Ops    0
--- PASS: TestInitialElection3A (3.84s)
=== RUN   TestReElection3A
Test (3A): election after network failure (reliable network)...
  ... Passed --  time  6.2s #peers 3 #RPCs    68 #Ops    0
--- PASS: TestReElection3A (6.54s)
=== RUN   TestManyElections3A
Test (3A): multiple elections (reliable network)...
  ... Passed --  time  9.8s #peers 7 #RPCs   684 #Ops    0
--- PASS: TestManyElections3A (10.68s)
PASS
ok      6.5840/raft1    22.095s
$
```

每个 "Passed" 行包含五个数字；这些分别是测试花费的时间（秒）、Raft 节点数量、测试期间发送的 RPC 数量、RPC 消息的总字节数，以及 Raft 报告已提交的日志条目数。你的数字将与此处显示的不同。如果你愿意，可以忽略这些数字，但它们可能帮助你检查实现发送的 RPC 数量的合理性。对于实验 3、4 和 5 的所有测试，评分脚本将在所有测试总时间超过 600 秒，或任何单个测试超过 120 秒时判定你的解决方案失败。

我们评分你的提交时，将在没有 `-race` 标志的情况下运行测试。但是，你应该确保你的代码在使用 `-race` 标志时也能一致地通过测试。

# 第 3B 部分：日志（困难）

实现领导者和跟随者追加新日志条目的代码，使得 `make RUN="-run 3B" raft1` 通过所有测试。

运行 `git pull` 获取最新的实验软件。

Raft 论文将日志视为从 1 开始索引，但我们建议你实现为从 0 开始索引，从索引为 0 的哑条目（dummy entry）开始，其任期为 0。这样第一个 AppendEntries RPC 就可以包含 0 作为 PrevLogIndex，且是日志中的有效索引。

你的首要目标应该是通过 `TestBasicAgree3B()`。首先实现 `Start()`，然后根据 Figure 2 编写通过 AppendEntries RPC 发送和接收新日志条目的代码。在每个节点上通过 `applyCh` 发送每个新提交的条目。

你需要实现选举限制（论文第 5.4.1 节）。

你的代码可能会有循环反复检查某些事件。不要让这些循环连续执行而不暂停，因为这会拖慢你的实现导致测试失败。使用 Go 的条件变量，或在每次循环迭代中插入 `time.Sleep(10 * time.Millisecond)`。

为了后续实验着想，请编写（或重写）清晰干净的代码。

如果你未能通过某个测试，请查看 `raft_test.go` 并从那里跟踪测试代码以理解测试的内容。

后续实验的测试可能会因为你的代码运行太慢而失败。你可以使用 `time` 命令检查解决方案的实际时间和 CPU 时间。以下是典型输出：

```
$ make RUN="-run 3B" raft1
go build -race -o main/raft1d main/raft1d.go
cd raft1 && go test -v -race -run 3B
=== RUN   TestBasicAgree3B
Test (3B): basic agreement (reliable network)...
  ... Passed --  time  1.6s #peers 3 #RPCs    18 #Ops    3
--- PASS: TestBasicAgree3B (1.96s)
=== RUN   TestRPCBytes3B
Test (3B): RPC byte count (reliable network)...
  ... Passed --  time  3.3s #peers 3 #RPCs    50 #Ops   11
--- PASS: TestRPCBytes3B (3.71s)
=== RUN   TestFollowerFailure3B
Test (3B): test progressive failure of followers (reliable network)...
  ... Passed --  time  5.4s #peers 3 #RPCs    58 #Ops    3
--- PASS: TestFollowerFailure3B (5.77s)
=== RUN   TestLeaderFailure3B
Test (3B): test failure of leaders (reliable network)...
  ... Passed --  time  6.5s #peers 3 #RPCs   110 #Ops    3
--- PASS: TestLeaderFailure3B (6.89s)
=== RUN   TestFailAgree3B
Test (3B): agreement after follower reconnects (reliable network)...
  ... Passed --  time  6.0s #peers 3 #RPCs    61 #Ops    7
--- PASS: TestFailAgree3B (6.37s)
=== RUN   TestFailNoAgree3B
Test (3B): no agreement if too many followers disconnect (reliable network)...
  ... Passed --  time  4.0s #peers 5 #RPCs   107 #Ops    2
--- PASS: TestFailNoAgree3B (4.55s)
=== RUN   TestConcurrentStarts3B
Test (3B): concurrent Start()s (reliable network)...
  ... Passed --  time  1.4s #peers 3 #RPCs    12 #Ops    0
--- PASS: TestConcurrentStarts3B (1.75s)
=== RUN   TestRejoin3B
Test (3B): rejoin of partitioned leader (reliable network)...
  ... Passed --  time  7.8s #peers 3 #RPCs   120 #Ops    4
--- PASS: TestRejoin3B (8.15s)
=== RUN   TestBackup3B
Test (3B): leader backs up quickly over incorrect follower logs (reliable network)...
  ... Passed --  time 27.7s #peers 5 #RPCs  1370 #Ops  102
--- PASS: TestBackup3B (28.27s)
=== RUN   TestCount3B
Test (3B): RPC counts aren't too high (reliable network)...
  ... Passed --  time  2.7s #peers 3 #RPCs    32 #Ops    0
--- PASS: TestCount3B (3.05s)
PASS
ok      6.5840/raft1    71.716s
$
```

`ok 6.5840/raft 71.716s` 表示 Go 测量出 3B 测试花费了 71.716 秒的实际（挂钟）时间。如果你的解决方案在 3B 测试上使用了远超几分钟的实际时间，你可能会在后续遇到麻烦。寻找在睡眠或等待 RPC 超时上花费的时间、不睡眠或不等待条件或 channel 消息就运行的循环，或者发送了大量 RPC。

# 第 3C 部分：持久化（困难）

如果基于 Raft 的服务器重启，它应该从上次中断的地方恢复服务。这要求 Raft 保持能够在重启后存活的持久化状态。论文的 Figure 2 提到了哪些状态应该是持久化的。

真实的实现会在每次 Raft 持久化状态改变时将其写入磁盘，并在重启后从磁盘读取状态。你的实现不会使用磁盘；相反，它将从 `Persister` 对象（参见 `tester1/persister.go`）保存和恢复持久化状态。调用 `Raft.Make()` 的人提供一个 `Persister`，它最初持有 Raft 最近持久化的状态（如果有的话）。Raft 应该从该 `Persister` 初始化其状态，并在每次状态改变时使用它保存持久化状态。使用 `Persister` 的 `ReadRaftState()` 和 `Save()` 方法。

通过在 `raft.go` 的 `persist()` 和 `readPersist()` 函数中添加代码来完成它们，以保存和恢复持久化状态。你需要将状态编码（或"序列化"）为字节数组，以便传递给 `Persister`。使用 `labgob` 编码器；参见 `persist()` 和 `readPersist()` 中的注释。`labgob` 类似于 Go 的 `gob` 编码器，但会在你尝试编码小写字段名的结构体时打印错误消息。目前，将 `nil` 作为第二个参数传递给 `persister.Save()`。在你的实现改变持久化状态的位置插入对 `persist()` 的调用。一旦你完成这些，并且如果你实现的其余部分是正确的，你应该通过所有 3C 测试。

你可能需要一次回退多个条目的 `nextIndex` 优化。参见扩展 Raft 论文从第 7 页底部和第 8 页顶部开始的部分（由灰线标记）。论文对这些细节描述模糊；你需要填补空白。一种可能是让拒绝消息包含：

```
    XTerm:  冲突条目的任期（如果有的话）
    XIndex: 该任期的第一个条目索引（如果有的话）
    XLen:   日志长度
```

然后领导者的逻辑可以类似于：

```
  Case 1: 领导者没有 XTerm：
    nextIndex = XIndex
  Case 2: 领导者有 XTerm：
    nextIndex = (领导者在 XTerm 的最后一个条目的索引) + 1
  Case 3: 跟随者的日志太短：
    nextIndex = XLen
```

一些其他提示：

- 运行 `git pull` 获取最新的实验软件。
- 3C 测试比 3A 或 3B 的测试要求更高，失败可能是由你 3A 或 3B 代码中的问题引起的。
- 你的代码应该通过所有 3C 测试（如下所示），以及 3A 和 3B 测试。

```
$ make RUN="-run 3C" raft1
go build -race -o main/raft1d main/raft1d.go
cd raft1 && go test -v -race -run 3C
=== RUN   TestPersist13C
Test (3C): basic persistence (reliable network)...
  ... Passed --  time  7.6s #peers 3 #RPCs    58 #Ops    6
--- PASS: TestPersist13C (7.99s)
=== RUN   TestPersist23C
Test (3C): more persistence (reliable network)...
  ... Passed --  time 21.6s #peers 5 #RPCs   287 #Ops   16
--- PASS: TestPersist23C (22.17s)
=== RUN   TestPersist33C
Test (3C): partitioned leader and one follower crash, leader restarts (reliable network)...
  ... Passed --  time  3.8s #peers 3 #RPCs    30 #Ops    4
--- PASS: TestPersist33C (4.11s)
=== RUN   TestFigure83C
Test (3C): Figure 8 (reliable network)...
  ... Passed --  time 48.5s #peers 5 #RPCs   499 #Ops    2
--- PASS: TestFigure83C (49.08s)
=== RUN   TestUnreliableAgree3C
Test (3C): unreliable agreement (unreliable network)...
  ... Passed --  time  5.1s #peers 5 #RPCs   288 #Ops  246
--- PASS: TestUnreliableAgree3C (5.68s)
=== RUN   TestFigure8Unreliable3C
Test (3C): Figure 8 (unreliable) (unreliable network)...
  ... Passed --  time 53.6s #peers 5 #RPCs  3200 #Ops    2
--- PASS: TestFigure8Unreliable3C (54.19s)
=== RUN   TestReliableChurn3C
Test (3C): churn (reliable network)...
  ... Passed --  time 18.2s #peers 5 #RPCs  1701 #Ops    1
--- PASS: TestReliableChurn3C (18.80s)
=== RUN   TestUnreliableChurn3C
Test (3C): unreliable churn (unreliable network)...
  ... Passed --  time 17.3s #peers 5 #RPCs  1253 #Ops    1
--- PASS: TestUnreliableChurn3C (17.92s)
PASS
ok      6.5840/raft1    180.983s
$
```

在提交之前多次运行测试是个好主意。

# 第 3D 部分：日志压缩（困难）

就目前而言，重启的服务器会重放完整的 Raft 日志以恢复其状态。然而，对于一个长期运行的服务来说，永久记住完整的 Raft 日志是不现实的。相反，你将修改 Raft 以配合那些不时持久化存储其状态"快照"的服务，此时 Raft 会丢弃快照之前的日志条目。结果是更少的持久化数据量和更快的重启。然而，现在跟随者可能落后得太多，以至于领导者已经丢弃了它追上所需的日志条目；领导者必须发送一个快照以及从快照时间点开始的日志。扩展 Raft 论文的第 7 节概述了该方案；你将需要设计细节。

你的 Raft 必须提供以下函数，服务可以用其状态的序列化快照来调用：

```go
Snapshot(index int, snapshot []byte)
```

在实验 3D 中，测试器会定期调用 `Snapshot()`。在实验 4 中，你将编写一个调用 `Snapshot()` 的键/值服务器；快照将包含完整的键/值对表。服务层在每个节点上（不仅仅是领导者）调用 `Snapshot()`。

`index` 参数表示反映在快照中的最高日志条目。Raft 应该丢弃该点之前的日志条目。你需要修改你的 Raft 代码，使其能在只存储日志尾部的情况下运行。

你需要实现论文中讨论的 `InstallSnapshot` RPC，它允许 Raft 领导者告诉落后的 Raft 节点用快照替换其状态。你可能需要仔细思考 `InstallSnapshot` 应该如何与 Figure 2 中的状态和规则交互。

当跟随者的 Raft 代码收到 `InstallSnapshot` RPC 时，它可以使用 `applyCh` 在 `ApplyMsg` 中将快照发送给服务。`raftapi/raftapi.go` 中的 `ApplyMsg` 结构体定义已经包含了你需要的字段（也是测试器期望的）。注意这些快照只能推进服务的状态，不要导致它后退。

如果服务器崩溃，它必须从持久化数据重新启动。你的 Raft 应该同时持久化 Raft 状态和相应的快照。使用 `persister.Save()` 的第二个参数来保存快照。如果没有快照，传递 `nil` 作为第二个参数。

当服务器重启时，应用层读取持久化的快照并恢复其保存的应用状态。重启后，应用层期望 `applyCh` 上的第一条消息要么包含一个 `SnapshotIndex` 高于初始恢复快照的快照，要么是一个 `CommandIndex` 紧接在初始恢复快照索引之后的普通命令。

实现 `Snapshot()` 和 `InstallSnapshot` RPC，以及 Raft 为支持这些所需的更改（例如，使用裁剪后的日志进行操作）。当你的解决方案通过 3D 测试（以及所有之前的实验 3 测试）时，即为完成。

- `git pull` 确保你拥有最新的软件。
- 一个好的起点是修改你的代码，使其能够只存储从某个索引 X 开始的日志部分。最初你可以将 X 设置为零并运行 3B/3C 测试。然后让 `Snapshot(index)` 丢弃 index 之前的日志，并将 X 设置为 index。如果一切顺利，你现在应该通过第一个 3D 测试。
- 未能通过第一个 3D 测试的常见原因是：跟随者花费太长时间才能追上领导者。
- 下一步：如果领导者没有使跟随者更新到最新所需的日志条目，则让领导者发送 `InstallSnapshot` RPC。
- 在单个 `InstallSnapshot` RPC 中发送整个快照。不要实现 Figure 13 中用于拆分快照的 offset 机制。
- Raft 必须以允许 Go 垃圾回收器释放和重用内存的方式丢弃旧日志条目；这要求没有可达的引用（指针）指向被丢弃的日志条目。
- 当 Raft 节点重新启动时，传递给 `Make()` 的 `persister` 将包含应用状态的快照以及 Raft 的已保存状态。Raft 必须在每次调用 `persister.Save()` 时包含一个非 nil 的快照（如果日志已被裁剪），这意味着 `Make()` 最好调用 `persister.ReadSnapshot()` 并保存结果。
- 不带 `-race` 的情况下，全套实验 3 测试（3A+3B+3C+3D）的合理时间消耗是 6 分钟实际时间和 1 分钟 CPU 时间。使用 `-race` 运行时，大约是 10 分钟实际时间和 2 分钟 CPU 时间。

你的代码应该通过所有 3D 测试（如下所示），以及 3A、3B 和 3C 测试。

```
$ make RUN="-run 3D" raft1
go build -race -o main/raft1d main/raft1d.go
cd raft1 && go test -v -race -run 3D
=== RUN   TestSnapshotBasic3D
Test (3D): snapshots basic (reliable network)...
  ... Passed --  time  8.4s #peers 3 #RPCs   279 #Ops   31
--- PASS: TestSnapshotBasic3D (8.74s)
=== RUN   TestSnapshotInstall3D
Test (3D): install snapshots (disconnect) (reliable network)...
  ... Passed --  time 59.6s #peers 3 #RPCs   919 #Ops   91
--- PASS: TestSnapshotInstall3D (59.99s)
=== RUN   TestSnapshotInstallUnreliable3D
Test (3D): install snapshots (disconnect) (unreliable network)...
  ... Passed --  time 82.1s #peers 3 #RPCs  1083 #Ops   91
--- PASS: TestSnapshotInstallUnreliable3D (82.49s)
=== RUN   TestSnapshotInstallCrash3D
Test (3D): install snapshots (crash) (reliable network)...
  ... Passed --  time 53.6s #peers 3 #RPCs   685 #Ops   91
--- PASS: TestSnapshotInstallCrash3D (53.99s)
=== RUN   TestSnapshotInstallUnCrash3D
Test (3D): install snapshots (crash) (unreliable network)...
  ... Passed --  time 66.2s #peers 3 #RPCs   717 #Ops   91
--- PASS: TestSnapshotInstallUnCrash3D (66.60s)
=== RUN   TestSnapshotAllCrash3D
Test (3D): crash and restart all servers (unreliable network)...
  ... Passed --  time 20.4s #peers 3 #RPCs   244 #Ops   45
--- PASS: TestSnapshotAllCrash3D (20.79s)
=== RUN   TestSnapshotInit3D
Test (3D): snapshot initialization after crash (unreliable network)...
  ... Passed --  time  7.4s #peers 3 #RPCs    79 #Ops   14
--- PASS: TestSnapshotInit3D (7.77s)
PASS
ok      6.5840/raft1    301.406s
$
```