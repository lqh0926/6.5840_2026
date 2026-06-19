6.5840 - Spring 2026
6.5840 Lab 5: 分片式键值服务
协作政策 // 提交实验 // 配置 Go // 指导 // Piazza

## 简介

你可以选择自行构思一个最终项目，也可以选择完成本实验。

在本实验中，你将构建一个键值存储系统，该系统将键分片（shard，即分区）到一组基于 Raft 复制的键值服务器组（shardgrp）上。分片是键值对的一个子集；例如，所有以 "a" 开头的键可能属于某个分片，以 "b" 开头的键属于另一个分片，以此类推。引入分片的主要动机是性能。每个 shardgrp 只负责其分配到的若干分片的 Put 和 Get 操作，各组可并行运行；因此，系统总吞吐量（单位时间内的 Put 和 Get 操作数）随 shardgrp 的数量线性增长。

## shardkv 设计

![shardkv 架构图](../shardkv.png)

上图展示了分片键值服务的各组件。Shardgrp（蓝色方块）存储对应分片的键值数据：shardgrp 1 持有存储键 "a" 的分片，shardgrp 2 持有存储键 "b" 的分片。客户端通过 clerk（绿色圆圈）与服务交互，clerk 实现了 Get 和 Put 方法。为确定 Put/Get 传入的键应由哪个 shardgrp 处理，clerk 从 kvsrv（黑色方块，即你在 Lab 2 中实现的键值服务）获取配置（configuration）。配置（未在图中显示）描述了分片到 shardgrp 的映射关系——例如，分片 1 由 shardgrp 3 服务。

管理员（即测试器）通过另一个客户端——controller（紫色圆圈）——来向集群添加或移除 shardgrp，并更新各 shardgrp 所负责的分片。controller 的核心方法是 ChangeConfigTo：接受新配置作为参数，驱动系统从当前配置过渡到新配置。这包括将分片迁移到新加入系统的 shardgrp，以及将分片从即将离开系统的 shardgrp 中移出。为此，controller 需要：1）向 shardgrp 发送 RPC（FreezeShard、InstallShard 和 DeleteShard）；2）更新 kvsrv 中存储的配置。

引入 controller 是因为分片存储系统必须支持在 shardgrp 之间迁移分片。原因有二：一是某些 shardgrp 的负载可能高于其他 shardgrp，需要重新分配分片以平衡负载；二是 shardgrp 会加入或离开系统——可能为扩容新增 shardgrp，现有 shardgrp 也可能因维护或退役而下线。

本实验的主要挑战在于：既要确保 Get/Put 操作的线性一致性，又要同时处理以下两种情况——1）分片到 shardgrp 的分配发生变更；2）ChangeConfigTo 执行期间 controller 发生故障或网络分区后的恢复。

ChangeConfigTo 将分片从某个 shardgrp 迁移到另一个 shardgrp。一个风险是：部分客户端可能仍在使用旧 shardgrp，而其他客户端已开始使用新 shardgrp，这可能破坏线性一致性。你需要确保在任意时刻，每个分片至多只有一个 shardgrp 在服务请求。
如果在重新配置期间 ChangeConfigTo 失败，某些已开始但尚未完成迁移的分片（从一个 shardgrp 到另一个 shardgrp）可能变得不可访问。为使系统继续推进，测试器会启动一个新的 controller，你的任务是确保新 controller 能完成前一 controller 已启动的重新配置过程。
本实验中使用的"配置"（configuration）指分片到 shardgrp 的分配关系，这与 Raft 集群成员变更不同。你无需实现 Raft 集群成员变更。

每个 shardgrp 服务器仅属于一个 shardgrp，且该 shardgrp 内的服务器集合永远不变。

客户端与服务器之间只能通过 RPC 交互。例如，你的服务器的不同实例不允许共享 Go 变量或文件。

在 Part A 中，你将实现 shardctrler——它通过 kvsrv 存储和检索配置；你还将实现 shardgrp（通过你的 Raft rsm 包进行复制）及对应的 shardgrp clerk。shardctrler 通过 shardgrp clerk 与各组通信，在不同组之间迁移分片。

在 Part B 中，你将修改 shardctrler 以处理配置变更期间的故障和分区。在 Part C 中，你将扩展 shardctrler 以支持多个 controller 并发运行而互不干扰。最后，在 Part D 中，你可以自由扩展你的解决方案。

本实验的分片键值服务与 Flat Datacenter Storage、BigTable、Spanner、FAWN、Apache HBase、Rosebud、Spinnaker 等众多系统遵循相同的整体设计思路。不过这些系统在诸多细节上与本实验不同，且通常更加复杂和强大。例如，本实验不涉及每个 Raft 组中对等节点集合的动态变化；其数据模型和查询模型也更为简单；等等。

Lab 5 将用到你 Lab 2 中实现的 kvsrv，以及 Lab 4 中的 rsm 与 Raft。你的 Lab 5 与 Lab 4 必须使用相同的 rsm 和 Raft 实现。

Part A 可以使用 late hours（迟交额度），但 Part B–D 不能使用。

## 开始

执行 `git pull` 获取最新的实验代码。

我们在 `src/shardkv1` 中为你提供了测试和骨架代码：

- `client.go` — shardkv clerk 的实现
- `shardcfg` 包 — 用于计算分片配置
- `shardgrp` 包 — shardgrp 的 clerk 与服务器实现
- `shardctrler` 包 — 包含 `shardctrler.go`，其中定义了 controller 的配置变更（ChangeConfigTo）与查询（Query）方法

要启动运行，执行以下命令：

```bash
$ cd ~/6.5840
$ git pull
...
$ cd src
$ make RUN="-run 5A" shardkv
go build -race -o main/kvsrv1d main/kvsrv1d.go
go build -race -o main/shardgrp1d main/shardgrp1d.go
cd shardkv1; go test -timeout 15m -v -race -run 5A
=== RUN  TestInitQuery5A
Test (5A): Init and Query ... (reliable network)...
...
```

## Part A: 迁移分片（较难）

你的第一个任务是实现 shardgrp，以及 InitConfig、Query 和 ChangeConfigTo 方法（在无故障场景下）。我们已在 `shardkv1/shardcfg` 中提供了描述配置的代码。每个 `shardcfg.ShardConfig` 拥有一个唯一的标识号 Num、一个从分片号到组号的映射、以及一个从组号到复制该组的服务器列表的映射。通常分片数量会多于组数量（这样每个组服务多个分片），以便能以较细粒度调整负载。

在 `shardctrler/shardctrler.go` 中实现以下两个方法：

- `InitConfig` 方法接收测试器传入的第一个配置（类型为 `shardcfg.ShardConfig`），并将其存储在 Lab 2 的 kvsrv 实例中。
- `Query` 方法返回当前配置；它应从 kvsrv 中读取之前由 InitConfig 存储的配置。

需要将配置存储在 kvsrv 中以实现 InitConfig 和 Query。当你的代码通过第一个测试时即完成此任务。注意此任务不涉及任何 shardgrp。

```bash
$ cd ~/6.5840/src
$ make RUN="-run TestInitQuery5A" shardkv
go build -race -o main/kvsrv1d main/kvsrv1d.go
go build -race -o main/shardgrp1d main/shardgrp1d.go
cd shardkv1; go test -timeout 15m -v -race -run TestInitQuery5A
=== RUN   TestInitQuery5A
Test (5A): Init and Query ... (reliable network)...
  ... Passed --  time  0.0s #peers 1 #RPCs     3 #Ops    0
--- PASS: TestInitQuery5A (0.13s)
PASS
ok      6.5840/shardkv1  1.143s
```

实现 InitConfig 和 Query 的方式：通过 `ShardCtrler.IKVClerk` 的 Get/Put 方法与 kvsrv 通信；用 `ShardConfig` 的 `String` 方法将 `ShardConfig` 序列化为可传给 Put 的字符串；用 `shardcfg.FromString()` 函数将字符串反序列化回 `ShardConfig`。

从你的 Lab 4 kvraft 解决方案中复制代码，在 `shardkv1/shardgrp/server.go` 中实现 shardgrp 的初始版本，并在 `shardkv1/shardgrp/client.go` 中实现对应的 clerk。

在 `shardkv1/client.go` 中实现 clerk：通过 Query 方法查找键对应的 shardgrp，然后与该 shardgrp 通信。当你的代码通过 Static 测试时即完成此任务。

```bash
$ cd ~/6.5840/src
$ make RUN="-run TestStatiOneShardGroup5A" shardkv
go build -race -o main/kvsrv1d main/kvsrv1d.go
go build -race -o main/shardgrp1d main/shardgrp1d.go
cd shardkv1; go test -timeout 15m -v -race -run TestStaticOneShardGroup5A
=== RUN   TestStaticOneShardGroup5A
Test (5A): one shard group ... (reliable network)...
  ... Passed --  time  9.4s #peers 1 #RPCs   822 #Ops  420
--- PASS: TestStaticOneShardGroup5A (9.52s)
PASS
ok      6.5840/shardkv1  10.542s
```

从你的 kvraft `client.go` 和 `server.go` 中复制 Put、Get 的代码，以及 kvraft 中其余需要的代码。
`shardkv1/client.go` 提供了整个系统的 Put/Get clerk：它通过调用 Query 方法确定哪个 shardgrp 持有目标键的分片，然后与该 shardgrp 通信。
实现 `shardkv1/client.go` 的 Put/Get 方法。用 `shardcfg.Key2Shard()` 查找键对应的分片号。测试器通过 `shardkv1/client.go` 的 `MakeClerk` 传入 `ShardCtrler` 对象，使用 Query 方法获取当前配置。
要对 shardgrp 执行 put/get，shardkv clerk 应调用 `shardgrp.MakeClerk` 为该 shardgrp 创建 shardgrp clerk，传入配置中列出的服务器以及 shardkv clerk 的 `ck.clnt`。用 `ShardConfig` 的 `GidServers()` 方法获取分片对应的组。
`shardkv1/client.go` 的 Put 在回复可能丢失时必须返回 `ErrMaybe`，但此 Put 在内部调用 shardgrp 的 Put 与特定 shardgrp 通信。内部的 Put 可通过错误码传递这一信号。
在初始创建时，第一个 shardgrp（`shardcfg.Gid1`）应将自身初始化为拥有所有分片。

接下来需要实现 ChangeConfigTo 方法，以支持分片在组之间迁移。该方法将系统从旧配置变更到新配置：新配置可能包含旧配置中不存在的 shardgrp，也可能排除旧配置中存在的 shardgrp。controller 应当迁移分片数据，使每个 shardgrp 持有的分片集合与新配置一致。

我们建议的迁移方案如下：ChangeConfigTo 首先在源 shardgrp 上"冻结"（freeze）分片，使该 shardgrp 拒绝针对正处于迁移中的分片的 Put 操作。然后，将分片复制（安装，install）到目标 shardgrp；接着删除已冻结的分片。最后，发布新配置，使客户端能找到已迁移的分片。这种方案的优点是避免了 shardgrp 之间的直接交互，且不影响正在进行的配置变更所不涉及的分片的服务。

为支持配置变更的排序，每个配置都有一个唯一的 Num（参见 `shardcfg/shardcfg.go`）。在 Part A 中，测试器按顺序调用 ChangeConfigTo，每次传入的配置的 Num 比前一配置大 1；因此，Num 越大的配置越新。

网络可能延迟 RPC 的投递，且 RPC 可能乱序到达 shardgrp。为拒绝过期的 FreezeShard、InstallShard 和 DeleteShard RPC，它们均应携带 Num（参见 `shardgrp/shardrpc/shardrpc.go`），且 shardgrp 必须记录每个分片上看到的最大 Num。

实现 ChangeConfigTo（在 `shardctrler/shardctrler.go` 中）并扩展 shardgrp 以支持 freeze、install 和 delete 操作。在 Part A 中 ChangeConfigTo 应总是成功，因为此部分测试不引入故障。你需要在 `shardgrp/client.go` 和 `shardgrp/server.go` 中使用 `shardgrp/shardrpc` 包中的 RPC 来实现 FreezeShard、InstallShard 和 DeleteShard，并根据 Num 拒绝旧 RPC。同时还需修改 `shardkv1/client.go` 中的 shardkv clerk 以处理 `ErrWrongGroup`：当 shardgrp 不负责某个分片时应返回该错误。

完成此任务后，需要通过 JoinBasic 和 DeleteBasic 测试。这些测试侧重于新 shardgrp 加入的场景，暂不需要处理 shardgrp 离开的情况。

当客户端对不属于其目标 shardgrp 的键执行 Put/Get 时，shardgrp 应返回 `ErrWrongGroup` 错误。你需要修改 `shardkv1/client.go` 以重新读取配置并重试 Put/Get。
注意：FreezeShard、InstallShard 和 DeleteShard 必须通过你的 rsm 包来执行，与 Put、Get 的处理方式相同。
你可以在 RPC 请求或回复中直接传输整个 map 的状态数据，这有助于简化分片迁移代码。
如果某个 RPC 处理函数的回复中包含一个 map（例如键值 map），且该 map 是服务器状态的一部分，你可能会因竞态条件引入 bug。RPC 系统必须读取该 map 以将其发送给调用方，但其并未持锁；而你的服务器可能在 RPC 系统读取 map 的同时仍在修改它。解决方案是让 RPC 处理函数在回复中包含该 map 的一份副本。

扩展 ChangeConfigTo 以处理 shardgrp 的离开，即当前配置中存在但新配置中不存在的 shardgrp。完成后，你的方案应通过 TestJoinLeaveBasic5A。（你可能已在之前的任务中处理了这种情况，但先前的测试未覆盖 shardgrp 离开的场景。）

使你的方案通过所有 Part A 测试。这些测试覆盖了：多组加入与离开、shardgrp 从快照重启、在部分分片下线或存在配置变更时处理 Get 操作、以及大量客户端并行操作而测试器同时调用 controller 的 ChangeConfigTo 进行分片重新平衡时的线性一致性。

```bash
$ cd ~/6.5840/src
$ make RUN="-run 5A" shardkv
go build -race -o main/kvsrv1d main/kvsrv1d.go
go build -race -o main/shardgrp1d main/shardgrp1d.go
cd shardkv1; go test -timeout 15m -v -race -run 5A
Test (5A): Init and Query ... (reliable network)...
  ... Passed --  time  0.0s #peers 1 #RPCs     3 #Ops    0
Test (5A): one shard group ... (reliable network)...
  ... Passed --  time  5.1s #peers 1 #RPCs   792 #Ops  180
Test (5A): a group joins... (reliable network)...
  ... Passed --  time 12.9s #peers 1 #RPCs  6300 #Ops  180
Test (5A): delete ... (reliable network)...
  ... Passed --  time  8.4s #peers 1 #RPCs  1533 #Ops  360
Test (5A): basic groups join/leave ... (reliable network)...
  ... Passed --  time 13.7s #peers 1 #RPCs  5676 #Ops  240
Test (5A): many groups join/leave ... (reliable network)...
  ... Passed --  time 22.1s #peers 1 #RPCs  3529 #Ops  180
Test (5A): many groups join/leave ... (unreliable network)...
  ... Passed --  time 54.8s #peers 1 #RPCs  5055 #Ops  180
Test (5A): shutdown ... (reliable network)...
  ... Passed --  time 11.7s #peers 1 #RPCs  2807 #Ops  180
Test (5A): progress ... (reliable network)...
  ... Passed --  time  8.8s #peers 1 #RPCs   974 #Ops   82
Test (5A): progress ... (reliable network)...
  ... Passed --  time 13.9s #peers 1 #RPCs  2443 #Ops  390
Test (5A): one concurrent clerk reliable... (reliable network)...
  ... Passed --  time 20.0s #peers 1 #RPCs  5326 #Ops 1248
Test (5A): many concurrent clerks reliable... (reliable network)...
  ... Passed --  time 20.4s #peers 1 #RPCs 21688 #Ops 10500
Test (5A): one concurrent clerk unreliable ... (unreliable network)...
  ... Passed --  time 25.8s #peers 1 #RPCs  2654 #Ops  176
Test (5A): many concurrent clerks unreliable... (unreliable network)...
  ... Passed --  time 25.3s #peers 1 #RPCs  7553 #Ops 1896
PASS
ok      6.5840/shardkv1 243.115s
$
```

你的方案必须在配置变更期间继续正常服务不受影响的分片。

## Part B: 处理 controller 故障（简单）

controller 是由管理员调用的短生命周期命令：它迁移分片后即退出。但在迁移分片过程中，它可能崩溃或丢失网络连接。本部分的主要任务是在 ChangeConfigTo 未完成的 controller 故障后实现恢复。测试器会在隔离第一个 controller 后启动一个新的 controller 并调用其 ChangeConfigTo；你需要修改 controller，使新 controller 能完成重新配置过程。测试器在启动 controller 时调用 `InitController`；你可以修改该函数以检查是否有中断的配置变更需要完成。

一个较好的让新 controller 完成前一 controller 启动的重新配置的方法，是在 controller 的 kvsrv 中维护两份配置：当前配置（current）和下一个配置（next）。controller 启动重新配置时存储 next 配置；待重新配置完成后，再将 next 配置变为 current 配置。修改 `InitController`，使其首先检查是否存在已存储的 next 配置且其 Num 大于 current 的 Num，若存在则完成迁移到 next 配置所需的分片移动。

修改 shardctrler 以实现以上方案。接管故障 controller 任务的 controller 可能重复发送 FreezeShard、InstallShard 和 DeleteShard RPC；shardgrp 可通过 Num 检测重复并拒绝。如果你的方案通过 Part B 测试，即完成此任务。

```bash
$ cd ~/6.5840/src
$ make RUN="-run 5B" shardkv
go build -race -o main/kvsrv1d main/k
go build -race -o main/shardgrp1d main/shardgrp1d.go
cd shardkv1; go test -timeout 15m -v -race -run 5B
Test (5B): Join/leave while a shardgrp is down... (reliable network)...
  ... Passed --  time  9.2s #peers 1 #RPCs   899 #Ops  120
Test (5B): recover controller ... (reliable network)...
  ... Passed --  time 26.4s #peers 1 #RPCs  3724 #Ops  360
PASS
ok      6.5840/shardkv1 35.805s
$
```

测试器在启动 controller 时调用 `InitController`；你可以在 `shardctrler/shardctrler.go` 的该方法中实现恢复逻辑。

## Part C: 并发配置变更（中等）

在本部分中，你将修改 controller 以允许同时运行多个 controller。当 controller 崩溃或分区时，测试器将启动新的 controller，新 controller 必须完成旧 controller 可能尚在进行中的工作（即像 Part B 那样完成分片迁移）。这意味着多个 controller 可能并发运行，并同时向 shardgrp 和存储配置的 kvsrv 发送 RPC。

主要挑战是确保这些 controller 互不干扰。在 Part A 中，你已通过 Num 对所有 shardgrp RPC 进行了防护（fencing），使得旧的 RPC 会被拒绝。即使多个 controller 同时接手旧 controller 的工作，其中只有一个成功而其他重复已有 RPC，shardgrp 也会忽略重复请求。

因此，剩余的挑战性场景是：确保只有一个 controller 能更新 next 配置，以避免两个 controller（例如一个被分区的和一个新的）写入不同的 next 配置。为突出这种场景，测试器并发运行多个 controller，每个 controller 通过读取当前配置并为离开或加入的 shardgrp 更新它来计算出 next 配置，然后测试器调用 ChangeConfigTo；因此，多个 controller 可能以相同 Num 的不同配置调用 ChangeConfigTo。你可以使用带版本号的键值和带版本的 Put 来确保只有一个 controller 更新 next 配置，而其他调用直接返回不做任何操作。

修改你的 controller，使得对于某个配置 Num，只有一个 controller 能发布 next 配置。测试器将启动许多 controller，但只有一个会真正开始针对新配置的 ChangeConfigTo。如果你通过 Part C 的并发测试，即完成此任务：

```bash
$ cd ~/6.5840/src
$ make RUN="-run TestConcurrentReliable5C" shardkv
go build -race -o main/kvsrv1d main/k
go build -race -o main/shardgrp1d main/shardgrp1d.go
cd shardkv1; go test -timeout 15m -v -race -run TestConcurrentReliable5C
Test (5C): Concurrent ctrlers ... (reliable network)...
  ... Passed --  time  8.2s #peers 1 #RPCs  1753 #Ops  120
PASS
ok      6.5840/shardkv1 8.364s
$ make RUN="-run TestAcquireLockConcurrentUnreliable5C" shardkv
go build -race -o main/kvsrv1d main/k
go build -race -o main/shardgrp1d main/shardgrp1d.go
cd shardkv1; go test -timeout 15m -v -race -run TestAcquireLockConcurrentUnreliable5C
Test (5C): Concurrent ctrlers ... (unreliable network)...
  ... Passed --  time 23.8s #peers 1 #RPCs  1850 #Ops  120
PASS
ok      6.5840/shardkv1 24.008s
$
```

参见 `test.go` 中的 `concurCtrler` 以了解测试器如何并发运行 controller。
在本练习中，你需要将旧 controller 的恢复与新 controller 的功能结合起来：新 controller 应执行 Part B 中的恢复逻辑。如果旧 controller 在 ChangeConfigTo 期间被分区，你必须确保旧 controller 不会干扰新 controller。如果所有 controller 的更新已通过 Part B 的 Num 检查进行了适当防护，则无需编写额外代码。如果你通过 Partition 测试，即完成此任务。

```bash
$ cd ~/6.5840/src
$ make RUN="-run Partition" shardkv
go build -race -o main/kvsrv1d main/k
go build -race -o main/shardgrp1d main/shardgrp1d.go
cd shardkv1; go test -timeout 15m -v -race -run Partition
Test (5C): partition controller in join... (reliable network)...
  ... Passed --  time  7.8s #peers 1 #RPCs   876 #Ops  120
Test (5C): controllers with leased leadership ... (reliable network)...
  ... Passed --  time 36.8s #peers 1 #RPCs  3981 #Ops  360
Test (5C): controllers with leased leadership ... (unreliable network)...
  ... Passed --  time 52.4s #peers 1 #RPCs  2901 #Ops  240
Test (5C): controllers with leased leadership ... (reliable network)...
  ... Passed --  time 60.2s #peers 1 #RPCs 27415 #Ops 11182
Test (5C): controllers with leased leadership ... (unreliable network)...
  ... Passed --  time 60.5s #peers 1 #RPCs 11422 #Ops 2336
PASS
ok      6.5840/shardkv1 217.779s
$
```

你已成功实现了一个高可用的分片式键值服务——具有多个 shardgrp 以支持水平扩展、能够根据负载变化重新配置分片，且 controller 具备容错能力。恭喜！

重新运行所有测试，检查你近期对 controller 的改动是否破坏了先前的测试。

Gradescope 将对你的提交重新运行 Lab 3A–D、Lab 4A–C 以及 Lab 5C 的全部测试。提交前请仔细确认方案正常工作：

```bash
$ make raft1
$ make kvraft1
$ make shardkv
```

## Part D: 扩展你的方案

在实验的最后部分，你可以自由扩展你的方案。你需要为所实现的任何扩展自行编写测试。

你可以实现以下某个想法，也可以自行构思。请在 `extension.md` 中简要描述你的扩展，并将 `extension.md` 上传至 Gradescope。如果你想做其中一个难度较高、开放性较强的扩展，可以与其他同学两人合作。

以下是一些可能的扩展想法（前面的相对简单，后面的更开放）：

- **（简单）** 将测试器改为使用 kvraft 而非 kvsrv（即将 `test.go` 中 `MakeTestMaxRaft` 里的 `kvsrv.StartKVServer` 替换为 `kvraft.StartKVServer`），使得 controller 依赖你的 kvraft 来存储配置。编写一个测试来验证：当某个 kvraft 节点下线时，controller 仍能正常查询和更新配置。测试器的现有代码分布在 `src/kvtest1`、`src/shardkv1` 和 `src/tester1` 中。

- **（中等）** 修改 kvsrv 以实现 Put/Get 的精确一次（exactly-once）语义，如 2024 年 Lab 2 中的那样（参见丢弃消息部分）。可能可以移植 2024 版本的部分测试，而不必完全从头编写。同样也在 kvraft 中实现精确一次语义。

- **（中等）** 修改 kvsrv 以支持 Range 函数，该函数返回 low key 到 high key 范围内的所有键。实现 Range 的简单方式是遍历服务器维护的键值 map；更好的方案是使用支持范围搜索的数据结构（例如 B 树）。编写一个测试，让简单方案失败而更优方案通过。

- **（困难）** 修改你的 kvraft 实现，允许 leader 在不通过 rsm 运行 Get 的情况下直接服务 Get 请求。即实现 Raft 论文第 8 节末尾描述的优化，包括租约（leases）机制，以确保 kvraft 保持线性一致性。你的实现应通过现有的 kvraft 测试。还应添加一个测试来验证：你的优化实现是否更快（例如通过比较 RPC 数量），以及任期切换是否会因为新 leader 必须等待租约到期而变慢。

- **（困难）** 在 kvraft 中支持事务，使开发者能原子地执行多个 Put 和 Get。有了事务后，带版本的 Put 就不再必需，事务可以取而代之。可参考 etcd 的事务接口作为示例。编写测试来展示你的扩展正常工作。

- **（困难）** 修改 shardkv 以支持跨分片事务，使开发者能原子地执行跨分片的多个 Put 和 Get。这需要实现两阶段提交（two-phase commit）和两阶段锁定（two-phase locking）。编写测试来展示你的扩展正常工作。

## 提交步骤

提交前，请最后一次运行所有测试。

提交前，请仔细检查你的方案是否正常：

```bash
$ make raft1
$ make kvraft1
$ make shardkv
```