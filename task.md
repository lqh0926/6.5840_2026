# Phase 0 · 抽象层 — 实现任务

> 总验收：`make lab3a/3b/3c/3d` 及 raft/kv 全部原测试照旧绿（含 `-race`）。
> 原则：每一步独立可编译、可测；不动 `tester1/`，把抽象的转换收在各服务的边界处，保证回归网完整。

---

## Step 1 · 传输抽象（RPC → 接口）✅ 完成

**设计决策（🟣 要能复述）**
- 传输接口**不依赖 raft 结构体**。传输层只负责"送一个调用、取回回复"，不懂投票/日志语义。
- 接口签名照搬 labrpc 现有的 `Call(svcMeth string, args, reply any) bool`（泛型）。
  - 好处 1：`*labrpc.ClientEnd` 已有此方法 → **结构化类型自动满足接口，无需适配器**。
  - 好处 2：同一接口 raft / kvraft / shardkv 三层都能复用，非 raft 专用。
- 为什么不用类型化接口（`RequestVote(*Args)(*Reply)`）：会让接口 import raft 结构体，而 raft 又要 import 接口 → **import 循环**。且现 `RequestVoteArgs`/`AppendEntries` 都定义在 `raft1/raft.go`。
- 代价（Phase 1 还）：gRPC 是强类型 stub，泛型 `Call` 落到 gRPC 适配器里需按 `svcMeth` 做 method-name dispatch（type switch），dispatch 收在适配器内部，可接受。

**改动清单**
- [ ] 新建 `src/transport/transport.go`，包 `transport`，**不 import 任何 raft 代码**：
  ```go
  package transport

  // ClientEnd 是对"能发起一次 RPC 调用"的最小抽象。
  // labrpc.ClientEnd / 未来的 gRPC client 都实现它。
  type ClientEnd interface {
      Call(svcMeth string, args any, reply any) bool
  }
  ```
- [ ] `raft1/raft.go`：
  - 字段 `peers []*labrpc.ClientEnd` → `peers []transport.ClientEnd`
  - `Make(peers []*labrpc.ClientEnd, ...)` → `Make(peers []transport.ClientEnd, ...)`
  - 删掉对 `labrpc` 的 import（若 raft.go 不再直接用到）
- [ ] `raft1/server.go`：`newRfsrv` 收到的仍是 `[]*labrpc.ClientEnd`（tester 传入，不改 tester）。
      在调用 `Make` 前做一次 slice 转换：
  ```go
  peers := make([]transport.ClientEnd, len(ends))
  for i, e := range ends { peers[i] = e }
  ```
  （`[]*labrpc.ClientEnd` 不能隐式转 `[]transport.ClientEnd`，必须显式循环）

**验收**
- [ ] `go build ./...` 通过
- [ ] `go test ./raft1/`（3A–3D）全绿
- [ ] `go test -race -run TestInitialElection3A ./raft1/` 绿（抽样确认 race 干净）

---

## Step 2 · 节点寻址抽象（int 下标 → NodeID + 地址映射）✅ 完成

> `ClientEnd` 解决"怎么调"，`NodeID` 解决"调谁"。两者配套。

- [ ] 定义稳定 `NodeID`（string，如 `"n1"`），与传输地址解耦
- [ ] raft 内部 peer 标识从 `int` 下标过渡到 `NodeID`（先做映射层，labrpc 仍按下标）
- [ ] 评估对 `me int`、`matchIndex/nextIndex` 等下标数组的影响（这里改动面较大，单列一步）
- [ ] 验收：原测试绿

---

## Step 3 · 持久化抽象（Persister → 接口）✅ 完成

- [ ] 把 `*tester.Persister` 抽到 `Persister` 接口背后，内存实现作为其一
- [ ] raft.go 中 `persister.Save/ReadRaftState/RaftStateSize` 走接口
- [ ] 验收：原测试绿（持久化相关 3C/3D 重点跑）

---

## Step 4 ~~真 binary~~ → 移至 Phase 1

> **决策**：binary 不是独立产物，它就是 gRPC server 的宿主进程。
> labrpc 是进程内模拟网络，两个 OS 进程无法用它通信——binary 必须有跨进程的真传输（gRPC）才有意义。
> 故 binary 与 Phase 1 的 gRPC 一起落地，Phase 0 收口在 Step 3。

**Phase 0 ✅ 全部完成（Step 1–3）。**

---

# Phase 1 · 真 RPC（gRPC + protobuf）

> 验收：labrpc 测试（L1 回归网）+ gRPC loopback 测试都绿。

## 设计决策 A · 进程架构（🟣 要能复述）

- **两个二进制，职责分离**：
  - `raftkvd`（server）：跑节点 = Raft + KV 状态机，暴露 gRPC。**自身不带发起操作的 CLI**。N 个 = 集群。
  - `raftkvctl`（client，独立，可选）：薄 gRPC client，对外戳集群用；或直接 `grpcurl`。
  - server 的"对外接口"就是它的 gRPC KV service。client 与 server 都走 gRPC。
- **两个通信平面**（都走 gRPC/TCP）：
  - Peer 平面（Raft）：node↔node，RequestVote/AppendEntries/InstallSnapshot
  - Client 平面（KV）：client→leader，Get/Put/Append
  - 注：测试里的"进程内通信"是 labrpc 的 channel（L1 回归层用），跟生产的 gRPC/TCP 是两套，别混。
- **配置：一个 Config struct，来源可切换**。`NodeID`/`ListenAddr`/`Peers`(NodeID→addr)/`DataDir`。本地用「共享 peer map 配置文件 + 每进程 `--node-id` 选'我是谁'」；Phase 3 进 k8s 换成 env/configmap，**struct 不变**。本地多进程靠 `scripts/run-local-cluster.sh` 起 3 个 `raftkvd`（临时脚手架，Phase 3 升级成 docker-compose，别在它上雕花）。
- **不暴露 `Raft.Start` 作对外/CLI 接口**——错的层：裸 submit 绕过去重 `(clientId,seq)`、leader 重定向、状态机语义。Start 永远留在 KV 层内部，不出包。
- Phase 1 之后**已经有对外 API**，只是 gRPC 的（`.proto` 含 KV Get/Put/Append）。REST（Phase 4）是其上的 HTTP 门面，不是"现在没接口"。验端到端优先 `grpcurl` 调 KV Put/Get（零代码）。

## 设计决策 B · 测试分层（🟣 要能复述）

**不"移植"原测试，要分层。** `tester1/config.go` 里 `net *labrpc.Network` 焊死，全部故障注入（Reliable/LongReordering/LongDelays/分区）走 labrpc.Network——强搬成本高且更不稳。

- **L1 — 原测试一行不改，继续跑 labrpc**。Phase 0 已把传输抽成 `transport.ClientEnd`，labrpc 只是其一实现；原测试通过接口跑，**不碰 gRPC**。这是主回归网（全覆盖 + `models1/` 线性化检查），负责"复现和调试"。
- **L2 — 新写精简 gRPC harness，只搬 5 个关键场景**：初始选主 / 日志复制 / 分区恢复 / leader crash / **响应回程丢失的去重**（labrpc 造不出，L2 独有价值）。不复用 tester1。
- **共享场景逻辑靠 `Cluster` 接口**（中间路径）：场景 = 一串"操作+断言"，与底座无关；把动作抽成接口，场景写一遍，接口实现两份。
  ```go
  type Cluster interface {
      Leader() NodeID
      Disconnect(NodeID); Connect(NodeID)
      Put(k, v string) error; Get(k string) (string, error)
  }
  // 场景写一次，针对接口：
  func ScenarioPartitionRecovery(t *testing.T, c Cluster) { ... }
  // 实现两份：labrpcCluster.Disconnect 调 net.Disconnect；
  //          grpcCluster.Disconnect 调 fault interceptor 的 DropTo
  // 调用：ScenarioX(t, newLabrpcCluster(5)) / ScenarioX(t, newGrpcCluster(5))
  ```
  **只对这 5 个场景做，别让全套 6.5840 测试跑这接口**（tester1 焊死 labrpc，参数化亏；gRPC 故障注入不如 labrpc 确定，跑全套回归更不稳）。
- L2 故障注入：labrpc 的 `net.Disconnect` 没了，改用 **gRPC interceptor** 编程注入 drop/delay/error。
- 一句话：**场景逻辑与传输解耦（`Cluster` 抽象）；全量回归留确定性的 labrpc 层，gRPC 层只验迁移正确性 + 它独有的失败模式（服务端已处理、响应丢、客户端只见 timeout）。**

## 任务

- [ ] 🟢 定义 `.proto`：Raft（RequestVote / AppendEntries / InstallSnapshot）+ KV（Get/Put/Append）
- [ ] 🟢 gRPC transport 实现 `transport.ClientEnd` 接口
  - 泛型 `Call(svcMeth, args, reply)` 落到 gRPC 适配器里按 `svcMeth` 做 method-name dispatch（type switch），dispatch 收在适配器内部
- [ ] 🟢 序列化 gob → protobuf（顺带拿到 schema 演进能力）
- [ ] 🟢 **真 binary**（原 Phase 0 Step 4）：`cmd/raftkvd` main 包
  - flag/env 配置加载：`--node-id` / `--listen` / `--peers`(NodeID→addr) / `--data-dir`
  - 起 gRPC server，挂 Raft handler + KV service；用 gRPC transport + 落盘 Persister 引导节点
  - 跑到 SIGTERM，优雅停机（leadership transfer 留到 Phase 3）
  - 结构化日志：slog（标准库零依赖，优先于 zap）
  - 验收：`go build` 出 `raftkvd`，3 进程能选主 + 复制，`grpcurl` 调 KV Put/Get 通
- [ ] 🟣 编写 fault interceptor（可编程注入 drop/delay/error）— 测试策略 L2
- [ ] 🟣 L2 测试：定义 `Cluster` 接口 + `labrpcCluster`/`grpcCluster` 两实现，搬 5 个关键场景（选主/复制/分区恢复/leader crash/响应丢失去重）
- [ ] 🟢 `scripts/run-local-cluster.sh`：本地多进程起 3 个 `raftkvd`（临时脚手架）
- [ ] ~~快照分块流式传输~~ **砍**：普通 InstallSnapshot 够用

---

## 备注
- 每步保持"原测试照旧绿"为硬门槛（L1 回归网），任一步骤打破立即停。
- gRPC 多一种 labrpc 没有的故障：服务端已处理、响应回程丢失 → 客户端只看到 timeout。这是 `(clientId,seq)` 去重的核心场景，靠 L2 interceptor 精准构造。
