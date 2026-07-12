# Raft KV 项目改造路线图

> 目标：把 6.5840 的课程作业改造成一个**完整、能跑、能演示、面试能讲深**的分布式 KV 项目。
> 定位：单独这一个项目（独立的 OTLP Collector 是另一个 repo，本项目只做可观测性桥接）。

## 分工图例

| 标记 | 含义 |
|------|------|
| 🟣 **自己写** | 设计决策 / 面试深挖区。可以和 AI 讨论方案，但最终决策要理解、认同、能复述推理链 |
| 🟢 **AI 写·你 review** | 脚手架 / 样板 / 配置。AI 写，你 review 并能讲清楚"为什么这么做" |
| ⚪ **懂原理·不写** | 写成本高且同维度重复。搞懂原理、面试能在白板讲清即可，一行不写 |

**核心原则**：AI 写代码，但判断和归因是你的。任何一块（含 AI 写的），假设面试官问"这里为什么这么设计"，你要能不看代码、不查 AI，把决策理由和权衡过的其他选项讲出来。

---

## Phase 0 · 抽象层 + 真 binary ⭐安全网

> 验收：所有原 6.5840 测试照旧绿。

- [ ] 🟣 把**传输**抽到接口背后（labrpc 成为接口的一个实现）
- [ ] 🟣 把**持久化**抽到接口背后（内存 Persister 成为一个实现）
- [ ] 🟣 把**节点寻址**抽象：下标 `int` → 稳定 `NodeID` + 地址映射

> ~~真 binary / 结构化日志~~ 移至 Phase 1：labrpc 是进程内模拟网络，binary 必须有跨进程真传输（gRPC）才有意义，二者一起落地。

---

## Phase 1 · 真 RPC（gRPC + protobuf）

> 脱离课程框架的硬信号。
> 验收：labrpc 测试 + gRPC loopback 测试都绿。

- [x] 🟢 定义 `.proto`：Raft（RequestVote / AppendEntries / InstallSnapshot）+ KV（Get/Put/Append）
- [x] 🟢 gRPC transport 实现传输接口（**B 方案**：proto 只在 `transport/grpc/`，raft 不 import proto；labgob 无法往返 proto 结构体，故 A 方案作废）
- [~] 🟢 序列化 gob → protobuf：**raft wire 已是 proto**；命令载荷（Op↔[]byte）与持久化仍用 labgob（刻意，仅内部/不跨对外契约）——真正的 raftapi 命令 `[]byte` 化延后
- [x] 🟢 **真 binary**：`cmd/raftkvd`（RaftService+KVService co-locate + reflection + slog + 落盘 `persist.FilePersister`）+ `cmd/raftkvctl`（薄客户端）。已验证：3 节点选主/复制/优雅停机 + KV put/get/version + **崩溃恢复（kill 全部重启数据还在）**
- [x] 🟢 埋最基础的结构化日志（slog，binary 生命周期事件）
- [x] 🟣 编写 fault interceptor（drop/delay + 「服务端处理后吞响应」）+ **L2 全部 5 场景**（`src/l2/`：选主/复制/leader crash/分区恢复/响应丢失去重）—— `-race` 稳定绿
- [ ] ~~快照分块流式传输~~ **砍**：边际价值低，普通 InstallSnapshot 够用

> **binary 不暴露 `Raft.Start`**（错的层，绕过去重/重定向/状态机）。Phase 1 后对外即有 gRPC KV API（Get/Put/Append）；
> 验端到端用 `grpcurl` 或薄 `cmd/raftkvctl`（调 KV 操作，非 Start）。REST（Phase 4）是其上的 HTTP 门面。

---

## Phase 2 · 存储层重做：Raft WAL（真持久化）+ 状态机落磁盘（LSM）⭐深度收尾

> 面试深挖区，必须吃透。**两条互不相干的轴，别塞进"持久化"一个词：**
> - **WAL（Raft log/meta）= 真持久化，命门、承重正确性**：append-only + fsync，崩溃后靠它对账重建。
> - **LSM（KV 状态机）= 存储模型，与持久化/正确性无关**：把 kvstate 从「内存 map」换成「磁盘结构」，
>   让状态可 > RAM（对标 etcd 的 bbolt / TiKV 的 RocksDB）。状态机 durability **不承重正确性**——
>   真值永远是「log + snapshot」，状态机丢了能重建；所以它不是"持久化"，是"内存模型→磁盘模型"。
> 验收：单节点崩溃恢复正确（靠 WAL）；原持久化相关测试绿。

- [x] 🟣 实现 WAL：term/votedFor/log entries，fsync 纪律（`src/wal` 原语 + `src/filewal` 组合，Step 1–3）
- [x] 🟣 崩溃恢复测试（kill -9 后重启对账）—— 命门。确定性层 `filewal` 组合对账 + 真机 `scripts/test-crash-recovery.sh` 全绿
- [x] 🟣 拆分存储：Raft log/metadata（fileWAL，append-only + fsync）与 KV 状态机分开
- **4a**（做）：状态机落 pebble（map → 磁盘 LSM，> RAM，存储模型改造、非持久化）
  - [ ] 🟢 **apply-id**：apply 时「KV 改动 + applied_index」同一 pebble batch 原子提交 —— durable 状态机的内在要求
        （否则重启 replay 双重 apply）；重启开 live pebble、从 K+1 重放。two-WAL 协调，落盘状态机唯一真深的点
  - [ ] 🟢 快照复用 raft 现成 blob 协议：`Snapshot()`=`pebble.Checkpoint()`（硬链，不遍历）→ blob；收 = 整库替换
        （无 tombstone，不能 merge）。**raft 快照协议不改**
- **4b**（可选优化，scale-only，大概率白板）：InstallSnapshot 从 bundle-blob 改流式发 checkpoint（快照大到塞不进 blob/RAM 才需要）

### 设计决策（🟣 要能复述）—— 为什么 Phase 1 的 FilePersister 不够

**问题**：现在 `persist.Persister.Save(raftstate, snapshot)` 是【全量 blob 原子写】——`rf.persist()`（7 个调用点）每次把 term+整条 log+votedFor 序列化成一个 blob **整体重写**。连「投一票」都要重写整条 log，是 append-only 的反面（O(log) 每次）。根因：接口只能表达「存全量」，表达不了「追加一条」。

**Raft 持久化的三类数据，写模式完全不同**：
| 数据 | 写模式 | 存法 |
|------|--------|------|
| 元数据 term/votedFor | 覆盖、极小、频繁 | 小文件原地覆盖 |
| **log entries** | **追加为主** + 偶尔截尾 + 快照丢前缀 | **append-only WAL** ← 命门 |
| snapshot（KV 状态机） | 大、偶发 | 独立文件/checkpoint（→ LSM） |

**方案 A（选它，真 WAL）**：把 `Save(blob)` 换成按操作分开的接口
`SaveMeta(term,vote)` / `AppendLog(entries)` / `TruncateSuffix(from)` / `Compact(upto,snap)` / `Load()`，
然后把 7 个 `rf.persist()` **逐点替换成对应的那一个调用**（投票只 `SaveMeta`、`Start` 只 `AppendLog`、冲突才 `TruncateSuffix`……）。
方案 B（etcd 级：append-only record WAL + 重放 + 段轮转/压缩）当白板延伸题，本项目过度。

**保住 L1 的手法（关键）**：raft 依赖新接口，给**两份实现**——
- **adapter over `tester.Persister`**（L1 用）：新接口全部落到「更新内存全量模型 → 调旧 `Save(整blob)`」，**行为逐字节等于今天**；必须 wrap `tester.Persister`（durable 字节跨重启活在它里面，崩溃恢复才成立）。
- **fileWAL**（binary 用）：真 append-only + fsync。
→ L1 走 adapter，测的是**算法**；真 WAL 的 append/truncate/compact/fsync 正确性**自己单测** + binary 级 kill-all-重启对账。

**fsync 纪律**：`AppendLog` fsync 后才算持久化、且**在 commitIndex 推进之前**；快照 fsync 后再删 log 前缀（顺序不能反）。

**改造纪律**：先扩接口 → 逐个 persist 调用点替换 → **每步跑 raft1/kvraft1 全回归**，别一把梭（关键路径）。

### 面试深挖点（🟣 6.5840 不涉及，Phase 2 净新增的知识）

> 这几条是复盘时补上的知识缺口（当时没 review `persist/file.go` 的 `atomicWrite`），面试 durability 连环追问区。

**① 崩溃安全 = 两把原语，不是"让单次 write+fsync 原子"**（崩溃能砸在 write 中间，挡不住）：
- **整文件重写（meta/snapshot）→ 原子性来自 `rename`**：写 tmp→fsync(tmp)→rename→fsync(dir)。rename 是
  POSIX 保证的目录项一次性换 inode，读者只见完整旧或完整新。两个 fsync 分卡 rename 两边：fsync(tmp) 在前
  （换过去的 inode 要有真数据），fsync(dir) 在后（持久化 rename 这个目录项改动，rename 得先发生）——**"rename
  之后才 fsync"不是 bug 是必须**。
- **尾部追加（log）→ 正确性来自 replay 校验**：record `[len][crc32][payload]`；replay 撞到 len 越界/crc 坏
  即判 torn tail、在该 offset 截断。append-only 下崩溃只砸没落盘的尾巴，已 fsync 的老 record 不被后来的崩溃碰坏。

**② crash 测试测的是"save 契约"，不是 Raft 算法**：Raft persist-before-reply → 丢"没 ack 的写"算法自容忍（低价值）。
真价值是三契约：replay 抗 torn（否则带垃圾状态起来去 ack → 安全性破）、meta 原子（撕裂 votedFor → 同 term 投两次）、
**SaveSnapshot 顺序**（压缩丢的是早已 ack 的前缀，persist-before-reply 够不到，只有 fsync 顺序挡着）。

**③ 压缩用整体重写、不做分段**：重写的是"活尾巴"（index>snapshotIndex，恒短，因 snapshotIndex≈appliedIndex），
不是整条 log → 压缩再频繁每次也 O(小)。分段的好处（免重写/O(1) unlink）只在活 log 很大时兑现，本项目不触发，
却要背段 roll/区间索引/GC/跨段 Load 四块复杂度 → 不划算。分段留白板题（etcd 为何分段 = 活 log 大到重写不可接受）。

**④ `Load()` 崩溃恢复重建**（6.5840 只"读回整块 blob decode"，无此逻辑）：读 meta+wal header+replay+snapshot 拼装，
**log 起点锚定在 snapshotIndex（从 snapshot 反推，不信 wal 物理布局）** → raft 永不见"snap@10 但 log@5"的错配。
snap/log 一致性是 Load 按构造保证的。

---

## Phase 3 · Docker + k8s 部署

> 证明非 toy，JD 高频要求。
> 验收：真 k8s 多节点能选主、复制、Pod 重启后恢复。

- [ ] 🟢 单 binary + 配置（env / flag / configmap）
- [ ] 🟢 Dockerfile（多阶段构建）
- [ ] 🟢 docker-compose：本地多进程联调
- [ ] 🟢 k8s StatefulSet（**不是** Deployment）+ headless Service + PVC
- [ ] 🟢 readiness / liveness 探针（基础版即可）
- [ ] 🟣 优雅停机（停机前 leadership transfer）
- [ ] 🟢 服务发现 / 集群引导（bootstrap）
- [ ] 🟣 **拆分 peer / client 两个平面**（Phase 1 是 co-locate 的简化，这里落地生产级分界）
  - 独立 listener/端口（etcd 式 peer `:2380` / client `:2379`）
  - 起因：① TLS 信任域不同（peer mTLS vs client TLS）② 网络暴露/NetworkPolicy 不同（peer 内网 only）③ 资源隔离（client 洪峰不能饿死 raft 心跳 → 误选举）
  - 连带：`transport/grpc.ClientEnd` 跟着拆（一条 conn 到不了两个端口，raftCli/kvCli 不再捆一起）
  - **同时做**：KV 平面的 `Call` 统一成收 kv 的 Go 结构体（对称 Raft 平面，当前仍传 proto）
  - **必答题延伸**：为什么 etcd 分 `2379`/`2380`？（就是上面①②③）

> **必答题**：为什么 Raft 节点用 StatefulSet 不用 Deployment？
> （StatefulSet 提供稳定网络标识 + 持久化存储，Pod 重启后名字和数据不变，节点靠固定身份重新加入集群；Deployment 的 Pod 名随机变，节点找不到彼此。）

---

## Phase 4 · 对外 REST API（砍到最简）

> 能演示端到端即可，不要过度工程。
> 验收：外部 client 经 REST 读写，leader 切换 / 重试不破坏线性化。

- [ ] 🟢 最简 HTTP 接口 + CLI 客户端
- [ ] 🟢 leader 重定向 + 客户端重试
- [ ] 🟣 幂等键：复用 `(clientId, seq)` 去重逻辑
- [ ] ~~API gateway / 连接池~~ **砍**：过度工程，面试不加分

---

## Phase 5 · 可观测性桥接

> 埋点对接独立的 OTLP Collector 项目，本 repo 只做桥接。
> 验收：能从面板看出集群健康与瓶颈。

- [ ] 🟣 选关键指标：leader 切换次数 / commit 延迟 / log lag / apply 滞后 / RPC 时延
- [ ] 🟢 OTLP 埋点，吐给 Collector
- [ ] 🟢 Prometheus 指标导出
- [ ] 🟢 Grafana dashboard

> ※ 简易 OTLP Collector 本体（接收 / 处理 / 导出，含背压控制）是**另一个独立 repo**，
> 对口工作经历。本项目只负责埋点 + 把数据吐过去这一段桥接。

---

## 测试策略（贯穿全程，不是独立阶段）

> 换掉 labrpc 后会**失去它的故障注入能力**，必须补。核心：不是"控 pod 网络通断"或"打桩 gRPC"二选一，而是分层。

| 层 | 机制 | 职责 | 标记 |
|----|------|------|------|
| L1 | labrpc（Phase 0 抽象后的一个实现） | 保留**全部** 6.5840 原测试 + `models1/` 线性化检查器，回归网。**原测试一行不改**（`tester1` 焊死 labrpc，强搬亏） | 🟢 现成 |
| L2 | 真 gRPC + fault interceptor | 验证换 gRPC 后不变式不破；**只搬 5 个关键场景**（选主/复制/分区恢复/leader crash/响应丢失去重） | 🟣 自己写 |
| L3/L4 | chaos-mesh 一个故障注入 demo | 真部署下的分区/杀 pod 演示，**别深搞** | ⚪ 懂原理 |

> **L2 不从零重写场景**：场景 = 一串"操作+断言"，与底座无关。抽 `Cluster` 接口（`Leader`/`Disconnect`/`Connect`/`Put`/`Get`），场景写一遍，
> `labrpcCluster`/`grpcCluster` 各实现一份（`Disconnect` 分别落到 `net.Disconnect` / fault interceptor）。**只对那 5 个场景做，别让全套测试跑这接口**——
> tester1 焊死 labrpc 参数化亏，且 gRPC 故障注入不如 labrpc 确定，跑全套回归更不稳。全量回归留 L1，L2 只验迁移正确性 + gRPC 独有失败模式。

> **必讲点**：labrpc 的 drop = 调用方明确得到 `false`；真 gRPC 多一种 ——
> **服务端已处理、响应回程丢失 → 客户端只看到 timeout**。这是 `(clientId, seq)` 去重（exactly-once）的核心场景，
> 几乎只能在 L2 interceptor 精准构造（注入"服务端正常处理但丢弃响应"）。面试这是 gRPC + 幂等的连环深挖点。
>
> **原则**：高保真层（chaos）负责"发现"，确定性层（labrpc）负责"复现和调试"。

---

## 懂原理·不写（面试延伸题）⚪

> 写成本高 + 同维度重复（都属"分布式系统理解力"），Raft 已经证明了这个维度。
> 搞懂原理、白板能讲清实现思路和 trade-off 即可，一行不写。被问到能接住就立住。

- [ ] ⚪ **2PC / percolator 分布式事务**
  - 2PC 流程、阻塞问题、单点问题
  - percolator 怎么用快照隔离 + primary lock 改进 2PC
  - 怎么架在 Raft KV 之上（用 KV 存事务锁和数据）
  - 一句 trade-off：为什么 TiKV 用 percolator 而非裸 2PC
- [ ] ⚪ **joint consensus 成员变更**：怎么加 / 删节点不停服
- [ ] ⚪ **ReadIndex / lease read 线性化读优化**：高频追问点

---

## Phase 6 · 硬化（可选，放最后，边投边补）

> 求职几乎不问，时间够再补，不做投递前提。

- [ ] ⚪ 成员变更（已在"懂原理不写"覆盖）
- [ ] ~~mTLS / 对外 API 鉴权 / secrets 管理~~ **砍**：求职几乎不问
- [ ] ⚪ 混沌测试（chaos-mesh）—— 一个故障注入 demo 即可，别深搞
- [ ] ~~备份 / 恢复、admin CLI、调试端点~~ **砍**：工程杂活不加分
- [ ] ⚪ 性能：ReadIndex / lease read（已在"懂原理不写"覆盖）

---

## 🚩 投递触发点

**Phase 0–3 + Phase 5 桥接做完 → 开始投。**

Phase 4、八股（Go / Redis / MySQL / Kafka）、chaos-mesh **边投边补**。

> 以早 9 晚 11 的强度，定"全做完再投"大概率会一直拖，然后冒出新的"还差一块"。
> 设一个能投的触发点，剩下的边面边补。k8s 探针、八股、chaos，面试官问到了再说。
> **别让这份计划变成推迟投递的新理由。**

---

## 三大翻车陷阱（实现时盯紧）

1. **Raft log 与状态机混存** → log 高频小写与状态机大写互相拖累，语义也不同。**必须分开**（WAL vs LSM）。→ Phase 2
2. **以为换 gRPC 后测试不用改** → labrpc 故障注入全失效。L1 留旧测试，L2 补 gRPC interceptor。→ 测试策略
3. **用 Deployment 部署 Raft** → 必须 StatefulSet：稳定身份 + 稳定卷，Pod 漂移会丢/错配数据。→ Phase 3
