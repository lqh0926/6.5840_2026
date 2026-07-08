# Phase 2 · 存储层重做：Raft WAL + 状态机落磁盘（LSM）— 实现任务

> 面试深挖区，必须吃透。**这里是两条互不相干的轴，别塞进"持久化"一个词：**
> - **WAL（Raft log/meta）= 真持久化，命门、承重正确性**：append-only + fsync，崩溃后靠它对账重建。
> - **LSM（KV 状态机）= 存储模型，与持久化/正确性无关**：把 kvstate 从「内存 map」换成「磁盘结构」，
>   让状态可 > RAM（对标 etcd 的 bbolt / TiKV 的 RocksDB）。状态机 durability **不承重正确性**——
>   真值永远是「log + snapshot」，状态机丢了能重建；所以它不是"持久化"，是"内存模型→磁盘模型"。
>
> 总验收：① raft1 / kvraft1 全部原测试照旧绿（含 `-race`）；② 真 WAL 自己单测通过；
> ③ binary 级 kill-all-重启对账正确（数据不丢、不错）。
> 核心纪律：**先扩接口 → 逐个 persist 调用点替换 → 每步跑 raft1/kvraft1 全回归**，别一把梭。

---

## 现状（改造起点）

- `persist.Persister` 只有 `Save(raftstate, snapshot)` —— **全量 blob 原子写**语义。
- `raft.persist()`（`raft1/raft.go:105`）每次把 `term + 整条 logs + voteFor + snapshotIndex`
  用 labgob 打包，连同 `snapshot` **整体重写**。连"投一票"都要重写整条 log（O(log) 每次）。
- 全代码里 **只有 7 个 `rf.persist()` 调用点**，但每个的真实写意图只碰一小块。
- 已有两个实现：`tester.Persister`（内存，L1 测试用）、`persist.FilePersister`（bundle 文件，binary 用）。

### 7 个 persist() 调用点 → 新接口映射（拆接口的地基）

| # | 位置 | 触发场景 | 实际改了什么 | 应落到的新接口 |
|---|------|----------|--------------|----------------|
| 1 | `raft.go:159` | `Snapshot()` | 裁 log 前缀 + 设 snapshot/snapshotIndex | **Compact**(upto, snap) |
| 2 | `raft.go:184` | `stepDown()` | term / voteFor="" | **SaveMeta**(term, vote) |
| 3 | `raft.go:209` | RequestVote 授票 | voteFor = candidate | **SaveMeta** |
| 4 | `raft.go:262` | InstallSnapshot handler | snapshotIndex + 裁 log 前缀 | **Compact** |
| 5 | `raft.go:317` | AppendEntries 冲突 | 截尾冲突 suffix + append 新 entries | **TruncateSuffix** + **AppendLog** |
| 6 | `raft.go:436` | `Start()` | append 一条 entry | **AppendLog**(entries) |
| 7 | `raft.go:459` | `startElection()` | term++ / voteFor=self | **SaveMeta** |

> 观察：7 点里 **3 个只碰元数据**（2/3/7），**2 个只碰 log 追加/截尾**（5/6），**2 个是快照压缩**（1/4）。
> 现在它们全走"重写整条 log"的 `Save`，这就是要拆的根因——接口只能表达"存全量"，表达不了"追加一条"。

---

## 设计决策（🟣 要能不看代码复述）

### 决策 1 · 为什么要扩接口，而非在 FilePersister 内部优化
`Save(blob)` 只拿到整块字节，**看不到增量**（哪几条是新 append、从哪截尾）。
真 append-only WAL 的前提是接口能表达"只追加这一条"。所以必须**把 `Save` 拆成按操作分开的方法**，
raft 在每个调用点告诉存储层"我这次到底干了什么"，存储层才能只 append 而非全量重写。

### 决策 2 · 新接口（方案 A，真 WAL；方案 B 段轮转当白板延伸题）
```go
type WAL interface {
    SaveMeta(term int, vote NodeID)      // 覆盖、极小、频繁 → 小文件原地覆盖
    AppendLog(entries []LogEntry)        // 追加为主 → append-only + fsync ← 命门
    TruncateSuffix(fromIndex int)        // 冲突截尾（丢尾部）
    Compact(uptoIndex int, snap []byte)  // 快照 + 丢 log 前缀
    Load() (State, error)                // 启动重放/恢复
}
```
> 签名以实现时最终为准，`LogEntry`/`NodeID` 复用 raft1/transport 现有类型。
> `SaveMeta` 独立小文件覆盖，不进 WAL（WAL 是 append-only，覆盖语义放这里是污染）。

### 决策 3 · 两份实现，保住 L1（关键）
raft 依赖新接口，给**两份实现**：
- **`adapter over tester.Persister`（L1 用）**：新接口每个方法都落到
  「更新内存里的全量模型 → 调旧 `Save(整 blob)`」，**行为逐字节等于今天**。
  必须 wrap `tester.Persister`（durable 字节跨重启活在它里面，崩溃恢复才成立）。
  → L1 测的是**算法正确性**，不测 WAL。
- **`fileWAL`（binary 用）**：真 append-only record + fsync + 重放。
  → 它的 append/truncate/compact/fsync 正确性**自己单测** + binary 级 kill-all-重启对账。

> 为什么不让 L1 直接跑 fileWAL：L1 焊死 labrpc + 内存模型是确定性回归网，
> 引入真磁盘 I/O 会让回归变慢变脆；WAL 正确性用专门单测 + binary 对账，职责分离。

### 决策 4 · fsync 纪律（命门，面试必答）
- `AppendLog` **fsync 成功后才算持久化**，且**必须在 commitIndex 推进之前**
  （否则 leader 认为已提交、崩溃后 log 却没落盘 → 丢已提交数据）。
- `Compact`：**snapshot fsync 成功后，再删 log 前缀**（顺序不能反，否则崩在中间 → 前缀和快照都没了）。
- `TruncateSuffix`：截尾要持久化后才能接受更前的新 entries。
- 元数据（term/vote）覆盖写也要 fsync 后才算数（投票持久化是不重复投票的地基）。

### 决策 5 · Raft log 与 KV 状态机分离存储（翻车陷阱 #1）
- Raft log/metadata → WAL（append-only 高频小写 + fsync）。
- KV 状态机 → LSM（pebble），独立目录。
- 二者写模式完全不同（log 高频顺序追加 vs 状态机随机点查/范围扫），混存互相拖累。这是分离目标。

### 决策 6 · LSM 的价值 = 存储模型（内存→磁盘），不是持久化（🟣 面试防坑）

**别把 Step 4 讲成"给状态机补持久化"——那是错的，也不影响正确性。** 正确框架：

- **状态机的 durability 不承重正确性**：Raft 保证状态机永远能从「持久化的 log + 持久化的 snapshot」
  重建。状态机在内存 map（崩了丢、重建）还是在 pebble（崩了还在、但也可以丢了重建）——正确性一样。
  真值（source of truth）是 log+snapshot，状态机存储只是它俩的一个物化视图。
- **pebble 的唯一实义 = 把 kvstate 从「内存模型」变成「磁盘模型」**：状态可以大于 RAM，冷数据在盘、
  热集在 block cache。对标 etcd（bbolt）/ TiKV（RocksDB）。这是**容量/存储模型**问题，不是持久化。
- **"快照"的两条路要分清**（否则会得出"pebble 没用"的错误结论）：
  | | 本地压缩（高频、纯本地） | InstallSnapshot RPC（罕见、走网络） |
  |---|---|---|
  | blob 方案 | 每次全量重序列化整个 map | 传输 O(数据集) + 接收方整块解码重建 |
  | pebble 方案 | 只推 appliedIndex 水位 + 砍 log 前缀，**不重序列化** | **一样** O(数据集)，谁都逃不掉 |
  pebble 的好处全在**本地压缩**这条高频路；InstallSnapshot 那条本就罕见、且任何引擎都是全量重建。
- **一个真 gotcha（面试可讲）**：快照是 live key 镜像、**不带删除（tombstone）**，所以把收到的
  InstallSnapshot 装进一个已有数据的 pebble 时**不能 merge**（旧 key 不会被"删"掉）——只能**整库替换**
  （另建 pebble 灌满 → 原子换掉旧的）。TiKV region snapshot 即此做法。
- **成本诚实**：lab 数据量下这些好处都看不到（map 小、重序列化不要钱）→ Step 4 在本项目是**纯深度演示**，
  不是功能刚需。保留它的理由是"面试深挖区、把上面这套讲清"，不是"项目需要它"。工作量很小（换个后端）。

---

## Step 1 · 扩接口 + adapter（不碰真 WAL，先保 L1 绿）✅ 完成

> 目标：把 7 个 `rf.persist()` 换成语义化调用，但底座仍是"更新全量模型→旧 Save"，
> **行为逐字节不变**。这一步跑通即证明"接口拆得对、映射对"，是最安全的第一步。

**实现记录**
- `raft1/wal.go`：`WAL` 接口（`SaveMeta`/`AppendLog`/`TruncateSuffix`/`SaveSnapshot`/`Load`）+
  `PersistState` + `persisterWAL` 适配器 + 唯一编码真源 `encodeRaftState`/`decodeRaftState`。
  - **接口放 `raft1` 包**（不放 `persist/`）：接口引用 `LogEntry`（raft 类型），若放 persist 会
    `persist→raft1→persist` 循环。真 fileWAL（Step 3）在独立包实现 `raft1.WAL` 并 import raft1，
    raft1 靠依赖注入拿实现、不反向 import，无环。
  - **接口只 5 个方法（非 6）**：InstallSnapshot 与服务层 Snapshot 结果形态一致
    （`[哨兵]+suffix`），差异仅在 sentinelTerm/suffix 由调用点算好传入 → 用一个 `SaveSnapshot`
    统一，不在持久化层重推两套日志变换。两处调用点因此是**同一行** `SaveSnapshot(idx, logs[0].Term, snap, logs[1:])`。
  - adapter 关键正确性：① 构造时从底层 seed 全量模型（否则首写用空 logs 覆盖磁盘）；
    全新节点 seed 哨兵 `[{Term:0}]` 与 Make 字面量一致。② `SaveSnapshot` 复制 suffix（传入是
    `rf.logs[1:]`，与 rf 共享底层数组，不复制会被 rf 后续改动污染）。
- `raft1/raft.go`：加 `wal WAL` 字段；删 `persist()`/`readPersist()`；7 个调用点逐一替换；
  Make 启动读走 `wal.Load()`。site5（冲突）= `TruncateSuffix(logIdx)` + `AppendLog(entries)` 两步。

**验收**（全部通过）
- [x] `go build ./raft1/... ./kvraft1/... ./shardkv1/...`（`main/` 旧 lab 的报错是既有、无关）
- [x] `go test -race ./raft1/` 全绿（3A–3D 含持久化/快照）—— `ok 416.5s`
- [x] `go test -race ./kvraft1/` 全绿（4A/4B，重度走 SaveSnapshot）—— `ok 271.4s`
- [ ] （可选加固）`make RUN=... shardkv` 抽样，确认 rsm→raft 持久化路径无碍

## Step 2 · 崩溃恢复单测（命门，自己想清楚）

> 在换真 WAL **之前**先把对账测试写好，作为 fileWAL 的验收标尺。

- [ ] 设计 kill-9-重启对账用例：随机 append/truncate/compact/saveMeta 序列 → 重启 → `Load` 结果必须等于崩溃点前"已 fsync 承诺"的状态
- [ ] 明确不变式：已 fsync 的 AppendLog 必须在；未 fsync 的可有可无；Compact 后前缀必须不可见且 snapshot 在
- [ ] 用文件级 fault 注入模拟"写一半崩"（截断 tmp / 中途 kill）

## Step 3 · 实现 fileWAL（真 append-only + fsync + 重放）

- [ ] WAL record 格式：`[len][crc][type][payload]`，type ∈ {append, truncate, meta, compact-marker}
- [ ] `AppendLog`：seek 到尾追加 record → fsync；`Load` 顺序重放 records 重建 log
- [ ] `TruncateSuffix`：写一条 truncate record（重放时逻辑截尾），不物理改历史
- [ ] `SaveMeta`：独立 `meta` 小文件原子覆盖（tmp→fsync→rename→fsync dir），不进 WAL
- [ ] `Compact`：snapshot 独立文件原子写+fsync → **后**写 compact record 丢 log 前缀（顺序！）
- [ ] 崩溃安全：重放遇 crc 失败 / 半截 record → 在最后一条完整 record 处截断（尾部撕裂容忍）
- [ ] 段轮转/压缩（避免 WAL 无限增长）—— 最简即可；etcd 级段管理当白板延伸题，别过度
- **验收**：Step 2 的对账单测全绿（含 `-race`）

## Step 4 · KV 状态机：内存 map → 磁盘 LSM（pebble）

> 定位见决策 6：这是**存储模型**改造（内存→磁盘、可 > RAM），**不是持久化、不影响正确性**。
> 本项目纯深度演示，工作量很小（换个状态机后端），价值在把决策 6 那套讲透。

- [ ] `cmd/raftkvd` 的 KV 状态机后端从内存 map 换成 pebble（`transport/grpc/kv_service.go` 那层）
- [ ] appliedIndex **与 KV 写入同一个 pebble write batch 原子提交**（two-WAL 协调：重启后知道从
      Raft log 哪条续放；这是状态机落盘唯一真正深的点）
- [ ] 快照：本地压缩只推 appliedIndex 水位 + 砍 Raft log 前缀；InstallSnapshot 的 wire 镜像按需产出
- [ ] **收到 InstallSnapshot = 整库替换**（另建 pebble 灌满 → 原子换旧的），不能 merge（快照无 tombstone）
- [ ] Raft log(WAL) 与 KV 状态机(pebble) **物理分目录**（决策 5）
- [ ] go.mod 引入 pebble；评估依赖体积（可接受，行业标准）
- **验收**：binary kill-all-重启，KV 数据 + raft log 都在，put/get/version 一致；数据集 > RAM 也能服务

## Step 5 · binary 级端到端对账（最终验收）

- [ ] `scripts/run-local-cluster.sh` 起 3 节点 → 写入若干 KV → `kill -9` 全部 → 重启
- [ ] 校验：leader 重新选出、log 无丢失、KV 全部命中、version 单调
- [ ] 覆盖：leader 崩 / follower 崩 / 全崩三种

---

## 验收总表（每步的硬门槛）

| Step | 硬门槛 |
|------|--------|
| 1 | raft1(3A-3D)+kvraft1(4A/4B) 全绿含 `-race`，行为逐字节等于改造前 |
| 2 | 崩溃对账单测覆盖 append/truncate/compact/meta + 撕裂尾部 |
| 3 | fileWAL 通过 Step 2 全部单测（含 `-race`） |
| 4 | KV 状态机走 pebble（磁盘模型）、与 log 分目录；appliedIndex 与写入原子提交；重启无需重建即服务 |
| 5 | 三种崩溃场景端到端对账通过 |

## 备注 / 陷阱
- **每步保持"原测试照旧绿"为硬门槛**（L1 回归网），任一步打破立即停、回滚该步。
- 逐点替换 persist：**一次一个调用点**，改完立即回归，别攒着一起测（关键路径调试成本高）。
- fsync 顺序错 = 崩溃恢复静默错数据，测试可能偶尔漏掉 → 靠 Step 2 的确定性注入兜底。
- 方案 B（etcd 级 record WAL + 段轮转/压缩）、joint consensus、ReadIndex 都是白板延伸，本项目不写。
