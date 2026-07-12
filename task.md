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

## Step 2 · 崩溃恢复单测（命门，自己想清楚）✅ 完成

> 在换真 WAL **之前**先把对账测试写好，作为 fileWAL 的验收标尺。

**实现记录**（新包 `src/wal/`，12 个测试全绿含 `-race`）
- 底层原语（Step 3 会复用）：`record.go` 崩溃安全 append-only record log（`[len][crc32][payload]`，
  replay 撞 torn 即截断）；`atomic.go` `AtomicWrite`（tmp→fsync→rename→fsync dir）；`fs.go` 可注入
  `FS`/`File` seam + `OSFS`。
- 故障注入：`fakefs_test.go` 的 `memFS`（立即持久化悲观模型），能造：写一半崩 / rename 前崩 / rename 后崩。
- 三契约 + 三负向变体：
  - #1 replay 抗 torn：`record_test.go`（torn header/payload、crc 位翻转）+ 负向 `replayRecordsNoCRC`（漏过位翻转）。
  - #2 meta 原子：`atomic_test.go`（三崩溃点 path 恒非旧即新）+ 负向 `atomicWriteInPlace`（原地覆盖会撕裂）。
  - #3 SaveSnapshot 顺序：`ordering_test.go`（snapshot 先→空隙可恢复）+ 负向"反序"（先删前缀→丢已提交数据）。
- 负向变体全部断言"能被对账检查抓到"，证明测试是承重的、非摆设。

### 测什么：不是 Raft 算法，是"save 有没有兑现契约"

Raft 是 **persist-before-reply**（授票/回 ack 前先落盘），所以"丢掉没 fsync、没 ack 的写"**算法本身
就容忍**（peer 重发），这类 fault **价值低、不测**。crash 测试测的是 **WAL 有没有兑现算法赖以成立的那个
"save"契约**——三条契约，前两条与"没回就没事"正交，第三条恰恰是 persist-before-reply 保护不到的：

1. **replay 抗 torn tail**（不是"丢没丢"，是"读回来是不是垃圾"）：崩在 record 写一半。naive 实现会
   panic（起不来=可用性）或把垃圾当合法 record 读进来（带错 term/log 起来 → 再去投票/ack → **安全性破**）。
   契约：replay 必须在**最后一条完整 record** 处干净截断。
2. **meta 覆盖原子**（旧 or 新，永不撕裂）：term/votedFor 覆盖崩在中间，非原子写 → 读回撕裂的 votedFor
   → 同 term 投两次 → 脑裂。契约：靠 rename，读到的永远是完整旧值或完整新值。
3. **SaveSnapshot 顺序**（persist-before-reply 够不到的地方）⭐：压缩丢弃的是**早就 committed / applied /
   ack 过**的前缀，reply 是过去发生的。顺序反了（compact marker 先 durable、snapshot 没落）+ 崩在中间
   → 丢已提交数据、本地无法重建。契约：snapshot 必须先 durable，才允许物理动 log 前缀。

### 怎么测：注入 seam + 负向变体

- [x] fileWAL 写盘走可注入 seam（`FS`/`File`），fake 能：① 写一半崩 ② rename 前/后崩。生产用 `OSFS`。
- [x] **不变式断言**：torn 尾巴干净截断不 panic；meta 非旧即新不撕裂；SaveSnapshot 崩在 snapshot-已落/
      前缀-未删时 → 用完整 log 恢复、零丢失。
- [x] **负向变体（让测试立得住）**：(a) `replayRecordsNoCRC` 不校验 crc、(b) `atomicWriteInPlace` 原地覆盖、
      (c) 反序压缩 —— 三个都断言"能被抓到"（必须 fail），非摆设。
- [→ Step 3] **端到端对账用例**（随机 op 序列 → 崩 → `Load()` 对账）需要真 fileWAL 的 `Load()`，随 Step 3
      的 fileWAL 一起落地：把这三契约的原语组合成 `raft1.WAL` 后，跑组合层的随机序列对账。

## Step 3 · 实现 fileWAL（真 append-only + fsync + 重放）

### 核心机关：两把原语（崩溃安全 ≠ 让单次 write+fsync 原子）

崩溃能砸在 write 中间，挡不住。所以**按写模式选原语**，不在那一层求原子：

- **整文件重写（meta / snapshot）→ 原子性来自 `rename`**：写 tmp → fsync(tmp) → rename → fsync(dir)。
  rename 是 POSIX 保证的目录项一次性换 inode，读者只见完整旧或完整新。**直接复用 `persist/file.go`
  的 `atomicWrite`**（注：它吞了 dir-fsync 的 error，真 WAL 想严谨可 propagate）。
  - 两个 fsync 顺序不能变：fsync(tmp) 在 rename **前**（换过去的 inode 要有真数据）；fsync(dir) 在
    rename **后**（持久化的是 rename 这个目录项改动，rename 得先发生）。
- **尾部追加（log）→ 正确性来自 replay 校验**：record = `[len:u32][crc32:u32][payload]`。append-only
  下崩溃只砸没落盘的尾巴，已 fsync 的老 record 不会被后来的崩溃碰坏。

### 决策：压缩用**整体重写**，不做分段（🟣 要能复述）

- **重写的是"活尾巴"（index > snapshotIndex），不是整条 log**。`snapshotIndex = appliedIndex`，稳态下
  applied ≈ 最新，活尾巴恒短（受在途复制窗口界定，非历史总量）。→ 压缩**再频繁**，每次重写也 O(小)：
  压缩本身在删历史，你只拷没被快照盖住的那点尾。"频繁压缩 = 频繁全量拷"是错觉。
- **分段的唯一好处（免重写、O(1) unlink 回收）只在"活 log 很大"时兑现**，本项目活尾恒小 → 不触发；
  而分段要背 4 块复杂度（段 roll / 段→区间索引 / 段 GC / Load 跨段+骑跨段前缀跳过）。→ **不划算**。
- 结论：**重写 over 分段**。热路径 append 两者都 O(1)（这才是 WAL 对"全量 blob 重写"的核心收益，重写
  方案 100% 拿到）；只在冷路径压缩用一次小重写，换掉整个分段体系。分段留作白板题（"etcd 为什么分段"
  = 活 log 大到重写不可接受时，用 O(1) unlink 换 O(活尾)重写）。

### 文件布局（实现：两份文件，snapshot 内嵌 wal —— 见下方取舍）

- `meta`：term / votedFor —— 高频覆盖，原子 rename（原语1）。
- `wal`：可选 `header`(内嵌 base=(N,T) + **snapshot 字节**) + 一串 entry / truncate record。正常 append；压缩时整体重写。

> **取舍：snapshot 内嵌 wal header，不做独立快照文件**（偏离最初"独立文件+顺序铁律"的设想，更简）。
> `SaveSnapshot` = 一次 `AtomicWrite` 重写整个 wal（header含snapshot + suffix），于是"落快照"与"丢弃前缀"
> 在**同一次原子写**里提交 → **顺序 hazard 直接消失**，独立快照文件的 orphan/GC 也不存在。代价：压缩重写
> wal 会连快照一起拷（O(snapshot)），但压缩本就要写新快照、且本项目数据小，可接受。契约 #3 的"顺序"
> 在此设计下退化为"单次原子写"（Step 2 的顺序测试仍留作原理演示：证明"分两次写时"为何必须有序）。

### 各操作实现 ✅ 完成（新包 `src/filewal/`，组合 `src/wal/` 原语）

- [x] `AppendLog`：`RecordLog.Append` 到 wal 尾 → **fsync 后才返回**（回 ack 前须 durable，决策 4）。
- [x] `TruncateSuffix`：append `truncate-to-K` marker（纯 append，不原地改历史）；replay 撞到丢 ≥K。
      site5（冲突）= truncate marker 后紧跟新 entry 的 append，replay 顺序处理 → 旧尾先逻辑丢、再灌新。
- [x] `SaveMeta`：`meta` 文件原子覆盖（`AtomicWrite`），不进 wal。
- [x] `SaveSnapshot`：`AtomicWrite` 整体重写 wal = `header(N,T,snapshot)` + suffix 条目，**一次原子提交**。
- [x] `Load` 重建算法（6.5840 不涉及的净新增，一致性保证所在）：读 meta → 解析 wal record：首条若 header
      取 base=(N,T)+内嵌 snapshot、log 置 `[sentinel@N(T)]`；其余 entry 追加 / truncate 丢 ≥K；撞坏即截断。
      **不变式：log 起点锚定 snapshotIndex（从 header 反推，不信物理布局）** → raft 永不见"snap@10 但 log@5"。
- [x] **组合对账测试**（Step 2 延后的那条）：`filewal_test.go` round-trip 逐字段对账 + 崩溃注入
      （SaveMeta 写 tmp 崩 / append 写一半崩 / 压缩 rename 前后崩）→ 重启 Load 均自洽零丢失。
- **验收**：`go test -race ./filewal/ ./wal/` 全绿（filewal 5 组、wal 12 个）；raft1/kvraft1 build 不受影响。

### binary 注入 ✅ 完成

- [x] `raft.MakeWithWAL(peers, me, nodeIDs, wal, applyCh)`；`Make` 委托 `newPersisterWAL`（L1 路径不动）。
      WAL 接口加 `Size()`（PersistBytes 走它，不再依赖 persister）；fileWAL 的 `Size()` 只算日志字节、不含内嵌快照。
- [x] WAL 顺 `StartKVServerGrpc → MakeRSMGrpc` 传下去（**只改 grpc 变体**）；`cmd/raftkvd` 构造 `filewal.Open(OSFS, dataDir)` 注入。
- [x] KV 层快照读：`StartKVServerGrpc` 改为 `w.Load().Snapshot` 做 Restore（不再 `persister.ReadSnapshot`）。
- [x] 回归：`go test -race ./raft1/`（3A–3D，强制 `-count=1`）`ok 400s`；`./kvraft1/` `ok 249s`；filewal/wal 全绿。

## Step 4 · KV 状态机：内存 map → 磁盘 LSM（pebble）

> 定位见决策 6：这是**存储模型**改造（内存→磁盘、可 > RAM），**不是持久化、不影响正确性**。
> 面试深挖区，价值在把决策 6 那套 + apply-id/two-WAL 协调讲透（换后端本身不难，难点在 apply-id 恢复语义）。

拆两步（分割线见下方"为什么 apply-id 属于 4a"）：**4a = 完整正确的 pebble KV（含 apply-id、复用 raft
现成 blob 快照协议）；4b = 只做 InstallSnapshot 流式发送的优化，scale-only、可选、大概率白板。**

### 为什么 apply-id 属于 4a、不是 4b（🟣 要能复述）

> 曾经想让 4a"raft 一行不改、apply-id 留 4b"，是**错的分割线**。

状态机一旦 durable（pebble 崩了数据还在），重启就**必须**知道它 apply 到哪了，否则 replay 会**双重 apply**
（崩前 `Append(x,"a")` 已落 pebble，重启从 log 头重放又来一遍 → `x="aa"`）。绕开 apply-id 的唯一办法是每次重启
**清空 pebble、从 snapshot+log 全量重建**——那等于把 pebble 的 durability 扔了，pebble 退化成"每次重启重灌的磁盘
map"，纯脚手架、4b 一来就删。所以 **apply-id 是"durable 状态机"的内在要求，不是可选增强**。而"raft 不动"靠的是
**复用 raft 现成的 blob 快照协议（角色 B）**，不是靠那个抛弃式的 wipe-restart。

> raft 快照两个角色：**A 本地恢复**（pebble+apply-id 顶掉，不再用快照）、**B InstallSnapshot 发给落后 follower**
> （协议强制，必须能发）。4a 里 A 用 apply-id，B 复用 raft 现成 blob 路 → raft 本体几乎不改。

### Step 4a · pebble KV（含 apply-id，复用 raft blob 快照）✅ 完成

**实现记录**
- `kvraft1/kvstore.go`：`KVStore` 接口（基础类型签名）+ `mapStore`（L1，行为等于旧 kvMap）。
  KV 后端做成**可插拔**——L1 仍用 map（快、确定性），pebble 只进 binary（像 fileWAL）。
- `kvraft1/pebblestore/`（独立包，import pebble；结构化满足 `KVStore`，不 import kvraft1 → pebble 不进 L1）：
  Get/Put on pebble；`Put` 把「KV 改动 + `applied_index`」放**同一 batch、`pebble.Sync` 提交**；`Snapshot()`=
  `pebble.Checkpoint()`（硬链，不遍历）→ tar 打包 blob；`Restore()`=解包→整库替换。
- `KVServer.DoOp(index, op)`：`index <= store.AppliedIndex()` 跳过（apply-id 去重）；map 恒 -1 不跳。
  `StateMachine.DoOp` 签名加 index（shardkv/rsm-test 忽略）。
- binary：raft fileWAL 落 `dataDir/raft`、pebble 落 `dataDir/db`（分目录，决策 5）；不在启动 Restore（live pebble）。
  新增 `--max-raft-bytes` flag（注意不能叫 `max-raft-state`——tester1 已注册）。

**验收**（全部通过）
- [x] `scripts/test-crash-recovery.sh` PASS（默认，无快照）：pebble 状态机数据跨 `kill -9` 存活。
- [x] `MAXRAFT=400 FILL=15 …` PASS：压缩真触发（`raft/wal` 从 2.6KB 涨到 9.5KB，内嵌 pebble checkpoint blob），压缩后恢复仍正确。
- [x] L1：`go test -race ./kvraft1/` `ok 236s`（map 后端，未受影响）。
- [x] 磁盘布局：`n1/db/`（真 pebble SSTable/MANIFEST）+ `n1/raft/`（fileWAL meta+wal）。
- 未覆盖：pebble `Restore`（整库替换）只在收 InstallSnapshot 触发，全崩 crash 测试不走此路（需落后 follower 场景，属 4b/加固）。
- **验收**：`scripts/test-crash-recovery.sh` 仍 PASS；开 `maxraftstate>0` 让 SaveSnapshot 真触发（走 checkpoint blob）。
- 自知代价：角色 B 的 blob 仍 O(dataset)、且会内嵌进 fileWAL 的 wal header（lab 数据量可接受）。这正是 4b 的优化点。

### Step 4b · InstallSnapshot 流式 checkpoint（可选优化，scale-only）

> 见 memory `filewal-embedded-snapshot-tradeoff`。**仅当快照大到"塞不进一个 blob / 装不下 RAM"才兑现**，
> lab 用不到 → 大概率归"懂原理·不写"。

- [ ] 把角色 B 从"bundle 整个 checkpoint 成 blob 走 raft" 改成"**流式发 checkpoint 文件**"，快照不再进 fileWAL、
      不再整块进内存；wal header 只存引用（applied_index 指向 checkpoint）。
- [ ] 收 InstallSnapshot 的协调点：先换 pebble(applied_index=K) 再置 raft snapshotIndex=K。
- **验收**：数据集 > RAM 也能发/收 InstallSnapshot；`test-crash-recovery.sh` 仍 PASS。

## Step 5 · binary 级端到端对账（真机 kill-all）✅ 完成（全崩场景）

> 唯一让"组合后的 fileWAL"撞真 fsync + 真 kill -9 的测试。也作为 4a/4b 的回归网。

- [x] `scripts/test-crash-recovery.sh`：起 3 节点 → 写 4 个 KV → `kill -9` 全部 → 重启同数据目录 → 校验全部存活。
      **PASS**（fileWAL 真落盘 `meta`+`wal`；重启经 raft 重放 log 重建 KV）。
- [ ]（可选加固）leader-only 崩 / follower-only 崩两种局部场景；version 单调专项校验。
- 注：当前状态机是内存 map、`maxraftstate=-1`（快照关）→ 测的是 log 重放恢复。4a 开快照后重跑，覆盖 SaveSnapshot。

---

## 验收总表（每步的硬门槛）

| Step | 硬门槛 |
|------|--------|
| 1 | raft1(3A-3D)+kvraft1(4A/4B) 全绿含 `-race`，行为逐字节等于改造前 |
| 2 | 对账单测覆盖三契约（replay 抗 torn / meta 原子 / SaveSnapshot 顺序）+ 三负向变体能被抓到 |
| 3 | fileWAL（重写压缩 + Load 锚定 snapshotIndex）通过 Step 2 全部单测（含 `-race`） |
| 4 | KV 状态机走 pebble（磁盘模型）、与 log 分目录；appliedIndex 与写入原子提交；重启无需重建即服务 |
| 5 | 三种崩溃场景端到端对账通过 |

## 备注 / 陷阱
- **每步保持"原测试照旧绿"为硬门槛**（L1 回归网），任一步打破立即停、回滚该步。
- 逐点替换 persist：**一次一个调用点**，改完立即回归，别攒着一起测（关键路径调试成本高）。
- fsync 顺序错 = 崩溃恢复静默错数据，测试可能偶尔漏掉 → 靠 Step 2 的确定性注入兜底。
- 方案 B（etcd 级 record WAL + 段轮转/压缩）、joint consensus、ReadIndex 都是白板延伸，本项目不写。
