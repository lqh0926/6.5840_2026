# 测试覆盖总览（Phase 2）

> 三层模型:每层测**不同的东西**,别混。高保真层负责发现,确定性层负责复现和调试。

| 层 | 底座 | 测什么 | 能不能测崩溃安全 |
|----|------|--------|------------------|
| **L1** 回归网 | labrpc + 内存 `tester.Persister`（经 `persisterWAL` 适配器） | Raft/KV **算法**正确性 + 线性化 | **不能**（无真磁盘/真 kill） |
| **wal / filewal** 单测 | 可注入 `MemFS`（内存 + 故障注入） | WAL 崩溃安全**逻辑**,确定性 | 能（确定性构造崩溃窗口） |
| **binary** kill-all | 真 OS（`OSFS`） | fileWAL 撞真 fsync / 真 kill -9 | 能（真机,Step 5 待做） |

---

## L1 · 回归网（算法正确性）

- `go test -race ./raft1/` —— 3A/3B/3C/3D 全部原测试。含选主/复制/持久化/快照/崩溃恢复（框架级:拿同一
  内存 Persister 重建 raft）。**走 `persisterWAL` 适配器,行为逐字节等于改造前。** ✅ `ok 416.5s`
- `go test -race ./kvraft1/` —— 4A/4B，重度走 `SaveSnapshot` 路径 + 线性化检查。 ✅ `ok 271.4s`
- **结构性盲区**:L1 的"崩溃"= 内存字节存活的重建,**没有** torn write / fsync 顺序 / 真 kill。fileWAL 的
  崩溃安全 L1 一律测不到 → 必须靠下面两层。

---

## wal 包 · 原语层（崩溃安全逻辑，12 个）

### 契约 #1 — replay 抗 torn tail（`record_test.go`）

| 测试 | 覆盖 |
|------|------|
| `TestFramingRoundTrip` | `[len][crc][payload]` 编解码往返;全缓冲有效时 validLen=全长 |
| `TestReplayTornHeader` | 尾部残缺头（< 8 字节）→ 丢弃,validLen 停在最后完整 record |
| `TestReplayTornPayload` | len 声称 100 实际 4 字节 → 越界判 torn,丢弃 |
| `TestReplayBitFlipCRC` | len 完整但 payload 翻转一位 → **crc 抓到**,丢弃（含 setup 自检 crc 确已不符） |
| `TestNegativeVariant_NoCRC_MissesBitFlip` | 负向:`replayRecordsNoCRC` 漏过位翻转（留 2 条）vs 正确实现丢它（留 1 条）→ 证明 crc 承重 |
| `TestRecordLogHealsTornTail` | 文件级（`OSFS`）:写 record→追加垃圾→重开自动截断 healing→还能继续 append |

### 契约 #2 — meta 覆盖原子（`atomic_test.go`）

| 测试 | 覆盖 |
|------|------|
| `TestAtomicWrite_NeverTorn` | 三崩溃点（写 tmp / rename 前 / rename 后）path 恒"完整旧 或 完整新",永不撕裂 |
| `TestNegativeVariant_InPlace_CanTear` | 负向:`atomicWriteInPlace` 原地覆盖崩在写一半 → 撕裂;而 `AtomicWrite` 同场景保持旧值 → 证明 rename 承重 |

### 契约 #3 — SaveSnapshot 顺序（`ordering_test.go`，两文件分离建模）

| 测试 | 覆盖 |
|------|------|
| `TestSaveSnapshotOrder_GapIsRecoverable` | 正确顺序(snapshot 先):崩在两步空隙 → snapshot 在 → 可恢复 |
| `TestSaveSnapshotOrder_CrashDuringWalRewrite` | 崩在重写 wal 的 rename 后(前缀已丢) → snapshot 在 → 仍可恢复 |
| `TestSaveSnapshotFullCompaction` | 完整压缩 → 可恢复且前缀确实回收 |
| `TestNegativeVariant_ReversedOrder_LosesData` | 负向:**反序**(先丢前缀后写 snapshot)崩在空隙 → snapshot 不在+前缀没了 → **丢已提交数据**,断言对账检查抓到 |

> 注:filewal 实现用 **snapshot 内嵌 wal**（单次原子写),契约 #3 在实现里退化为"单次原子提交、无顺序问题"。
> 这组分离建模的测试仍保留,作为"分两次写时为何必须有序"的原理演示 + 负向标尺。

---

## filewal 包 · 组合对账层（`filewal_test.go`）

> 把原语组合成 `raft.WAL` 后,验证**组合**正确 + 崩溃后 `Load()` 重建自洽。手法:`driver` 把每个 op 同时
> 施加到 FileWAL 与参考模型 `ref`;崩溃 = 注入故障 →（`must` panic 模拟进程死）→ `MemFS.Reboot()` 同盘重启
> → 新 FileWAL `Load()` 逐字段对账。

| 测试 | 覆盖 |
|------|------|
| `TestFileWAL_RoundTrip` | meta→append→append→truncate→append→snapshot→append→meta 全序列,重开 Load 逐字段（term/vote/snapIdx/snapshot/logs）对账 |
| `TestFileWAL_CrashDuringSaveMeta` | meta 覆盖崩在写 tmp → 恢复后 meta 保持旧值、log 不变 |
| `TestFileWAL_CrashDuringAppend` | append 写一半崩 → torn record 被 healing 丢弃,log 回到崩前（丢的是没 ack 的,安全） |
| `TestFileWAL_CrashDuringSnapshot/crash_before_rewrite_commits` | 压缩 rename 前崩 → wal 保持旧(完整 log) → 恢复到压缩前,**零丢失** |
| `TestFileWAL_CrashDuringSnapshot/crash_after_rewrite_commits` | 压缩 rename 后崩 → wal 已是压缩态 → 恢复到压缩后 |

共同断言:`Load` 成功、snap/log **锚定一致**（log 起点 = header 的 snapshotIndex）、无已提交数据丢失。

---

## 覆盖矩阵（契约 × 层）

| | L1（算法） | wal 原语 | filewal 组合 | binary 真机 |
|---|---|---|---|---|
| Raft 算法 / 线性化 | ✅ | — | — | ⬜(Step 5) |
| #1 replay 抗 torn | ✗ 测不到 | ✅ | ✅(append 崩) | ⬜ |
| #2 meta 原子 | ✗ | ✅ | ✅(SaveMeta 崩) | ⬜ |
| #3 快照/前缀顺序 | ✗ | ✅(分离建模) | ✅(内嵌原子) | ⬜ |
| snap/log 锚定一致 | ✗ | — | ✅ | ⬜ |

---

## 尚未覆盖（已知缺口）

- **binary kill-all（Step 5）**:真 OS 上起 3 节点 → 写 KV → `kill -9` 全部 → 重启对账。唯一撞真
  fsync/真 kill 的地方,**不是可选**（L1 结构上测不了）。
- **binary 注入回归（Step 3 收尾）**:`MakeWithWAL` 接线后,raft1/kvraft1 需再跑一遍确认老路径没崩。
- **组合层穷举随机序列**:目前是手挑的代表性崩溃点;可加随机 op 序列 × 每个 fsync/rename 边界的穷举对账
  （更强,但当前四个场景已覆盖三契约的组合形态）。
- **pebble 状态机（Step 4）**:换 LSM 后端后的 kill 对账（存储模型,非持久化,见 ROADMAP 决策 6）。
- **dir fsync 失败路径**:`atomicWrite` 目前吞了 dir-fsync error（best-effort）,未测其失败传播。
