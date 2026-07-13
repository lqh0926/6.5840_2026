# pebble 速查（Phase 2 Step 4a 用到 + 常用）

> pebble = RocksDB 的 Go 版嵌入式 LSM：写先进 **memtable + WAL** → flush 成 **SSTable** → 后台**分层
> compaction**。点写点读走 Set/Get，范围扫走 Iterator，一致快照走 Checkpoint。单进程（LOCK 文件），
> 崩溃恢复靠它**自己的 WAL**（和 raft 的 fileWAL 是两套，别混——two-WAL）。

## 布隆过滤器：默认**不带**，要手动开

源码 `FilterPolicy` 字段注释原话："The default value means to use no filter"。`&pebble.Options{}` = 无过滤器。

```go
import "github.com/cockroachdb/pebble/bloom"
opts := &pebble.Options{}
opts.Levels = []pebble.LevelOptions{{
    FilterPolicy: bloom.FilterPolicy(10), // 每 key 10 bit ≈ 1% 假阳性（RocksDB 经典值）
    FilterType:   pebble.TableFilter,     // 表级：省一次 index lookup，官方推荐
}}
```

**作用**：专治**点查未命中**——Get 前先问布隆"这 SSTable 可能有该 key 吗"，答"没有"就跳过盘读。
对"每次 Put 先 Get 判版本、且多为 create（key 不存在）"的 workload 净赚；对范围扫描无用（布隆只帮点查）。
（本项目暂未开，够用；开了是标准调优 + 面试点。）

## 核心 API（pebblestore 用到）

```go
pebble.Open(dir, *pebble.Options) (*DB, error)   // dir 存 SSTable/WAL/MANIFEST/CURRENT/OPTIONS
db.Set(key, val []byte, *WriteOptions) error     // 点写
db.Get(key []byte) (val []byte, closer io.Closer, err error)
    // ★ closer 必须 Close：val 零拷贝指向内部 buffer，Close 前有效；用完拷走再 Close
    // 缺 key → err == pebble.ErrNotFound
db.NewBatch() *Batch                             // 原子多写
  b.Set(key, val, *WriteOptions) error
  b.Commit(pebble.Sync) error                    // = db.Apply(b)，应用；Sync=fsync
  b.Close()                                       // ★ 归还 sync.Pool；Commit 不代做！漏了不丢数据但增 GC
db.Checkpoint(destDir) error                     // 一致磁盘快照：硬链 SSTable，不拷数据（≠ NewSnapshot）
db.Close() error
pebble.Sync   = &WriteOptions{Sync:true}         // 提交即 fsync、立刻 durable
pebble.NoSync = &WriteOptions{Sync:false}        // 快但崩溃丢未 flush 的写
```

**两个必记语义**：
- `pebble.Sync`：每写 fsync → 永远 durable 到最新。本项目靠它保证"live pebble 恢复不丢数据"。换 NoSync 会破。
- `Get` 的 closer：忘 Close 泄漏（pin 住内部块）。

## 常用但没用到（值得知道）

```go
db.Delete(key, opts)                  // 删 key = 写 tombstone（LSM 不原地删，compaction 才回收）
db.DeleteRange(start, end, opts)      // 范围删
db.NewIter(*IterOptions) (*Iterator)  // 范围扫描（LSM 看家本领）：SeekGE/First/Next/Valid/Key/Value/Close
db.NewSnapshot() *Snapshot            // 一致的【内存】读视图（MVCC），随 DB 关闭消失，≠ Checkpoint（磁盘副本）
db.Flush()                            // 强制 memtable → SSTable
db.Ingest([]string)                   // 批量灌外部造好的 SSTable（快；对应收 InstallSnapshot 场景）
db.NewIndexedBatch()                  // 可读 batch（read-your-writes）
db.Metrics()                          // LSM 层级 / compaction / cache 统计
```

**易混点**：`NewSnapshot`（MVCC 内存一致读视图）vs `Checkpoint`（磁盘物理副本，可独立打开/打包带走）。
raft 快照要"能发给 follower" → 用 **Checkpoint**，不是 Snapshot。

## 常用 Options

| 字段 | 作用 |
|------|------|
| `Cache` | 共享 block cache（`pebble.NewCache(bytes)`）；多个 DB 可共享 |
| `MemTableSize` | 单个 memtable 大小（默认 4MB）；越大写越快但内存/恢复重放越多 |
| `MemTableStopWritesThreshold` | memtable 队列积压到几个就阻塞写（反压） |
| `L0CompactionThreshold` / `L0StopWritesThreshold` | L0 文件数触发 compaction / 阻塞写的阈值 |
| `LBaseMaxBytes` | L1 目标大小，决定各层容量（配 `LevelMultiplier`） |
| `Levels[i].Compression` | 每层压缩：`pebble.SnappyCompression`（默认快）/ `ZstdCompression`（更省空间） |
| `Levels[i].FilterPolicy` / `FilterType` | 布隆过滤器（见上，默认无） |
| `Levels[i].TargetFileSize` / `BlockSize` | SSTable 目标大小 / 块大小 |
| `MaxOpenFiles` | 最大打开的 SSTable fd 数 |
| `Logger` | 日志（默认打 stderr；测试可换 no-op） |
| `EventListener` | flush/compaction/WAL 事件回调（可观测性埋点） |
| `FS` | vfs 抽象——**可注入内存 FS 做测试**（和 wal 包的 FS seam 一个思路） |
| `DisableWAL` / `WALDir` | 关 WAL（不 durable）/ WAL 单独放另一目录（分盘） |
| `ReadOnly` | 只读打开 |
| `Comparer` / `Merger` | 自定义 key 排序 / merge 算子 |

> 不设 Options 或设了不全，`Open` 会 `EnsureDefaults()` 补默认（但**不含布隆**）。
