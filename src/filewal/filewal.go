// Package filewal 把 wal 包的崩溃安全原语（RecordLog / AtomicWrite）组合成 raft.WAL
// 的真实现（Phase 2 Step 3）。它是 raft.WAL 接口的另一份实现（对应 raft1 内置的
// persisterWAL 适配器），由 binary 注入使用；L1 测试仍走适配器，不碰这里。
//
// 磁盘布局（dir 下两份文件，按写模式分）：
//   - meta：term / votedFor —— 高频覆盖，走 AtomicWrite（rename 换原子）。
//   - wal ：可选 header record + 一串 entry / truncate record —— 正常 append；压缩时整体重写。
//
// 快照**内嵌在 wal 的 header record** 里，而非独立文件：SaveSnapshot = 一次 AtomicWrite
// 重写整个 wal（header含snapshot + suffix），于是快照与"丢弃前缀"在**同一次原子写**里提交
// —— 顺序 hazard 与独立快照文件的 orphan/GC 问题都消失。代价是压缩时重写 wal 会连快照一起
// 拷（O(snapshot)），但压缩本就要写新快照、且本项目数据量小，可接受。
package filewal

import (
	"bytes"
	"fmt"
	"path/filepath"

	"6.5840/labgob"
	raft "6.5840/raft1"
	"6.5840/transport"
	"6.5840/wal"
)

// record 类型标签（payload 的第 0 字节）。
const (
	recHeader   byte = 1 // walHeader：base=(snapshotIndex, sentinelTerm) + 内嵌 snapshot
	recEntry    byte = 2 // 一条 raft.LogEntry
	recTruncate byte = 3 // 冲突截尾：绝对 fromIndex
)

// walHeader 是 wal 文件的首条 record（仅压缩后存在），记录压缩基点与内嵌快照。
type walHeader struct {
	SnapshotIndex int
	SentinelTerm  int
	Snapshot      []byte
}

// metaState 是 meta 文件的内容。
type metaState struct {
	Term int
	Vote transport.NodeID
}

// FileWAL 实现 raft.WAL。
type FileWAL struct {
	fs       wal.FS
	dir      string
	metaPath string
	walPath  string

	rl      *wal.RecordLog // wal 文件的末尾追加句柄
	records [][]byte       // Open 时读回并 healing 后的 record（供 Load 一次性解析）
}

var _ raft.WAL = (*FileWAL)(nil)

// Open 在 dir 下打开（或初始化）FileWAL：healing wal 的撕裂尾巴、准备追加句柄。
// 调用方需保证 dir 已存在（binary 侧 MkdirAll）。随后应立即调 Load() 取回状态。
func Open(fs wal.FS, dir string) (*FileWAL, error) {
	fw := &FileWAL{
		fs:       fs,
		dir:      dir,
		metaPath: filepath.Join(dir, "meta"),
		walPath:  filepath.Join(dir, "wal"),
	}
	rl, records, err := wal.OpenRecordLog(fs, fw.walPath)
	if err != nil {
		return nil, err
	}
	fw.rl = rl
	fw.records = records
	return fw, nil
}

// Load 从 meta + wal record 重建全量持久化状态。
// 不变式：log 起点锚定在 wal header 的 snapshotIndex（从快照反推，不信物理布局），
// raft 永远拿不到 "snap@10 但 log@5" 的错配。
func (fw *FileWAL) Load() (raft.PersistState, bool) {
	var st raft.PersistState

	metaBytes, err := fw.fs.ReadFile(fw.metaPath)
	must(err)
	metaExists := len(metaBytes) > 0
	if metaExists {
		m := decodeMeta(metaBytes)
		st.Term, st.Vote = m.Term, m.Vote
	}

	// 解析 wal record：首条若为 header 则确定 base + 内嵌快照，其余为 entry / truncate。
	base, sentinelTerm := 0, 0
	var snapshot []byte
	recs := fw.records
	if len(recs) > 0 && recs[0][0] == recHeader {
		h := decodeHeader(recs[0][1:])
		base, sentinelTerm, snapshot = h.SnapshotIndex, h.SentinelTerm, h.Snapshot
		recs = recs[1:]
	}
	logs := []raft.LogEntry{{Term: sentinelTerm}} // 哨兵 @ base
	for _, p := range recs {
		switch p[0] {
		case recEntry:
			logs = append(logs, decodeEntry(p[1:]))
		case recTruncate:
			k := decodeTruncate(p[1:])
			logs = logs[:k-base] // 绝对索引 → 相对下标（与 rf.relIdx 同公式）
		default:
			panic(fmt.Sprintf("filewal: unknown record type %d", p[0]))
		}
	}

	st.Logs = logs
	st.SnapshotIndex = base
	st.Snapshot = snapshot
	return st, metaExists || len(fw.records) > 0
}

// SaveMeta 覆盖 meta 文件（原子）。
func (fw *FileWAL) SaveMeta(term int, vote transport.NodeID) {
	must(wal.AtomicWrite(fw.fs, fw.dir, fw.metaPath, encodeMeta(metaState{Term: term, Vote: vote})))
}

// AppendLog 追加日志条目（每条一 record，fsync 后返回）。
func (fw *FileWAL) AppendLog(entries []raft.LogEntry) {
	for i := range entries {
		must(fw.rl.Append(encodeEntry(entries[i])))
	}
}

// TruncateSuffix 追加一条 truncate marker（纯 append，不原地改历史）。
func (fw *FileWAL) TruncateSuffix(fromIndex int) {
	must(fw.rl.Append(encodeTruncate(fromIndex)))
}

// SaveSnapshot 整体重写 wal：新 header（含内嵌 snapshot）+ suffix 条目，一次原子写。
// 快照与"丢弃前缀"在同一次 AtomicWrite 提交，无中间窗口。
func (fw *FileWAL) SaveSnapshot(snapshotIndex, sentinelTerm int, snapshot []byte, suffix []raft.LogEntry) {
	var content []byte
	content = wal.AppendRecord(content, encodeHeader(walHeader{
		SnapshotIndex: snapshotIndex,
		SentinelTerm:  sentinelTerm,
		Snapshot:      snapshot,
	}))
	for i := range suffix {
		content = wal.AppendRecord(content, encodeEntry(suffix[i]))
	}
	must(wal.AtomicWrite(fw.fs, fw.dir, fw.walPath, content))

	// 旧追加句柄指向被 rename 换掉的旧 inode，失效 → 重开到新 wal 的末尾。
	fw.rl.Close()
	rl, _, err := wal.OpenRecordLog(fw.fs, fw.walPath)
	must(err)
	fw.rl = rl
}

// Close 关闭底层追加句柄。
func (fw *FileWAL) Close() error { return fw.rl.Close() }

// --- 编解码（record payload = [type byte][labgob content]）---

func encodeEntry(e raft.LogEntry) []byte    { return append([]byte{recEntry}, gobEncode(e)...) }
func encodeTruncate(k int) []byte           { return append([]byte{recTruncate}, gobEncode(k)...) }
func encodeHeader(h walHeader) []byte        { return append([]byte{recHeader}, gobEncode(h)...) }
func encodeMeta(m metaState) []byte          { return gobEncode(m) }

func decodeEntry(b []byte) raft.LogEntry {
	var e raft.LogEntry
	gobDecode(b, &e)
	return e
}
func decodeTruncate(b []byte) int {
	var k int
	gobDecode(b, &k)
	return k
}
func decodeHeader(b []byte) walHeader {
	var h walHeader
	gobDecode(b, &h)
	return h
}
func decodeMeta(b []byte) metaState {
	var m metaState
	gobDecode(b, &m)
	return m
}

func gobEncode(v any) []byte {
	var buf bytes.Buffer
	if err := labgob.NewEncoder(&buf).Encode(v); err != nil {
		panic(fmt.Sprintf("filewal: gob encode: %v", err))
	}
	return buf.Bytes()
}

func gobDecode(b []byte, v any) {
	if err := labgob.NewDecoder(bytes.NewReader(b)).Decode(v); err != nil {
		panic(fmt.Sprintf("filewal: gob decode: %v", err))
	}
}

// must 把 IO 错误升级为 panic —— 与 raft 对持久化失败"不可安全恢复即 fatal"的假设一致
// （旧 FilePersister.Save 亦然）。
func must(err error) {
	if err != nil {
		panic(fmt.Sprintf("filewal: %v", err))
	}
}
