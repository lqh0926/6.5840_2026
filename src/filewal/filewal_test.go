package filewal

import (
	"bytes"
	"reflect"
	"testing"

	"6.5840/labgob"
	raft "6.5840/raft1"
	"6.5840/transport"
	"6.5840/wal"
)

// cmd 是测试用的日志命令类型（LogEntry.Command 是 interface{}，labgob 需注册具体类型）。
type cmd struct{ V int }

func init() { labgob.Register(cmd{}) }

func e(v, term int) raft.LogEntry { return raft.LogEntry{Command: cmd{v}, Term: term} }

// ref 是参考模型：按 raft 持久化字段的语义演进，用来对账 Load 的结果。
type ref struct {
	term    int
	vote    transport.NodeID
	logs    []raft.LogEntry
	snapIdx int
	snap    []byte
}

func newRef() *ref { return &ref{logs: []raft.LogEntry{{Term: 0}}} }

func (r *ref) saveMeta(t int, v transport.NodeID) { r.term, r.vote = t, v }
func (r *ref) appendLog(es []raft.LogEntry)       { r.logs = append(r.logs, es...) }
func (r *ref) truncate(k int)                     { r.logs = r.logs[:k-r.snapIdx] }
func (r *ref) saveSnapshot(n, st int, snap []byte, suffix []raft.LogEntry) {
	r.logs = append([]raft.LogEntry{{Term: st}}, suffix...)
	r.snapIdx, r.snap = n, snap
}

// driver 把同一操作同时施加到 FileWAL 和参考模型。
type driver struct {
	fw *FileWAL
	r  *ref
}

func (d *driver) saveMeta(t int, v transport.NodeID) { d.fw.SaveMeta(t, v); d.r.saveMeta(t, v) }
func (d *driver) appendLog(es []raft.LogEntry)       { d.fw.AppendLog(es); d.r.appendLog(es) }
func (d *driver) truncate(k int)                     { d.fw.TruncateSuffix(k); d.r.truncate(k) }
func (d *driver) saveSnapshot(n, st int, snap []byte, suffix []raft.LogEntry) {
	d.fw.SaveSnapshot(n, st, snap, suffix)
	d.r.saveSnapshot(n, st, snap, suffix)
}

// loadFresh 新开一个 FileWAL 对同一磁盘 Load（= 崩溃恢复/重启的读法）。
func loadFresh(t *testing.T, fs wal.FS) (raft.PersistState, bool) {
	t.Helper()
	fw, err := Open(fs, ".")
	if err != nil {
		t.Fatal(err)
	}
	defer fw.Close()
	return fw.Load()
}

func assertState(t *testing.T, st raft.PersistState, ok bool, r *ref) {
	t.Helper()
	if !ok {
		t.Fatal("Load ok=false, want true")
	}
	if st.Term != r.term || st.Vote != r.vote {
		t.Fatalf("meta: got (%d,%q), want (%d,%q)", st.Term, st.Vote, r.term, r.vote)
	}
	if st.SnapshotIndex != r.snapIdx {
		t.Fatalf("snapshotIndex: got %d, want %d", st.SnapshotIndex, r.snapIdx)
	}
	if !bytes.Equal(st.Snapshot, r.snap) {
		t.Fatalf("snapshot: got %q, want %q", st.Snapshot, r.snap)
	}
	if !reflect.DeepEqual(st.Logs, r.logs) {
		t.Fatalf("logs: got %+v, want %+v", st.Logs, r.logs)
	}
}

// TestFileWAL_RoundTrip：一串 meta/append/truncate/snapshot 操作后重开 Load，逐字段对账。
func TestFileWAL_RoundTrip(t *testing.T) {
	fs := wal.NewMemFS()
	fw, err := Open(fs, ".")
	if err != nil {
		t.Fatal(err)
	}
	d := &driver{fw: fw, r: newRef()}

	d.appendLog([]raft.LogEntry{e(10, 1), e(11, 1)}) // idx 1,2
	d.saveMeta(1, "n1")
	d.appendLog([]raft.LogEntry{e(12, 2)}) // idx 3
	d.truncate(3)                          // 丢 idx 3
	d.appendLog([]raft.LogEntry{e(13, 2)}) // idx 3
	d.saveSnapshot(2, 1, []byte("S2"), []raft.LogEntry{e(13, 2)})
	d.appendLog([]raft.LogEntry{e(14, 3)}) // idx 4
	d.saveMeta(3, "n3")
	fw.Close()

	st, ok := loadFresh(t, fs)
	assertState(t, st, ok, d.r)
}

// mustCrash 运行 fn，期望它因注入崩溃而 panic（must() 把 IO 错升级为 panic，模拟进程死）。
func mustCrash(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected a crash (panic), got none")
		}
	}()
	fn()
}

// TestFileWAL_CrashDuringSaveMeta：meta 覆盖崩在写 tmp → 恢复后 meta 保持旧值、log 不变。
func TestFileWAL_CrashDuringSaveMeta(t *testing.T) {
	fs := wal.NewMemFS()
	fw, _ := Open(fs, ".")
	fw.SaveMeta(1, "n1")
	fw.AppendLog([]raft.LogEntry{e(10, 1)})
	fw.Close()

	fs.FaultWriteLimit = 3 // 崩在写 meta.tmp
	mustCrash(t, func() {
		fw2, _ := Open(fs, ".")
		fw2.SaveMeta(2, "n2")
	})
	fs.Reboot()

	st, ok := loadFresh(t, fs)
	r := newRef()
	r.saveMeta(1, "n1") // 旧 meta（新写未提交）
	r.appendLog([]raft.LogEntry{e(10, 1)})
	assertState(t, st, ok, r)
}

// TestFileWAL_CrashDuringAppend：append 崩在写一半 → torn record 被 healing 丢弃，
// log 恢复到崩溃前（丢的是没 ack 的那条，安全）。
func TestFileWAL_CrashDuringAppend(t *testing.T) {
	fs := wal.NewMemFS()
	fw, _ := Open(fs, ".")
	fw.AppendLog([]raft.LogEntry{e(10, 1), e(11, 1)})

	fs.FaultWriteLimit = 3 // 下一条 append 写一半崩
	mustCrash(t, func() { fw.AppendLog([]raft.LogEntry{e(12, 1)}) })
	fs.Reboot()

	st, ok := loadFresh(t, fs)
	r := newRef()
	r.appendLog([]raft.LogEntry{e(10, 1), e(11, 1)}) // e(12) 撕裂被丢
	assertState(t, st, ok, r)
}

// TestFileWAL_CrashDuringSnapshot：压缩（重写 wal）崩在原子写的两侧 —— 无论落在哪，
// Load 都得到一个自洽、无丢失的状态（崩前=完整旧 log；崩后=压缩态），snap/log 锚定一致。
func TestFileWAL_CrashDuringSnapshot(t *testing.T) {
	build := func() (*wal.MemFS, *ref) {
		fs := wal.NewMemFS()
		fw, _ := Open(fs, ".")
		fw.SaveMeta(1, "n1")
		fw.AppendLog([]raft.LogEntry{e(10, 1), e(11, 1), e(12, 1)}) // idx 1,2,3
		fw.Close()
		r := newRef()
		r.saveMeta(1, "n1")
		r.appendLog([]raft.LogEntry{e(10, 1), e(11, 1), e(12, 1)})
		return fs, r
	}

	// 崩在 rename 前：wal 保持旧（完整 log，未压缩）→ 恢复到压缩前，零丢失。
	t.Run("crash before rewrite commits", func(t *testing.T) {
		fs, r := build()
		fs.FaultRename = "before"
		mustCrash(t, func() {
			fw, _ := Open(fs, ".")
			fw.SaveSnapshot(2, 1, []byte("S2"), []raft.LogEntry{e(12, 1)})
		})
		fs.Reboot()
		st, ok := loadFresh(t, fs)
		assertState(t, st, ok, r) // 完整旧 log，snapIdx=0，snap=nil
	})

	// 崩在 rename 后：wal 已是新（压缩态）→ 恢复到压缩后。
	t.Run("crash after rewrite commits", func(t *testing.T) {
		fs, r := build()
		fs.FaultRename = "after"
		mustCrash(t, func() {
			fw, _ := Open(fs, ".")
			fw.SaveSnapshot(2, 1, []byte("S2"), []raft.LogEntry{e(12, 1)})
		})
		fs.Reboot()
		st, ok := loadFresh(t, fs)
		r.saveSnapshot(2, 1, []byte("S2"), []raft.LogEntry{e(12, 1)}) // 压缩态
		assertState(t, st, ok, r)
	})
}
