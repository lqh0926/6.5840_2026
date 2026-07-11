package wal

import (
	"bytes"
	"testing"
)

// 契约 #3：SaveSnapshot 的顺序铁律 —— snapshot 必须先 durable，才允许动 log 前缀。
//
// 这是 persist-before-reply **保护不到**的地方：压缩丢弃的是早已 committed/applied/ack 过的
// 前缀，reply 是过去发生的。唯一挡着"丢已提交数据"的就是这条 fsync 顺序。
//
// 建模：两份文件 —— "wal"（压缩前含 PREFIX+TAIL，压缩后只剩 TAIL）与 "snapshot"（压缩后
// 才有 SNAP）。可恢复 := snapshot 在（覆盖了前缀状态）或 wal 仍含 PREFIX（能重放出前缀）。
// 丢失 iff：snapshot 不在 且 wal 前缀已删。

var (
	prefixMark = []byte("PREFIX:committed-1..N;")
	tailMark   = []byte("TAIL:N+1..;")
	snapMark   = []byte("SNAP@N")
)

func oldWal() []byte { return append(append([]byte{}, prefixMark...), tailMark...) }

// recoverable：崩溃后还能不能重建出 ≤N 的已提交状态。
func recoverable(fs *memFS) bool {
	snap, _ := fs.content("snapshot")
	snapPresent := bytes.Equal(snap, snapMark)
	wal, _ := fs.content("wal")
	walHasPrefix := bytes.Contains(wal, prefixMark)
	return snapPresent || walHasPrefix
}

func seedPreCompaction(t *testing.T) *memFS {
	t.Helper()
	fs := newMemFS()
	if err := AtomicWrite(fs, ".", "wal", oldWal()); err != nil {
		t.Fatal(err)
	}
	return fs
}

// TestSaveSnapshotOrder_GapIsRecoverable：正确顺序（snapshot 先），崩在两步之间的空隙
// （snapshot 已落、log 前缀还没动）→ 一定可恢复。这是顺序铁律保护的核心窗口。
func TestSaveSnapshotOrder_GapIsRecoverable(t *testing.T) {
	fs := seedPreCompaction(t)

	// 正确顺序 step 1：snapshot 落盘。
	if err := AtomicWrite(fs, ".", "snapshot", snapMark); err != nil {
		t.Fatal(err)
	}
	// —— 崩在这里（还没重写 wal 丢前缀）——
	if !recoverable(fs) {
		t.Fatal("crash after snapshot-durable, before log-prefix-touched must be recoverable")
	}
}

// TestSaveSnapshotOrder_CrashDuringWalRewrite：正确顺序，崩在第二步（重写 wal 丢前缀）
// 中途 —— 无论 wal 是旧是新，snapshot 已在 → 仍可恢复。
func TestSaveSnapshotOrder_CrashDuringWalRewrite(t *testing.T) {
	fs := seedPreCompaction(t)
	if err := AtomicWrite(fs, ".", "snapshot", snapMark); err != nil {
		t.Fatal(err)
	}
	fs.faultRename = "after" // 重写 wal 时崩在 rename 后：wal=新（前缀已丢）
	_ = AtomicWrite(fs, ".", "wal", tailMark)

	if wal, _ := fs.content("wal"); bytes.Contains(wal, prefixMark) {
		t.Fatal("setup: expected wal prefix dropped by the crash-after-rename")
	}
	if !recoverable(fs) {
		t.Fatal("snapshot durable → must still be recoverable even after prefix dropped")
	}
}

// TestSaveSnapshotFullCompaction：完整跑完正确顺序 → 可恢复且前缀确实回收。
func TestSaveSnapshotFullCompaction(t *testing.T) {
	fs := seedPreCompaction(t)
	if err := AtomicWrite(fs, ".", "snapshot", snapMark); err != nil {
		t.Fatal(err)
	}
	if err := AtomicWrite(fs, ".", "wal", tailMark); err != nil {
		t.Fatal(err)
	}
	if !recoverable(fs) {
		t.Fatal("completed compaction must be recoverable")
	}
	if wal, _ := fs.content("wal"); bytes.Contains(wal, prefixMark) {
		t.Fatal("completed compaction should have reclaimed the prefix")
	}
}

// TestNegativeVariant_ReversedOrder_LosesData：**故意反序**（先重写 wal 丢前缀、后写
// snapshot），崩在空隙 → snapshot 不在 且 前缀已删 → 丢已提交数据。断言对账检查能抓到它。
func TestNegativeVariant_ReversedOrder_LosesData(t *testing.T) {
	fs := seedPreCompaction(t)

	// 反序 step 1：先重写 wal 丢前缀。
	if err := AtomicWrite(fs, ".", "wal", tailMark); err != nil {
		t.Fatal(err)
	}
	// —— 崩在这里（snapshot 还没写）——
	if recoverable(fs) {
		t.Fatal("reversed order should have LOST data here (snapshot absent && prefix gone), " +
			"but recoverable() reports OK → the ordering contract is not being checked")
	}
}
