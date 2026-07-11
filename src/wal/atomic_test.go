package wal

import (
	"bytes"
	"testing"
)

// atomicWriteInPlace 是**故意有 bug** 的负向变体：原地覆盖，不走 tmp+rename。
// 崩在写一半 → path = 新前缀 + 旧尾巴 = 撕裂。用来证明 AtomicWrite 的 rename 是承重的。
func atomicWriteInPlace(fs FS, path string, data []byte) error {
	f, err := fs.Open(path) // 覆盖已存在文件（offset 0）
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

var (
	oldVal = []byte("OLD_VALUE_AAAA")
	newVal = []byte("NEW_VALUE_BBBB") // 与 old 等长，便于制造清晰的撕裂
)

// isTorn：内容既不是完整旧值、也不是完整新值 → 撕裂。
func isTorn(got []byte) bool {
	return !bytes.Equal(got, oldVal) && !bytes.Equal(got, newVal)
}

// TestAtomicWrite_NeverTorn：在每个崩溃点，path 都只会是完整旧值或完整新值。
func TestAtomicWrite_NeverTorn(t *testing.T) {
	cases := []struct {
		name  string
		setup func(*memFS)
		want  []byte // 期望崩溃后 path 的内容
	}{
		{"crash writing tmp", func(m *memFS) { m.faultWriteLimit = 3 }, oldVal},
		{"crash before rename", func(m *memFS) { m.faultRename = "before" }, oldVal},
		{"crash after rename", func(m *memFS) { m.faultRename = "after" }, newVal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := newMemFS()
			if err := AtomicWrite(fs, ".", "meta", oldVal); err != nil {
				t.Fatalf("seed write: %v", err)
			}
			tc.setup(fs)
			_ = AtomicWrite(fs, ".", "meta", newVal) // 预期崩溃、返回 error

			got, ok := fs.content("meta")
			if !ok {
				t.Fatal("meta disappeared")
			}
			if isTorn(got) {
				t.Fatalf("TORN content after crash: %q", got)
			}
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("content: got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestNegativeVariant_InPlace_CanTear：同一个"写一半崩"，AtomicWrite 保持旧值不撕裂，
// 而原地覆盖变体会撕裂。证明 rename 契约是承重的、且测试能区分正确与错误实现。
func TestNegativeVariant_InPlace_CanTear(t *testing.T) {
	// 正确实现：写 tmp 崩 → path 仍是完整旧值。
	fsGood := newMemFS()
	if err := AtomicWrite(fsGood, ".", "meta", oldVal); err != nil {
		t.Fatal(err)
	}
	fsGood.faultWriteLimit = 3
	_ = AtomicWrite(fsGood, ".", "meta", newVal)
	good, _ := fsGood.content("meta")
	if isTorn(good) {
		t.Fatalf("AtomicWrite should never tear, got %q", good)
	}

	// 负向变体：原地覆盖崩在写一半 → 撕裂。
	fsBad := newMemFS()
	if err := AtomicWrite(fsBad, ".", "meta", oldVal); err != nil {
		t.Fatal(err)
	}
	fsBad.faultWriteLimit = 3
	_ = atomicWriteInPlace(fsBad, "meta", newVal)
	bad, _ := fsBad.content("meta")

	if !isTorn(bad) {
		t.Fatalf("in-place variant should tear on mid-write crash, got %q (test not exercising the bug)", bad)
	}
}
