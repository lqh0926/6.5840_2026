package wal

import (
	"errors"
	"os"
)

// ErrCrash 是 MemFS 注入的"崩溃"信号：一旦触发，后续所有操作都失败、durable 状态冻结。
var ErrCrash = errors.New("wal: injected crash")

// MemFS 是可注入崩溃点的内存 FS 实现，用于确定性构造崩溃窗口来测 WAL 崩溃安全。
//
// 采用**立即持久化**模型（Write 直接落到 durable 内容，Sync 是 no-op）——这是悲观模型，
// 让撕裂更容易暴露；正确的 rename 方案在此模型下依旧永不撕裂（rename 是原子 map 交换）。
type MemFS struct {
	files map[string][]byte

	// 故障注入（一次性）：
	FaultWriteLimit int    // >=0：下一次 Write 最多写这么多字节，然后崩溃
	FaultRename     string // "before" | "after" | ""：相对 rename 交换的崩溃时点
	crashed         bool
}

func NewMemFS() *MemFS {
	return &MemFS{files: make(map[string][]byte), FaultWriteLimit: -1}
}

// Content 返回 name 当前的 durable 内容（副本；不存在返回 nil, false）。
func (m *MemFS) Content(name string) ([]byte, bool) {
	b, ok := m.files[name]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), b...), true
}

// Crashed 报告是否已触发注入的崩溃。
func (m *MemFS) Crashed() bool { return m.crashed }

// Reboot 模拟"同一块盘上进程重启"：清掉崩溃/故障注入，但 durable 文件内容保留。
func (m *MemFS) Reboot() {
	m.crashed = false
	m.FaultWriteLimit = -1
	m.FaultRename = ""
}

func (m *MemFS) Create(name string) (File, error) {
	if m.crashed {
		return nil, ErrCrash
	}
	m.files[name] = []byte{}
	return &memFile{fs: m, name: name}, nil
}

func (m *MemFS) Open(name string) (File, error) {
	if m.crashed {
		return nil, ErrCrash
	}
	if _, ok := m.files[name]; !ok {
		return nil, os.ErrNotExist
	}
	return &memFile{fs: m, name: name}, nil
}

func (m *MemFS) OpenAppend(name string) (File, error) {
	if m.crashed {
		return nil, ErrCrash
	}
	if _, ok := m.files[name]; !ok {
		m.files[name] = []byte{}
	}
	return &memFile{fs: m, name: name, off: len(m.files[name])}, nil
}

func (m *MemFS) ReadFile(name string) ([]byte, error) {
	if m.crashed {
		return nil, ErrCrash
	}
	b, ok := m.files[name]
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), b...), nil
}

func (m *MemFS) Truncate(name string, size int64) error {
	if m.crashed {
		return ErrCrash
	}
	b, ok := m.files[name]
	if !ok || int64(len(b)) < size {
		return errors.New("wal: truncate past end")
	}
	m.files[name] = b[:size]
	return nil
}

func (m *MemFS) Rename(oldpath, newpath string) error {
	if m.crashed {
		return ErrCrash
	}
	if m.FaultRename == "before" {
		m.crashed = true
		return ErrCrash // 交换未发生 → path 保持旧
	}
	m.files[newpath] = m.files[oldpath]
	delete(m.files, oldpath)
	if m.FaultRename == "after" {
		m.crashed = true
		return ErrCrash // 交换已发生 → path 已是新
	}
	return nil
}

func (m *MemFS) Remove(name string) error {
	if m.crashed {
		return ErrCrash
	}
	delete(m.files, name)
	return nil
}

func (m *MemFS) SyncDir(dir string) error {
	if m.crashed {
		return ErrCrash
	}
	return nil
}

type memFile struct {
	fs   *MemFS
	name string
	off  int
}

func (f *memFile) Write(p []byte) (int, error) {
	if f.fs.crashed {
		return 0, ErrCrash
	}
	if f.fs.FaultWriteLimit >= 0 {
		lim := f.fs.FaultWriteLimit
		f.fs.FaultWriteLimit = -1 // 一次性
		if lim < len(p) {
			f.writeAt(p[:lim]) // 只落前 lim 字节 → 撕裂
			f.fs.crashed = true
			return lim, ErrCrash
		}
	}
	f.writeAt(p)
	return len(p), nil
}

// writeAt 把 p 覆盖写到 name 的 durable 内容的当前 offset 处（立即持久化）。
func (f *memFile) writeAt(p []byte) {
	cur := f.fs.files[f.name]
	need := f.off + len(p)
	if need > len(cur) {
		cur = append(cur, make([]byte, need-len(cur))...)
	}
	copy(cur[f.off:], p)
	f.fs.files[f.name] = cur
	f.off = need
}

func (f *memFile) Sync() error {
	if f.fs.crashed {
		return ErrCrash
	}
	return nil
}

func (f *memFile) Close() error {
	if f.fs.crashed {
		return ErrCrash
	}
	return nil
}
