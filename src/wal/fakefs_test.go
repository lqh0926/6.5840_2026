package wal

import (
	"errors"
	"os"
)

// errCrash 是 fake FS 注入的"崩溃"信号：一旦触发，后续所有操作都失败、durable 状态冻结。
var errCrash = errors.New("injected crash")

// memFS 是可注入崩溃点的内存 FS，用于确定性构造崩溃窗口。
// 采用**立即持久化**模型（write 直接落到 durable 内容，Sync 是 no-op）——这是悲观模型，
// 让撕裂更容易暴露；正确的 rename 方案在此模型下依旧永不撕裂（rename 是原子 map 交换）。
type memFS struct {
	files map[string][]byte

	// 故障注入（一次性）：
	faultWriteLimit int    // >=0：下一次 Write 最多写这么多字节，然后崩溃
	faultRename     string // "before" | "after" | ""：相对 rename 交换的崩溃时点
	crashed         bool
}

func newMemFS() *memFS {
	return &memFS{files: make(map[string][]byte), faultWriteLimit: -1}
}

// content 返回 name 当前的 durable 内容（不存在返回 nil, false）。
func (m *memFS) content(name string) ([]byte, bool) {
	b, ok := m.files[name]
	return b, ok
}

func (m *memFS) Create(name string) (File, error) {
	if m.crashed {
		return nil, errCrash
	}
	m.files[name] = []byte{}
	return &memFile{fs: m, name: name}, nil
}

func (m *memFS) Open(name string) (File, error) {
	if m.crashed {
		return nil, errCrash
	}
	if _, ok := m.files[name]; !ok {
		return nil, os.ErrNotExist
	}
	return &memFile{fs: m, name: name}, nil
}

func (m *memFS) Rename(oldpath, newpath string) error {
	if m.crashed {
		return errCrash
	}
	if m.faultRename == "before" {
		m.crashed = true
		return errCrash // 交换未发生 → path 保持旧
	}
	m.files[newpath] = m.files[oldpath]
	delete(m.files, oldpath)
	if m.faultRename == "after" {
		m.crashed = true
		return errCrash // 交换已发生 → path 已是新
	}
	return nil
}

func (m *memFS) Remove(name string) error {
	if m.crashed {
		return errCrash
	}
	delete(m.files, name)
	return nil
}

func (m *memFS) SyncDir(dir string) error {
	if m.crashed {
		return errCrash
	}
	return nil
}

type memFile struct {
	fs   *memFS
	name string
	off  int
}

func (f *memFile) Write(p []byte) (int, error) {
	if f.fs.crashed {
		return 0, errCrash
	}
	if f.fs.faultWriteLimit >= 0 {
		lim := f.fs.faultWriteLimit
		f.fs.faultWriteLimit = -1 // 一次性
		if lim < len(p) {
			f.writeAt(p[:lim]) // 只落前 lim 字节 → 撕裂
			f.fs.crashed = true
			return lim, errCrash
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
		return errCrash
	}
	return nil
}

func (f *memFile) Close() error {
	if f.fs.crashed {
		return errCrash
	}
	return nil
}
