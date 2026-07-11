package wal

import (
	"errors"
	"os"
)

// File 是 WAL 写盘的最小文件句柄 seam。*os.File 天然满足；测试用 fake 模拟撕裂写。
type File interface {
	Write(p []byte) (int, error)
	Sync() error
	Close() error
}

// FS 是文件系统 seam：把 create/append/read/rename/truncate/fsync-dir 抽到接口背后，
// 测试可注入崩溃点（写一半 / rename 前 / rename 后），生产用 osFS 走真 syscall。
type FS interface {
	Create(name string) (File, error)       // 截断创建（O_RDWR|O_CREATE|O_TRUNC），供 AtomicWrite 写 tmp
	Open(name string) (File, error)          // 打开已存在文件（offset 0，覆盖写）
	OpenAppend(name string) (File, error)    // 打开/创建并定位到末尾（供 RecordLog 追加）
	ReadFile(name string) ([]byte, error)    // 整文件读；不存在返回 (nil, nil)
	Truncate(name string, size int64) error  // 截断文件到 size（healing 撕裂尾巴）
	Rename(oldpath, newpath string) error
	Remove(name string) error
	SyncDir(dir string) error
}

// OSFS 返回走真 os syscall 的 FS 实现。
func OSFS() FS { return osFS{} }

type osFS struct{}

func (osFS) Create(name string) (File, error) {
	return os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
}

func (osFS) Open(name string) (File, error) {
	return os.OpenFile(name, os.O_RDWR, 0o644)
}

func (osFS) OpenAppend(name string) (File, error) {
	return os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
}

func (osFS) ReadFile(name string) ([]byte, error) {
	data, err := os.ReadFile(name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil // 不存在视为空
		}
		return nil, err
	}
	return data, nil
}

func (osFS) Truncate(name string, size int64) error { return os.Truncate(name, size) }

func (osFS) Rename(oldpath, newpath string) error { return os.Rename(oldpath, newpath) }

func (osFS) Remove(name string) error { return os.Remove(name) }

// SyncDir fsync 目录，让其中的 rename/create 本身持久化。
func (osFS) SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
