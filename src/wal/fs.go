package wal

import "os"

// File 是 WAL 写盘的最小文件 seam。*os.File 天然满足；测试用可注入 fake 模拟撕裂写。
type File interface {
	Write(p []byte) (int, error)
	Sync() error
	Close() error
}

// FS 是文件系统 seam：把 create/rename/fsync-dir 抽到接口背后，测试可注入崩溃点
// （崩在 rename 前 / 后 / 写一半），生产用 osFS 走真 syscall。
type FS interface {
	Create(name string) (File, error) // 截断创建（O_RDWR|O_CREATE|O_TRUNC）
	Open(name string) (File, error)   // 打开已存在文件（读写，offset 0）
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
