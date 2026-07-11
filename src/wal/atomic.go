package wal

// AtomicWrite 原子地把 data 写到 path —— 「整文件重写」写模式的原语（meta / snapshot）。
//
// 原子性**来自 rename**，不来自 write：写临时文件 → fsync(tmp) → rename → fsync(dir)。
// rename 是 POSIX 保证的目录项一次性换 inode，任一时刻崩溃，读者只看到完整旧文件或完整
// 新文件，**永不撕裂**。两个 fsync 顺序卡在 rename 两边：
//   - fsync(tmp) 在 rename **前**：保证换过去的 inode 里已是真数据（否则 path 指向半截）。
//   - fsync(dir) 在 rename **后**：持久化 rename 这个目录项改动本身（rename 得先发生才有得刷）。
func AtomicWrite(fs FS, dir, path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := fs.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil { // ← rename 前：tmp 数据落盘
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := fs.Rename(tmp, path); err != nil { // ← 原子换
		return err
	}
	return fs.SyncDir(dir) // ← rename 后：持久化 rename 本身
}
