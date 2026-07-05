package persist

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// FilePersister 是 Persister 的磁盘文件实现（Phase 1）。
//
// 抽象说明（给 Phase 2）：
//   - Persister 接口是稳定的抽象缝，raft 只依赖它；Phase 1 用文件、Phase 2 换
//     WAL/LSM，raft 代码一行不用动——只需再写一个实现该接口的类型。
//   - 已知限制：本接口的 Save(raftstate, snapshot) 是【全量 blob 原子写】语义
//     （每次把整个 raft 状态重写一遍）。真正的增量 WAL（只 append 新增 log 条目、
//     单独持久化 term/votedFor、单独存 snapshot）在这个接口背后无法高效实现，
//     因为 Save 只拿到整块 blob、看不到「增量」。Phase 2 若要上真 WAL，需要
//     【扩展接口】（如 AppendLog / SaveMetadata / SaveSnapshot 分开），这是
//     Phase 2 的设计决策，此处不预设，只留路标。
//
// 存储布局：单个 bundle 文件（raftstate + snapshot 打包），一次原子 rename 落盘。
// 之所以不拆成两个文件：raft 的 Save 要求二者【一起原子写入】（接口注释即此意），
// 分文件会有「崩在两次 rename 之间 → raftstate 与 snapshot 不一致」的坑。
// bundle 格式：[uint32 len(raftstate)][raftstate][uint32 len(snapshot)][snapshot]，
// 均为大端。
type FilePersister struct {
	mu   sync.Mutex
	dir  string
	path string // <dir>/state.bin

	raftstate []byte // 内存缓存，读走缓存、Size 走缓存
	snapshot  []byte
}

const stateFileName = "state.bin"

// OpenFilePersister 打开（或初始化）dir 下的持久化状态。
// 若已存在 state 文件则载入；否则从空开始。
func OpenFilePersister(dir string) (*FilePersister, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("persist: mkdir %s: %w", dir, err)
	}
	p := &FilePersister{
		dir:  dir,
		path: filepath.Join(dir, stateFileName),
	}
	if err := p.load(); err != nil {
		return nil, err
	}
	return p, nil
}

// load 读取并解码 bundle 文件到内存缓存。文件不存在视为空状态。
func (p *FilePersister) load() error {
	data, err := os.ReadFile(p.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // 空状态
		}
		return fmt.Errorf("persist: read %s: %w", p.path, err)
	}
	rs, snap, err := decodeBundle(data)
	if err != nil {
		return fmt.Errorf("persist: decode %s: %w", p.path, err)
	}
	p.raftstate, p.snapshot = rs, snap
	return nil
}

// Save 原子性地把 raftstate 和 snapshot 一起写盘。
func (p *FilePersister) Save(raftstate []byte, snapshot []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.raftstate = clone(raftstate)
	p.snapshot = clone(snapshot)

	bundle := encodeBundle(p.raftstate, p.snapshot)
	if err := atomicWrite(p.dir, p.path, bundle); err != nil {
		// 持久化失败无法安全恢复：与 raft 对持久化的假设一致，直接 panic。
		panic(fmt.Sprintf("persist: save: %v", err))
	}
}

func (p *FilePersister) ReadRaftState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return clone(p.raftstate)
}

func (p *FilePersister) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return clone(p.snapshot)
}

func (p *FilePersister) RaftStateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.raftstate)
}

// --- helpers ---

// atomicWrite 把 data 原子写到 path：写临时文件 → fsync → rename → fsync 目录。
// rename 在同目录内对 POSIX 是原子的；崩溃时要么看到旧文件、要么看到新文件，
// 不会看到半截。
func atomicWrite(dir, path string, data []byte) error {
	tmp, err := os.CreateTemp(dir, "state-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // rename 成功后这是 no-op；失败时清理残留

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	// fsync 目录，确保 rename 本身持久化（best-effort，部分平台不支持目录 fsync）。
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

func encodeBundle(raftstate, snapshot []byte) []byte {
	buf := make([]byte, 0, 8+len(raftstate)+len(snapshot))
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(raftstate)))
	buf = append(buf, hdr[:]...)
	buf = append(buf, raftstate...)
	binary.BigEndian.PutUint32(hdr[:], uint32(len(snapshot)))
	buf = append(buf, hdr[:]...)
	buf = append(buf, snapshot...)
	return buf
}

func decodeBundle(data []byte) (raftstate, snapshot []byte, err error) {
	if len(data) < 4 {
		return nil, nil, fmt.Errorf("truncated bundle: %d bytes", len(data))
	}
	n := binary.BigEndian.Uint32(data[:4])
	data = data[4:]
	if uint32(len(data)) < n {
		return nil, nil, fmt.Errorf("truncated raftstate: want %d, have %d", n, len(data))
	}
	if n > 0 {
		raftstate = clone(data[:n])
	}
	data = data[n:]
	if len(data) < 4 {
		return nil, nil, fmt.Errorf("truncated snapshot header")
	}
	m := binary.BigEndian.Uint32(data[:4])
	data = data[4:]
	if uint32(len(data)) < m {
		return nil, nil, fmt.Errorf("truncated snapshot: want %d, have %d", m, len(data))
	}
	if m > 0 {
		snapshot = clone(data[:m])
	}
	return raftstate, snapshot, nil
}

// clone 返回 b 的副本；nil/空 返回 nil，避免缓存与调用方共享底层数组。
func clone(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
