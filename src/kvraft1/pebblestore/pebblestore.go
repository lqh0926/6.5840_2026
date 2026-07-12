// Package pebblestore 用 pebble（磁盘 LSM）实现 KV 状态机的存储后端（Phase 2 Step 4a）。
//
// 定位（决策 6）：**存储模型**改造——状态机从内存 map 变磁盘 LSM，可 > RAM；不是持久化诉求。
// 结构化满足 kvraft.KVStore（不 import kvraft1，避免把 pebble 拖进 L1）。
//
// apply-id：每次成功 Put 把「KV 改动 + applied_index」放**同一 pebble batch、Sync 提交**，于是
// pebble 的 durable 状态永远自带一个自洽的"我 apply 到 raft index K"。重启开 live pebble → 读 K →
// raft 从 K+1 重放、DoOp 跳过 ≤K（角色 A 本地恢复，不用快照）。
// 快照（角色 B，InstallSnapshot 发落后 follower）：`Snapshot()`=`pebble.Checkpoint()`（硬链 SSTable，
// 不遍历序列化）→ 打包 blob；`Restore()`=解包 → **整库替换**（快照无 tombstone，不能 merge）。
package pebblestore

import (
	"archive/tar"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble"

	"6.5840/kvsrv1/rpc"
)

// 保留 key 与用户 key 用前缀字节隔开，避免撞。
var appliedKey = []byte{0x00} // applied_index

func userKey(k string) []byte { return append([]byte{0x01}, k...) }

type entry struct {
	Value   string
	Version rpc.Tversion
}

// Store 实现 kvraft.KVStore。
type Store struct {
	dir string
	db  *pebble.DB
}

// Open 打开（或初始化）dir 处的 pebble。重启时 live 数据已在其中（durable 到 applied_index）。
func Open(dir string) (*Store, error) {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &Store{dir: dir, db: db}, nil
}

func (s *Store) Get(key string) (string, rpc.Tversion, bool) {
	v, closer, err := s.db.Get(userKey(key))
	if err == pebble.ErrNotFound {
		return "", 0, false
	}
	must(err)
	defer closer.Close()
	e := decodeEntry(v)
	return e.Value, e.Version, true
}

// Put 把「key=(value,version)」与「applied_index=index」放同一 batch、Sync 原子提交。
func (s *Store) Put(index int, key, value string, version rpc.Tversion) {
	b := s.db.NewBatch()
	must(b.Set(userKey(key), encodeEntry(entry{Value: value, Version: version}), nil))
	must(b.Set(appliedKey, encodeIndex(index), nil))
	must(b.Commit(pebble.Sync)) // fsync：写 + applied_index 一起 durable
}

// AppliedIndex 返回 pebble 里 durable 的 applied_index；全新库返回 0（→ 从不跳过）。
func (s *Store) AppliedIndex() int {
	v, closer, err := s.db.Get(appliedKey)
	if err == pebble.ErrNotFound {
		return 0
	}
	must(err)
	defer closer.Close()
	return decodeIndex(v)
}

// Snapshot = pebble checkpoint（硬链一致快照）→ 打包成 blob。供 raft 现成 blob 快照协议（角色 B）。
func (s *Store) Snapshot() []byte {
	tmp, err := os.MkdirTemp(filepath.Dir(s.dir), "ckpt-*")
	must(err)
	defer os.RemoveAll(tmp)
	ckpt := filepath.Join(tmp, "db")
	must(s.db.Checkpoint(ckpt))
	return tarDir(ckpt)
}

// Restore = 解包 blob → 整库替换 live pebble（关旧、换目录、重开）。仅 InstallSnapshot 触发。
// 注：本实现整库替换非跨崩溃原子（4b/硬化再管）；4a 的 crash 测试不走此路（全崩无落后 follower）。
func (s *Store) Restore(data []byte) {
	newDir := s.dir + ".restore"
	must(os.RemoveAll(newDir))
	untarDir(data, newDir)
	must(s.db.Close())
	must(os.RemoveAll(s.dir))
	must(os.Rename(newDir, s.dir))
	db, err := pebble.Open(s.dir, &pebble.Options{})
	must(err)
	s.db = db
}

func (s *Store) Close() error { return s.db.Close() }

// --- 编解码 ---

func encodeEntry(e entry) []byte {
	var buf bytes.Buffer
	must(gob.NewEncoder(&buf).Encode(e))
	return buf.Bytes()
}

func decodeEntry(b []byte) entry {
	var e entry
	must(gob.NewDecoder(bytes.NewReader(b)).Decode(&e))
	return e
}

func encodeIndex(i int) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(i))
	return b[:]
}

func decodeIndex(b []byte) int { return int(binary.BigEndian.Uint64(b)) }

// --- checkpoint 目录 ↔ blob（tar，递归） ---

func tarDir(dir string) []byte {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		hdr := &tar.Header{Name: rel, Mode: 0o644, Size: info.Size()}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(tw, f)
		return err
	})
	must(err)
	must(tw.Close())
	return buf.Bytes()
}

func untarDir(data []byte, dir string) {
	must(os.MkdirAll(dir, 0o755))
	tr := tar.NewReader(bytes.NewReader(data))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		must(err)
		out := filepath.Join(dir, hdr.Name)
		must(os.MkdirAll(filepath.Dir(out), 0o755))
		f, err := os.Create(out)
		must(err)
		_, err = io.Copy(f, tr)
		must(err)
		must(f.Close())
	}
}

func must(err error) {
	if err != nil {
		panic(fmt.Sprintf("pebblestore: %v", err))
	}
}
