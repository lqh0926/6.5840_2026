package kvraft

import (
	"bytes"

	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
)

// KVStore 是 KV 状态机的可插拔存储后端。
//   - `mapStore`：内存 map，L1 测试用（快、确定性）。
//   - `pebblestore.Store`：磁盘 LSM，binary 用（可 > RAM）。放独立包，不把 pebble 拖进 L1。
//
// 接口用基础类型（string / rpc.Tversion）而非未导出的 kvEntry，好让别的包实现。
// 版本判定逻辑留在 KVServer.doPut；Store 只负责「读一个 entry」「原子写一个 entry + 推进
// appliedIndex」。appliedIndex 供落盘后端在重启 replay 时做 apply-id 去重（内存后端返回 -1
// 表示不去重，恒重放）。
type KVStore interface {
	Get(key string) (value string, version rpc.Tversion, ok bool)
	// Put 原子地写 key=（value,version），并把 appliedIndex 推进到 index（落盘后端同一 batch 提交）。
	Put(index int, key, value string, version rpc.Tversion)
	AppliedIndex() int
	Snapshot() []byte
	Restore(data []byte)
	Close() error
}

// mapStore 是 KVStore 的内存实现（L1）。行为等于改造前的 kvMap + labgob 快照。
type mapStore struct {
	m map[string]kvEntry
}

func newMapStore() *mapStore { return &mapStore{m: make(map[string]kvEntry)} }

func (s *mapStore) Get(key string) (string, rpc.Tversion, bool) {
	e, ok := s.m[key]
	return e.Value, e.Version, ok
}

func (s *mapStore) Put(_ int, key, value string, version rpc.Tversion) {
	s.m[key] = kvEntry{Value: value, Version: version}
}

// AppliedIndex 恒 -1：内存态崩了全丢、从快照+log 全量重建，不需要（也无法）去重。
func (s *mapStore) AppliedIndex() int { return -1 }

func (s *mapStore) Snapshot() []byte {
	buf := new(bytes.Buffer)
	if err := labgob.NewEncoder(buf).Encode(Snapshot{KvMap: s.m}); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (s *mapStore) Restore(data []byte) {
	var snapshot Snapshot
	if err := labgob.NewDecoder(bytes.NewBuffer(data)).Decode(&snapshot); err != nil {
		panic(err)
	}
	s.m = snapshot.KvMap
}

func (s *mapStore) Close() error { return nil }
