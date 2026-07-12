package kvraft

import (
	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
	"6.5840/transport"
)

type kvEntry struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me    int
	rsm   *rsm.RSM
	store KVStore // 可插拔后端：mapStore（L1）/ pebblestore.Store（binary）
}

type Snapshot struct {
	KvMap map[string]kvEntry
}

// DoOp 施加一条已提交 op；index 是它的 raft 绝对索引。
// 落盘后端（pebble）用 index 做 apply-id 去重：index <= 已 durable apply 的水位 → 跳过
// （重启 replay 时已在 pebble 里，避免重复处理）。内存后端 AppliedIndex 恒 -1，从不跳过。
func (kv *KVServer) DoOp(index int, req any) any {
	if index <= kv.store.AppliedIndex() {
		return nil // 已 durable apply，跳过（客户端也不在等这些重放条目）
	}
	switch r := req.(type) {
	case rpc.GetArgs:
		return kv.doGet(r)
	case rpc.PutArgs:
		return kv.doPut(index, r)
	}
	return nil
}

func (kv *KVServer) doGet(args rpc.GetArgs) rpc.GetReply {
	value, version, ok := kv.store.Get(args.Key)
	if ok {
		return rpc.GetReply{Value: value, Version: version, Err: rpc.OK}
	}
	return rpc.GetReply{Err: rpc.ErrNoKey}
}

// doPut 做版本判定，成功才 store.Put（落盘后端把「写 + appliedIndex=index」同 batch 原子提交）。
// 被拒（版本不符 / 无 key）不写、不推 appliedIndex —— 重启会重放它、再次被拒，幂等无副作用。
func (kv *KVServer) doPut(index int, args rpc.PutArgs) rpc.PutReply {
	_, version, ok := kv.store.Get(args.Key)
	if ok {
		if args.Version == version {
			kv.store.Put(index, args.Key, args.Value, version+1)
			return rpc.PutReply{Err: rpc.OK}
		}
		return rpc.PutReply{Err: rpc.ErrVersion}
	}
	if args.Version == 0 {
		kv.store.Put(index, args.Key, args.Value, 1)
		return rpc.PutReply{Err: rpc.OK}
	}
	return rpc.PutReply{Err: rpc.ErrNoKey}
}

func (kv *KVServer) Snapshot() []byte     { return kv.store.Snapshot() }
func (kv *KVServer) Restore(data []byte)  { kv.store.Restore(data) }

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	getReply := res.(rpc.GetReply)
	reply.Value = getReply.Value
	reply.Version = getReply.Version
	reply.Err = getReply.Err
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	putReply := res.(rpc.PutReply)
	reply.Err = putReply.Err
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(kvEntry{})
	labgob.Register(Snapshot{})

	kv := &KVServer{me: me, store: newMapStore()}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	if persister.SnapshotSize() > 0 {
		kv.Restore(persister.ReadSnapshot())
	}
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}

// StartKVServerGrpc 是 StartKVServer 的 gRPC/生产版：用 transport.ClientEnd +
// 稳定 NodeID + 落盘 Persister 装配，供真 binary（cmd/raftkvd）使用。
// 返回 KVServer 以及其内部 raft（binary 用它挂 RaftService peer 平面）。
// labrpc 老路 StartKVServer 保持不变，L1 测试不受影响。
func StartKVServerGrpc(ends []transport.ClientEnd, me transport.NodeID, nodeIDs []transport.NodeID, w raft.WAL, store KVStore, maxraftstate int) (*KVServer, raftapi.Raft) {
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(kvEntry{})
	labgob.Register(Snapshot{})

	kv := &KVServer{store: store}
	kv.rsm = rsm.MakeRSMGrpc(ends, me, nodeIDs, w, maxraftstate, kv)
	// 不在启动时 Restore：pebble 是 live durable 存储，Open 时已到位到其 appliedIndex；
	// raft 从 snapshotIndex+1 重放，DoOp 按 appliedIndex 去重即可（角色 A）。快照 blob 只在
	// 收 InstallSnapshot（角色 B）时经 applyCh 触发 Restore 整库替换。
	return kv, kv.rsm.Raft()
}
