package shardgrp

import (

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"

	"bytes"
)

const (
	ENVkey = "65840ENV"
)

// 每个分片在本组中的归属状态。零值 NotOwned 表示「本组不负责该分片」，
// 这样新组、反序列化、漏赋值都自动落到最安全的不服务态。
type ShardState int

const (
	NotOwned ShardState = iota // 0：不归本组，Get/Put 返回 ErrWrongGroup
	Ready                      // 1：本组拥有该分片，正常读写
	Frozen                     // 2：已冻结，停止读写、数据待迁出
)

type kvEntry struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid

	kvMap map[string]kvEntry
	// 每分片归属状态，下标即 shardcfg.Key2Shard 的返回值（0..NShards-1）
	shardState [shardcfg.NShards]ShardState
	// 每分片最近一次迁移操作所属的配置号，用于幂等去重（控制器可能重试/换主）。
	cfgNum [shardcfg.NShards]shardcfg.Tnum
}

type Snapshot struct {
	KvMap      map[string]kvEntry
	ShardState [shardcfg.NShards]ShardState
	CfgNum     [shardcfg.NShards]shardcfg.Tnum
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// RSM 是类型无关的：Submit 进来什么类型，这里就按类型分发。
	// 迁移类操作复用 shardrpc 里现成的 Args 结构（已在启动时 labgob 注册）。
	switch args := req.(type) {
	case rpc.GetArgs:
		return kv.DoGet(args)
	case rpc.PutArgs:
		return kv.DoPut(args)
	case shardrpc.FreezeShardArgs:
		return kv.DoFreezeShard(args)
	case shardrpc.InstallShardArgs:
		return kv.DoInstallShard(args)
	case shardrpc.DeleteShardArgs:
		return kv.DoDeleteShard(args)
	}
	return nil
}

func (kv *KVServer) DoGet(args rpc.GetArgs) rpc.GetReply {
	var reply rpc.GetReply
	// 先判分片归属：不是 Ready（不归本组 / 已冻结）就让客户端去别的组重试。
	// 必须先于 kvMap 查询，否则会把「不归我」误当成「key 不存在(ErrNoKey)」。
	if kv.shardState[shardcfg.Key2Shard(args.Key)] != Ready {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}
	value, ok := kv.kvMap[args.Key]
	if ok {
		reply.Value = value.Value
		reply.Version = value.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
	return reply
}

func (kv *KVServer) DoPut(args rpc.PutArgs) rpc.PutReply {
	reply := rpc.PutReply{}
	// 同 DoGet：先判分片归属，不是 Ready 就让客户端去别的组重试。
	if kv.shardState[shardcfg.Key2Shard(args.Key)] != Ready {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}
	value, ok := kv.kvMap[args.Key]
	if ok {
		if args.Version == value.Version {
			kv.kvMap[args.Key] = kvEntry{Value: args.Value, Version: value.Version + 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	} else {
		if args.Version == 0 {
			kv.kvMap[args.Key] = kvEntry{Value: args.Value, Version: 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	}
	return reply
}

// 以下三个 Do* 在 apply 协程里串行执行（经由 rsm），可安全读写 kv 状态。
// 留空骨架，逻辑由你填充。

// dumpShard 把属于 shard 的所有键值序列化成字节，供迁移到目标组。
func (kv *KVServer) dumpShard(shard shardcfg.Tshid) []byte {
	sub := make(map[string]kvEntry)
	for k, v := range kv.kvMap {
		if shardcfg.Key2Shard(k) == shard {
			sub[k] = v
		}
	}
	buf := new(bytes.Buffer)
	w := labgob.NewEncoder(buf)
	if err := w.Encode(sub); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// loadShard 把 dumpShard 序列化的字节并入本组 kvMap。
func (kv *KVServer) loadShard(state []byte) {
	if len(state) == 0 {
		return
	}
	buf := bytes.NewBuffer(state)
	r := labgob.NewDecoder(buf)
	var sub map[string]kvEntry
	if err := r.Decode(&sub); err != nil {
		panic(err)
	}
	for k, v := range sub {
		kv.kvMap[k] = v
	}
}

// deleteShardData 删除属于 shard 的所有键值。
func (kv *KVServer) deleteShardData(shard shardcfg.Tshid) {
	for k := range kv.kvMap {
		if shardcfg.Key2Shard(k) == shard {
			delete(kv.kvMap, k)
		}
	}
}

func (kv *KVServer) DoFreezeShard(args shardrpc.FreezeShardArgs) shardrpc.FreezeShardReply {
	reply := shardrpc.FreezeShardReply{Num: args.Num}
	// 落后的旧请求：本分片已经处理过更高的配置号，幂等忽略。
	if args.Num < kv.cfgNum[args.Shard] {
		reply.Err = rpc.OK
		return reply
	}
	// 冻结：停止该分片的读写，但保留数据以便导出。
	// 冻结后数据不再变化，重复 freeze 会得到相同字节，天然幂等。
	kv.shardState[args.Shard] = Frozen
	kv.cfgNum[args.Shard] = args.Num
	reply.State = kv.dumpShard(args.Shard)
	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) DoInstallShard(args shardrpc.InstallShardArgs) shardrpc.InstallShardReply {
	reply := shardrpc.InstallShardReply{}
	// 已在 >= 此 num 装过了：幂等 no-op，绝不用传入数据覆盖。
	// （Part B 重跑时源组可能已删数据、传来空 state，覆盖会丢数据。）
	// 注意这里是 <=：Install 是目标组在 num=N 的唯一一步，重复即应跳过。
	if args.Num <= kv.cfgNum[args.Shard] {
		reply.Err = rpc.OK
		return reply
	}
	kv.loadShard(args.State)
	kv.shardState[args.Shard] = Ready // Install 完成即立即服务，不依赖后续 Delete
	kv.cfgNum[args.Shard] = args.Num
	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) DoDeleteShard(args shardrpc.DeleteShardArgs) shardrpc.DeleteShardReply {
	reply := shardrpc.DeleteShardReply{}
	// 严格 <：只挡更老的请求。Delete(N) 紧跟 Freeze(N)（cfgNum 已是 N），
	// N < N 为假 → 放行；过期的 Delete 会被拒，避免删掉已迁回的新数据。
	if args.Num < kv.cfgNum[args.Shard] {
		reply.Err = rpc.OK
		return reply
	}
	kv.deleteShardData(args.Shard)
	kv.shardState[args.Shard] = NotOwned
	kv.cfgNum[args.Shard] = args.Num
	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) Snapshot() []byte {
	// You can use labgob to turn a Snapshot struct into a byte array.
	snapshot := Snapshot{KvMap: kv.kvMap, ShardState: kv.shardState, CfgNum: kv.cfgNum}
	buf := new(bytes.Buffer)
	w := labgob.NewEncoder(buf)
	if err := w.Encode(snapshot); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// You can use labgob to turn a byte array into a Snapshot struct.
	buf := bytes.NewBuffer(data)
	r := labgob.NewDecoder(buf)
	var snapshot Snapshot
	if err := r.Decode(&snapshot); err != nil {
		panic(err)
	}
	kv.kvMap = snapshot.KvMap
	kv.shardState = snapshot.ShardState
	kv.cfgNum = snapshot.CfgNum
}

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
	// Use kv.rsm.Submit() to submit args
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	putReply := res.(rpc.PutReply)
	reply.Err = putReply.Err
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// 对外是 RPC 入口；对内提交到本组自己的 raft 走共识（不要 make clerk 调自己）。
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = res.(shardrpc.FreezeShardReply)
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = res.(shardrpc.InstallShardReply)
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = res.(shardrpc.DeleteShardReply)
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})
	labgob.Register(kvEntry{})
	labgob.Register(Snapshot{})

	kv := &KVServer{gid: gid, me: me}
	kv.kvMap = make(map[string]kvEntry)
	// 配置号基准线为 NumFirst(1)：所有组、所有分片都从配置 1 起步。
	for s := range kv.cfgNum {
		kv.cfgNum[s] = shardcfg.NumFirst
	}
	// 第一个组 Gid1 是引导组：初始配置把所有分片都分给它，但没有任何
	// InstallShard 会通知它，所以它必须在启动时自认拥有全部分片。
	// 其余组默认 NotOwned（零值），靠后续 InstallShard 接管分片。
	if gid == shardcfg.Gid1 {
		for s := range kv.shardState {
			kv.shardState[s] = Ready
		}
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	if persister.SnapshotSize() > 0 {
		kv.Restore(persister.ReadSnapshot())
	}

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
