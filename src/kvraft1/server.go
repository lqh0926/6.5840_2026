package kvraft

import (
	"bytes"

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
	kvMap map[string]kvEntry
	// Your definitions here.
}

type Snapshot struct {
	KvMap map[string]kvEntry
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	if getArgs, ok := req.(rpc.GetArgs); ok {
		return kv.DoGet(getArgs)
	}
	if putArgs, ok := req.(rpc.PutArgs); ok {
		return kv.DoPut(putArgs)
	}
	return nil
}

func (kv *KVServer) DoGet(args rpc.GetArgs) rpc.GetReply {
	// Your code here
	value, ok := kv.kvMap[args.Key]
	var reply rpc.GetReply
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
	// Your code here
	value, ok := kv.kvMap[args.Key]
	reply := rpc.PutReply{}
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

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	// You can use labgob to turn a Snapshot struct into a byte array.
	snapshot := Snapshot{KvMap: kv.kvMap}
	buf := new(bytes.Buffer)
	w := labgob.NewEncoder(buf)
	if err := w.Encode(snapshot); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	// You can use labgob to turn a byte array into a Snapshot struct.
	buf := bytes.NewBuffer(data)
	r := labgob.NewDecoder(buf)
	var snapshot Snapshot
	if err := r.Decode(&snapshot); err != nil {
		panic(err)
	}
	kv.kvMap = snapshot.KvMap
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

	kv := &KVServer{me: me}
	kv.kvMap = make(map[string]kvEntry)

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
func StartKVServerGrpc(ends []transport.ClientEnd, me transport.NodeID, nodeIDs []transport.NodeID, w raft.WAL, maxraftstate int) (*KVServer, raftapi.Raft) {
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(kvEntry{})
	labgob.Register(Snapshot{})

	kv := &KVServer{}
	kv.kvMap = make(map[string]kvEntry)
	kv.rsm = rsm.MakeRSMGrpc(ends, me, nodeIDs, w, maxraftstate, kv)
	// 快照现由 WAL 持有（fileWAL 内嵌于 wal header）；启动时经 Load 取回做 Restore。
	if st, _ := w.Load(); len(st.Snapshot) > 0 {
		kv.Restore(st.Snapshot)
	}
	return kv, kv.rsm.Raft()
}
