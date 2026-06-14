package kvraft

import (
	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type kvEntry struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	me    int
	rsm   *rsm.RSM
	kvMap map[string]kvEntry
	// Your definitions here.
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
		reply.Value = value.value
		reply.Version = value.version
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
		if args.Version == value.version {
			kv.kvMap[args.Key] = kvEntry{value: args.Value, version: value.version + 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	} else {
		if args.Version == 0 {
			kv.kvMap[args.Key] = kvEntry{value: args.Value, version: 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	}
	return reply
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
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

	kv := &KVServer{me: me}
	kv.kvMap = make(map[string]kvEntry)

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
