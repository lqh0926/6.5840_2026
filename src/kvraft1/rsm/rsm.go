package rsm

import (
	"bytes"
	"math/rand"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/persist"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
	"6.5840/transport"
)

func init() {
	// The command that flows through Raft is now an opaque []byte (see
	// encodeOp/decodeOp), so Raft's LogEntry.Command interface{} always
	// carries a []byte. Register it so gob can encode/decode that interface.
	labgob.Register([]byte(nil))
}

// Op 是 RSM 通过 Raft 复制的命令包装。
//
// 关于 Opcommad interface{} 与调用方的 labgob.Register：
// RSM 是通用层，不认识具体的应用命令类型（PutArgs/GetArgs/Inc/...），
// 因此这里用 interface{} 承载任意应用请求。gob 编码结构体里的 interface
// 字段时需要具体类型名，故各服务在启动时对自己的请求类型调用
// labgob.Register（见 kvraft1/server.go、shardkv1/.../server.go 等）。
//
// 这个 interface{}+Register 是【刻意保留】的：
//   - 它只在 RSM 内部用于 Op ↔ []byte 的编解码（encodeOp/decodeOp）；
//   - 编出来的 []byte 对 Raft 完全不透明（Raft 只当字节货物存储/复制），
//     对外部 gRPC 客户端也不可见；
//   - 因此它不跨任何对外契约边界，无需迁到 proto。
//
// 若日后要彻底消除 interface{}/Register，方向是把命令序列化下沉到应用层、
// 让 Op 变成 {Payload []byte; HashNum int64} 的纯具体结构（见讨论），
// 但那是独立的一步，与 Raft/gRPC 迁移解耦。
type Op struct {
	Opcommad interface{}
	HashNum  int64
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

// encodeOp serializes an Op into an opaque byte slice. RSM passes these bytes
// through Raft (as the command), so Raft never sees KV-layer types — it treats
// the command as opaque. The interface-typed Op.Opcommad still requires its
// concrete payload types (PutArgs/GetArgs/Inc/...) to be labgob.Register'd by
// the caller.
func encodeOp(op Op) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(&op); err != nil {
		panic(err)
	}
	return w.Bytes()
}

// decodeOp is the inverse of encodeOp.
func decodeOp(data []byte) Op {
	var op Op
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&op); err != nil {
		panic(err)
	}
	return op
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	chMap        map[int]chan any
	hashNumMap   map[int]int64
	// Your definitions here.
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister persist.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		chMap:        make(map[int]chan any),
		hashNumMap:   make(map[int]int64),
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.MakeFromLabrpc(servers, me, persister, rsm.applyCh)
	}
	go rsm.readApplyCh()
	return rsm
}

// MakeRSMGrpc 是 MakeRSM 的 gRPC/生产版：直接用 transport.ClientEnd + 稳定
// NodeID 装配（走 raft.Make，而非 labrpc 适配器）。仅供真 binary 使用，
// 不经 tester，故不看 UseRaftStateMachine。labrpc 老路（MakeRSM）保持不变，
// L1 测试不受影响。
// MakeRSMGrpc 走真 binary 路径：直接注入一个 raft.WAL（filewal.FileWAL），经
// raft.MakeWithWAL 引导，不再包 persist.Persister。
func MakeRSMGrpc(ends []transport.ClientEnd, me transport.NodeID, nodeIDs []transport.NodeID, w raft.WAL, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           0, // 仅用于日志；gRPC 路径用 NodeID 标识，无 int 下标
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		chMap:        make(map[int]chan any),
		hashNumMap:   make(map[int]int64),
	}
	rsm.rf = raft.MakeWithWAL(ends, me, nodeIDs, w, rsm.applyCh)
	go rsm.readApplyCh()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	_, rfIsLeader := rsm.rf.GetState()
	if !rfIsLeader {
		return rpc.ErrWrongLeader, nil
	}
	rsm.mu.Lock()
	ch := make(chan any, 1)
	hashNum := rand.Int63()
	op := Op{Opcommad: req, HashNum: hashNum}
	logId, term, ok := rsm.rf.Start(encodeOp(op))
	if !ok {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	rsm.hashNumMap[logId] = hashNum
	oldCh, ok := rsm.chMap[logId]
	if ok {
		close(oldCh)
	}
	rsm.chMap[logId] = ch
	rsm.mu.Unlock()
	overallTimeout := time.After(1 * time.Second)
	for {
		select {
		case res, ok := <-ch:
			if !ok {
				return rpc.ErrWrongLeader, nil
			}
			return rpc.OK, res
		case <-time.After(20 * time.Millisecond):
			newTerm, rfIsLeader := rsm.rf.GetState()
			if !rfIsLeader || newTerm != term {
				rsm.mu.Lock()
				delete(rsm.chMap, logId)
				delete(rsm.hashNumMap, logId)
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
		case <-overallTimeout:
			rsm.mu.Lock()
			delete(rsm.chMap, logId)
			delete(rsm.hashNumMap, logId)
			rsm.mu.Unlock()
			return rpc.ErrWrongLeader, nil
		}
	}

}

func (rsm *RSM) readApplyCh() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			op := decodeOp(msg.Command.([]byte))
			res := rsm.sm.DoOp(op.Opcommad)
			rsm.mu.Lock()
			hashNum, hasHash := rsm.hashNumMap[msg.CommandIndex]
			ch, hasCh := rsm.chMap[msg.CommandIndex]
			if hasCh {
				if hasHash && hashNum == op.HashNum {
					ch <- res
				} else {
					close(ch)
				}
			}
			delete(rsm.hashNumMap, msg.CommandIndex)
			delete(rsm.chMap, msg.CommandIndex)
			rsm.mu.Unlock()
			if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() > rsm.maxraftstate {
				snapshot := rsm.sm.Snapshot()
				rsm.rf.Snapshot(msg.CommandIndex, snapshot)
			}
		} else if msg.SnapshotValid {
			rsm.sm.Restore(msg.Snapshot)
		}
	}
}
