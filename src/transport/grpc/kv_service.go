package grpc

import (
	"context"

	kvraft "6.5840/kvraft1"
	"6.5840/kvsrv1/rpc"
	"6.5840/proto"
)

// KVService 把 *kvraft.KVServer 适配成生成的 proto.KVServer，
// 使 KV client 平面（Get/Put/Append）能通过 gRPC 对外提供。
//
// 只做翻译：proto → kvraft 的 rpc.* 结构体 → 调 kvraft 的 Get/Put → 转回 proto。
// kvraft 内部会经 rsm.Submit 走 Raft 复制；非 leader 返回 ERR_WRONG_LEADER。
//
// 注：kvraft 目前只实现 Get/Put（Put 的 version 语义覆盖 append 场景），
// 未实现独立的 Append，故 Append 走 UnimplementedKVServer（返回 Unimplemented）。
type KVService struct {
	proto.UnimplementedKVServer
	kv *kvraft.KVServer
}

func NewKVService(kv *kvraft.KVServer) *KVService {
	return &KVService{kv: kv}
}

func (s *KVService) Get(_ context.Context, pb *proto.GetArgs) (*proto.GetReply, error) {
	args := rpc.GetArgs{Key: pb.Key}
	var reply rpc.GetReply
	s.kv.Get(&args, &reply)
	return &proto.GetReply{
		Value:   reply.Value,
		Version: uint64(reply.Version),
		Err:     errToPB(reply.Err),
	}, nil
}

func (s *KVService) Put(_ context.Context, pb *proto.PutArgs) (*proto.PutReply, error) {
	args := rpc.PutArgs{Key: pb.Key, Value: pb.Value, Version: rpc.Tversion(pb.Version)}
	var reply rpc.PutReply
	s.kv.Put(&args, &reply)
	return &proto.PutReply{Err: errToPB(reply.Err)}, nil
}

// errToPB 把 kvraft 的字符串 Err 映射到 proto 的 Err 枚举。
func errToPB(e rpc.Err) proto.Err {
	switch e {
	case rpc.OK:
		return proto.Err_OK
	case rpc.ErrNoKey:
		return proto.Err_ERR_NO_KEY
	case rpc.ErrVersion:
		return proto.Err_ERR_VERSION
	case rpc.ErrWrongLeader:
		return proto.Err_ERR_WRONG_LEADER
	case rpc.ErrMaybe:
		return proto.Err_ERR_MAYBE
	default:
		return proto.Err_ERR_UNSPECIFIED
	}
}
