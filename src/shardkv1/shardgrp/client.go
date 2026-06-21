package shardgrp

import (

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"

	"math/rand"
	"time"
)

type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can  add to this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{Clnt: clnt, servers: servers, leader: rand.Intn(len(servers))}
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// maxRetries 限制对一个 shardgrp 的重试轮数。整组下线/离开时，clerk 会一直
// 收到网络失败而非 ErrWrongGroup，若无限重试则上层 shardkv clerk 永远没机会
// 刷新配置重新路由。超过此轮数就返回 ErrWrongGroup，让上层重读配置。
const maxRetries = 30

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}

	for tries := 0; tries < maxRetries; tries++ {
		reply := rpc.GetReply{}
		ok := ck.Call(ck.servers[ck.leader], "KVServer.Get", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			return reply.Value, reply.Version, reply.Err
		}
		// Either network failure (ok==false) or ErrWrongLeader: try next server
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
	// 多轮联系不上：可能整组已离开/下线，让上层刷新配置重新路由。
	return "", 0, rpc.ErrWrongGroup
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{}
	ok := ck.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply)
	if ok && reply.Err != rpc.ErrWrongLeader {
		return reply.Err
	}
	// Either network failure (ok==false) or ErrWrongLeader: try next server
	ck.leader = (ck.leader + 1) % len(ck.servers)
	for tries := 0; tries < maxRetries; tries++ {
		reply = rpc.PutReply{}
		ok = ck.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply)
		if ok {
			if reply.Err == rpc.ErrVersion {
				return rpc.ErrMaybe
			} else if reply.Err != rpc.ErrWrongLeader {
				return reply.Err
			}
		}
		// Either network failure (ok==false) or ErrWrongLeader: try next server
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
	// 多轮联系不上：可能整组已离开/下线，让上层刷新配置重新路由。
	return rpc.ErrWrongGroup
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
	for {
		reply := shardrpc.FreezeShardReply{}
		ok := ck.Call(ck.servers[ck.leader], "KVServer.FreezeShard", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			return reply.State, reply.Err
		}
		// 网络失败或非 leader：换下一个 server 重试。
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
	for {
		reply := shardrpc.InstallShardReply{}
		ok := ck.Call(ck.servers[ck.leader], "KVServer.InstallShard", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			return reply.Err
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
	for {
		reply := shardrpc.DeleteShardReply{}
		ok := ck.Call(ck.servers[ck.leader], "KVServer.DeleteShard", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			return reply.Err
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
