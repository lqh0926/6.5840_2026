package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"time"

	"6.5840/shardkv1/shardgrp"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	rcks map[tester.Tgid]*shardgrp.Clerk

	cfg *shardcfg.ShardConfig
	// You will have to modify this struct.
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	ck.rcks = make(map[tester.Tgid]*shardgrp.Clerk)
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	rck, ok := ck.rcks[gid]
	return rck, ok
}

// getOrCreateGrpClerk returns a cached shardgrp clerk for gid, or makes
// a new one if none is cached.
func (ck *Clerk) getOrCreateGrpClerk(gid tester.Tgid, servers []string) *shardgrp.Clerk {
	if rck, ok := ck.rcks[gid]; ok {
		return rck
	}
	rck := shardgrp.MakeClerk(ck.clnt, servers)
	ck.rcks[gid] = rck
	return rck
}

// refreshCfg queries the shardctrler for the latest config and caches it.
func (ck *Clerk) refreshCfg() {
	ck.cfg = ck.sck.Query()
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	shard := shardcfg.Key2Shard(key)
	for {
		if ck.cfg == nil {
			ck.refreshCfg()
		}
		gid := ck.cfg.Shards[shard]
		servers := ck.cfg.Groups[gid]
		rck := ck.getOrCreateGrpClerk(gid, servers)
		v, ver, err := rck.Get(key)
		if err == rpc.ErrWrongGroup {
			// 配置可能还没更新到位，稍等再重新 Query，避免忙等打爆控制器。
			ck.cfg = nil
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return v, ver, err
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	shard := shardcfg.Key2Shard(key)
	for {
		if ck.cfg == nil {
			ck.refreshCfg()
		}
		gid := ck.cfg.Shards[shard]
		servers := ck.cfg.Groups[gid]
		rck := ck.getOrCreateGrpClerk(gid, servers)
		err := rck.Put(key, value, version)
		if err == rpc.ErrWrongGroup {
			// 配置可能还没更新到位，稍等再重新 Query，避免忙等打爆控制器。
			ck.cfg = nil
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return err
	}
}
