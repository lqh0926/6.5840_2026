package shardctrler

//
// Shardctrler 包含 InitConfig、Query 和 ChangeConfigTo 方法
//

import (
	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

// ShardCtrler 用于控制器和 KV 客户端。
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // 由 Kill() 设置
	// 你的数据写在这里。
}

// 创建一个 ShardCtrler，将其状态存储在 kvsrv 中。
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// 你的代码写在这里。
	return sck
}

// 测试器在启动新控制器之前调用 InitController()。
// 在 Part A 中，此方法无需做任何事情。
// 在 Part B 和 C 中，此方法实现恢复逻辑。
func (sck *ShardCtrler) InitController() {
}

// 由测试器调用一次，用于提供初始配置。
// 你可以用 shardcfg.String() 将 ShardConfig 序列化为字符串，
// 然后以版本 0 Put 到控制器的 kvsrv 中。
// 你可以选择任意 key 来命名这个配置。
// 初始配置中所有分片都分配给 shardcfg.Gid1。
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// 你的代码写在这里
	cfgString := cfg.String()
	// 同时 seed current(cfg)和 next，两者版本都从 1 起、各自与 Num 恒等，
	// 这样首次 ChangeConfigTo 写 next 时 version=Num-1 能对得上。
	sck.IKVClerk.Put("cfg", cfgString, 0)
	sck.IKVClerk.Put("next", cfgString, 0)
}

// 由测试器调用，请求控制器将配置从当前配置变更为新配置。
// 在控制器变更配置的过程中，它可能会被另一个控制器取代。
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	cur := sck.Query()
	if new.Num <= cur.Num {
		return // 旧的或重复的配置，忽略
	}
	// ① 发布意图：CAS 写 next。version=Num-1 与 Num 恒等，
	//    同一 Num 只有一个 controller 写成功（Part A 无并发，必成功）。
	sck.IKVClerk.Put("next", new.String(), rpc.Tversion(new.Num-1))

	// ② 迁移：对每个换了归属的分片，Freeze 源 → Install 目标 → Delete 源。
	//    通讯录来自配置本身：源组在 cur.Groups，目标组在 new.Groups。
	for sh := shardcfg.Tshid(0); sh < shardcfg.NShards; sh++ {
		oldGid := cur.Shards[sh]
		newGid := new.Shards[sh]
		if oldGid == newGid {
			continue
		}
		srcck := shardgrp.MakeClerk(sck.clnt, cur.Groups[oldGid])
		state, _ := srcck.FreezeShard(sh, new.Num) // Part A 无故障，忽略 err
		dstck := shardgrp.MakeClerk(sck.clnt, new.Groups[newGid])
		dstck.InstallShard(sh, state, new.Num)
		srcck.DeleteShard(sh, new.Num)
	}

	// ③ 发布 current：迁移全部完成后才更新 cfg，
	//    使 Query 读到的配置始终反映「已完成」的迁移（崩溃时退回旧配置不丢数据）。
	sck.IKVClerk.Put("cfg", new.String(), rpc.Tversion(new.Num-1))
}

// 返回当前配置
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// 你的代码写在这里。
	cfgstring, _, _ := sck.IKVClerk.Get("cfg")
	return shardcfg.FromString(cfgstring)
}
