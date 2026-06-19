package shardctrler

//
// Shardctrler 包含 InitConfig、Query 和 ChangeConfigTo 方法
//

import (

	"6.5840/kvsrv1"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/tester1"
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
}

// 由测试器调用，请求控制器将配置从当前配置变更为新配置。
// 在控制器变更配置的过程中，它可能会被另一个控制器取代。
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// 你的代码写在这里。
}


// 返回当前配置
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// 你的代码写在这里。
	return nil
}

