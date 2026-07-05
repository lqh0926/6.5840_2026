// Package l2 是测试策略里的 L2 层：真 gRPC + 可编程故障注入。
//
// 设计（见 ROADMAP 测试分层）：场景 = 「一串操作 + 断言」，与底座无关。
// 把动作抽成 Cluster 接口，场景写一遍，底座各实现一份（现有 gRPC；未来可加
// labrpc 作参考基准）。L2 不复用 tester1，只搬 5 个关键场景，验证换 gRPC 后
// 不变式不破 + gRPC 独有失败模式（响应丢失）。
package l2

// Result 是一次 KV 操作的结果。
// Err 为 "OK"/"ErrNoKey"/"ErrVersion"/"ErrWrongLeader"/"ErrMaybe"；
// Err=="" 表示传输层失败或超时未找到 leader（调用本身没拿到定性回复）。
type Result struct {
	Value   string
	Version uint64
	Err     string
}

// Cluster 抽象一个 KV 集群对测试可见的行为。
type Cluster interface {
	// NodeIDs 返回全部节点的稳定 ID。
	NodeIDs() []string

	// Leader 返回当前 leader 的 NodeID；ok=false 表示暂无 leader。
	Leader() (id string, ok bool)

	// Put/Get 经真 gRPC 打到集群，自动找 leader（遇 ErrWrongLeader 换节点重试）。
	Put(key, value string, version uint64) Result
	Get(key string) Result

	// Crash 真崩一个节点（销毁内存实例 + 停服），只保留落盘状态。
	// Restart 从落盘 Persister 重建同一节点（同 addr，peers 自动重连）。
	Crash(id string)
	Restart(id string)

	// Disconnect/Connect 网络分区（骨架阶段未实现，fault interceptor 阶段补）。
	Disconnect(id string)
	Connect(id string)

	// Cleanup 停掉全部节点、释放资源。
	Cleanup()
}
