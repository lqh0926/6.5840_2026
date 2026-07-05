package l2

import (
	"testing"
	"time"
)

// waitLeader 轮询直到出现 leader 或超时。
func waitLeader(t *testing.T, c Cluster, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if id, ok := c.Leader(); ok {
			return id
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("超时未选出 leader")
	return ""
}

// ── 场景 1：选主 ──────────────────────────────────────────────
func ScenarioElection(t *testing.T, c Cluster) {
	leader := waitLeader(t, c, 5*time.Second)
	t.Logf("选出 leader: %s", leader)
}

// ── 场景 2：日志复制（Put 后 Get 拿得到）────────────────────────
func ScenarioReplication(t *testing.T, c Cluster) {
	waitLeader(t, c, 5*time.Second)

	if r := c.Put("k", "v", 0); r.Err != "OK" {
		t.Fatalf("Put 期望 OK，得到 %q", r.Err)
	}
	r := c.Get("k")
	if r.Err != "OK" || r.Value != "v" {
		t.Fatalf("Get 期望 OK/v，得到 %q/%q", r.Err, r.Value)
	}
	if r.Version != 1 {
		t.Fatalf("Get 期望 version=1，得到 %d", r.Version)
	}
}

// ── 场景 4：leader crash（崩掉 leader 后仍能选主 + 服务，数据不丢）──
func ScenarioLeaderCrash(t *testing.T, c Cluster) {
	old := waitLeader(t, c, 5*time.Second)

	if r := c.Put("k", "v1", 0); r.Err != "OK" {
		t.Fatalf("crash 前 Put 期望 OK，得到 %q", r.Err)
	}

	c.Crash(old)
	t.Logf("崩掉 leader %s，等新 leader", old)

	newLeader := waitLeader(t, c, 5*time.Second)
	if newLeader == old {
		t.Fatalf("新 leader 不应是已崩节点 %s", old)
	}

	// 崩溃前写入的数据应仍在（已复制到多数派）
	if r := c.Get("k"); r.Err != "OK" || r.Value != "v1" {
		t.Fatalf("crash 后 Get 期望 OK/v1，得到 %q/%q", r.Err, r.Value)
	}
	// 新 leader 仍能服务新写入（version 从 1 更新到 2）
	if r := c.Put("k", "v2", 1); r.Err != "OK" {
		t.Fatalf("crash 后 Put 期望 OK，得到 %q", r.Err)
	}
	if r := c.Get("k"); r.Err != "OK" || r.Value != "v2" || r.Version != 2 {
		t.Fatalf("crash 后 Get 期望 OK/v2/ver2，得到 %q/%q/%d", r.Err, r.Value, r.Version)
	}
}

// assertEventually 轮询 cond 直到为真或超时。
func assertEventually(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("超时未满足条件: %s", msg)
}

// ── 场景 3：分区恢复（隔离 leader，多数派仍服务；恢复后收敛）────────
func ScenarioPartitionRecovery(t *testing.T, c Cluster) {
	old := waitLeader(t, c, 5*time.Second)
	if r := c.Put("k", "v1", 0); r.Err != "OK" {
		t.Fatalf("分区前 Put 期望 OK，得到 %q", r.Err)
	}

	// 把 leader 隔离到少数派：多数派应重新选主并继续服务。
	c.Disconnect(old)
	t.Logf("分区 leader %s", old)

	// 新写入靠 sweep 找到多数派的新 leader（version 1→2）。
	if r := c.Put("k", "v2", 1); r.Err != "OK" {
		t.Fatalf("分区后（多数派）Put 期望 OK，得到 %q", r.Err)
	}

	// 恢复：旧 leader 重新入群、降级、追上；集群最终一致到 v2。
	c.Connect(old)
	t.Logf("恢复 %s", old)
	assertEventually(t, 5*time.Second, func() bool {
		r := c.Get("k")
		return r.Err == "OK" && r.Value == "v2" && r.Version == 2
	}, "恢复后集群应服务 v2")
}

// ── gRPC 底座上的场景入口 ──────────────────────────────────────

func TestGrpcElection(t *testing.T) {
	c := newGrpcCluster(t, 3)
	defer c.Cleanup()
	ScenarioElection(t, c)
}

func TestGrpcReplication(t *testing.T) {
	c := newGrpcCluster(t, 3)
	defer c.Cleanup()
	ScenarioReplication(t, c)
}

func TestGrpcLeaderCrash(t *testing.T) {
	c := newGrpcCluster(t, 3)
	defer c.Cleanup()
	ScenarioLeaderCrash(t, c)
}

func TestGrpcPartitionRecovery(t *testing.T) {
	c := newGrpcCluster(t, 3)
	defer c.Cleanup()
	ScenarioPartitionRecovery(t, c)
}

// ── 场景 5：响应丢失 + 版本守卫（gRPC 独有，labrpc 造不出，故不走 Cluster 接口）──
//
// 复现「服务端已处理、响应回程丢失 → 客户端只看到 error」，验证版本化 Put 的
// exactly-once【应用】：重试命中版本守卫、不 double-apply；而客户端拿到 ErrVersion
// （真实 clerk 会解读为 ErrMaybe）—— 即「应用 exactly-once，响应 at-most-once」。
func TestGrpcResponseLossDedup(t *testing.T) {
	c := newGrpcCluster(t, 3)
	defer c.Cleanup()

	leader := waitLeader(t, c, 5*time.Second)

	// leader 处理下一次 KV 请求后吞掉响应。
	c.fault.LoseNextKVResponses(leader, 1)

	// 创建 k=v1(version=0)：第一次被应用(version→1)但响应丢失，sweep 重试同一 Put，
	// 命中版本守卫 → ErrVersion（不是再次应用）。
	r := c.Put("k", "v1", 0)
	t.Logf("响应丢失后重试的 Put 结果=%q（真实 clerk 会解读为 ErrMaybe）", r.Err)

	// 核心不变式：exactly-once 应用 —— 值只写了一次，version=1（绝不是 2）。
	g := c.Get("k")
	if g.Err != "OK" || g.Value != "v1" || g.Version != 1 {
		t.Fatalf("响应丢失+重试不得 double-apply：期望 OK/v1/version=1，得到 %q/%q/%d",
			g.Err, g.Value, g.Version)
	}
	// 重试应拿到 ErrVersion（version 已被自己的首次尝试推进）。
	if r.Err != "ErrVersion" {
		t.Logf("提示：重试 Put 返回 %q（确定性上预期 ErrVersion）", r.Err)
	}
}
