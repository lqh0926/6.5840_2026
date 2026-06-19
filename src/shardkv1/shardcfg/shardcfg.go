package shardcfg

import (
	"encoding/json"
	"hash/fnv"
	"log"
	"runtime/debug"
	"slices"
	"testing"

	"6.5840/tester1"
)

type Tshid int
type Tnum int

const (
	NShards  = 12 // 分片数量。
	NumFirst = Tnum(1)
)

const (
	Gid1 = tester.Tgid(1)
)

// 根据 key 确定其属于哪个分片。
// 请使用此函数，
// 请勿修改。
func Key2Shard(key string) Tshid {
	h := fnv.New32a()
	h.Write([]byte(key))
	shard := Tshid(Tshid(h.Sum32()) % NShards)
	return shard
}

// 配置 -- 分片到组的分配关系。
// 请勿修改。
type ShardConfig struct {
	Num    Tnum                     // config number
	Shards [NShards]tester.Tgid     // shard -> gid
	Groups map[tester.Tgid][]string // gid -> servers[]
}

func MakeShardConfig() *ShardConfig {
	c := &ShardConfig{
		Groups: make(map[tester.Tgid][]string),
	}
	return c
}

func (cfg *ShardConfig) String() string {
	b, err := json.Marshal(cfg)
	if err != nil {
		log.Fatalf("Unmarshall err %v", err)
	}
	return string(b)
}

func FromString(s string) *ShardConfig {
	scfg := &ShardConfig{}
	if err := json.Unmarshal([]byte(s), scfg); err != nil {
		log.Fatalf("Unmarshall err %v", err)
	}
	return scfg
}

func (cfg *ShardConfig) Copy() *ShardConfig {
	c := MakeShardConfig()
	c.Num = cfg.Num
	c.Shards = cfg.Shards
	for k, srvs := range cfg.Groups {
		s := make([]string, len(srvs))
		copy(s, srvs)
		c.Groups[k] = s
	}
	return c
}

// 返回最多分片组、最多分片数、最少分片组、最少分片数
func analyze(c *ShardConfig) (tester.Tgid, int, tester.Tgid, int) {
	counts := map[tester.Tgid]int{}
	for _, g := range c.Shards {
		counts[g] += 1
	}

	mn := -1
	var mg tester.Tgid = -1
	ln := 257
	var lg tester.Tgid = -1
	// 强制确定性排序，因为 Go 中 map 的迭代顺序是随机的
	groups := make([]tester.Tgid, len(c.Groups))
	i := 0
	for k := range c.Groups {
		groups[i] = k
		i++
	}
	slices.Sort(groups)
	for _, g := range groups {
		if counts[g] < ln {
			ln = counts[g]
			lg = g
		}
		if counts[g] > mn {
			mn = counts[g]
			mg = g
		}
	}

	return mg, mn, lg, ln
}

// 返回被分配分片数最少的组的 GID。
func least(c *ShardConfig) tester.Tgid {
	_, _, lg, _ := analyze(c)
	return lg
}

// 平衡分片在各组之间的分配。
// 会修改 c。
func (c *ShardConfig) Rebalance() {
	// 如果没有组，取消所有分片的分配
	if len(c.Groups) < 1 {
		for s, _ := range c.Shards {
			c.Shards[s] = 0
		}
		return
	}

	// 分配所有未分配的分片
	for s, g := range c.Shards {
		_, ok := c.Groups[g]
		if ok == false {
			lg := least(c)
			c.Shards[s] = lg
		}
	}

	// 将分片从负载最重的组迁移到负载最轻的组
	for {
		mg, mn, lg, ln := analyze(c)
		if mn < ln+2 {
			break
		}
		// 将 1 个分片从 mg 迁移到 lg
		for s, g := range c.Shards {
			if g == mg {
				c.Shards[s] = lg
				break
			}
		}
	}
}

func (cfg *ShardConfig) Join(servers map[tester.Tgid][]string) bool {
	changed := false
	for gid, servers := range servers {
		_, ok := cfg.Groups[gid]
		if ok {
			log.Printf("re-Join %v", gid)
			return false
		}
		for xgid, xservers := range cfg.Groups {
			for _, s1 := range xservers {
				for _, s2 := range servers {
					if s1 == s2 {
						log.Fatalf("Join(%v) puts server %v in groups %v and %v", gid, s1, xgid, gid)
					}
				}
			}
		}
		// 新 GID
		// 修改 cfg 以反映 Join() 操作
		cfg.Groups[gid] = servers
		changed = true
	}
	if changed == false {
		log.Fatalf("Join but no change")
	}
	cfg.Num += 1
	return true
}

func (cfg *ShardConfig) Leave(gids []tester.Tgid) bool {
	changed := false
	for _, gid := range gids {
		_, ok := cfg.Groups[gid]
		if ok == false {
			// 该 GID 已经不存在！
			log.Printf("Leave(%v) but not in config", gid)
			return false
		} else {
			// 修改 cfg 以反映 Leave() 操作
			delete(cfg.Groups, gid)
			changed = true
		}
	}
	if changed == false {
		debug.PrintStack()
		log.Fatalf("Leave but no change")
	}
	cfg.Num += 1
	return true
}

func (cfg *ShardConfig) JoinBalance(servers map[tester.Tgid][]string) bool {
	if !cfg.Join(servers) {
		return false
	}
	cfg.Rebalance()
	return true
}

func (cfg *ShardConfig) LeaveBalance(gids []tester.Tgid) bool {
	if !cfg.Leave(gids) {
		return false
	}
	cfg.Rebalance()
	return true
}

func (cfg *ShardConfig) GidServers(sh Tshid) (tester.Tgid, []string, bool) {
	gid := cfg.Shards[sh]
	srvs, ok := cfg.Groups[gid]
	return gid, srvs, ok
}

func (cfg *ShardConfig) IsMember(gid tester.Tgid) bool {
	for _, g := range cfg.Shards {
		if g == gid {
			return true
		}
	}
	return false
}

func (cfg *ShardConfig) CheckConfig(t *testing.T, groups []tester.Tgid) {
	if len(cfg.Groups) != len(groups) {
		fatalf(t, "wanted %v groups, got %v", len(groups), len(cfg.Groups))
	}

	// 组是否符合预期？
	for _, g := range groups {
		_, ok := cfg.Groups[g]
		if ok != true {
			fatalf(t, "missing group %v", g)
		}
	}

	// 是否存在未分配的分片？
	if len(groups) > 0 {
		for s, g := range cfg.Shards {
			_, ok := cfg.Groups[g]
			if ok == false {
				fatalf(t, "shard %v -> invalid group %v", s, g)
			}
		}
	}

	// 分片分配是否大致平衡？
	counts := map[tester.Tgid]int{}
	for _, g := range cfg.Shards {
		counts[g] += 1
	}
	min := 257
	max := 0
	for g, _ := range cfg.Groups {
		if counts[g] > max {
			max = counts[g]
		}
		if counts[g] < min {
			min = counts[g]
		}
	}
	if max > min+1 {
		fatalf(t, "max %v too much larger than min %v", max, min)
	}
}

func fatalf(t *testing.T, format string, args ...any) {
	debug.PrintStack()
	t.Fatalf(format, args...)
}
