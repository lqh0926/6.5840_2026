package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"6.5840/transport"
)

const (
	defaultPeerPort   = 7000
	defaultClientPort = 7001
)

// config 同时承载本地进程和容器部署的配置。在 Phase 3 Step 7 拆分两个
// gRPC 平面前，listen 仍是实际的单监听地址；peerPort/clientPort 先用于成员地址派生
// 和部署配置，为后续拆分保留明确边界。
type config struct {
	nodeID     transport.NodeID
	listen     string
	peerPort   int
	clientPort int
	dataDir    string
	peers      map[transport.NodeID]string // NodeID → peer-plane host:port

	replicas        int
	statefulSetName string
	serviceDNS      string
	peersDerived    bool

	maxRaftState int // 日志字节超过它就触发快照压缩；-1 关闭
}

type configFlagValues struct {
	nodeID          string
	listen          string
	peerPort        string
	clientPort      string
	dataDir         string
	peers           string
	replicas        string
	statefulSetName string
	serviceDNS      string
	maxRaftBytes    string
}

func newConfigFlagSet(output io.Writer) (*flag.FlagSet, *configFlagValues) {
	fs := flag.NewFlagSet("raftkvd", flag.ContinueOnError)
	fs.SetOutput(output)
	v := &configFlagValues{}
	fs.StringVar(&v.nodeID, "node-id", "", "本节点的稳定 NodeID（env: NODE_ID）")
	fs.StringVar(&v.listen, "listen", "", "gRPC 监听地址 host:port（env: LISTEN）")
	fs.StringVar(&v.peerPort, "peer-port", "", "Raft peer 平面端口（env: PEER_PORT，默认 7000）")
	fs.StringVar(&v.clientPort, "client-port", "", "KV client 平面端口（env: CLIENT_PORT，默认 7001）")
	fs.StringVar(&v.dataDir, "data-dir", "", "持久化数据目录（env: DATA_DIR）")
	fs.StringVar(&v.peers, "peers", "", "集群成员表 NodeID=host:port,...（env: PEERS；留空则自动派生）")
	fs.StringVar(&v.replicas, "replicas", "", "静态成员数（env: REPLICAS；自动派生 peers 时必填）")
	fs.StringVar(&v.statefulSetName, "statefulset-name", "", "StatefulSet 名（env: STATEFULSET_NAME）")
	fs.StringVar(&v.serviceDNS, "service-dns", "", "headless Service DNS（env: SERVICE_DNS）")
	// 注意：flag 名不能用 "max-raft-state"——tester1 已在 init 注册了它。
	fs.StringVar(&v.maxRaftBytes, "max-raft-bytes", "", "raft 日志快照阈值（env: MAX_RAFT_BYTES，默认 -1 关闭）")
	return fs, v
}

func printConfigUsage(w io.Writer) {
	fs, _ := newConfigFlagSet(w)
	fmt.Fprintln(w, "Usage: raftkvd [flags]")
	fs.PrintDefaults()
}

func parseFlags() (config, error) {
	return parseConfig(os.Args[1:], os.LookupEnv)
}

// parseConfig 实现统一优先级：显式 flag > 非空 env > 默认值。
// 数值先以字符串解析，使显式 flag 能真正覆盖一个无效 env，而不会被 env 提前报错。
func parseConfig(args []string, lookupEnv func(string) (string, bool)) (config, error) {
	fs, values := newConfigFlagSet(io.Discard)
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if fs.NArg() != 0 {
		return config{}, fmt.Errorf("不接受位置参数: %s", strings.Join(fs.Args(), " "))
	}

	explicit := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) { explicit[f.Name] = true })
	resolve := func(flagName, flagValue, envName, defaultValue string) string {
		if explicit[flagName] {
			return strings.TrimSpace(flagValue)
		}
		if value, ok := lookupEnv(envName); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
		return defaultValue
	}

	peerPort, err := parsePort("--peer-port/PEER_PORT",
		resolve("peer-port", values.peerPort, "PEER_PORT", strconv.Itoa(defaultPeerPort)))
	if err != nil {
		return config{}, err
	}
	clientPort, err := parsePort("--client-port/CLIENT_PORT",
		resolve("client-port", values.clientPort, "CLIENT_PORT", strconv.Itoa(defaultClientPort)))
	if err != nil {
		return config{}, err
	}
	replicas, err := parseInt("--replicas/REPLICAS",
		resolve("replicas", values.replicas, "REPLICAS", "0"))
	if err != nil {
		return config{}, err
	}
	maxRaftState, err := parseInt("--max-raft-bytes/MAX_RAFT_BYTES",
		resolve("max-raft-bytes", values.maxRaftBytes, "MAX_RAFT_BYTES", "-1"))
	if err != nil {
		return config{}, err
	}

	cfg := config{
		nodeID:          transport.NodeID(resolve("node-id", values.nodeID, "NODE_ID", "")),
		listen:          resolve("listen", values.listen, "LISTEN", ""),
		peerPort:        peerPort,
		clientPort:      clientPort,
		dataDir:         resolve("data-dir", values.dataDir, "DATA_DIR", ""),
		replicas:        replicas,
		statefulSetName: resolve("statefulset-name", values.statefulSetName, "STATEFULSET_NAME", ""),
		serviceDNS:      strings.TrimSuffix(resolve("service-dns", values.serviceDNS, "SERVICE_DNS", ""), "."),
		maxRaftState:    maxRaftState,
	}
	if cfg.nodeID == "" {
		return cfg, fmt.Errorf("--node-id 或 NODE_ID 必填")
	}
	if cfg.listen == "" {
		return cfg, fmt.Errorf("--listen 或 LISTEN 必填")
	}
	if cfg.dataDir == "" {
		return cfg, fmt.Errorf("--data-dir 或 DATA_DIR 必填")
	}

	peersValue := resolve("peers", values.peers, "PEERS", "")
	if peersValue != "" {
		cfg.peers, err = parsePeers(peersValue)
	} else {
		cfg.peers, err = derivePeers(cfg.replicas, cfg.statefulSetName, cfg.serviceDNS, cfg.peerPort)
		cfg.peersDerived = true
	}
	if err != nil {
		return cfg, err
	}
	if _, ok := cfg.peers[cfg.nodeID]; !ok {
		return cfg, fmt.Errorf("node ID %q 不在集群成员表中", cfg.nodeID)
	}
	return cfg, nil
}

func parseInt(name, value string) (int, error) {
	v, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s 必须是整数，得到 %q", name, value)
	}
	return v, nil
}

func parsePort(name, value string) (int, error) {
	port, err := parseInt(name, value)
	if err != nil {
		return 0, err
	}
	if port < 1 || port > 65535 {
		return 0, fmt.Errorf("%s 必须在 1..65535 之间，得到 %d", name, port)
	}
	return port, nil
}

// derivePeers 从 StatefulSet 的稳定 ordinal 和 headless Service DNS 纯计算成员表。
// 例如 replicas=3, name=raftkv, serviceDNS=raftkv 会生成：
// raftkv-0.raftkv:7000, raftkv-1.raftkv:7000, raftkv-2.raftkv:7000。
func derivePeers(replicas int, statefulSetName, serviceDNS string, peerPort int) (map[transport.NodeID]string, error) {
	if replicas <= 0 {
		return nil, fmt.Errorf("未提供 --peers/PEERS 时，--replicas 或 REPLICAS 必须大于 0")
	}
	if statefulSetName == "" {
		return nil, fmt.Errorf("未提供 --peers/PEERS 时，--statefulset-name 或 STATEFULSET_NAME 必填")
	}
	if serviceDNS == "" {
		return nil, fmt.Errorf("未提供 --peers/PEERS 时，--service-dns 或 SERVICE_DNS 必填")
	}

	peers := make(map[transport.NodeID]string, replicas)
	for ordinal := 0; ordinal < replicas; ordinal++ {
		id := transport.NodeID(fmt.Sprintf("%s-%d", statefulSetName, ordinal))
		peers[id] = fmt.Sprintf("%s.%s:%d", id, serviceDNS, peerPort)
	}
	return peers, nil
}

// parsePeers 解析 "n1=host1:port1,n2=host2:port2" 成 NodeID→addr。
func parsePeers(s string) (map[transport.NodeID]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, fmt.Errorf("--peers/PEERS 为空")
	}
	m := make(map[transport.NodeID]string)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		id, addr = strings.TrimSpace(id), strings.TrimSpace(addr)
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("--peers/PEERS 条目格式应为 NodeID=host:port，得到 %q", part)
		}
		if _, dup := m[transport.NodeID(id)]; dup {
			return nil, fmt.Errorf("--peers/PEERS 中 NodeID %q 重复", id)
		}
		m[transport.NodeID(id)] = addr
	}
	if len(m) == 0 {
		return nil, fmt.Errorf("--peers/PEERS 为空")
	}
	return m, nil
}
