// Command raftkvd 是一个 Raft 节点的宿主进程：起一个 gRPC server，
// 挂上 Raft peer 平面服务（RaftService），N 个进程组成一个集群。
//
// Phase 1 里程碑 ②：先只跑 Raft（选主 + 复制），暂不挂 KV service。
// applyCh 目前被排空丢弃；里程碑 ③ 会换成真正的 KV 状态机。
//
// 用法示例（本地 3 节点）：
//
//	raftkvd --node-id n1 --listen 127.0.0.1:5001 --data-dir ./data/n1 \
//	        --peers n1=127.0.0.1:5001,n2=127.0.0.1:5002,n3=127.0.0.1:5003
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"6.5840/filewal"
	kvraft "6.5840/kvraft1"
	"6.5840/kvraft1/pebblestore"
	"6.5840/proto"
	raft "6.5840/raft1"
	"6.5840/transport"
	grpcx "6.5840/transport/grpc"
	"6.5840/wal"

	grpclib "google.golang.org/grpc"
	grpcHealth "google.golang.org/grpc/health"
	grpcHealthV1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

const (
	livenessServiceName       = "raftkv-liveness"
	readinessServiceName      = "raftkv-readiness"
	leadershipTransferTimeout = 2 * time.Second
	grpcDrainTimeout          = 10 * time.Second
)

type grpcServerStopper interface {
	GracefulStop()
	Stop()
}

func main() {
	cfg, err := parseFlags()
	if err != nil {
		if err == flag.ErrHelp {
			printConfigUsage(os.Stdout)
			return
		}
		fmt.Fprintln(os.Stderr, "config error:", err)
		printConfigUsage(os.Stderr)
		os.Exit(2)
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})).
		With("node_id", string(cfg.nodeID))
	slog.SetDefault(log)

	if err := run(cfg, log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(cfg config, log *slog.Logger) error {
	// --- 稳定顺序的 nodeIDs，peers[i] 对应 nodeIDs[i] ---
	nodeIDs := make([]transport.NodeID, 0, len(cfg.peers))
	for id := range cfg.peers {
		nodeIDs = append(nodeIDs, id)
	}
	sort.Slice(nodeIDs, func(i, j int) bool { return nodeIDs[i] < nodeIDs[j] })

	// --- 每个 peer 一条 gRPC ClientEnd（惰性连接，peer 未就绪也没关系）---
	ends := make([]transport.ClientEnd, len(nodeIDs))
	grpcEnds := make([]*grpcx.ClientEnd, len(nodeIDs))
	for i, id := range nodeIDs {
		ce, err := grpcx.NewClientEnd(cfg.peers[id])
		if err != nil {
			return fmt.Errorf("dial peer %s (%s): %w", id, cfg.peers[id], err)
		}
		ends[i] = ce
		grpcEnds[i] = ce
	}

	// One cleanup stack keeps shutdown ordering explicit on both normal exit
	// and initialization failures: stop Raft first, then peer connections,
	// Pebble, and finally the Raft WAL.
	var fw *filewal.FileWAL
	var store *pebblestore.Store
	var rf *raft.Raft
	var err error
	defer func() {
		if rf != nil {
			rf.Kill()
		}
		for _, ce := range grpcEnds {
			if err := ce.Close(); err != nil {
				log.Warn("close peer connection", "addr", ce.Addr(), "err", err)
			}
		}
		if store != nil {
			if err := store.Close(); err != nil {
				log.Error("close pebble", "err", err)
			}
		}
		if fw != nil {
			if err := fw.Close(); err != nil {
				log.Error("close filewal", "err", err)
			}
		}
	}()

	// --- 落盘：raft fileWAL 与 KV pebble **分目录**（决策 5：log 高频顺序 vs 状态机随机读写）---
	raftDir := filepath.Join(cfg.dataDir, "raft")
	dbDir := filepath.Join(cfg.dataDir, "db")
	if err := os.MkdirAll(raftDir, 0o755); err != nil {
		return fmt.Errorf("mkdir raft dir: %w", err)
	}
	fw, err = filewal.Open(wal.OSFS(), raftDir)
	if err != nil {
		return fmt.Errorf("open filewal: %w", err)
	}
	store, err = pebblestore.Open(dbDir)
	if err != nil {
		return fmt.Errorf("open pebble: %w", err)
	}

	// --- 启动 KV 节点（内部经 rsm 创建 Raft，applyCh 喂 KV 状态机 = pebble）---
	// maxRaftState<0：不快照（log 重放恢复）；>0：日志涨过它就 pebble.Checkpoint→blob→raft.Snapshot 压缩。
	kv, rfi := kvraft.StartKVServerGrpc(ends, cfg.nodeID, nodeIDs, fw, store, cfg.maxRaftState)
	raftImpl, ok := rfi.(*raft.Raft)
	if !ok {
		return fmt.Errorf("raft 返回类型非 *raft.Raft，无法挂 RaftService")
	}
	rf = raftImpl

	// --- gRPC server 同时挂 RaftService(peer 平面) + KVService(client 平面) ---
	// Phase 1 两平面 co-locate 在一个端口；Phase 3 再按 etcd 式拆分。
	lis, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.listen, err)
	}
	defer lis.Close()
	srv := grpclib.NewServer()
	proto.RegisterRaftServer(srv, grpcx.NewRaftService(rf))
	proto.RegisterKVServer(srv, grpcx.NewKVService(kv))
	healthSrv := grpcHealth.NewServer()
	grpcHealthV1.RegisterHealthServer(srv, healthSrv)
	// Use separate named statuses so Kubernetes can keep liveness deliberately
	// weak while readiness remains an independently evolvable application gate.
	healthSrv.SetServingStatus(livenessServiceName, grpcHealthV1.HealthCheckResponse_SERVING)
	healthSrv.SetServingStatus(readinessServiceName, grpcHealthV1.HealthCheckResponse_NOT_SERVING)
	reflection.Register(srv) // 便于 grpcurl 等工具免 .proto 直接调用

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()
	readinessStop := make(chan struct{})
	go markReadyAfterRaftParticipation(rf, healthSrv, readinessStop)
	log.Info("raftkvd started",
		"listen", cfg.listen,
		"peer_port", cfg.peerPort,
		"client_port", cfg.clientPort,
		"data_dir", cfg.dataDir,
		"peers", len(nodeIDs),
		"peers_derived", cfg.peersDerived)

	// --- 等 SIGTERM/SIGINT 优雅停机 ---
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sig)
	select {
	case s := <-sig:
		log.Info("shutting down", "signal", s.String())
		close(readinessStop)
		// Leave Service endpoints before draining in-flight RPCs. Shutdown marks
		// every registered health service NOT_SERVING and rejects later changes.
		healthSrv.Shutdown()

		term, isLeader := rf.GetState()
		if isLeader {
			ctx, cancel := context.WithTimeout(context.Background(), leadershipTransferTimeout)
			started := time.Now()
			err := rf.TransferLeadership(ctx)
			cancel()
			if err != nil {
				// Transfer is an availability optimization, not a shutdown
				// prerequisite. Fall back to the normal Raft election after exit.
				log.Warn("leadership transfer failed; continuing shutdown",
					"term", term, "elapsed", time.Since(started), "err", err)
			} else {
				log.Info("leadership transfer initiated",
					"term", term, "elapsed", time.Since(started))
			}
		}

		if !gracefulStopWithTimeout(srv, grpcDrainTimeout) {
			log.Warn("gRPC drain timed out; forced stop", "timeout", grpcDrainTimeout)
		}
		return nil
	case err := <-serveErr:
		close(readinessStop)
		healthSrv.Shutdown()
		if err != nil {
			return fmt.Errorf("grpc serve: %w", err)
		}
		return nil
	}
}

// gracefulStopWithTimeout drains in-flight RPCs but guarantees that shutdown
// cannot consume the entire Kubernetes termination grace period. It returns
// false when Stop had to force the server down.
func gracefulStopWithTimeout(srv grpcServerStopper, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		srv.Stop()
		return false
	}
}

// markReadyAfterRaftParticipation keeps readiness independent of leadership.
// A positive term means this node has started an election or observed a valid
// Raft RPC, so WAL/Pebble/Raft/gRPC initialization alone is not enough to pass.
// GetState exposes no safe replication-lag metric; KV reads still go through
// Raft, so we deliberately avoid inventing an unreliable lag gate here.
func markReadyAfterRaftParticipation(rf *raft.Raft, healthSrv *grpcHealth.Server, stop <-chan struct{}) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		term, _ := rf.GetState()
		if term > 0 {
			healthSrv.SetServingStatus(readinessServiceName, grpcHealthV1.HealthCheckResponse_SERVING)
			return
		}
		select {
		case <-stop:
			return
		case <-ticker.C:
		}
	}
}
