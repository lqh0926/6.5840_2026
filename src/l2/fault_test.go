package l2

import (
	"context"
	"strings"
	"sync"
	"time"

	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FaultPolicy 是一份【运行时可翻转】的故障策略，被 client/server 拦截器每次调用时读取。
// 测试可随时调 Partition/Heal/LoseNextKVResponses 改变它。对应 labrpc 的 net.Disconnect
// 等能力，外加 labrpc 造不出的「服务端已处理、响应丢失」。
type FaultPolicy struct {
	mu          sync.Mutex
	partitioned map[string]bool // 被分区的节点（peer 平面双向丢）
	loseRespN   map[string]int  // 该节点 server 侧还要吞掉几次 KV 响应
	delay       time.Duration   // peer 调用注入的延迟
}

func NewFaultPolicy() *FaultPolicy {
	return &FaultPolicy{partitioned: map[string]bool{}, loseRespN: map[string]int{}}
}

// Partition/Heal：把某节点从 peer 平面隔离 / 恢复（= labrpc net.Disconnect/Connect）。
func (p *FaultPolicy) Partition(id string) { p.mu.Lock(); p.partitioned[id] = true; p.mu.Unlock() }
func (p *FaultPolicy) Heal(id string)      { p.mu.Lock(); delete(p.partitioned, id); p.mu.Unlock() }

func (p *FaultPolicy) isPartitioned(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.partitioned[id]
}

func (p *FaultPolicy) SetDelay(d time.Duration) { p.mu.Lock(); p.delay = d; p.mu.Unlock() }
func (p *FaultPolicy) getDelay() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.delay
}

// LoseNextKVResponses：让 id 节点接下来 n 次 KV 请求「处理成功但吞掉响应」。
// 这是 gRPC 独有失败（服务端已改状态，客户端只看到 error/timeout），场景 5 用。
func (p *FaultPolicy) LoseNextKVResponses(id string, n int) {
	p.mu.Lock()
	p.loseRespN[id] = n
	p.mu.Unlock()
}

func (p *FaultPolicy) takeLoseKV(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.loseRespN[id] > 0 {
		p.loseRespN[id]--
		return true
	}
	return false
}

// ClientInterceptor 装在 source→target 的 peer 连接上：任一端被分区就丢（双向断链），
// 否则按需延迟后放行。
func (p *FaultPolicy) ClientInterceptor(source, target string) grpclib.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpclib.ClientConn, invoker grpclib.UnaryInvoker, opts ...grpclib.CallOption) error {
		if p.isPartitioned(source) || p.isPartitioned(target) {
			return status.Error(codes.Unavailable, "fault: partitioned")
		}
		if d := p.getDelay(); d > 0 {
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// ServerInterceptor 装在 self 节点的 gRPC server 上：正常处理请求（状态可能已变），
// 但若该节点被设置为「吞 KV 响应」，则对 KV 方法在 handler 执行后丢弃响应，
// 使客户端只看到 error/timeout —— 精确复现「服务端已处理、响应回程丢失」。
func (p *FaultPolicy) ServerInterceptor(self string) grpclib.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpclib.UnaryServerInfo, handler grpclib.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req) // 真处理：状态可能已经改变
		if err == nil && isKVMethod(info.FullMethod) && p.takeLoseKV(self) {
			return nil, status.Error(codes.Unavailable, "fault: response lost")
		}
		return resp, err
	}
}

func isKVMethod(fullMethod string) bool {
	// KV 平面方法形如 "/proto.KV/Put"；Raft 平面为 "/proto.Raft/..."（不含 "KV"）。
	return strings.Contains(fullMethod, "KV")
}
