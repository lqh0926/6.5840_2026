// Package transport defines the minimal abstraction for RPC communication,
// independent of any Raft semantics or concrete transport implementation.
package transport

// ClientEnd is the minimal interface for making a single RPC call.
// labrpc.ClientEnd and future gRPC clients both satisfy it.
type ClientEnd interface {
	Call(svcMeth string, args any, reply any) bool
}

// NodeID 是 Raft 节点的稳定标识符，与传输地址解耦。
// 它与 labrpc 使用的 int 下标互不依赖，因此节点可以不依赖其在
// peers 切片中的位置来标识。
type NodeID string
