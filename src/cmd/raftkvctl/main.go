// Command raftkvctl 是一个薄 gRPC KV 客户端，对 raftkvd 集群做 put/get。
// 它自动找 leader：对每个 peer 依次尝试，遇 ERR_WRONG_LEADER 或连接失败就换
// 下一个，整轮失败则退避重试（等待选主完成）。
//
// 用法：
//
//	raftkvctl --peers n1=127.0.0.1:5001,n2=127.0.0.1:5002,n3=127.0.0.1:5003 put k v [version]
//	raftkvctl --peers ... get k
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"6.5840/proto"

	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const findLeaderTimeout = 10 * time.Second

func main() {
	peersFlag := flag.String("peers", "", "NodeID=host:port,...（逗号分隔）")
	flag.Parse()
	args := flag.Args()

	addrs := parseAddrs(*peersFlag)
	if len(addrs) == 0 || len(args) == 0 {
		usage()
	}

	clients := make([]proto.KVClient, 0, len(addrs))
	for _, a := range addrs {
		conn, err := grpclib.NewClient(a, grpclib.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Fprintf(os.Stderr, "dial %s: %v\n", a, err)
			os.Exit(1)
		}
		defer conn.Close()
		clients = append(clients, proto.NewKVClient(conn))
	}

	switch args[0] {
	case "put":
		if len(args) < 3 {
			usage()
		}
		var ver uint64
		if len(args) >= 4 {
			v, err := strconv.ParseUint(args[3], 10, 64)
			if err != nil {
				fmt.Fprintln(os.Stderr, "version 必须是无符号整数:", err)
				os.Exit(2)
			}
			ver = v
		}
		if err := doPut(clients, args[1], args[2], ver); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "get":
		if len(args) < 2 {
			usage()
		}
		if err := doGet(clients, args[1]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	default:
		usage()
	}
}

func doPut(clients []proto.KVClient, key, value string, version uint64) error {
	deadline := time.Now().Add(findLeaderTimeout)
	for time.Now().Before(deadline) {
		for _, c := range clients {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			rep, err := c.Put(ctx, &proto.PutArgs{Key: key, Value: value, Version: version})
			cancel()
			if err != nil {
				continue // 连接/超时，换下一个
			}
			if rep.Err == proto.Err_ERR_WRONG_LEADER {
				continue // 非 leader，换下一个
			}
			fmt.Println(rep.Err.String())
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("put 失败：超时未找到 leader")
}

func doGet(clients []proto.KVClient, key string) error {
	deadline := time.Now().Add(findLeaderTimeout)
	for time.Now().Before(deadline) {
		for _, c := range clients {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			rep, err := c.Get(ctx, &proto.GetArgs{Key: key})
			cancel()
			if err != nil {
				continue
			}
			if rep.Err == proto.Err_ERR_WRONG_LEADER {
				continue
			}
			if rep.Err == proto.Err_OK {
				fmt.Printf("%s\t(version=%d)\n", rep.Value, rep.Version)
			} else {
				fmt.Println(rep.Err.String())
			}
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("get 失败：超时未找到 leader")
}

func parseAddrs(s string) []string {
	var addrs []string
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		_, addr, ok := strings.Cut(part, "=")
		if !ok {
			addr = part // 也允许直接给 host:port
		}
		if addr = strings.TrimSpace(addr); addr != "" {
			addrs = append(addrs, addr)
		}
	}
	return addrs
}

func usage() {
	fmt.Fprintln(os.Stderr, "用法:")
	fmt.Fprintln(os.Stderr, "  raftkvctl --peers n1=addr,... put <key> <value> [version]")
	fmt.Fprintln(os.Stderr, "  raftkvctl --peers n1=addr,... get <key>")
	os.Exit(2)
}
