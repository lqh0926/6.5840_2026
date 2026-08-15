# Docker + k8s 部署速查（Phase 3 落地 + 常用）

> 定位：Phase 1/2 把系统做**对**（gRPC + WAL + LSM），Phase 3 把它做**得像生产**——稳定身份 + 稳定卷 +
> 探针 + 优雅停机 + 两平面分界。**代码增量小，深度全在"为什么这么部署"。** 部署层不侵入 raft/kv 算法路径
> （唯一动 raft 的是最小版 leadership transfer）。产物：`Dockerfile` / `compose.yaml` / `deploy/k8s/*`。

## 一句话对账（config ↔ code，都对得上）

| 契约 | YAML 侧 | 代码侧（`cmd/raftkvd`） |
|------|---------|------------------------|
| NodeID = Pod 身份 | `NODE_ID ← metadata.name`（downward API） | `derivePeers` 生成的成员 ID 同为 `raftkv-{0..N-1}` |
| peers 派生 | `REPLICAS/STATEFULSET_NAME/SERVICE_DNS/PEER_PORT` | `derivePeers` = `raftkv-{i}.<serviceDNS>:peerPort` |
| 探针 service 名 | probe `service: raftkv-liveness` / `raftkv-readiness` | `livenessServiceName`/`readinessServiceName` 注册进 gRPC health server |
| 停机预算 | `terminationGracePeriodSeconds: 30`，preStop `sleep 2` | transfer 2s + drain 10s → 2+2+10=14s < 30s |

## Dockerfile：多阶段 + distroless + nonroot

```dockerfile
FROM golang:1.25.12-bookworm AS build         # gRPC/x/net 已要求 Go 1.25（非原计划 1.22）
RUN go mod edit -go=1.25.8 && go mod download  # ★ 只改 builder 私有副本；课程 go.mod 的 go 1.22 不动（L1 一行不改）
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/raftkvd ./cmd/raftkvd
                                               # ★ CGO=0 静态二进制（pebble 纯 Go 无 CGO）→ 运行镜像可用 static

FROM gcr.io/distroless/static-debian12:nonroot AS runtime  # 无 shell/包管理/编译器/源码；nonroot 给 UID/GID 65532
COPY --from=build --chown=65532:65532 /out/raftkvd /usr/local/bin/raftkvd
COPY --from=build --chown=65532:65532 /out/data/  /var/lib/raftkv/   # 预建可写数据目录
USER 65532:65532
STOPSIGNAL SIGTERM                             # 对齐优雅停机
EXPOSE 7000 7001                               # peer / client（仅文档；真正路由靠 run/compose/k8s）
```

**要点**：① Go 版本校正只落 builder，源码模块声明不动。② `CGO_ENABLED=0` 是 distroless-static 的前提（pebble
纯 Go 才成立）。③ distroless 无 `/bin/sh` → 探针/ preStop 都不能用 exec，必须走**原生 gRPC probe / 原生 sleep action**。

## compose.yaml：容器化 + Docker DNS 的快速验证台

```yaml
environment:
  NODE_ID: n1
  PEERS: n1=n1:7000,n2=n2:7000,n3=n3:7000   # 用 compose service 名硬写，靠 compose 自带 DNS（还不走派生）
user: "65532:65532"
read_only: true                             # 只读根文件系统
cap_drop: [ALL]                             # 丢掉所有 Linux capabilities
security_opt: [no-new-privileges:true]      # 禁提权
stop_grace_period: 15s                      # > transfer2s+drain10s=12s
volumes: [n1-data:/var/lib/raftkv]          # named volume 模拟 PVC
```

**用途**：compose 比 k8s 迭代快，先在这里验"容器化 + env 配置 + 服务发现 DNS + 卷持久化"，再上 k8s。
**验证口径**：起集群 → put/get → `docker compose up --force-recreate n2`（比 restart 更严）→ 从原卷读回数据。

## deploy/k8s/：StatefulSet + headless Service + PVC

### statefulset.yaml（决策 1/3/4/5 的落地）
```yaml
serviceName: raftkv                 # 必须指向 headless Service，二者一起给每个 ordinal 稳定 DNS
replicas: 3
podManagementPolicy: Parallel       # 静态成员并行 bootstrap，不让 raftkv-0 成为 1/2 的前置
persistentVolumeClaimRetentionPolicy:
  whenDeleted: Retain               # ★ 显式：删/缩 StatefulSet 不能顺手删 Raft/Pebble 数据
  whenScaled: Retain
securityContext:                    # Pod 级
  runAsNonRoot: true; runAsUser: 65532; runAsGroup: 65532
  fsGroup: 65532                    # ★ PVC 挂载盖掉镜像预建目录 owner，靠 fsGroup 让卷可写
  fsGroupChangePolicy: OnRootMismatch
  seccompProfile: {type: RuntimeDefault}
env:
  - {name: NODE_ID, valueFrom: {fieldRef: {fieldPath: metadata.name}}}  # downward API 注入稳定身份
  - {name: REPLICAS, value: "3"}
  - {name: SERVICE_DNS, value: raftkv.raftkv.svc.cluster.local}
livenessProbe:  {grpc: {port: 7000, service: raftkv-liveness}}   # 判据弱：进程/gRPC 活着即可
readinessProbe: {grpc: {port: 7000, service: raftkv-readiness}}  # 独立命名，语义可单独演进
startupProbe:   {grpc: {port: 7000, service: raftkv-liveness}}
lifecycle: {preStop: {sleep: {seconds: 2}}}   # 原生 sleep（distroless 无 shell）；给 endpoint 移除传播留时间
securityContext: {allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: {drop: [ALL]}}
volumeClaimTemplates: [{name: data, ...}]     # 每 ordinal 一块 PVC：data-raftkv-0/1/2
```

### services.yaml（两个 Service，对应两平面）
```yaml
# headless：无 ClusterIP、不负载均衡，DNS 给每个 Pod 一条稳定记录
kind: Service; metadata: {name: raftkv}
spec:
  clusterIP: None
  publishNotReadyAddresses: true    # ★★ 命门：peer 互联/选主不能被 readiness 门控，否则新集群互等 ready → 死锁选不出主
  ports: [{name: peer, port: 7000, targetPort: rpc}]
---
# client：普通 ClusterIP，只路由 ready 的 Pod
kind: Service; metadata: {name: raftkv-client}
spec:
  ports: [{name: client, port: 7001, targetPort: rpc}]  # Step 7 前 targetPort 仍是 rpc(7000)；拆分后改 client
```

## 优雅停机时序（main.go，SIGTERM 后）

```
preStop sleep 2s（k8s 先把 Pod 移出 client endpoints）
  → SIGTERM
  → healthSrv.Shutdown()          // 所有 health service 置 NOT_SERVING，readiness 失败
  → if leader: TransferLeadership(ctx 2s)   // 选 matchIndex 最高 follower 发 TimeoutNow；失败仅回退到选举超时
  → gracefulStopWithTimeout(srv, 10s)       // 放干在途 RPC；超时则强制 Stop()
```
把 leader 不可用窗口从"一个选举超时"压到"≈ 一次 RTT"。**transfer 是可用性优化、不是停机前提**（失败照常退出）。

## 常用 kubectl / 验证

```bash
kind create cluster --config deploy/k8s/kind-config.yaml
kind load docker-image raftkv:phase3          # 把本地镜像塞进 kind 节点（imagePullPolicy: IfNotPresent）
kubectl apply -k deploy/k8s                    # kustomization：namespace + services + statefulset
kubectl -n raftkv get pods -w                  # 等 3 个 Running + Ready
kubectl -n raftkv delete pod raftkv-1          # ★ = Phase 2 kill-9 的 k8s 版：挂回同名 PVC、数据存活、集群自愈
kubectl -n raftkv port-forward svc/raftkv-client 7001:7001   # 外部读写入口
```

## 常用 StatefulSet / 安全字段速查

| 字段 | 作用 |
|------|------|
| `serviceName` | 绑 headless Service，给 ordinal 稳定 DNS（**有状态服务必填**） |
| `volumeClaimTemplates` | 每 ordinal 一块独立 PVC，重建挂回同一卷（durability 落地点） |
| `podManagementPolicy: Parallel` | 并行起 Pod；默认 `OrderedReady` 会串行等前一个 ready |
| `persistentVolumeClaimRetentionPolicy` | 删/缩 StatefulSet 时保留 or 删 PVC（数据安全，宜 `Retain`） |
| `updateStrategy: RollingUpdate` | 按 ordinal 逆序滚动；配 leadership transfer 减少抖动 |
| `publishNotReadyAddresses`（Service） | headless 上置 true，让未 ready 的 Pod 也进 DNS（peer 发现不被门控） |
| `securityContext.fsGroup` | 卷 owner 归组，非 root 容器才写得动 PVC |
| `fsGroupChangePolicy: OnRootMismatch` | 只在根目录属主不符时递归 chown，省大卷启动耗时 |
| `runAsNonRoot`/`readOnlyRootFilesystem`/`capabilities.drop:[ALL]` | 最小权限三件套 |
| `seccompProfile: RuntimeDefault` | 用 runtime 默认 syscall 过滤 |
| `terminationGracePeriodSeconds` | SIGTERM→SIGKILL 的宽限；要 ≥ preStop+transfer+drain |
| `startup/liveness/readinessProbe.grpc` | 原生 gRPC 探针（distroless 无 shell 时的正解） |

## Gotchas（面试连环追问 / 刻意取舍）

1. **readiness 别拿 leadership 做门**：会门控 peer 平面 → 新集群互等 ready → **死锁选不出主**。靠 headless
   Service 的 `publishNotReadyAddresses: true` + readiness 只判"参与集群(term>0)"来避开。
2. **liveness 判据要弱**：只探进程/gRPC 活着。拿"是不是 leader"当 liveness → 所有 follower 被反复重启（灾难）。
3. **readiness 目前是一次性 latch**（term>0 置 SERVING 后不再翻）：活着但严重落后/被分区的节点仍 Ready、继续吃
   client 流量（靠 `ERR_WRONG_LEADER` + 客户端重试兜）。生产会加 ReadIndex/lag 阈值——本项目边界不做。
4. **leadership transfer 是 best-effort 单发**：leader 选 matchIndex 最高 follower 直接发 `TimeoutNow`，若它没追平
   (`LastLogIndex != 自己的 lastLogIndex`) 就 reject → 回退正常选举。标准 Raft 会"先追平再发"，这里省了那步（停机
   优化、失败不影响安全性）。
5. **StatefulSet 不用 Deployment**（必答）：Deployment 的 Pod 名随机 + 卷不绑身份 → 重建后找不到彼此 / 挂错卷丢数据。
6. **distroless 无 shell**：exec probe、`sh -c` preStop 全失效 → 用原生 gRPC probe + 原生 sleep action。
7. **两平面仍单端口**（Step 7 未做）：client Service:7001 现 `targetPort: rpc(7000)`；三处注释留了接缝。讲"为什么
   要拆"（TLS 信任域 / 网络暴露 / 资源隔离，对标 etcd 2379/2380）没问题，但**代码层面还没拆**，别说成已完成。

## 必须能独立复述的三条（AI 写的也要讲得出）
- **StatefulSet vs Deployment**：稳定名 + 稳定卷，对上 Raft 的"固定身份互寻址"和"数据跟 ordinal 走"。
- **publishNotReadyAddresses / readiness 不门控 peer 平面**：否则选主死锁——最能体现真懂的点。
- **fsGroup 为什么必要**：PVC 挂载盖掉镜像预设 owner，非 root 容器写不了卷，靠 fsGroup 兜。
