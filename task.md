# Phase 3 · Docker + k8s 部署 —— 实现任务

> 证明非 toy、脱离本地 3 进程脚本，JD 高频要求。
> **一句话定位**：Phase 1/2 把系统做**对**（gRPC + WAL + LSM），Phase 3 把它做**得像生产**——
> 稳定身份 + 稳定卷 + 探针 + 优雅停机 + 两平面分界。**代码增量小，深度全在"为什么这么部署"。**
>
> 总验收：① 真 k8s（kind/minikube 即可）N 节点能**选主 + 复制**；② `kubectl delete pod raftkv-1`
> 后 Pod 重启、**数据还在**、集群自愈；③ 原 6.5840 测试（L1）与 `scripts/test-crash-recovery.sh` 照旧绿
> （容器化不碰 raft/kv 逻辑，只加宿主/配置/部署层）。
> 核心纪律：**部署层的改动不得侵入 raft/kvraft 算法路径**（同 Phase 2 的 L1 保命原则）。

---

## 现状（改造起点，Phase 1/2 交付的 binary）

- **单 binary `cmd/raftkvd`**，配置**只有 flag**（`main.go:147 parseFlags`）：
  `--node-id` / `--listen` / `--data-dir` / `--peers` / `--max-raft-bytes`。无 env、无 configmap。
- **`--peers` 是静态手列**的 `n1=host:port,n2=...,n3=...`（含本节点）。本地脚本里写死，**k8s 里没法手列**
  （Pod IP 动态、副本数可变）→ 这是 Phase 3 服务发现要解决的核心缺口。
- **两平面 co-locate 一个端口**（`main.go:113-122`）：一个 `net.Listen(cfg.listen)` 上同时挂
  `RegisterRaftServer`（peer 平面）+ `RegisterKVServer`（client 平面）+ reflection。
- **优雅停机半成品**（`main.go:129-136`）：SIGTERM/SIGINT → `srv.GracefulStop()`，**但没有 leadership
  transfer**（注释写着"留到 Phase 3"）。leader 被杀 = 一个选举超时的不可用窗口。
- **无 health / readiness 端点**：k8s 探针没东西可探。
- **落盘**：`data-dir/raft`（fileWAL：meta+wal）+ `data-dir/db`（pebble：SSTable/WAL/MANIFEST）。
  Pod 重启要数据不丢 → 这个目录**必须落在 PVC** 上（决策见下）。
- **client（`cmd/raftkvctl`）** 靠逐个试 peer + `ERR_WRONG_LEADER` 换下一个来找 leader，退避重试等选主。
- gRPC 全程 **insecure（无 TLS）**（`raftkvctl` 用 `insecure.NewCredentials()`，`raftkvd` 裸 `NewServer()`）。

---

## 设计决策（🟣 要能不看代码复述 —— 这就是面试深挖区）

### 决策 1 · StatefulSet 不用 Deployment（**必答题**，翻车陷阱 #3）
Raft 节点是**有状态、有身份**的成员，不是可互换的无状态副本。Deployment 的坑：
- **Pod 名随机**（`raftkv-7d9f-xktp`）、重建后又换一个 → 节点靠什么名字重新加入集群？找不到彼此。
- **卷不绑定身份**：重建的 Pod 可能挂到别的卷 / 空卷 → fileWAL+pebble 数据**错配或丢失**（数据是 Raft 命门）。

StatefulSet 给两个稳定保证，正好对上 Raft 的两个需求：
- **稳定网络标识**：Pod 名固定 `raftkv-0/1/2`，配 headless Service 得稳定 DNS
  `raftkv-0.raftkv.<ns>.svc.cluster.local` → 节点用**固定 DNS 名**互相寻址，Pod 漂移（换 IP）不影响。
- **稳定存储**（`volumeClaimTemplates`）：每个 ordinal 绑**自己那块 PVC**，重建后**挂回同一卷** → 数据跨重启存活。
  这直接兑现 Phase 2 的 durability：fileWAL/pebble 落 PVC，`kubectl delete pod` = 一次真 crash-restart。

### 决策 2 · 服务发现 / bootstrap：从"手列 peers"到"从身份派生"（🟣 核心）
k8s 里不能手写 IP。改成**从稳定身份推导集群成员表**：
- **NodeID = Pod 序号身份**：`$POD_NAME`（downward API 注入，等于 hostname `raftkv-0`）→ 直接当 NodeID。
  稳定、可复述、Pod 重建后不变。取代手传 `--node-id`。
- **peers 从"副本数 + StatefulSet 名 + headless Service 域"生成**：已知 `replicas=N`、`raftkv`、`.raftkv.<ns>.svc`，
  就能算出 `raftkv-0.raftkv.<ns>.svc:PORT ... raftkv-(N-1)...`。**不再手列 host:port**，只传 `--replicas`（或读 env）。
- **静态成员集（本项目边界）**：副本数固定、成员集编译期已知（**不做动态成员变更**——joint consensus 归"懂原理·不写"）。
  bootstrap = 所有节点用同一份派生的成员表启动，选主自然发生。DNS 未就绪的 peer：gRPC ClientEnd 惰性连接，重试即可。

### 决策 3 · PVC per Pod：durability 的落地点
- `volumeClaimTemplates` 每 ordinal 一块 PVC，挂到容器的 `--data-dir`。
- Pod 重启/重调度 → StatefulSet 保证挂回**同名 PVC** → fileWAL+pebble 原样还在 → Raft 从 log/snapshot 恢复。
- **验证口径**：`kubectl delete pod raftkv-1` 就是 Phase 2 kill-9 测试的 k8s 版；数据必须存活、集群自愈。

### 决策 4 · 探针语义：readiness ≠ "是不是 leader"（🟣 易错深挖点）
- **liveness（活着吗）**：进程在、gRPC 端口能应答即可。挂了 → k8s 重启 Pod。**判据要弱**——
  别拿"是 leader"做 liveness，否则所有 follower 被反复重启（灾难）。做法：gRPC health check / 一个轻量 TCP/HTTP ping。
- **readiness（能进 Service endpoint 吗）**：能**参与集群**即 ready（Raft 已起、applyCh 在推进、能转发/服务读写），
  **不是"必须是 leader"**。若拿 leadership 当 readiness gate → 只有 1 个 Pod 进 endpoint，peer 平面 DNS 还没 ready
  就无法互联 → **死锁选不出主**。**peer 平面（节点互联）不能被 readiness 门控**——这是关键。
- 结论：readiness 判"本节点 Raft 存活且不落后太多"，leader 重定向交给 **client 平面的重试**（现成，`raftkvctl` 已做）。

### 决策 5 · 优雅停机 + leadership transfer（🟣 自己写，有 raft 依赖）
- 现状 `srv.GracefulStop()` 只停止收新请求、放干在途 RPC，**不主动交权** → leader 被停 = 等一个选举超时才有新 leader。
- 目标：SIGTERM 时**若本节点是 leader，先把 leadership 转给最跟得上的 follower，再退**，把不可用窗口从"选举超时"压到"一次 RTT"。
- **raft 依赖（诚实标注）**：课程 raft **没有 `TransferLeadership`**。两条路：
  - (a) 🟣 给 raft 加**最小版转移**：leader 选一个 `matchIndex` 最高的 follower，给它发 `TimeoutNow`（或复用一次
    立即触发选举的信号）让它**立刻发起选举**、自己 stepDown 不再参选。小改动、面试可讲。
  - (b) ⚪ 不改 raft，停机只 `GracefulStop`，**接受一个选举超时的窗口**，把 (a) 当白板延伸。
  - **本项目建议 (a) 的最小版**（这是 Phase 3 少数值得动 raft 的地方；否则 Phase 3 全是 YAML，深度不够）。
- k8s 配套：`preStop` hook + `terminationGracePeriodSeconds` 给足转移 + graceful drain 的时间，避免 SIGKILL 打断。

### 决策 6 · 拆分 peer / client 两个平面（🟣 **本 Phase 最重的自己写区**，对标 etcd 2380/2379）
现状两平面 co-locate 一个端口是 Phase 1 的**刻意简化**，Phase 3 落地生产级分界。**三个必答理由**：
1. **TLS 信任域不同**：peer 平面是**节点↔节点**（内部，宜 **mTLS 双向**）；client 平面是**外部↔leader**（单向 server TLS）。
   混在一个端口 = 一套证书策略套两个信任域，讲不清。
2. **网络暴露不同**：peer 只该在**集群内网**可达（k8s NetworkPolicy 限死 pod↔pod）；client 要对外暴露（Service/Ingress）。
   一个端口没法分别设暴露面。
3. **资源隔离**：client 洪峰不能**饿死 Raft 心跳/复制** → 心跳延迟 → 误触发选举 → 集群抖动。分端口 = 分 goroutine/连接池/限流域。
- **落地**：`raftkvd` 起**两个 `net.Listen`**（peer `:PEER_PORT` 挂 RaftService、client `:CLIENT_PORT` 挂 KVService+reflection）；
  两个 `grpc.Server`。k8s 里 headless Service 只暴露 peer 端口给内部、另开 Service 暴露 client 端口。
- **连带改造**：`transport/grpc.ClientEnd` 现在一条 conn 假设"一个 peer 一个地址"。peer 平面互联只需 peer 端口
  （raftCli），不再和 kvCli 捆在一起；派生 peers 时地址用 peer 端口。
- **对称收尾**（顺手做）：KV client 平面的 `Call` 统一成收 **kv 的 Go 结构体**（当前 `KVService`/`raftkvctl` 仍直传 proto），
  和 Raft 平面对称（Raft 平面已是 Go 结构体 + transport 层翻译）。
- **必答题延伸**：为什么 etcd 分 `2379`（client）/`2380`（peer）？就是上面 ①②③。

### 决策 7 · 运行时最小权限：只给 raftkv 写数据目录

容器隔离不等于可以默认用 root。本项目的 raftkv 只需要绑定高位端口和写数据目录，
不需要 root 身份、Linux capabilities、提权或可写根文件系统。

- **镜像**：使用 distroless `nonroot` 的 UID/GID `65532:65532`；`COPY --chown=65532:65532` 预创建
  `/var/lib/raftkv`；`USER 65532:65532`。二进制 root-owned + `0755` 也能执行，数据目录则必须可写。
- **docker-compose**：显式 `user: "65532:65532"`、`read_only: true`、`cap_drop: [ALL]`、
  `security_opt: [no-new-privileges:true]`；只把每节点自己的 named volume 挂到 `/var/lib/raftkv`。
- **Kubernetes**：`runAsNonRoot/runAsUser/runAsGroup=65532`、`allowPrivilegeEscalation: false`、
  `readOnlyRootFilesystem: true`、`capabilities.drop: [ALL]`、`seccompProfile: RuntimeDefault`。Pod 级设
  `fsGroup: 65532`，因为 PVC 挂载会覆盖镜像里预创建目录的 ownership；若存储驱动不支持
  `fsGroup`，再用最小 initContainer 只修正该卷权限，不让主容器跑 root。
- **验收**：`docker top` 看到 UID 65532；非 root 能创建 WAL/Pebble；root filesystem 只读时仍能运行；
  卷目录 owner/group 与写权限正确。

---

## Steps（按依赖排序；每步跑 L1 + crash 脚本回归）

### Step 1 · 配置层：flag → flag + env（容器友好）+ 从身份派生 peers 🟢
> 容器里配置走 env/downward API，本地仍可用 flag。**优先级：flag > env > 默认**。
- [x] 每个配置项加 env 兜底：`NODE_ID`(=`$POD_NAME`)、`LISTEN`/`PEER_PORT`/`CLIENT_PORT`、`DATA_DIR`、
      `REPLICAS`、`STATEFULSET_NAME`、`SERVICE_DNS`、`MAX_RAFT_BYTES`。flag 显式给则覆盖 env。
- [x] `--peers` 变为**可选**：不给则由 `REPLICAS + STATEFULSET_NAME + SERVICE_DNS + PEER_PORT` **派生成员表**
      （决策 2）。本地脚本继续显式传 `--peers`（不回归破坏）。
- [x] NodeID 从 `$POD_NAME` 取（downward API），ordinal 稳定。
- **验收**：本地 `raftkvd --peers ...` 行为不变；给 env 版能自算 peers 起集群（可先用 docker-compose 验，Step 3）。

### Step 2 · Dockerfile（多阶段构建）🟢
> 实际依赖 `grpc v1.82.0` / `x/net v0.56.0` 已要求 Go 1.25，根目录 `go.work` 也是 Go 1.25.8；
> 因此 builder 从原计划的 Go 1.22 校正为 `golang:1.25.12-bookworm`。
- [x] 多阶段：`golang:1.25.12-bookworm` build（`CGO_ENABLED=0` 静态，pebble 是纯 Go 无 CGO 依赖 ✓）→ `distroless` 运行镜像。
- [x] 只拷 `raftkvd`；镜像小、无源码。`raftkvctl` 保留为宿主机/独立工具，不放进每个 server 镜像。
- [x] `ENTRYPOINT ["/usr/local/bin/raftkvd"]`，配置全走 env/flag；`EXPOSE` peer + client 两个端口。
- [x] **验收**：`docker build` 产出 `raftkv:phase3`；单节点容器以 UID 65532 启动并可写数据目录；
      `docker stop` 经 SIGTERM 优雅退出且 exit code=0；二次 build 全部命中 BuildKit cache。

### Step 3 · docker-compose：本地多进程/多容器联调 🟢
> 先在 compose 里验证"容器化 + env 配置 + Docker DNS + named volume"，再上 k8s（compose 比 k8s
> 迭代快）。当前 peer/KV 仍 co-locate 在 7000；7001 只预留，两平面真拆分后在 Step 7 复验。
- [x] 3 服务 `n1/n2/n3`，各自 env（NODE_ID、端口、DATA_DIR），`--peers` 用 compose service 名（compose 自带 DNS）。
- [x] 各挂一个 named volume 到 DATA_DIR（模拟 PVC）；三个卷内均实测生成 Pebble + Raft WAL，owner 为 `65532:65532`。
- [x] 起集群 → `raftkvctl` put/get → **强制重建** n2（比 `restart` 更严格）→ 从原 named volume 读回数据。
- **验收**：compose 集群选主/复制/单容器替换恢复正常；n2 容器 ID 已变化、卷名仍为 `raftkv_n2-data`，
  `phase3-step3=compose-ok (version=1)` 可读。三个容器均实测 `user=65532:65532`、只读 rootfs、
  `cap_drop=ALL`、`no-new-privileges=true`（= crash 脚本的容器版 + 决策 7 权限实践）。

### Step 4 · k8s：StatefulSet + headless Service + PVC 🟢（决策 1/2/3 落地）
- [x] **headless Service**（`clusterIP: None`）给每个 Pod 稳定 DNS（peer 平面互联靠它）；
      `publishNotReadyAddresses: true` 避免 peer DNS 被 readiness 门控。
- [x] **StatefulSet**：`replicas: 3`、`serviceName: raftkv`、`volumeClaimTemplates`（每 Pod 一块 1Gi RWO PVC 挂 DATA_DIR）。
- [x] Pod env：downward API 注入 `POD_NAME`→NODE_ID；`REPLICAS`/`SERVICE_DNS`/端口走 env；日志实测
      `node_id=raftkv-1 peers=3 peers_derived=true`。
- [x] 另一个 **ClusterIP Service** 暴露 client Service 端口 7001。Step 7 前容器仍单监听 7000，故暂时
      `port: 7001 -> targetPort: rpc(7000)`；双 listener 落地后再把 targetPort 改为 client。
- **验收**：kind 集群中 3 Pod Running、3 PVC Bound，选主并通过逐 Pod port-forward 完成 put/get；
  `kubectl delete pod raftkv-1` 后 Pod UID `1f1c... -> bee791...`，PVC UID 始终为 `a909...`，新 Pod
  仍挂 `data-raftkv-1` 且 Pebble 日志显示重放 2 keys；最终读回 `phase3-step4=k8s-ok (version=1)`，
  三节点均推进到 `commit=3 applied=3`。

#### Step 4 配置速查（面试复习）

- **文件边界**：`kind-config.yaml` 只给 kind 创建本地集群，不交给 k8s API；`namespace.yaml` 做资源隔离；
  `services.yaml` 管稳定寻址/暴露；`statefulset.yaml` 管 Pod 身份、生命周期和卷；`kustomization.yaml` 让
  `kubectl apply -k deploy/k8s` 聚合后三份 k8s 资源。kind 单 control-plane 能测 Pod/PVC 恢复，**不能证明节点级 HA**。
- **headless Service**：`clusterIP: None` 不分配虚拟 IP、不替 peer 负载均衡，而是让 DNS 直接发布 Pod endpoint；
  `publishNotReadyAddresses: true` 保证未 Ready 的新节点也能被 peer 发现，避免“先 Ready 才有 DNS、先有 DNS
  才能组 Raft”的 bootstrap 死锁。稳定地址公式：`<pod>.<service>.<namespace>.svc.cluster.local`。
- **client Service**：`type: ClusterIP` 只提供集群内稳定入口；本地调试用 `port-forward`，生产再选
  LoadBalancer/Gateway。`port` 是 Service 入口端口，`targetPort` 是 Pod 容器端口；Step 7 前临时为
  `7001 -> rpc(7000)`，拆平面后改为 `7001 -> client(7001)`。
- **StatefulSet 身份**：`serviceName: raftkv` 必须指向 headless Service；`replicas: 3` 产生稳定 ordinal
  `raftkv-0/1/2`；`podManagementPolicy: Parallel` 让静态 Raft 成员并行 bootstrap；`selector.matchLabels`
  必须与 `template.metadata.labels` 一致；`RollingUpdate` 逐 Pod 替换（Step 6 再补 leader 交权）。
- **自动成员表**：downward API 的 `fieldPath: metadata.name` 把稳定 Pod 名注入 `NODE_ID`；
  `REPLICAS + STATEFULSET_NAME + SERVICE_DNS + PEER_PORT` 推导全部 peer DNS，不依赖动态 Pod IP。
- **PVC per ordinal**：`volumeClaimTemplates.metadata.name: data` 自动展开成 `data-raftkv-0/1/2`；
  `ReadWriteOnce + 1Gi`，未写 `storageClassName` 就用集群默认 StorageClass；`volumeMounts.name` 必须和模板名
  `data` 一致。`whenDeleted/whenScaled: Retain` 保留 StatefulSet 管理的 PVC，但**显式删 PVC、删 namespace、
  `kind delete cluster` 仍会丢本地数据**。StatefulSet 给稳定身份，真正承载 durability 的是 PVC/PV。
- **最小权限**：Pod 级 `runAsNonRoot/runAsUser/runAsGroup=65532`；`fsGroup=65532` 解决 PVC 覆盖镜像目录后
  的写权限，`OnRootMismatch` 避免每次启动递归改 owner；`seccompProfile: RuntimeDefault`。容器级
  `allowPrivilegeEscalation: false + readOnlyRootFilesystem: true + capabilities.drop: ALL`，只有 PVC 挂载点可写。
- **生命周期/资源**：`automountServiceAccountToken: false`（进程不用访问 k8s API）；
  `terminationGracePeriodSeconds: 30` 是 SIGTERM 到 SIGKILL 的排空窗口；`requests` 用于调度，CPU `limits`
  超限会 throttle，memory `limits` 超限可能 OOMKill。`containerPort` 只是声明/命名端口，真正监听由
  `LISTEN` 和 Go 的 `net.Listen` 决定。

**面试五问一句话版**

1. **为什么不用 Deployment？** Raft 成员不能互换，需要 StatefulSet 的稳定 ordinal、稳定 DNS 和 ordinal→PVC 绑定。
2. **为什么 headless Service？** peer 要找到确定节点，不能经普通 Service 把发给 n1 的 RPC 随机负载均衡到 n2。
3. **为什么发布未 Ready 地址？** peer 发现不能被 readiness 门控，否则新集群可能循环等待、永远选不出主。
4. **删 Pod 为什么数据还在？** Pod UID 变了，但同 ordinal 挂回同一 PVC；WAL/Pebble 从卷恢复。
5. **StatefulSet 是否等于持久化？** 不等于；它保证身份和 PVC 绑定，数据耐久仍取决于 PVC/PV、应用 fsync/原子写和备份策略。

### Step 5 · 探针 readiness / liveness 🟢（决策 4）
- [ ] 加轻量 health 端点：优先 **gRPC health checking protocol**（`grpc_health_v1`，标准、k8s `grpcProbe` 直接支持）；
      或退而求其次一个 HTTP `/healthz`（liveness）+ `/readyz`（readiness）。
- [ ] liveness = 进程/gRPC 活着（判据弱，别用 leadership）；readiness = Raft 已参与集群、不落后太多。
- [ ] StatefulSet 挂 `livenessProbe` / `readinessProbe`。
- **验收**：探针不误杀 follower、不因"非 leader"把 Pod 踢出 endpoint；集群仍能选主（peer 平面未被 readiness 门控）。

### Step 6 · 优雅停机 + leadership transfer 🟣（决策 5，本 Phase 唯一动 raft 处）
- [ ] raft 加最小版 `TransferLeadership`（leader→ matchIndex 最高 follower 发 `TimeoutNow` / 立即选举信号，自己 stepDown）。
- [ ] `main.go` 停机流程：SIGTERM → 若 leader 先 `TransferLeadership`（带超时兜底）→ 再 `GracefulStop`。
- [ ] k8s：`preStop` hook + 合理 `terminationGracePeriodSeconds`。
- **验收**：滚动重启 leader Pod，客户端不可用窗口 ≈ 一次 RTT（而非一个选举超时）；raft1 L1 全绿（转移是叠加、不破原选举）。

### Step 7 · 拆分 peer / client 两平面 🟣（决策 6，本 Phase 最重自己写区）
- [ ] `raftkvd` 起两个 listener + 两个 `grpc.Server`：peer 端口挂 RaftService、client 端口挂 KVService+reflection。
- [ ] 派生 peers 用 **peer 端口**；`transport/grpc.ClientEnd` peer 互联只连 peer 端口（raftCli 与 kvCli 解耦）。
- [ ] k8s：headless Service 暴露 peer 端口（内网 only + NetworkPolicy 限 pod↔pod）；另一 Service 暴露 client 端口。
- [ ] 对称收尾：KV `Call` 统一收 kv Go 结构体（对齐 Raft 平面）。
- **验收**：两平面各自端口独立可用；client 洪峰不影响 peer 心跳；L2/L1 回归绿。
- **注**：mTLS（peer 双向）/ client server-TLS 属**信任域分离的自然延伸**，但鉴权/证书管理在 ROADMAP 已"砍"
      （求职几乎不问）→ 本 Phase **只落地"端口/暴露/连接池"分离**，TLS 当白板讲"分了端口后怎么分别配信任域"。

---

## 验收总表（每步硬门槛）

| Step | 硬门槛 |
|------|--------|
| 1 | env/flag 优先级正确；不给 `--peers` 能从身份派生成员表；本地行为不变 |
| 2 | 多阶段镜像可 build/run；两端口 EXPOSE；纯静态无 CGO |
| 3 | compose 3 容器选主/复制；单容器 restart 数据存活 |
| 4 | k8s StatefulSet 选主 + 复制；`delete pod` 后挂回 PVC、数据存活、自愈 |
| 5 | 探针不误杀 follower、不门控 peer 平面；集群仍能选主 |
| 6 | leader 滚动重启不可用窗口 ≈ 一次 RTT；raft1 L1 全绿 |
| 7 | peer/client 端口独立；ClientEnd 解耦；client 洪峰不扰 raft 心跳 |

## 备注 / 陷阱
- **部署层不侵入算法**（同 Phase 2 L1 保命）：Step 1–5、7 全在宿主/配置/YAML；唯一动 raft 的是 Step 6 的
  最小 leadership transfer，且必须叠加式、跑 raft1 全回归。
- **StatefulSet 是硬性**（翻车陷阱 #3）：Deployment 会让 Pod 名/卷漂移 → 丢/错配 Raft 数据。
- **readiness 别拿 leadership 做门**（决策 4）：会门控 peer 平面互联 → 死锁选不出主。
- **服务发现走静态成员集**（决策 2）：副本数固定、成员集编译期已知；动态成员变更（joint consensus）归"懂原理·不写"。
- **停机不交权只是"能用"不是"好用"**：leader 被杀留一个选举超时窗口；Step 6 的转移把它压到一次 RTT（面试加分点）。
- 白板延伸（不写）：joint consensus 动态成员、mTLS/证书轮转、ReadIndex/lease read（都在 ROADMAP "懂原理·不写"）。
