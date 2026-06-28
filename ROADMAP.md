# Raft KV 项目改造路线图

> 目标：把 6.5840 的课程作业改造成一个**完整、能跑、能演示、面试能讲深**的分布式 KV 项目。
> 定位：单独这一个项目（独立的 OTLP Collector 是另一个 repo，本项目只做可观测性桥接）。

## 分工图例

| 标记 | 含义 |
|------|------|
| 🟣 **自己写** | 设计决策 / 面试深挖区。可以和 AI 讨论方案，但最终决策要理解、认同、能复述推理链 |
| 🟢 **AI 写·你 review** | 脚手架 / 样板 / 配置。AI 写，你 review 并能讲清楚"为什么这么做" |
| ⚪ **懂原理·不写** | 写成本高且同维度重复。搞懂原理、面试能在白板讲清即可，一行不写 |

**核心原则**：AI 写代码，但判断和归因是你的。任何一块（含 AI 写的），假设面试官问"这里为什么这么设计"，你要能不看代码、不查 AI，把决策理由和权衡过的其他选项讲出来。

---

## Phase 0 · 抽象层 + 真 binary ⭐安全网

> 验收：所有原 6.5840 测试照旧绿。

- [ ] 🟣 把**传输**抽到接口背后（labrpc 成为接口的一个实现）
- [ ] 🟣 把**持久化**抽到接口背后（内存 Persister 成为一个实现）
- [ ] 🟣 把**节点寻址**抽象：下标 `int` → 稳定 `NodeID` + 地址映射
- [ ] 🟢 让代码能编成真 binary（`main` 包 + flag/配置加载 + 启动引导）
- [ ] 🟢 埋最基础的结构化日志（zap / slog）

---

## Phase 1 · 真 RPC（gRPC + protobuf）

> 脱离课程框架的硬信号。
> 验收：labrpc 测试 + gRPC loopback 测试都绿。

- [ ] 🟢 定义 `.proto`：Raft（RequestVote / AppendEntries / InstallSnapshot）+ KV（Get/Put/Append）
- [ ] 🟢 gRPC transport 实现传输接口
- [ ] 🟢 序列化 gob → protobuf（顺带拿到 schema 演进能力）
- [ ] 🟣 编写 fault interceptor（可编程注入 drop/delay/error）
- [ ] ~~快照分块流式传输~~ **砍**：边际价值低，普通 InstallSnapshot 够用

---

## Phase 2 · 真持久化（LSM）⭐深度收尾

> 面试深挖区，必须吃透。
> 验收：单节点崩溃恢复正确；原持久化相关测试绿。

- [ ] 🟣 实现 WAL：currentTerm / votedFor / log entries，正确的 fsync 纪律
- [ ] 🟣 崩溃恢复测试（kill -9 后重启对账）—— 这是命门，自己想清楚
- [ ] 🟣 拆分存储：Raft log/metadata（WAL，append-only + fsync）与 KV 状态机分开
- [ ] 🟢 状态机落 pebble / badger
- [ ] 🟢 快照 = pebble snapshot / checkpoint

---

## Phase 3 · Docker + k8s 部署

> 证明非 toy，JD 高频要求。
> 验收：真 k8s 多节点能选主、复制、Pod 重启后恢复。

- [ ] 🟢 单 binary + 配置（env / flag / configmap）
- [ ] 🟢 Dockerfile（多阶段构建）
- [ ] 🟢 docker-compose：本地多进程联调
- [ ] 🟢 k8s StatefulSet（**不是** Deployment）+ headless Service + PVC
- [ ] 🟢 readiness / liveness 探针（基础版即可）
- [ ] 🟣 优雅停机（停机前 leadership transfer）
- [ ] 🟢 服务发现 / 集群引导（bootstrap）

> **必答题**：为什么 Raft 节点用 StatefulSet 不用 Deployment？
> （StatefulSet 提供稳定网络标识 + 持久化存储，Pod 重启后名字和数据不变，节点靠固定身份重新加入集群；Deployment 的 Pod 名随机变，节点找不到彼此。）

---

## Phase 4 · 对外 REST API（砍到最简）

> 能演示端到端即可，不要过度工程。
> 验收：外部 client 经 REST 读写，leader 切换 / 重试不破坏线性化。

- [ ] 🟢 最简 HTTP 接口 + CLI 客户端
- [ ] 🟢 leader 重定向 + 客户端重试
- [ ] 🟣 幂等键：复用 `(clientId, seq)` 去重逻辑
- [ ] ~~API gateway / 连接池~~ **砍**：过度工程，面试不加分

---

## Phase 5 · 可观测性桥接

> 埋点对接独立的 OTLP Collector 项目，本 repo 只做桥接。
> 验收：能从面板看出集群健康与瓶颈。

- [ ] 🟣 选关键指标：leader 切换次数 / commit 延迟 / log lag / apply 滞后 / RPC 时延
- [ ] 🟢 OTLP 埋点，吐给 Collector
- [ ] 🟢 Prometheus 指标导出
- [ ] 🟢 Grafana dashboard

> ※ 简易 OTLP Collector 本体（接收 / 处理 / 导出，含背压控制）是**另一个独立 repo**，
> 对口工作经历。本项目只负责埋点 + 把数据吐过去这一段桥接。

---

## 测试策略（贯穿全程，不是独立阶段）

> 换掉 labrpc 后会**失去它的故障注入能力**，必须补。核心：不是"控 pod 网络通断"或"打桩 gRPC"二选一，而是分层。

| 层 | 机制 | 职责 | 标记 |
|----|------|------|------|
| L1 | labrpc（Phase 0 抽象后的一个实现） | 保留**全部** 6.5840 原测试 + `models1/` 线性化检查器，回归网 | 🟢 现成 |
| L2 | 真 gRPC + fault interceptor | 验证换 gRPC 后不变式不破；搬关键场景（分区/drop/leader crash） | 🟣 自己写 |
| L3/L4 | chaos-mesh 一个故障注入 demo | 真部署下的分区/杀 pod 演示，**别深搞** | ⚪ 懂原理 |

> **必讲点**：labrpc 的 drop = 调用方明确得到 `false`；真 gRPC 多一种 ——
> **服务端已处理、响应回程丢失 → 客户端只看到 timeout**。这是 `(clientId, seq)` 去重（exactly-once）的核心场景，
> 几乎只能在 L2 interceptor 精准构造（注入"服务端正常处理但丢弃响应"）。面试这是 gRPC + 幂等的连环深挖点。
>
> **原则**：高保真层（chaos）负责"发现"，确定性层（labrpc）负责"复现和调试"。

---

## 懂原理·不写（面试延伸题）⚪

> 写成本高 + 同维度重复（都属"分布式系统理解力"），Raft 已经证明了这个维度。
> 搞懂原理、白板能讲清实现思路和 trade-off 即可，一行不写。被问到能接住就立住。

- [ ] ⚪ **2PC / percolator 分布式事务**
  - 2PC 流程、阻塞问题、单点问题
  - percolator 怎么用快照隔离 + primary lock 改进 2PC
  - 怎么架在 Raft KV 之上（用 KV 存事务锁和数据）
  - 一句 trade-off：为什么 TiKV 用 percolator 而非裸 2PC
- [ ] ⚪ **joint consensus 成员变更**：怎么加 / 删节点不停服
- [ ] ⚪ **ReadIndex / lease read 线性化读优化**：高频追问点

---

## Phase 6 · 硬化（可选，放最后，边投边补）

> 求职几乎不问，时间够再补，不做投递前提。

- [ ] ⚪ 成员变更（已在"懂原理不写"覆盖）
- [ ] ~~mTLS / 对外 API 鉴权 / secrets 管理~~ **砍**：求职几乎不问
- [ ] ⚪ 混沌测试（chaos-mesh）—— 一个故障注入 demo 即可，别深搞
- [ ] ~~备份 / 恢复、admin CLI、调试端点~~ **砍**：工程杂活不加分
- [ ] ⚪ 性能：ReadIndex / lease read（已在"懂原理不写"覆盖）

---

## 🚩 投递触发点

**Phase 0–3 + Phase 5 桥接做完 → 开始投。**

Phase 4、八股（Go / Redis / MySQL / Kafka）、chaos-mesh **边投边补**。

> 以早 9 晚 11 的强度，定"全做完再投"大概率会一直拖，然后冒出新的"还差一块"。
> 设一个能投的触发点，剩下的边面边补。k8s 探针、八股、chaos，面试官问到了再说。
> **别让这份计划变成推迟投递的新理由。**

---

## 三大翻车陷阱（实现时盯紧）

1. **Raft log 与状态机混存** → log 高频小写与状态机大写互相拖累，语义也不同。**必须分开**（WAL vs LSM）。→ Phase 2
2. **以为换 gRPC 后测试不用改** → labrpc 故障注入全失效。L1 留旧测试，L2 补 gRPC interceptor。→ 测试策略
3. **用 Deployment 部署 Raft** → 必须 StatefulSet：稳定身份 + 稳定卷，Pod 漂移会丢/错配数据。→ Phase 3
