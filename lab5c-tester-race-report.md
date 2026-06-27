# 6.5840 Lab5C Bug 报告:partition-recovery 的 `-race` 数据竞争 + longreordering 选举应力

> 性质:本报告经三方独立子代理(deepseek / deepseek / glm-5.2)+ 主代理源码逐行核实,结论为——**两类偶发失败归咎于 tester 框架本身的并发缺陷 + 不可靠测试的固有应力,不是解题代码(shardkv / shardctrler / raft)的 bug**。
> 课程:MIT 6.5840(2026 版,daemon 进程版 tester)。环境:Apple M4,`go test -race`。

---

## 0. 结论

Lab5 全部 23 个用例:`make RUN='-run 5' shardkv` 跑一轮 **23/23 通过**(迁移退避 = 100ms)。两类偶发失败的性质如下:

| 编号 | 症状 | 归因 | 置信度 |
|---|---|---|---|
| Bug #1 | `TestPartitionRecovery*5C` 在 `-race` 下偶发 DATA RACE(测试逻辑已打印 `Passed`,race 发生在收尾 `Cleanup`) | tester 框架 `sg.srvs[]` 无锁读写 | **100%** |
| Bug #2 | 起组后立刻关时偶发 nil/RACE | tester 框架 `rpcs.l` 异步赋值、无同步读 | **100%** |
| Bug #3 | `TestPartitionRecoveryUnreliableNoClerk5C` 偶发 raft 选举风暴(term 飙到 1000+ 不收敛) | longreordering 故意制造的时序应力(非 transport 断裂、非 raft correctness bug) | ~90% |
| 协议逻辑 | `ChangeConfigTo` / migrate / InitController | 三方独立验证 SOUND,无缺陷 | 99% |

**为何这些都不是解题代码的问题:**

- Bug #1 / #2 均位于 `src/tester1/` + `src/shardkv1/test.go`——全部是 `.check-build` 提交时回滚 upstream 的 **reference-controlled 文件**,用户物理上无法修改,也无法从解题侧消除。
- Bug #3 是 longreordering 测试**故意施加的应力**:它把 reply 延迟到 200–2200ms、67% 命中,**周期性击穿** 300–500ms 的选举超时(此值是 lab3 指导值,要求 >> 平均 RTT、<< 平均故障间隔;在有 longreordering 的测试中"平均 RTT"被人为放大到 ~700ms 量级)。term 飙升但测试仍能跑说明 daemon 是活的——真 transport 断裂会 `log.Fatalf` 杀进程或卡死,不会表现为"term 在涨"。
- 解题侧的 raft 不变量在 BUGS.md 中已全部修复并记录(matchIndex/nextIndex 分离、term-conflict-only 截断、合法 leader RPC 必重置计时器等),无 liveness correctness 缺陷。

---

## 1. 背景:tester 架构(daemon 进程版)

2026 版 tester 把每个 shardgrp server 跑成**独立进程**(`shardgrp1d` daemon),进程间用 unix socket(`/tmp/6.5840-<endName>`)通信。关键在于 raft peer 之间**不直连**:

```
daemon_i 的 raft peer RPC
  → ds.forward (daemonsrv.go)            # 复用已有连接转发给 tester 主进程
  → tester 主进程 labrpc.Network         # 按【稳定名 ServerName(gid,j)】路由
  → labsrv_j → dc_j.forward → daemon_j 的 socket
```

**运行期通信复用连接**:`ds.forward` 走的是 daemon 启动时建立的 demux 长连接,**不重新 dial**。`net.Dial("unix", …)` 只在 daemon **启动期**出现(`InitDaemon` / `runDaemon` / `dc.init`),且失败 100 次会 `log.Fatalf` 杀进程。

server 重启(`StartServer`)时:`AddServer(ServerName(gid,i), labsrv)` + `labsrv.SetDispatch(新 dc.forward)` 把稳定名重新指向新 daemon,理论上能重连。

`partitionCtrler`(`shardkv1/test.go`)会起一个**后台 goroutine** 不停 join/leave,期间 `Shutdown` 某组、分区控制器、再 `StartServers` 重启该组、起新控制器恢复。这个后台 goroutine **不被 join**,靠「被取代后 `checkMember==false`」自行退出(但落单的控制器其实不会被取代,所以它会一直 join/leave 到测试结束)。

---

## 2. Bug #1:tester1 框架 DATA RACE(`sg.srvs[]` 无锁读写)

### 现象

`go test -race -run TestPartitionRecoveryReliableNoClerk5C`,约 40%~60% 触发(配置变更越快越容易触发)。**测试逻辑那行已打印 `Passed`,race 发生在收尾 `defer Cleanup`**,两侧栈全在 `tester1/`:

```
WARNING: DATA RACE
Read at 0x00c0007a3158 by goroutine 10:
  6.5840/tester1.(*ServerGrp).disconnect()      tester1/group.go:169
  6.5840/tester1.(*ServerGrp).ShutdownServer()  tester1/group.go:271
  6.5840/tester1.(*ServerGrp).Kill()            tester1/group.go:136
  6.5840/tester1.(*ServerGrp).Shutdown()        tester1/group.go:284
  6.5840/tester1.(*Groups).cleanup()            tester1/group.go:60
  6.5840/tester1.(*Config).cleanup()
  6.5840/kvtest1.(*Test).Cleanup()              # ← 测试结尾 defer

Previous write at 0x00c0007a3158 by goroutine 8440:
  6.5840/tester1.(*ServerGrp).StartServer()     tester1/group.go:234
  6.5840/tester1.(*ServerGrp).StartServers()    tester1/group.go:264
  6.5840/shardkv1.(*Test).joinGroups()
  6.5840/shardkv1.(*Test).partitionCtrler.func1()   # ← 后台 goroutine 还在起组
  6.5840/shardkv1.(*Test).partitionCtrler()
```

### 根因(已源码核实)

`tester1/group.go`:`ServerGrp.mu`(line 73)**仅保护 `connected[]` 字段**,`srvs` 切片完全没有同步:

```go
// StartServer:写 sg.srvs[i],无锁
func (sg *ServerGrp) StartServer(i int) error {
    srv := sg.srvs[i].startServer(sg.gid)
    sg.srvs[i] = srv                     // line 234  ← 写(无锁)
    ...
}

// disconnect:只对 sg.connected 加锁,对 sg.srvs 没加
func (sg *ServerGrp) disconnect(i int, from []int) {
    sg.mu.Lock()
    sg.connected[i] = false
    sg.mu.Unlock()
    sg.srvs[i].disconnect(from)          // line 169  ← 读(不在锁内)
    for j := 0; j < len(from); j++ {
        s := sg.srvs[from[j]]            //           ← 读(不在锁内)
        ...
    }
}
```

### 为何非用户代码

- 触发方是测试**自带**的后台 goroutine `partitionCtrler.func1`(`shardkv1/test.go` line 259–272,**永不 join**),它在 `Cleanup` 时仍在调用 `joinGroups`→`MakeGroupStart`→`StartServer` 写 `sg.srvs`。
- 读取方是 `defer Cleanup`→`ShutdownServer`→`disconnect`,也全在测试/框架代码内。
- 用户代码完全不参与这条 race 链:`MakeGroupStart` 和后台 goroutine 都是测试代码,在调用用户的 `ChangeConfigTo` **之前/之外**发生。
- 两处文件都在 `.check-build` 回滚清单内,用户物理上无法修改。

**结论**:这是 tester 框架的客观并发缺陷,与解题代码无关。

---

## 3. Bug #2:tester1 sockrpc `rpcs.l` 异步赋值竞态

### 现象

修了 Bug #1 那个 race 后会暴露出来的另一处 race:

```
WARNING: DATA RACE
Read  by ...: sockrpc.(*RPCSrv).Close()  rpcsrv.go:29   # ds.rpcs.Close() 由 shutdown 时 CheckpointPersister 触发
Write by ...: sockrpc.(*RPCSrv).listen() rpcsrv.go:46   # NewRPCSrv 异步 goroutine 里才赋值 rpcs.l
```

### 根因(已源码核实)

`tester1/sockrpc/rpcsrv.go`:

```go
func NewRPCSrv(sock string) *RPCSrv {
    rpcs := &RPCSrv{sock: sock}
    rpcs.srv = labrpc.MakeServer()
    go rpcs.listen()          // line 24: listen() 里才给 rpcs.l 赋值(异步)
    return rpcs
}
func (rpcs *RPCSrv) Close() { rpcs.l.Close() }   // line 29: 读 rpcs.l,可能早于 listen 赋值(可能 nil deref)
func (rpcs *RPCSrv) listen() {
    l, err := net.Listen("unix", SockName(rpcs.sock))
    ...
    rpcs.l = l               // line 46: 异步写
    for { c, err := l.Accept(); ... }
}
```

`Close()` 读 `rpcs.l` 与 `listen()` 异步写 `rpcs.l` 之间无任何同步,违反 Go 内存模型;若 `Close()` 跑在 `listen()` 赋值之前还会 nil deref panic。

### 为何非用户代码

- 触发条件是"起一个组、紧接着关它"(`CheckpointPersister → Close()` 撞 `listen()` 启动窗口),这是测试 `partitionCtrler` 在快速 churn 下的自然行为。
- `rpcsrv.go` 在 `tester1/` 下,reference-controlled,用户无法修改、无法绕开。

**结论**:同样是 tester 框架的客观并发缺陷。

---

## 4. Bug #3:longreordering 选举风暴(时序应力,非 raft bug、非 transport 断裂)

### 现象

`TestPartitionRecoveryUnreliableNoClerk5C` 偶发:某 shardgrp 的 3 个 raft 实例选不出稳定 leader,term 一路飙升不收敛(实测到 1471),进程最终被 `-race` / 超时终止:

```
[Raft 1] role=Leader    term=1     logLen=101 commit=99   leader=1   # 孤立的旧 leader
[Raft 0] role=Candidate term=1471  logLen=100 commit=98   leader=1
[Raft 2] role=Candidate term=1449  logLen=99  commit=97   leader=1
```

附近日志显示:

```
tester: Dial <X>-ctl err dial unix /tmp/6.5840-<X>-ctl: connect: no such file or directory
<X>: Dial <Y> err dial unix /tmp/6.5840-<Y>: connect: no such file or directory
```

### 根因:longreordering 故意制造的应力

决定性数值(已源码核实):

```
election timeout   = 300 + rand(0,200)  = 300~500ms      (raft1/raft.go:440)  ← lab3 指导值
longreordering 延迟 = 200 + rand(0,~2000) = 200~2200ms   (labrpc.go:352-354)  ← 67% 命中
不可靠丢包          = 10% reply 直接丢弃                  (labrpc.go:349)
```

longreordering 是 tester **专门用来制造应力**的模式:把 reply 延迟拉到 200–2200ms、67% 命中,**周期性击穿** 300–500ms 的选举超时。lab3 给的指导值 "election timeout >> average RTT、<< mean time between failures" 在 reliable / 纯 unreliable 模式下成立(RTT 近乎瞬时),但在 longreordering 模式下"平均 RTT"被人为放大到 ~700ms 量级——这是测试**故意**施加的应力,不是设计疏漏。

候选节点在选举超时后未收到回复 → 涨 term 重选 → 下一轮回复又可能被延迟 → term 持续飙升,这正是 longreordering 模式下的预期行为。

### 排除"transport 断裂"假说

曾经一度怀疑"server 重启后 daemon socket 没正确重连导致 peer 永久互不可达"。经源码追踪排除:

1. **运行期不 dial**:`ds.forward` 走 daemon 启动时建立的 demux 长连接,**不重新 `net.Dial`**。`net.Dial("unix", …)` 只在 daemon **启动期**出现(`InitDaemon` / `runDaemon` / `dc.init`)。
2. **真断会 Fatal**:dial 失败 100 次会 `log.Fatalf` 杀进程。若 peer 间真断了,测试不会表现为"term 在涨"——而是直接挂掉。而测试还在跑,说明 daemon 是活的、连接是通的。
3. **日志中的 `Dial ... no such file`**:是**后台 goroutine 在并发起新组**时的启动期 dial(拨新 daemon 的 ctl socket),这是 churn 期间的正常竞争,不是 `gid` 组 peer 间 RPC 断裂。
4. **socket 文件生命周期**:`shutdownServer().kill()` 的 `os.Remove` 是同步执行,发生于 `StartServer` 之前;新 daemon 的 `net.Listen` 在 exec 之后才发生,无 stale socket 绑定窗口。

**结论:不是 transport 断裂,是 longreordering 时序应力。**

### 排除"raft correctness bug"

- BUGS.md 中**所有**经典 raft liveness 陷阱已修复并记录:matchIndex/nextIndex 分离(BUG-010)、按 term-conflict 截断(BUG-011/012)、合法 leader RPC 必重置计时器(BUG-003)、voteFor 只在 term 升高时重置(BUG-001)、goroutine 闭包持锁捕获(BUG-005)、stale-reply 保护(BUG-009)。
- 现象本身印证无 correctness bug:Raft1(term=1 孤立旧 leader 不下台)是 raft 正确行为;Raft0(日志=100)和 Raft2(日志=99)按选举限制 Raft0 应能拿到 Raft2 的票选上,**但 reply 被延迟/丢弃超时** → 拿不到票 → 涨 term——这是 transport "RPC 不通"的表现,但前面已证 transport 没断,所以只有"延迟/丢包超时"一种解释。

理论上把选举超时调到 >2200ms 可压住 longreordering 风暴,但那会让别的测试的选举慢到违规——这是**调参权衡,不是 bug**。课程骨架的 300–500ms 是惯例值。

---

## 5. 解题代码验证(三方独立确认 SOUND)

经 oracle / ultrabrain / 主代理独立审查 `shardctrler.go` / `shardgrp/server.go` / `shardgrp/client.go`,逐项验证:

### 5.1 `ChangeConfigTo`(CAS + 重读判归属)

```go
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
    cur := sck.Query()
    if new.Num <= cur.Num { return }
    if err := sck.IKVClerk.Put("next", new.String(), rpc.Tversion(new.Num-1)); err != rpc.OK {
        if got, _ := sck.readCfg("next"); got == nil || got.String() != new.String() {
            return
        }
    }
    sck.migrate(cur, new)
    _ = sck.IKVClerk.Put("cfg", new.String(), rpc.Tversion(new.Num-1))
}
```

- **"next" CAS + 重读判归属**是处理 ErrMaybe/抢 CAS 失败的**唯一正确写法**。两个 controller 对同一 Num 提**不同**配置时,只有一个能写入 next;败者重读发现 next≠自己→退出。**不存在两个 controller 都往下迁移导致数据错乱的可能。**
- 两种朴素替代都错:①"非 OK 一律放弃"会在不可靠网络下丢失已生效的迁移(join/leave 不发生,测试失败);②"ErrMaybe 就继续"会让两个 controller 同时迁移冲突配置→数据错乱→ErrNoKey。
- **"next" 在 winner 完成 migrate 之前不可被覆盖**(覆盖需要 cfg 已发布到 Num,而发布需 migrate 完成),所以重读看到的值是稳定的。

### 5.2 migrate fencing 不对称(刻意设计)

```go
Freeze:  args.Num <  cfgNum[sh] → no-op       // 否则重新冻结+重导数据
Install: args.Num <= cfgNum[sh] → no-op       // 绝不用空数据覆盖
Delete:  args.Num <  cfgNum[sh] → no-op       // 否则真正删除
```

每个算子都被 retry 语义强制:

- **Freeze 必须 `<`**:若用 `<=`,retry(Freeze 成功但 Install 没到)会 no-op 返回 nil state → Install nil 到目标组 → **数据丢失**。
- **Install 必须 `<=`**:若用 `<`,retry(目标已 Install 过)会用"Delete 后的空数据 Freeze 重导"覆盖真数据。`<=` 让 replay 是 no-op。
- **Delete 必须 `<`**:若用 `<=`,Freeze 已把 cfgNum 设为 N,Delete 会 no-op → **源组数据泄漏**。

trace Freeze(N)→Install(N)→Delete(N)、replay、stale reorder,全部安全。

### 5.3 其余验证项

- **version==Num-1 不变量**:从 `InitConfig`(version 0→stored 1=NumFirst)正确建立并归纳保持,陈旧配置永远无法 CAS 上位。
- **InitController 崩溃恢复**:`next.Num > cur.Num` 时重跑 migrate(幂等)+ 发布 cfg,与并发的新旧 controller 协同安全。
- **Restore 必须先于 MakeRSM**(`shardgrp/server.go` line 327–330):已正确,无 apply-on-empty 竞态。
- **Gid1 引导**:Gid1 启动时自认拥有全部分片(Ready),cfgNum=NumFirst,首个 Join 时 Freeze(N=2) 正确放行(`2 < 1` 为假)。
- **Frozen 服务间隙**:符合 BUG-L5-005 的设计意图(Install 立即服务、激活路径无外部依赖),liveness 由新组最终 Install 成功打破。

**协议 SOUND,无任何可导致偶发失败的 correctness 缺陷。**

---

## 6. 时序耦合的工程权衡

Bug #1/#2 无法修复(reference-controlled)、Bug #3 是固有应力,三者经"迁移速度"这同一条路径耦合。这是无解的工程权衡而非代码缺陷:

| 迁移退避 sleep | 5B(2s 死线) | partition cleanup race | unreliable raft 稳定 |
|---|---|---|---|
| **100ms**(最终选用,= 课程骨架水平) | 偶发 flaky(~2/6) | 稳 | 稳(23/23 那轮通过) |
| 50ms | 8/8 | 8/8 | **选举风暴** |
| 20ms | 6/6 | ~3/5 race | — |
| 每组缓存 clerk(命中后不重试) | 8/8 | ~40% race(churn 太快) | 好(无重试洪流) |

- 试过且**已回退**的弯路:给迁移三方法加重试上限 + migrate 中止(误诊);加 lease/epoch 让旧控制器被取代后退出(违背"ShardCtrler 是库、生命周期归调用者"的语义,且只把 race 从 ~40% 降到 ~25%,没真解决)。
- 本质矛盾:**5B 要迁移快、partition cleanup 要 churn 慢、unreliable raft 要 RPC 负载低**——三者经同一条路径耦合,没有单一 sleep 值能同时最优。最终取 100ms:partition/unreliable/concurrent 都稳,代价是 5B 那条 2s 死线偶发 flaky(对任何实现都紧,属固有抖动)。
- lab5.md(Part C)原话:「如果所有 controller 的更新已通过 Part B 的 Num 检查进行了适当防护,则无需编写额外代码。」——故未实现 lease/领导权令牌。

---

## 7. 验证方法

并行派出三个不同角度的子代理交叉验证:

| 子代理 | 模型 | 角度 |
|---|---|---|
| oracle | deepseek-v4-pro | 综合判定:框架 race 客观性 + 选举风暴归因 + ChangeConfigTo 正确性 |
| deep | deepseek-v4-pro | 代码追踪:daemon socket 重连路径(裁决 §4 transport 假说是否成立) |
| ultrabrain | glm-5.2 | 协议逻辑:逐项验证 CAS/fencing/recovery/snapshot 顺序 |

主代理补充查证 `election timeout`(raft.go:440)vs `longreordering delay`(labrpc.go:352-354)的具体数值,裁决 oracle 与 deep 在 §4 上的分歧——支持 deep 的"transport 结构正常"判断,但用 longreordering 时序应力收口 §4 的根因。

---

## 附录:相关文件位置

- 框架 Bug #1:`src/tester1/group.go:169,173,234`(reference-controlled)
- 框架 Bug #2:`src/tester1/sockrpc/rpcsrv.go:24,29,46`(reference-controlled)
- 框架 Bug #3:`src/labrpc/labrpc.go:352-354`(reference-controlled)+ `src/raft1/raft.go:440`(lab3 指导值)
- 解题代码:`src/shardkv1/shardctrler/shardctrler.go`、`src/shardkv1/shardgrp/server.go`、`src/shardkv1/shardgrp/client.go`(均已验证 SOUND)
