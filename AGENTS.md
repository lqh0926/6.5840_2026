# AGENTS.md

Compact guidance for OpenCode sessions in this repo. Read before editing. `CLAUDE.md` also exists; its "Run a single test" commands (`go test -run … ./raft1/`) are misleading — see "Running tests" below and use `make` instead.

## What this repo is

MIT 6.5840 (Distributed Systems) 2026 lab codebase. Five labs build on each other: MapReduce (`src/mr/`) → Simple KV (`src/kvsrv1/`) → Raft (`src/raft1/`) → Raft-backed KV (`src/kvraft1/`) → Sharded KV (`src/shardkv1/`). Shared libs: `tester1/` (harness), `labrpc/` (simulated RPC), `labgob/`, `models1/`, `raftapi/`, `kvtest1/`.

## Running tests — CRITICAL, do not use bare `go test`

Each server runs as a **separate OS process** started from a daemon binary (`src/main/raft1d`, `kvraft1d`, `kvsrv1d`, `rsm1d`, `shardgrp1d`). The test process drives them over `tester1/sockrpc`. The daemon binary must be rebuilt when source changes. The Makefile rebuilds it via `.FORCE`; `go test ./pkg/` does not — it reuses a stale daemon and gives false pass/fail.

From `src/`:

```bash
make raft1                              # all raft tests, -v -race
make RUN="-run 3A" raft1                # one part
make RUN="-run TestInitialElection3A" raft1   # one test
make mr | kvsrv1 | lock1 | rsm1 | kvraft1 | shardkv
make RAFT=--raft-state-machine raft1    # alt raft impl selected by tester1
```

`make shardkv` already sets `-timeout 15m`. For MR plugin build outside `make mr`: `cd src && go build -buildmode=plugin ../mrapps/wc.go`.

This is the source of "tests pass locally but fail intermittently / pass after a code change I reverted" — the daemon was stale. Always go through `make`.

## Submitting a lab

```bash
make lab1   # or lab2 lab3a..3d lab4a..4c lab5a..5c, from repo root
```

Runs `.check-build`: `git fetch git://g.csail.mit.edu/6.5840-golabs-2026`, copies `src/` to a tmpdir, reverts a fixed file list to upstream, rebuilds. If it fails, you edited a reference file (see below). Produces `labN-handin.tar.gz` for manual Gradescope upload (excludes `pg-*.txt`, `*.so`, compiled binaries). Requires network access to MIT CSAIL.

## Files you MUST NOT edit (reference-controlled)

`.check-build` reverts these to upstream before grading, so edits silently break submission. Includes:

- All `*_test.go` and all `test.go`
- `src/raft1/server.go`, `src/kvraft1/rsm/server.go`, `src/kvraft1/test.go`, `src/shardkv1/test.go`, `src/kvsrv1/test.go`
- All of `src/tester1/`, `src/labgob/`, `src/labrpc/`, `src/models1/`, `src/kvtest1/`
- All `src/main/*d.go` (daemon mains) and `src/main/{mrcoordinator,mrworker,mrsequential}.go`
- All `src/mrapps/*.go` and `src/mr/util.go`
- `src/kvsrv1/lock/lock_test.go`

Editable per lab (the actual lab work): `mr/{coordinator,worker,rpc}.go`; `kvsrv1/{server,client}.go` + `kvsrv1/rpc/` + `kvsrv1/lock/lock.go`; `raft1/{raft,proxy,util}.go`; `kvraft1/{server,client}.go` + `kvraft1/rsm/{rsm,proxy}.go`; `shardkv1/{client,shardcfg/shardcfg,shardctrler/shardctrler,shardgrp/server,shardgrp/client}.go` + `shardkv1/shardgrp/shardrpc/`.

## Module / toolchain

- Module path is `6.5840` (imports look like `6.5840/raft1`, `6.5840/labrpc`).
- `go.work` at repo root points to `./src` and currently selects Go 1.25.8. `src/go.mod` intentionally still declares
  `go 1.22` for the course module; do not bump it just to build the container.
- The Phase 3 Docker builder uses `golang:1.25.12-bookworm` and runs `go mod edit -go=1.25.8` only on its private
  copied `go.mod`, because the current gRPC/`x/net` graph requires Go 1.25. Host/course metadata remains unchanged.
- Phase 1/2 added production-path dependencies including gRPC/protobuf and Pebble; Porcupine remains test-only.
- Run `go` commands from `src/`, not repo root.

## Phase 3 roadmap — Docker + Kubernetes (read `task.md` before work)

`task.md` is the **authoritative design and acceptance log**. Read it completely before any Phase 3 edit; this section is
the execution summary so a new agent does not invent a different design. Phase 3 makes the Phase 1/2 binary production-
shaped: stable identity, stable storage, health probes, graceful shutdown, and separate peer/client planes. The global
acceptance is a real local k8s cluster that elects/replicates, survives `kubectl delete pod raftkv-1` with the same PVC,
and keeps the relevant course tests plus `scripts/test-crash-recovery.sh` green.

### Current baseline and hard boundaries

- One binary: `src/cmd/raftkvd`. Durable state is `DATA_DIR/raft` (fileWAL meta+wal) plus `DATA_DIR/db` (Pebble).
- `raftkvctl` discovers the leader by trying each endpoint and retrying `ERR_WRONG_LEADER`; servers do not proxy clients.
- gRPC is currently insecure/no TLS. Dynamic Raft membership, joint consensus, mTLS/cert rotation, ReadIndex, and lease
  reads are whiteboard extensions, **not implementation scope**.
- Deployment changes must not leak into the course Raft/KV algorithm path. The only planned consensus change is Step 6's
  additive leadership transfer. Step 7 changes the production binary/transport boundary, not the Raft protocol itself.
- `raftkvd` currently has one real listener on container port 7000 for both Raft and KV. Port 7001 is reserved/configured,
  but the true split is Step 7. Until then the client Service intentionally maps `7001 -> rpc(7000)`.

### Seven decisions that are normative

1. **StatefulSet, never Deployment for Raft members.** Ordinals `raftkv-0/1/2` provide stable NodeIDs; a headless Service
   provides stable DNS; `volumeClaimTemplates` binds each ordinal to its own PVC. Random Pod identity or volume swapping
   is a correctness/durability bug.
2. **Derive a static member set; never list Pod IPs.** Downward API injects `metadata.name` as `NODE_ID`. If `PEERS` is
   absent, derive all peers from `REPLICAS + STATEFULSET_NAME + SERVICE_DNS + PEER_PORT`. Membership is fixed; scaling
   replicas without implementing Raft membership change does not safely change the cluster configuration.
3. **One PVC per Pod is the durability boundary.** WAL and Pebble both live under the PVC-mounted `DATA_DIR`. The recovery
   proof is: Pod UID changes, PVC UID does not, Pebble/WAL replay succeeds, cluster data remains readable.
4. **Readiness is not leadership.** Liveness is a weak “process/gRPC alive” check. Readiness means the node can participate
   in the cluster, never “is leader”; otherwise followers are removed/restarted and bootstrap can deadlock. Peer discovery
   must remain independent of readiness via headless Service `publishNotReadyAddresses: true`.
5. **Graceful stop includes leadership transfer.** `grpc.Server.GracefulStop` drains RPCs but does not avoid an election
   outage. Step 6 must transfer from a leader to the most caught-up follower (minimal TimeoutNow/immediate-election path)
   with a timeout fallback, then drain. Kubernetes supplies `preStop`/termination grace; SIGKILL must be the last resort.
6. **Split peer and client planes.** Peer is internal node-to-node traffic; client is externally exposed. They need distinct
   trust domains, network exposure, and resource isolation so a client spike cannot starve Raft heartbeats. Final shape:
   peer `:7000` hosts RaftService; client `:7001` hosts KVService+reflection; each has its own listener/server/connection
   concerns. mTLS is out of scope, but the split must make separate TLS policies possible later.
7. **Least privilege.** Runtime UID/GID is `65532:65532`; root filesystem is read-only; drop all capabilities; disallow
   privilege escalation; use RuntimeDefault seccomp. Only `DATA_DIR` is writable. Kubernetes needs `fsGroup: 65532`
   because a PVC hides the directory ownership prepared in the image.

### Ordered delivery plan and acceptance status

| Step | Status | Required result |
|---|---|---|
| 1. env/config/member derivation | done (`48bdd6e`) | `flag > non-empty env > default`; explicit local `PEERS` unchanged; omitted `PEERS` derives stable DNS members |
| 2. multi-stage Dockerfile | done (`48bdd6e`) | Go 1.25 builder → static `raftkvd` only → distroless nonroot; SIGTERM exit 0; cached rebuild |
| 3. Docker Compose | done (`48bdd6e`) | three services/DNS/independent named volumes; put/get; force-recreate one container and retain data/security controls |
| 4. StatefulSet/headless Service/PVC | done (`f05f965`) | 3 Pods + 3 bound PVCs; derived peers elect/replicate; delete `raftkv-1`, reattach same PVC, read data |
| 5. health probes | done (`b8a9a40`) | named standard gRPC health statuses + native startup/liveness/readiness probes; all followers remain Ready; restart recovery passes |
| 6. leadership transfer | done (this commit) | highest-`matchIndex` TimeoutNow before bounded drain; real process/kind recovery and full `make raft1` pass |
| 7. peer/client plane split | **next** | two listeners/servers and decoupled transport; peer internal-only, client Service targets real 7001; L2/L1 regressions pass |

Completed contracts must not regress:

- Config inputs are `NODE_ID`, `LISTEN`, `PEER_PORT`, `CLIENT_PORT`, `DATA_DIR`, `PEERS`, `REPLICAS`,
  `STATEFULSET_NAME`, `SERVICE_DNS`, and `MAX_RAFT_BYTES`, with explicit flag overriding non-empty env overriding default.
- Compose maps host `17001/17002/17003` to the current shared container port 7000 and mounts separate named volumes.
  No `depends_on` correctness dependency: peer clients must tolerate arbitrary startup order and reconnect lazily.
- Kubernetes uses namespace `raftkv`, headless Service `raftkv`, client ClusterIP Service `raftkv-client`, StatefulSet
  `serviceName: raftkv`, `replicas: 3`, `podManagementPolicy: Parallel`, retained 1Gi RWO PVCs, and downward API NodeID.
  `selector.matchLabels` must equal Pod-template labels; volume mount name `data` must equal the claim-template name.

Completed Step 5 contract (do not regress):

- Standard `grpc_health_v1` names are `raftkv-liveness` and `raftkv-readiness`. Liveness is SERVING when the gRPC process
  is alive. Readiness starts NOT_SERVING and becomes SERVING only after production dependencies initialize and Raft
  `currentTerm > 0`; it never requires leadership. Health is shut down before gRPC drain on SIGTERM.
- StatefulSet uses native gRPC startup (`2s x 30`), liveness (`10s x 3`), and readiness (`2s x 2`) probes on numeric port
  7000. No probe binary is added to distroless. Headless peer DNS remains readiness-independent. If lag gating is added
  later, expose a trustworthy progress metric first; do not substitute leadership.
- Acceptance observed one leader plus two Ready followers, zero probe restarts/Unhealthy events, all three client
  endpoints, follower deletion with the same PVC, and successful data read/replication after recovery.

Completed Step 6 contract (do not regress):

- `TimeoutNow` carries leader term/id and last-log index/term. The target validates the leader/log, atomically enters the
  next election term, and returns that new term; `Accepted` means election started, not election won. The sender selects
  the highest-`matchIndex` follower, steps down on the higher reply term, and bounds the non-context-aware transport call
  with the caller context. Do not add a catch-up wait or election-result wait without revisiting the documented minimal
  transfer semantics in `task.md`.
- All election triggers use `beginElectionLocked`; `electionGeneration` invalidates timer events made stale by a valid
  heartbeat/vote reset. Preserve this atomic trigger-to-persist transition; see `BUG-013`.
- SIGTERM order is health shutdown → leader transfer (2s bound) → gRPC drain (10s bound) → process/resource exit.
  Kubernetes uses native `preStop.sleep: 2s` because the distroless image has no shell, within a 30s termination grace.
- Acceptance: full `make raft1` and `make kvraft1` passed with `-race`; crash recovery passed; real-process transfer was
  19.269ms. In kind, transfer initiation was 5.308ms and the chosen follower became leader in the next term; Pod UID
  changed, PVC UID/data survived, and all Pods returned Ready with zero restarts. The recreated member later campaigned
  once more before peer reconnection settled; treat this legal extra leader churn as a Step 7 transport/reconnect
  observation, not permission to weaken normal Raft elections.

Step 7 implementation contract:

- Start two `net.Listen`s and two `grpc.Server`s. RaftService exists only on the peer server; KVService/reflection only on
  the client server. Peer derivation always uses `PEER_PORT`.
- Decouple `transport/grpc.ClientEnd`: Raft peer replication must not require a KV client on the same connection/address.
  Complete the planned symmetric cleanup so KV transport accepts KV Go structs and performs protobuf translation at the
  transport boundary. Update Services/NetworkPolicy and re-run real-binary/L2 plus relevant L1 tests.

For Steps 1–5 deployment-only changes, use proportionate image/container/kind acceptance rather than the full Raft suite.
Step 6 requires full `make raft1`; Step 7 requires production-path/L2 and relevant L1 regression. Any non-trivial Raft/KV
bug found along the way must also be recorded in `BUGS.md` using the repository format.

### Local Docker / kind operations

Local Docker runs through Colima. It may be stopped between sessions to preserve resources and data:

```bash
colima start
docker start raftkv-control-plane          # needed if the preserved kind node did not auto-start
kubectl --context kind-raftkv get pods,pvc -n raftkv
```

The `raftkv-control-plane` container is the kind node; the three raftkv Pods run inside its containerd. `colima stop`
preserves the VM disk. `kind delete cluster --name raftkv` destroys this local cluster and its PVC data.

Image / Compose commands (repo root):

```bash
docker build -t raftkv:phase3 .
docker compose up -d --build
docker compose down                       # containers/network removed; named volumes retained
```

Never casually use `docker compose down -v`: it deletes `raftkv_n1-data`, `raftkv_n2-data`, and `raftkv_n3-data`.
Each Raft node must have its own volume; sharing a Pebble/WAL directory is invalid. On this Mac, a cold Docker build may
need `HTTP_PROXY`/`HTTPS_PROXY=http://192.168.5.2:7897` as build args because the host proxy is on `127.0.0.1:7897`;
do not bake this machine-local proxy into the Dockerfile.

kind / Kubernetes commands (repo root):

```bash
kind create cluster --name raftkv --config deploy/k8s/kind-config.yaml
kind load docker-image raftkv:phase3 --name raftkv    # repeat after every image rebuild
kubectl --context kind-raftkv apply -k deploy/k8s
kubectl --context kind-raftkv get statefulset,pod,pvc,service -n raftkv
```

The headless Service uses `clusterIP: None` plus `publishNotReadyAddresses: true`; peer discovery must not be readiness-
gated. StatefulSet ordinal identity (`raftkv-0/1/2`) maps one-to-one to PVCs (`data-raftkv-0/1/2`). Deleting a Pod must
change the Pod UID while preserving the PVC UID. `persistentVolumeClaimRetentionPolicy: Retain` does not protect against
explicit PVC deletion, namespace deletion, or deleting the whole kind cluster.

## 2026 design — differs from old 6.824 writeups online

Most online Raft-KV tutorials are for the old 6.824 and will mislead you:

- KV uses **CAS version semantics**: `Put(key, value, version)` applies only if the server-side version matches, else `ErrVersion`. There is no `clientId`/`seq` dedup table.
- Server errors: `OK`, `ErrWrongLeader` (not leader / not committed — retry elsewhere), `ErrVersion` (version mismatch → not applied), `ErrNoKey`.
- `ErrMaybe` is synthesized **by Clerk only** — servers never return it. Clerk returns it when a resend (after a lost RPC or `ErrWrongLeader`) gets `ErrVersion`, because the first attempt may already have applied.
- Snapshots contain KV data + versions only. No client state to persist.
- `Put` is idempotent, so version dedup gives exactly-once *effect*. `Append` (Get→CAS Put loop) is non-idempotent, so resends after uncertainty degrade to at-most-once + `ErrMaybe`. Intentional tradeoff.

## Raft invariants repeatedly violated here (see BUGS.md)

Real, hard-won fixes. Don't regress:

- **`matchIndex` and `nextIndex` are separate.** `matchIndex` is monotonic (drives quorum commit); `nextIndex` is retractable (drives fast-backtrack). Merging them into one field breaks commit under RPC reordering. Init both on becoming leader: `matchIndex[i]=0`, `nextIndex[i]=lastLogIndex+1`.
- **Truncate log only on term conflict, never by length alone.** `labrpc` longreordering delays RPCs up to ~2s; a late short AppendEntries will erase already-committed entries if you truncate unconditionally. Walk entries; at the first index where `rf.log[i].Term != args.Entries[i].Term`, truncate from there. Same rule for InstallSnapshot tail retention (keep tail only if `log[SnapshotIndex].Term == SnapshotTerm`).
- **Reset election timer when a legitimate-leader RPC arrives, regardless of log consistency outcome.** Rejecting AppendEntries for log mismatch does not mean the leader is dead.
- **Reset `voteFor = -1` only when term increases**, never on every heartbeat — otherwise same-term replayed RequestVotes can produce split brain.
- **Capture shared state in goroutine closures while holding the lock.** Snapshot `term`, `leaderCommit`, `lastLogIndex/Term` as locals before spawning. Deep-copy any log subslice passed to a goroutine (`append([]LogEntry{}, rf.logs[...]...)`) — the backing array can mutate.
- **Stale-reply guards in the `sendAppendEntries` failure path.** Out-of-order failure replies can regress `matchIndex`/`nextIndex`. Check `rf.term == args.Term && state == Leader` and `args.PrevLogIndex >= matchIndex[server]` before applying.

## Snapshot triggers (two orthogonal mechanisms)

- Lab 3 isolation tests (`raft1/server.go`, reference): snapshot every `SnapShotInterval = 10` applied commands inside `applierSnap`.
- Lab 4 / 5 (`kvraft1/rsm/rsm.go`, editable): snapshot when `rf.PersistBytes() > maxraftstate`. `maxraftstate` is passed to the daemon via `--max-raft-state=N`; `-1` (default) means no snapshotting. Lab 3D uses `--max-raft-state=1`. KV tests check that log size stays under `8 * maxraftstate` after snapshot.

## Performance & RPC

- `TestCount3B` enforces RPC count upper bounds — O(n) heartbeats + fast-backtrack are required, not just correctness. Reverting stale failure replies (above) is what keeps counts under budget.
- `labrpc` is a SIMULATED network: configurable loss, delay, reordering, partition. There is no real network anywhere.
- All servers use `sync.Mutex`; acquire before touching shared state.
- Raft-based services receive committed commands via an `ApplyMsg` channel from Raft (`rf.Start` → `applyCh`).
- RPC field names MUST be exported (capitalized) for GOB encoding.

## Git

- Push by convention to `mygithub`: `git push mygithub master`. Both `mygithub` and `origin` currently point to
  `https://github.com/lqh0926/6.5840_2026.git`; keep `mygithub` as the explicit push target.
- Only commit / push when explicitly requested.

## Bug log

When you find and fix a non-trivial bug, append to `BUGS.md` (root) using the existing format: `BUG-XXX · <title> <severity>`, then **文件 / 错误代码 / 问题 / 正确做法 / 核心不变式**. Read `BUGS.md` before any Raft or KV work — most traps are already documented there in depth.
