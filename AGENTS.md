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
- `go.work` at repo root points to `./src` (go 1.23). `go.mod` is in `src/`, declares `go 1.22`. Use Go 1.22+.
- Only external dep: `github.com/anishathalye/porcupine` (linearizability checker, test-only).
- Run `go` commands from `src/`, not repo root.

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

- Push to `mygithub` (GitHub), not `origin` (MIT CSAIL requires auth): `git push mygithub master`. Remote: `https://github.com/lqh0926/6.5840_2026.git`.
- Only commit / push when explicitly requested.

## Bug log

When you find and fix a non-trivial bug, append to `BUGS.md` (root) using the existing format: `BUG-XXX · <title> <severity>`, then **文件 / 错误代码 / 问题 / 正确做法 / 核心不变式**. Read `BUGS.md` before any Raft or KV work — most traps are already documented there in depth.
