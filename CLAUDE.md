# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the MIT 6.5840 (Distributed Systems) lab codebase (2026 version). All source code lives under `src/`. The labs build on each other: MapReduce (lab1) → KV Server (lab2) → Raft (lab3) → Fault-tolerant KV (lab4) → Sharded KV (lab5).

## Build & Test Commands

All commands run from the `src/` directory:

```bash
cd src

# Build MapReduce plugin apps (required before running MR tests)
go build -buildmode=plugin ../mrapps/wc.go

# Run MapReduce tests
cd main && bash test-mr.sh          # all MR tests
bash test-mr.sh quiet               # quiet mode
bash test-mr-many.sh 10             # repeat tests N times

# Run a single test for any lab (from src/)
go test -run TestName ./raft1/      # Raft
go test -run TestName ./kvraft1/    # KV server
go test -run TestName ./shardkv1/   # Sharded KV
go test -run TestName ./kvsrv1/     # Simple KV server (Lab 2)

# Race detection
go test -race -run TestName ./raft1/

# Submit a lab
make lab1    # or lab2, lab3a, lab3b, lab3c, lab3d, lab4a, lab4b, lab5a, lab5b
```

MapReduce manual run:
```bash
cd src/main
go run mrcoordinator.go pg-*.txt &   # start coordinator
go run mrworker.go wc.so &            # start workers (one per invocation)
```

## Architecture (2026 version)

### Lab 1 — MapReduce (`mr/`)
- **`coordinator.go`**: Central task scheduler. Assigns Map/Reduce tasks to workers via RPC, tracks completion, reassigns timed-out tasks.
- **`worker.go`**: Polls coordinator for tasks, executes mapf/reducef, reports completion.
- **`rpc.go`**: RPC types and UNIX socket path.
- **`mrapps/`**: Plugin `.so` files (wc, indexer, crash, mtiming, etc.) loaded by workers at runtime.

### Lab 2 — Simple KV Server (`kvsrv1/`)
- Standalone single-machine KV server without Raft. Handles Get/Put/Append with basic locking.

### Lab 3 — Raft (`raft1/`)
- **`raft.go`**: Full Raft consensus implementation — leader election, log replication, persistence, snapshotting.
- **`server.go`**: Server wrapper.
- **`proxy.go`**: Client proxy for Raft.

### Lab 4 — Fault-tolerant KV (`kvraft1/`)
- KV server backed by Raft. Handles Get/Put/Append with linearizability via duplicate detection.

### Lab 5 — Sharded KV (`shardkv1/`)
- KV server that serves only its assigned shards. Handles shard migration on config changes.

### Shared Libraries
- **`labrpc/`**: Simulated RPC framework for testing (unreliable network, partitions).
- **`labgob/`**: GOB encoder/decoder wrapper with registration helpers.
- **`tester1/`**: Test harness framework.
- **`models1/`**: Data models for testing.
- **`raftapi/`**: Raft API definitions.
- **`kvtest1/`**: KV test utilities.

## Git Configuration

- **Push remote**: Use `mygithub` (GitHub) instead of `origin` (MIT CSAIL requires auth)
  - Remote URL: `https://github.com/lqh0926/6.5840_2026.git`
  - Push command: `git push mygithub master`

## Key Patterns

- All servers use `sync.Mutex` for concurrency control.
- Raft-based services use `ApplyMsg` channel to receive committed commands from Raft.
- Duplicate detection: each KV server tracks `(clientId, clientSeq)` to avoid re-applying operations.
- RPC field names must be exported (capitalized) for GOB encoding.
- Tests use `test.go` harnesses — read them to understand test expectations.
