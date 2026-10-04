# Distributed Log

A distributed append-only log written in Go. It uses Raft to replicate writes
across a small cluster and provides resource-level locks so only the current
lock owner can append to a resource.

This project is a proof of concept for learning and testing consensus,
replication, recovery, snapshots, and high-concurrency gRPC workloads.

## Features

- Raft leader election, pre-vote, log replication, and quorum commits
- gRPC APIs generated from Protocol Buffers
- Acquire, release, append, and read operations
- Resource-level locks with expiry and idempotent request handling
- Synchronous write-ahead-log persistence
- In-memory application state rebuilt from snapshots and Raft replay
- Snapshot creation and follower snapshot installation
- Leader failover and no-quorum protection
- Batched proposals and state-machine application

## How it works

Clients send requests to any server. Writes are forwarded to the current
leader, replicated to a majority of the Raft cluster, and applied to the state
machine before the client receives a successful response.

Raft logs and metadata are stored with `tidwall/wal`. Locks, processed request
records, resource locations, and application logs are kept in memory and are
recovered using snapshots and committed Raft entries.

## Requirements

- Go 1.25 or newer
- PowerShell for the included Windows benchmark script

## Run the tests

```powershell
go test ./...
go vet ./...
```

## Quick local test

Build the server and run an automatically managed three-node cluster:

```powershell
New-Item -ItemType Directory -Force .test-artifacts | Out-Null

go build -buildvcs=false -trimpath `
  -o .test-artifacts/raft-node.exe ./cmd/server

go run ./testtools/clustercheck `
  --manage-servers `
  --mode=sanity `
  --server-exe=.test-artifacts/raft-node.exe `
  --server-work-root=.test-artifacts/quick-start
```

The runner starts three local servers, performs the lock and log workflow, and
stops the cluster when the test completes.

## Performance

A fresh local Windows benchmark on October 4, 2026 produced the following
result:

| Metric | Result |
|---|---:|
| Messages | 1,048,576 |
| Logical clients | 1,024 |
| Concurrent workers | 512 |
| Resources | 1,000 |
| Messages per lock | 128 |
| Duration | 27.25 seconds |
| Peak tested throughput | **40,481 messages/s** |
| p50 append latency | 7.50 ms |
| p95 append latency | 13.84 ms |
| p99 append latency | 24.45 ms |
| Failed appends | 0 |
| Correctness | PASS: 1,048,576 unique messages |

The measured duration includes lock acquisition, message appends, and lock
release. Throughput counts committed application messages. Results will vary
with CPU, storage, antivirus activity, and operating system scheduling.

Reproduce the workload with:

```powershell
.\testtools\million_clients.ps1 `
  -Clients 1024 `
  -MessagesPerClient 1024 `
  -Resources 1000 `
  -Concurrency 512 `
  -LockBatch 128 `
  -Timeout 30m
```

## Project layout

```text
client/       Reusable gRPC client
cmd/          Server, client, and resource-management commands
internal/     Raft, storage, state-machine, API, and configuration code
proto/        Protocol Buffer definitions
gen/          Generated protobuf and gRPC code
testtools/    Cluster simulation and benchmark runner
```
