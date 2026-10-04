# Correctness tests

Run the complete local suite from PowerShell:

```powershell
.\tests\run_correctness.ps1
```

The suite builds its own server binaries, starts isolated three-node clusters,
and stores logs under `.test-artifacts/correctness-<timestamp>`.

It checks:

- WAL append, truncation, metadata persistence, close, and reopen
- memory state, snapshots, restore, lock contention, stale tokens, and expiry
- vote and term durability before state becomes visible
- linearizable reads waiting for a current-term commit
- snapshot installation with matching and conflicting Raft suffixes
- lock acquire, append, read, and release
- duplicate requests and exact idempotent responses
- invalid resources, positions, lengths, and stale lock tokens
- concurrent messages with no missing, unexpected, or duplicate records
- concurrent appends under one lock
- continued writes after one follower fails
- follower restart, catch-up, and participation in quorum
- leader failure and election of a replacement
- rejection of writes after quorum is lost
- snapshot creation, full three-node restart, WAL-tail replay, and exact recovery

The suite is intended for local correctness testing. It does not simulate
Byzantine nodes, packet corruption, clock corruption, or exhaustive model
checking of every possible event ordering.
