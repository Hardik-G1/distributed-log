package raft

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/Hardik-G1/distributed-log/internal/config"
	"github.com/Hardik-G1/distributed-log/internal/storage"
)

func TestReadIndexWaitsForCurrentTermCommit(t *testing.T) {
	ctx := context.Background()
	store, err := storage.OpenWALRaftStore(filepath.Join(t.TempDir(), "raft"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.AppendRaftLogs(ctx, []storage.RaftLog{
		{LogIndex: 1, Term: 1, OperationType: 4},
		{LogIndex: 2, Term: 2, OperationType: 4},
	}); err != nil {
		t.Fatal(err)
	}
	node, err := NewNode(ctx, &config.ServerConfig{
		NodeID: "node-1", PeerAddrs: map[string]string{},
		Heartbeat: 100 * time.Millisecond, ElectionTimeout: 500 * time.Millisecond,
		SnapshotThreshold: 1000, SnapshotDirectory: t.TempDir(),
		ProposalBatchWait: time.Millisecond, ProposalBatchSize: 16, ProposalBatchMax: 64,
	}, store, testStateMachine{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	node.stateMu.Lock()
	node.state.Role = RoleLeader
	node.state.CurrentTerm = 2
	node.state.CommitIndex = 1
	node.state.LastAppliedIndex = 1
	node.stateMu.Unlock()

	type result struct {
		index int64
		err   error
	}
	resultCh := make(chan result, 1)
	readCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	go func() {
		index, err := node.ReadIndex(readCtx)
		resultCh <- result{index: index, err: err}
	}()

	select {
	case got := <-resultCh:
		t.Fatalf("read completed before current-term commit: %+v", got)
	case <-time.After(30 * time.Millisecond):
	}

	node.stateMu.Lock()
	node.state.CommitIndex = 2
	node.notifyCommitLocked()
	node.stateMu.Unlock()

	select {
	case got := <-resultCh:
		if got.err != nil || got.index != 2 {
			t.Fatalf("read result=%+v, want index 2", got)
		}
	case <-time.After(time.Second):
		t.Fatal("read did not unblock after current-term commit")
	}
}
