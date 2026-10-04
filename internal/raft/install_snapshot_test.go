package raft

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"github.com/Hardik-G1/distributed-log/internal/config"
	"github.com/Hardik-G1/distributed-log/internal/storage"
)

type snapshotTestStateMachine struct {
	restored []byte
}

func (*snapshotTestStateMachine) Apply(context.Context, storage.RaftLog) error {
	return nil
}

func (machine *snapshotTestStateMachine) Restore(ctx context.Context, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	machine.restored = append([]byte(nil), data...)
	return nil
}

func newSnapshotTestNode(
	t *testing.T,
	entries []storage.RaftLog,
) (*Node, *storage.WALRaftStore, *snapshotTestStateMachine) {
	t.Helper()
	ctx := context.Background()
	store, err := storage.OpenWALRaftStore(filepath.Join(t.TempDir(), "raft"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AppendRaftLogs(ctx, entries); err != nil {
		store.Close()
		t.Fatal(err)
	}
	machine := &snapshotTestStateMachine{}
	node, err := NewNode(ctx, &config.ServerConfig{
		NodeID: "node-2", PeerAddrs: map[string]string{},
		Heartbeat: 100 * time.Millisecond, ElectionTimeout: 500 * time.Millisecond,
		SnapshotThreshold: 1000, SnapshotDirectory: t.TempDir(),
		ProposalBatchWait: time.Millisecond, ProposalBatchSize: 16, ProposalBatchMax: 64,
	}, store, machine, nil)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	return node, store, machine
}

func TestInstallSnapshotDiscardsConflictingSuffix(t *testing.T) {
	ctx := context.Background()
	node, store, machine := newSnapshotTestNode(t, []storage.RaftLog{
		{LogIndex: 1, Term: 1, OperationType: 4},
		{LogIndex: 2, Term: 2, OperationType: 4},
		{LogIndex: 3, Term: 2, OperationType: 4},
	})
	defer store.Close()
	payload := []byte("conflicting-snapshot")
	response, err := node.HandleInstallSnapshot(ctx, &pb.InstallSnapshotRequest{
		LeaderId: "node-1", CurrentTerm: 3,
		LastIncludedIndex: 2, LastIncludedTerm: 9,
		Data: payload, Done: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Status != pb.SnapshotStatus_SNAPSHOT_STATUS_INSTALLED {
		t.Fatalf("status=%s", response.Status)
	}
	if _, err := store.GetRaftLog(ctx, 3); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("conflicting suffix survived: %v", err)
	}
	if node.lastLogIndex != 2 || node.lastLogTerm != 9 {
		t.Fatalf("last log=(%d,%d), want (2,9)", node.lastLogIndex, node.lastLogTerm)
	}
	if !bytes.Equal(machine.restored, payload) {
		t.Fatalf("restored=%q", machine.restored)
	}
}

func TestInstallSnapshotRetainsMatchingSuffix(t *testing.T) {
	ctx := context.Background()
	node, store, _ := newSnapshotTestNode(t, []storage.RaftLog{
		{LogIndex: 1, Term: 1, OperationType: 4},
		{LogIndex: 2, Term: 2, OperationType: 4},
		{LogIndex: 3, Term: 3, OperationType: 4},
	})
	defer store.Close()
	response, err := node.HandleInstallSnapshot(ctx, &pb.InstallSnapshotRequest{
		LeaderId: "node-1", CurrentTerm: 3,
		LastIncludedIndex: 2, LastIncludedTerm: 2,
		Data: []byte("matching-snapshot"), Done: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Status != pb.SnapshotStatus_SNAPSHOT_STATUS_INSTALLED {
		t.Fatalf("status=%s", response.Status)
	}
	entry, err := store.GetRaftLog(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	if entry.Term != 3 || node.lastLogIndex != 3 || node.lastLogTerm != 3 {
		t.Fatalf("matching suffix not retained: entry=%+v last=(%d,%d)", entry, node.lastLogIndex, node.lastLogTerm)
	}
}
