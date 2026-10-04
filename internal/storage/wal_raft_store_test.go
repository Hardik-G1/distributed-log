package storage

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestWALRaftStoreLifecycle(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "raft")
	store, err := OpenWALRaftStore(path)
	if err != nil {
		t.Fatal(err)
	}
	entries := []RaftLog{
		{LogIndex: 1, Term: 1, OperationType: 1, OperationPayload: []byte("one")},
		{LogIndex: 2, Term: 1, OperationType: 1, OperationPayload: []byte("two")},
		{LogIndex: 3, Term: 1, OperationType: 1, OperationPayload: []byte("three")},
	}
	if err := store.AppendRaftLogs(ctx, entries); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveRaftMetadata(ctx, RaftMetadata{CurrentTerm: 4, VotedFor: "node-2"}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceRaftSuffix(ctx, 2, []RaftLog{
		{LogIndex: 2, Term: 2, OperationType: 1, OperationPayload: []byte("replacement")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = OpenWALRaftStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	metadata, err := store.LoadRaftMetadata(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.CurrentTerm != 4 || metadata.VotedFor != "node-2" {
		t.Fatalf("unexpected metadata: %+v", metadata)
	}
	last, err := store.GetLastRaftLog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if last.LogIndex != 2 || last.Term != 2 || string(last.OperationPayload) != "replacement" {
		t.Fatalf("unexpected last entry: %+v", last)
	}
	if _, err := store.GetRaftLog(ctx, 3); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected missing truncated entry, got %v", err)
	}
	if err := store.DeleteRaftEntriesThrough(ctx, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetLastRaftLog(ctx); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected empty WAL, got %v", err)
	}
	if err := store.AppendRaftLogs(ctx, []RaftLog{{
		LogIndex: 3, Term: 3, OperationType: 1, OperationPayload: []byte("after-snapshot"),
	}}); err != nil {
		t.Fatal(err)
	}
}
