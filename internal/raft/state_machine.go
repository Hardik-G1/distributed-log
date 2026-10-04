package raft

import (
	"context"

	"github.com/Hardik-G1/distributed-log/internal/storage"
)

type StateMachine interface {
	Apply(ctx context.Context, entry storage.RaftLog) error
}

type BatchStateMachine interface {
	ApplyBatch(
		ctx context.Context,
		entries []storage.RaftLog,
	) error
}
type AppliedIndexProvider interface {
	LastAppliedIndex(
		ctx context.Context,
	) (int64, error)
}
type SnapshotProvider interface {
	Snapshot(ctx context.Context) ([]byte, error)
}

type SnapshotRestorer interface {
	Restore(ctx context.Context, data []byte) error
}
