package raft

import (
	"context"

	"github.com/Hardik-G1/distributed-log/internal/storage"
)

type StateMachine interface {
	Apply(ctx context.Context, entry storage.RaftLog) error
}

type SnapshotProvider interface {
	Snapshot(ctx context.Context) ([]byte, error)
}

type SnapshotRestorer interface {
	Restore(ctx context.Context, data []byte) error
}
