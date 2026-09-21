package raft

import (
	"context"

	"github.com/Hardik-G1/distributed-log/internal/storage"
)

type StateMachine interface {
	Apply(ctx context.Context, entry storage.RaftLog) error
}
