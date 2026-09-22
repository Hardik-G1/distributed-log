package raft

import (
	"context"
	"errors"

	"github.com/Hardik-G1/distributed-log/internal/storage"
	"github.com/jackc/pgx/v5"
)

type recoveredRaftState struct {
	Metadata          storage.RaftMetadata
	LastLogIndex      int64
	LastIncludedIndex int64
	LastIncludedTerm  int64
}

func recoveryRaftState(
	ctx context.Context,
	store *storage.PostgresStore,
	stateMachine StateMachine,
) (recoveredRaftState, error) {
	if ctx == nil {
		return recoveredRaftState{}, errors.New("context is nil")
	}
	if store == nil {
		return recoveredRaftState{}, errors.New("store is nil")
	}
	if stateMachine == nil {
		return recoveredRaftState{}, errors.New("state machine is nil")
	}
	metadata, err := store.LoadRaftMetadata(ctx)
	if err != nil {
		return recoveredRaftState{}, err
	}

	recovered := recoveredRaftState{
		Metadata: metadata,
	}
	snapshot, data, err := store.LoadLatestSnapshot(ctx)
	switch {
	case err == nil:
		restorer, ok := stateMachine.(SnapshotRestorer)
		if !ok {
			return recoveredRaftState{}, errors.New(
				"state machine cannot restore snapshots",
			)
		}
		if err := restorer.Restore(ctx, data); err != nil {
			return recoveredRaftState{}, err
		}
		recovered.LastIncludedIndex = snapshot.LastIncludedIndex
		recovered.LastIncludedTerm = snapshot.LastIncludedTerm

	case errors.Is(err, pgx.ErrNoRows):

	default:
		return recoveredRaftState{}, err
	}
	lastLog, err := store.GetLastRaftLog(ctx)
	switch {
	case err == nil:
		recovered.LastLogIndex = lastLog.LogIndex
	case errors.Is(err, pgx.ErrNoRows):
		recovered.LastLogIndex = recovered.LastIncludedIndex
	default:
		return recoveredRaftState{}, err
	}
	if recovered.LastLogIndex < recovered.LastIncludedIndex {
		recovered.LastLogIndex = recovered.LastIncludedIndex
	}
	return recovered, nil
}
