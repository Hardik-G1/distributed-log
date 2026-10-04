package raft

import (
	"context"
	"errors"

	"github.com/Hardik-G1/distributed-log/internal/storage"
)

type recoveredRaftState struct {
	Metadata          storage.RaftMetadata
	LastLogIndex      int64
	LastLogTerm       int64
	LastIncludedIndex int64
	LastIncludedTerm  int64
	LastAppliedIndex  int64
}

func recoveryRaftState(
	ctx context.Context,
	store storage.RaftLogStore,
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
		recovered.LastAppliedIndex = snapshot.LastIncludedIndex

	case errors.Is(err, storage.ErrNotFound):

	default:
		return recoveredRaftState{}, err
	}
	lastLog, err := store.GetLastRaftLog(ctx)
	switch {
	case err == nil:
		recovered.LastLogIndex = lastLog.LogIndex
		recovered.LastLogTerm = lastLog.Term
	case errors.Is(err, storage.ErrNotFound):
		recovered.LastLogIndex = recovered.LastIncludedIndex
		recovered.LastLogTerm = recovered.LastIncludedTerm
	default:
		return recoveredRaftState{}, err
	}
	if recovered.LastLogIndex < recovered.LastIncludedIndex {
		recovered.LastLogIndex = recovered.LastIncludedIndex
		recovered.LastLogTerm = recovered.LastIncludedTerm
	}
	if provider, ok := stateMachine.(AppliedIndexProvider); ok {
		appliedIndex, err := provider.LastAppliedIndex(ctx)
		if err != nil {
			return recoveredRaftState{}, err
		}
		if appliedIndex > recovered.LastAppliedIndex {
			recovered.LastAppliedIndex = appliedIndex
		}
	}
	if recovered.LastAppliedIndex > recovered.LastLogIndex {
		return recoveredRaftState{}, errors.New("application state is ahead of the raft log")
	}
	return recovered, nil
}
