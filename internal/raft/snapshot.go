package raft

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/Hardik-G1/distributed-log/internal/storage"
)

func (node *Node) maybeCreateSnapshot(
	ctx context.Context,
) error {

	if ctx == nil {
		return errors.New("context is nil")
	}
	node.snapshotMu.Lock()
	defer node.snapshotMu.Unlock()
	provider, ok := node.stateMachine.(SnapshotProvider)
	if !ok {
		return errors.New("state machine does not support snapshot")
	}
	node.stateMachineMu.Lock()
	node.stateMu.Lock()
	lastAppliedIndex := node.state.LastAppliedIndex
	lastIncludedIndex := node.state.LastIncludedIndex
	threshold := node.config.SnapshotThreshold
	nodeID := node.state.NodeID
	node.stateMu.Unlock()

	if threshold <= 0 {
		node.stateMachineMu.Unlock()
		return nil
	}
	if lastAppliedIndex-lastIncludedIndex < int64(threshold) {
		node.stateMachineMu.Unlock()
		return nil
	}

	node.logMu.Lock()

	lastEntry, cached := node.cachedRaftLogLocked(lastAppliedIndex)
	if !cached {
		var err error
		lastEntry, err = node.store.GetRaftLog(ctx, lastAppliedIndex)
		if err != nil {
			node.logMu.Unlock()
			node.stateMachineMu.Unlock()
			if errors.Is(err, storage.ErrNotFound) {
				return errors.New("snapshot boundary log entry is missing")
			}
			return err
		}
	}
	node.logMu.Unlock()

	data, err := provider.Snapshot(ctx)
	node.stateMachineMu.Unlock()
	if err != nil {
		return err
	}
	snapshotID := fmt.Sprintf(
		"%s-%d-%d-%d",
		nodeID,
		lastAppliedIndex,
		lastEntry.Term,
		time.Now().UnixNano(),
	)
	location := filepath.Join(
		node.config.SnapshotDirectory,
		snapshotID+".bin",
	)
	snapshot := storage.RaftSnapshot{
		SnapshotID:        snapshotID,
		LastIncludedIndex: lastAppliedIndex,
		LastIncludedTerm:  lastEntry.Term,
		Location:          location,
		CreatedAt:         time.Now(),
	}

	if err := node.store.SaveSnapshot(
		ctx,
		snapshot,
		data,
	); err != nil {
		return err
	}
	node.logMu.Lock()
	err = node.store.DeleteRaftEntriesThrough(
		ctx,
		lastAppliedIndex,
	)
	if err == nil {
		node.discardCachedRaftLogsThroughLocked(lastAppliedIndex)
	}
	node.logMu.Unlock()
	if err != nil {
		return err
	}

	node.stateMu.Lock()
	if lastAppliedIndex > node.state.LastIncludedIndex {
		node.state.LastIncludedIndex = lastAppliedIndex
		node.state.LastIncludedTerm = lastEntry.Term
	}
	node.stateMu.Unlock()
	return nil

}
