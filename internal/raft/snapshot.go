package raft

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/Hardik-G1/distributed-log/internal/storage"
	"github.com/jackc/pgx/v5"
)

func (node *Node) maybeCreateSnapshot(
	ctx context.Context,
) error {

	if ctx == nil {
		return errors.New("context is nil")
	}
	node.stateMachineMu.Lock()
	defer node.stateMachineMu.Unlock()
	node.stateMu.Lock()
	lastAppliedIndex := node.state.LastAppliedIndex
	lastIncludedIndex := node.state.LastIncludedIndex
	threshold := node.config.SnapshotThreshold
	nodeID := node.state.NodeID
	node.stateMu.Unlock()

	if threshold <= 0 {
		return nil
	}
	if lastAppliedIndex-lastIncludedIndex < int64(threshold) {
		return nil
	}

	provider, ok := node.stateMachine.(SnapshotProvider)
	if !ok {
		return errors.New("state machine does not support snapshot")
	}

	lastEntry, err := node.store.GetRaftLog(
		ctx,
		lastAppliedIndex,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return errors.New("snapshot boundary log entry is missing")
	}
	if err != nil {
		return err
	}

	data, err := provider.Snapshot(ctx)
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
	node.logMu.Unlock()
	if err != nil {
		return err
	}

	node.stateMu.Lock()
	node.state.LastIncludedIndex = lastAppliedIndex
	node.state.LastIncludedTerm = lastEntry.Term
	node.stateMu.Unlock()
	return nil

}
