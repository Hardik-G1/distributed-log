package raft

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/Hardik-G1/distributed-log/internal/storage"
)

const maxApplyBatch = 1024

func (node *Node) applyLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-node.applyCh:
			if err := node.applyCommitted(ctx); err != nil {
				log.Printf("apply loop stopped %v", err)
				return
			}
		case <-ticker.C:
			if err := node.applyCommitted(ctx); err != nil {
				log.Printf("apply loop stopped %v", err)
				return
			}
			if err := node.maybeCreateSnapshot(ctx); err != nil {
				log.Printf("create raft snapshot %v", err)
			}
		}
	}
}

func (node *Node) applyCommitted(ctx context.Context) error {
	if node.stateMachine == nil {
		return errors.New("state machine is not configured")
	}
	for {
		node.stateMu.Lock()
		nextIndex := node.state.LastAppliedIndex + 1
		commitIndex := node.state.CommitIndex
		node.stateMu.Unlock()

		if nextIndex > commitIndex {
			return nil
		}
		limit := int(commitIndex - nextIndex + 1)
		if limit > maxApplyBatch {
			limit = maxApplyBatch
		}
		node.logMu.Lock()
		entries, cached := node.cachedRaftEntriesLocked(nextIndex, limit)
		var err error
		if !cached {
			entries, err = node.store.GetRaftEntriesFromLimit(ctx, nextIndex, limit)
		}

		node.logMu.Unlock()
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return errors.New("committed raft entry is missing")
			}
			return err
		}
		if len(entries) == 0 {
			return errors.New("committed raft entry is missing")
		}

		for offset, entry := range entries {
			expectedIndex := nextIndex + int64(offset)
			if entry.LogIndex != expectedIndex || entry.LogIndex > commitIndex {
				return errors.New("committed raft entries are not contiguous")
			}
		}
		node.stateMachineMu.Lock()
		var applyErr error
		if batchStateMachine, ok := node.stateMachine.(BatchStateMachine); ok {
			applyErr = batchStateMachine.ApplyBatch(ctx, entries)
		} else {
			for _, entry := range entries {
				if err := node.stateMachine.Apply(
					ctx,
					entry,
				); err != nil {
					applyErr = err
					break
				}
			}
		}
		if applyErr == nil {
			lastApplied := entries[len(entries)-1].LogIndex
			node.stateMu.Lock()
			node.state.LastAppliedIndex = lastApplied
			node.notifyAppliedLocked()
			node.stateMu.Unlock()
		}
		node.stateMachineMu.Unlock()
		if applyErr != nil {
			return applyErr
		}
	}
}

func (node *Node) signalApply() {
	select {
	case node.applyCh <- struct{}{}:
	default:
	}
}
func (node *Node) notifyCommitLocked() {
	close(node.commitWaitCh)
	node.commitWaitCh = make(chan struct{})
	node.signalApply()
}
func (node *Node) notifyAppliedLocked() {
	close(node.applyWaitCh)
	node.applyWaitCh = make(chan struct{})
}
