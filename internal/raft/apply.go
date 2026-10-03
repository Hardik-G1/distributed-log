package raft

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
)

func (node *Node) applyLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := node.applyCommitted(ctx); err != nil {
				log.Printf("apply loop stopped %v", err)
				continue
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
		node.logMu.Lock()
		entry, err := node.store.GetRaftLog(ctx, nextIndex)
		node.logMu.Unlock()
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.New("committed raft entry is missing")
		}
		if err != nil {
			return err
		}
		node.stateMachineMu.Lock()
		err = node.stateMachine.Apply(ctx, entry)
		if err != nil {
			node.stateMachineMu.Unlock()
			return err
		}

		node.stateMu.Lock()
		if nextIndex > node.state.LastAppliedIndex {
			node.state.LastAppliedIndex = nextIndex
		}
		node.stateMu.Unlock()
		node.stateMachineMu.Unlock()

	}
}
