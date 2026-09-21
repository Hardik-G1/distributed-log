package raft

import (
	"context"
	"errors"
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
				return
			}
		}
	}
}

func (node *Node) applyCommitted(ctx context.Context) error {
	for {
		node.stateMu.Lock()
		nextIndex := node.state.LastAppliedIndex + 1
		commitIndex := node.state.CommitIndex
		node.stateMu.Unlock()

		if nextIndex > commitIndex {
			return nil
		}
		entry, err := node.store.GetRaftLog(ctx, nextIndex)
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.New("committed raft entry is missing")
		}
		if err != nil {
			return err
		}
		if node.stateMachine == nil {
			return errors.New("state machine is not configured")
		}
		if err := node.stateMachine.Apply(ctx, entry); err != nil {
			return err
		}
		node.stateMu.Lock()
		node.state.LastAppliedIndex = max(nextIndex, node.state.LastAppliedIndex)
		node.stateMu.Unlock()
	}
}
