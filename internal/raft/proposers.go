package raft

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Hardik-G1/distributed-log/internal/storage"
)

var ErrNotLeader = errors.New("node is not the leader")

type NotLeaderError struct {
	LeaderID string
}

func (err *NotLeaderError) Error() string {
	if err.LeaderID == "" {
		return "node is not the leader or leader is unknown"
	}
	return fmt.Sprintf("node is not the leader %s", err.LeaderID)
}

func (err *NotLeaderError) Unwrap() error {
	return ErrNotLeader
}

// Propose appends a new command to local leader's raft log
func (node *Node) Propose(
	ctx context.Context,
	operationType int32,
	payload []byte,
) (storage.RaftLog, error) {
	if ctx == nil {
		return storage.RaftLog{}, errors.New("context is nil")
	}
	if operationType <= 0 {
		return storage.RaftLog{}, errors.New("operation type is invalid")
	}
	node.proposalMu.Lock()
	defer node.proposalMu.Unlock()
	node.stateMu.Lock()
	if node.state.Role != RoleLeader {
		leaderID := node.state.LeaderID
		node.stateMu.Unlock()
		return storage.RaftLog{}, &NotLeaderError{
			LeaderID: leaderID,
		}
	}
	term := node.state.CurrentTerm
	node.stateMu.Unlock()
	node.logMu.Lock()
	lastIndex, _, err := node.lastLogInfo(ctx)
	if err != nil {
		node.logMu.Unlock()
		return storage.RaftLog{}, err
	}
	node.stateMu.Lock()
	if node.state.Role != RoleLeader || node.state.CurrentTerm != term {
		leaderID := node.state.LeaderID
		node.stateMu.Unlock()
		node.logMu.Unlock()
		return storage.RaftLog{}, &NotLeaderError{LeaderID: leaderID}
	}
	node.stateMu.Unlock()
	entry := storage.RaftLog{
		LogIndex:         lastIndex + 1,
		Term:             term,
		OperationType:    operationType,
		OperationPayload: payload,
	}

	if err := node.store.AppendRaftLogs(
		ctx,
		[]storage.RaftLog{entry},
	); err != nil {
		node.logMu.Unlock()
		return storage.RaftLog{}, err
	}
	node.logMu.Unlock()
	//commit single node proposals
	if err := node.advanceCommitIndex(ctx); err != nil {
		return storage.RaftLog{}, err
	}

	return entry, nil
}

// wait until the entry is committed
func (node *Node) WaitForCommit(
	ctx context.Context,
	index int64,
) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		node.stateMu.Lock()
		commitIndex := node.state.CommitIndex
		role := node.state.Role
		leaderID := node.state.LeaderID
		node.stateMu.Unlock()

		if commitIndex >= index {
			return nil
		}

		if role != RoleLeader {
			return &NotLeaderError{
				LeaderID: leaderID,
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:

		}

	}
}

// Waits for apply
func (node *Node) WaitForApplied(
	ctx context.Context,
	index int64,
) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		node.stateMu.Lock()
		lastApplied := node.state.LastAppliedIndex
		node.stateMu.Unlock()

		if lastApplied >= index {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:

		}
	}
}

func (node *Node) ProposeAndWait(
	ctx context.Context,
	operationType int32,
	payload []byte,
) (storage.RaftLog, error) {
	entry, err := node.Propose(ctx, operationType, payload)
	if err != nil {
		return storage.RaftLog{}, err
	}
	if err := node.WaitForCommit(ctx, entry.LogIndex); err != nil {
		return storage.RaftLog{}, err
	}
	if err := node.WaitForApplied(ctx, entry.LogIndex); err != nil {
		return storage.RaftLog{}, err
	}
	return entry, nil
}
func (node *Node) ProposeAndWaitWithKey(
	ctx context.Context,
	key string,
	operationType int32,
	payload []byte,
) (storage.RaftLog, error) {
	if ctx == nil {
		return storage.RaftLog{}, errors.New("context is nil")
	}
	if key == "" {
		return node.ProposeAndWait(ctx, operationType, payload)
	}
	node.pendingMu.Lock()
	pending, exists := node.pendingProposals[key]
	if !exists {
		pending = &pendingProposal{
			done: make(chan struct{}),
		}
		node.pendingProposals[key] = pending
	}
	node.pendingMu.Unlock()
	if !exists {
		node.lifecycleMu.Lock()
		runCtx := node.ctx
		running := node.started && !node.stopped
		node.lifecycleMu.Unlock()

		if !running || runCtx == nil {
			node.finishedPendingProposal(
				key,
				pending,
				storage.RaftLog{},
				errors.New("node is not running"),
			)
		} else {
			go node.startPendingProposal(
				key,
				pending,
				runCtx,
				operationType,
				append([]byte(nil), payload...),
			)
		}
	}
	select {
	case <-ctx.Done():
		return storage.RaftLog{}, ctx.Err()
	case <-pending.done:
		return pending.entry, pending.err
	}
}

func (node *Node) startPendingProposal(
	key string,
	pending *pendingProposal,
	ctx context.Context,
	operationType int32,
	payload []byte,
) {
	entry, err := node.Propose(ctx, operationType, payload)
	if err == nil {
		err = node.WaitForCommit(ctx, entry.LogIndex)
	}
	if err == nil {
		err = node.WaitForApplied(ctx, entry.LogIndex)
	}
	node.finishedPendingProposal(key, pending, entry, err)
}

func (node *Node) finishedPendingProposal(
	key string,
	pending *pendingProposal,
	entry storage.RaftLog,
	err error,
) {
	node.pendingMu.Lock()
	pending.entry = entry
	pending.err = err
	close(pending.done)
	delete(node.pendingProposals, key)
	node.pendingMu.Unlock()
}
