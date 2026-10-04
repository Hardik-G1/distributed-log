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
	node.lifecycleMu.Lock()
	runCtx := node.ctx
	running := node.started && !node.stopped
	node.lifecycleMu.Unlock()

	if !running || runCtx == nil {
		return storage.RaftLog{}, errors.New("node is not running")
	}
	request := &proposalRequest{
		operationType: operationType,
		payload:       append([]byte(nil), payload...),
		result:        make(chan proposalResult, 1),
	}
	select {
	case node.proposalCh <- request:
	case <-ctx.Done():
		return storage.RaftLog{}, ctx.Err()
	case <-runCtx.Done():
		return storage.RaftLog{}, errors.New("node is stopping")
	}
	select {
	case result := <-request.result:
		return result.entry, result.err
	case <-ctx.Done():
		return storage.RaftLog{}, ctx.Err()
	case <-runCtx.Done():
		return storage.RaftLog{}, errors.New("node is stopping")
	}
}

func (node *Node) proposalLoop(
	ctx context.Context,
) {
	maxBatch := node.config.ProposalBatchMax
	if maxBatch <= 0 {
		maxBatch = 1024
	}
	readyBatch := node.config.ProposalBatchSize
	if readyBatch <= 0 || readyBatch > maxBatch {
		readyBatch = min(256, maxBatch)
	}
	maxWait := node.config.ProposalBatchWait
	if maxWait < 0 {
		maxWait = 0
	}
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case first := <-node.proposalCh:
			batch := make([]*proposalRequest, 0, maxBatch)
			batch = append(batch, first)
		drain:
			for len(batch) < maxBatch {
				select {
				case request := <-node.proposalCh:
					batch = append(batch, request)
				default:
					break drain
				}
			}
			if len(batch) < readyBatch && len(batch) < maxBatch && maxWait > 0 {
				timer.Reset(maxWait)
			collect:
				for len(batch) < readyBatch && len(batch) < maxBatch {
					select {
					case request := <-node.proposalCh:
						batch = append(batch, request)
					case <-timer.C:
						break collect
					case <-ctx.Done():
						stopProposalTimer(timer)
						node.finishProposalBatch(batch, nil, ctx.Err())
						return

					}
				}
				stopProposalTimer(timer)
			}
			node.processProposalBatch(ctx, batch)
		}
	}
}

func stopProposalTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
func (node *Node) processProposalBatch(
	ctx context.Context,
	requests []*proposalRequest,
) {
	node.stateMu.Lock()
	if node.state.Role != RoleLeader {
		leaderID := node.state.LeaderID
		node.stateMu.Unlock()
		node.finishProposalBatch(requests, nil, &NotLeaderError{LeaderID: leaderID})
		return
	}
	term := node.state.CurrentTerm
	node.stateMu.Unlock()
	node.logMu.Lock()
	lastIndex := node.lastLogIndex
	node.stateMu.Lock()

	if node.state.Role != RoleLeader || node.state.CurrentTerm != term {
		leaderID := node.state.LeaderID
		node.stateMu.Unlock()
		node.logMu.Unlock()
		node.finishProposalBatch(requests, nil, &NotLeaderError{LeaderID: leaderID})
		return
	}
	node.stateMu.Unlock()
	entries := make([]storage.RaftLog, len(requests))
	for index, request := range requests {
		entries[index] = storage.RaftLog{
			LogIndex:         lastIndex + int64(index) + 1,
			Term:             term,
			OperationType:    request.operationType,
			OperationPayload: request.payload,
		}
	}
	err := node.store.AppendRaftLogs(ctx, entries)
	if err == nil {
		node.cacheRaftLogsLocked(entries, 0)
	}
	node.logMu.Unlock()
	if err != nil {
		node.finishProposalBatch(requests, nil, err)
		return
	}
	node.finishProposalBatch(requests, entries, nil)
	node.signalReplication()
	_ = node.advanceCommitIndex(ctx)
}

func (node *Node) finishProposalBatch(
	requests []*proposalRequest,
	entries []storage.RaftLog,
	err error,
) {
	for index, request := range requests {
		result := proposalResult{
			err: err,
		}
		if err == nil {
			result.entry = entries[index]
		}
		request.result <- result
	}
}

// wait until the entry is committed
func (node *Node) WaitForCommit(
	ctx context.Context,
	index int64,
) error {
	for {
		node.stateMu.Lock()
		commitIndex := node.state.CommitIndex
		role := node.state.Role
		leaderID := node.state.LeaderID
		waitCh := node.commitWaitCh
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
		case <-waitCh:

		}

	}
}

// Waits for apply
func (node *Node) WaitForApplied(
	ctx context.Context,
	index int64,
) error {

	for {
		node.stateMu.Lock()
		lastApplied := node.state.LastAppliedIndex
		waitCh := node.applyWaitCh
		node.stateMu.Unlock()

		if lastApplied >= index {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-waitCh:

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
			payloadCopy := append([]byte(nil), payload...)
			go node.startPendingProposal(
				key,
				pending,
				runCtx,
				operationType,
				payloadCopy,
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
