package raft

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"github.com/Hardik-G1/distributed-log/internal/storage"
	"github.com/jackc/pgx/v5"
)

// reject stale leaders
// step down for newer terms
// recognize valid leaders
// reset election timer
// verify prev_log_index and prev_log_term
// delete conflicting entries
// append new entries
// handle empty heartbeats
// advance follower commitIndex
func validateAppendEntriesRequest(req *pb.AppendEntriesRequest) error {
	if req == nil {
		return errors.New("request is nil")
	}
	if req.SenderId == "" {
		return errors.New("sender id is empty")
	}
	if req.LeaderTerm < 0 || req.PrevLogIndex < 0 || req.PrevLogTerm < 0 || req.LeaderCommitIndex < 0 {
		return errors.New("raft values cannot be negative")
	}
	for _, entry := range req.Entries {
		if entry == nil {
			return errors.New("entry is nil")
		}
		if entry.Term < 0 {
			return errors.New("entry has a negative term")
		}
	}
	return nil
}
func (node *Node) HandleAppendEntries(
	ctx context.Context,
	req *pb.AppendEntriesRequest,
) (*pb.AppendEntriesResponse, error) {
	if err := validateAppendEntriesRequest(req); err != nil {
		return nil, err
	}
	node.stateMu.Lock()
	defer node.stateMu.Unlock()

	response := &pb.AppendEntriesResponse{
		CurrentTerm:  node.state.CurrentTerm,
		AppendStatus: pb.AppendResponse_APPEND_RESPONSE_LOG_MISMATCH,
	}

	//reject stale leader
	if req.LeaderTerm < node.state.CurrentTerm {
		response.AppendStatus = pb.AppendResponse_APPEND_RESPONSE_OLD_TERM
		return response, nil
	}
	//a newer term makes this node a follower
	if req.LeaderTerm > node.state.CurrentTerm {
		if err := node.stepDownForTermLocked(ctx, req.LeaderTerm); err != nil {
			return nil, err
		}
	}
	node.state.Role = RoleFollower
	node.state.LeaderID = req.SenderId
	response.CurrentTerm = node.state.CurrentTerm

	node.resetElectionTimer()

	if err := node.verifyPreviousEntry(
		ctx,
		req.PrevLogIndex,
		req.PrevLogTerm,
	); err != nil {
		if errors.Is(err, ErrLogMismatch) {
			response.AppendStatus = pb.AppendResponse_APPEND_RESPONSE_LOG_MISMATCH
			return response, nil
		}
		return nil, err
	}

	lastIndex := req.PrevLogIndex
	lastTerm := req.PrevLogTerm

	if len(req.Entries) > 0 {
		if err := node.reconcileFollowerLog(ctx, req); err != nil {
			return nil, err
		}
		lastEntry := req.Entries[len(req.Entries)-1]
		lastIndex = req.PrevLogIndex + int64(len(req.Entries))
		lastTerm = lastEntry.Term
	}

	localLastIndex, _, err := node.lastLogInfo(ctx)
	if err != nil {
		return nil, err
	}

	if req.LeaderCommitIndex > node.state.CommitIndex {
		newCommitIndex := req.LeaderCommitIndex
		newCommitIndex = min(newCommitIndex, localLastIndex)
		node.state.CommitIndex = newCommitIndex
	}
	response.MatchIndex = lastIndex
	response.LatestAppendedTerm = lastTerm
	response.AppendStatus = pb.AppendResponse_APPEND_RESPONSE_SUCCESS
	return response, nil
}

func (node *Node) verifyPreviousEntry(
	ctx context.Context,
	index int64,
	term int64,
) error {
	if index < 0 {
		return ErrLogMismatch
	}
	node.stateMu.Lock()
	lastIncludedIndex := node.state.LastIncludedIndex
	lastIncludedTerm := node.state.LastIncludedTerm
	node.stateMu.Unlock()

	if index < lastIncludedIndex {
		return ErrLogMismatch
	}
	if index == lastIncludedIndex {
		if term != lastIncludedTerm {
			return ErrLogMismatch
		}
		return nil
	}

	entry, err := node.store.GetRaftLog(ctx, index)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrLogMismatch
	}
	if err != nil {
		return err
	}
	if entry.Term != term {
		return ErrLogMismatch
	}
	return nil
}

func (node *Node) reconcileFollowerLog(
	ctx context.Context,
	req *pb.AppendEntriesRequest,
) error {
	for i, incoming := range req.Entries {
		index := req.PrevLogIndex + int64(i) + 1
		existing, err := node.store.GetRaftLog(ctx, index)
		if errors.Is(err, pgx.ErrNoRows) {
			return node.replaceFollowerSuffix(
				ctx,
				req,
				i,
				index,
			)
		}
		if err != nil {
			return err
		}
		if existing.Term != incoming.Term {
			return node.replaceFollowerSuffix(
				ctx,
				req,
				i,
				index,
			)
		}
	}
	return nil
}

func (node *Node) replaceFollowerSuffix(
	ctx context.Context,
	req *pb.AppendEntriesRequest,
	start int,
	startIndex int64,
) error {
	if start < 0 || start > len(req.Entries) {
		return errors.New("invalid replacement start index")
	}
	entries := make([]storage.RaftLog, 0, len(req.Entries)-start)
	for _, incoming := range req.Entries[start:] {
		entries = append(entries, storage.RaftLog{
			LogIndex:         startIndex,
			Term:             incoming.Term,
			OperationType:    int32(incoming.OperationType),
			OperationPayload: []byte(incoming.OperationPayload),
		})
		startIndex++
	}
	if len(entries) == 0 {
		return nil
	}
	if err := node.store.ReplaceRaftSuffix(
		ctx,
		entries[0].LogIndex,
		entries,
	); err != nil {
		return fmt.Errorf("replace follower raft suffix %w", err)
	}
	return nil
}
