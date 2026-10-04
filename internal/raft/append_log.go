package raft

import (
	"context"
	"errors"
	"fmt"
	"time"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"github.com/Hardik-G1/distributed-log/internal/storage"
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
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	if err := validateAppendEntriesRequest(req); err != nil {
		return nil, err
	}
	node.stateMu.Lock()

	response := &pb.AppendEntriesResponse{
		CurrentTerm:  node.state.CurrentTerm,
		AppendStatus: pb.AppendResponse_APPEND_RESPONSE_LOG_MISMATCH,
	}

	//reject stale leader
	if req.LeaderTerm < node.state.CurrentTerm {
		response.AppendStatus = pb.AppendResponse_APPEND_RESPONSE_OLD_TERM
		node.stateMu.Unlock()
		return response, nil
	}
	previousTerm := node.state.CurrentTerm
	node.stateMu.Unlock()
	if req.LeaderTerm > previousTerm {
		if err := node.stepDownForTerm(ctx, req.LeaderTerm); err != nil {
			return nil, err
		}
	}

	node.stateMu.Lock()
	if req.LeaderTerm < node.state.CurrentTerm {
		response.CurrentTerm = node.state.CurrentTerm
		response.AppendStatus = pb.AppendResponse_APPEND_RESPONSE_OLD_TERM
		node.stateMu.Unlock()
		return response, nil
	}

	//a newer term makes this node a follower

	node.state.Role = RoleFollower
	node.state.LeaderID = req.SenderId
	node.lastLeaderContact = time.Now()

	response.CurrentTerm = node.state.CurrentTerm
	lastIncludedIndex := node.state.LastIncludedIndex
	lastIncludedTerm := node.state.LastIncludedTerm

	node.stateMu.Unlock()
	node.resetElectionTimer()

	node.logMu.Lock()
	defer node.logMu.Unlock()

	if err := node.verifyPreviousEntry(
		ctx,
		req.PrevLogIndex,
		req.PrevLogTerm,
		lastIncludedIndex,
		lastIncludedTerm,
	); err != nil {
		if errors.Is(err, ErrLogMismatch) {
			switch {
			case req.PrevLogIndex < lastIncludedIndex:
				response.MatchIndex = lastIncludedIndex
			default:
				if req.PrevLogIndex > node.lastLogIndex {
					response.MatchIndex = node.lastLogIndex
				} else if req.PrevLogIndex > 0 {
					response.MatchIndex = req.PrevLogIndex - 1
				}
			}
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

	localLastIndex := node.lastLogIndex
	node.stateMu.Lock()
	if req.LeaderCommitIndex > node.state.CommitIndex {
		newCommitIndex := min(req.LeaderCommitIndex, localLastIndex)
		if newCommitIndex > node.state.CommitIndex {
			node.state.CommitIndex = newCommitIndex
			node.notifyCommitLocked()
		}
	}
	node.lastLeaderContact = time.Now()
	node.stateMu.Unlock()
	response.MatchIndex = lastIndex
	response.LatestAppendedTerm = lastTerm
	response.AppendStatus = pb.AppendResponse_APPEND_RESPONSE_SUCCESS
	return response, nil
}

func (node *Node) verifyPreviousEntry(
	ctx context.Context,
	index int64,
	term int64,
	lastIncludedIndex int64,
	lastIncludedTerm int64,
) error {
	if index < 0 {
		return ErrLogMismatch
	}

	if index < lastIncludedIndex {
		return ErrLogMismatch
	}
	if index == lastIncludedIndex {
		if term != lastIncludedTerm {
			return ErrLogMismatch
		}
		return nil
	}

	entry, cached := node.cachedRaftLogLocked(index)
	if !cached {
		var err error
		entry, err = node.store.GetRaftLog(ctx, index)
		if errors.Is(err, storage.ErrNotFound) {
			return ErrLogMismatch
		}
		if err != nil {
			return err
		}
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
	for offset, incoming := range req.Entries {
		index := req.PrevLogIndex + int64(offset) + 1
		existing, cached := node.cachedRaftLogLocked(index)
		if !cached {
			var err error
			existing, err = node.store.GetRaftLog(ctx, index)
			if errors.Is(err, storage.ErrNotFound) {
				return node.appendFollowerSuffix(
					ctx,
					req,
					offset,
					index,
				)
			}
			if err != nil {
				return err
			}
		}

		if existing.Term != incoming.Term {
			return node.replaceFollowerSuffix(
				ctx,
				req,
				offset,
				index,
			)
		}
	}
	return nil
}
func (node *Node) appendFollowerSuffix(
	ctx context.Context,
	req *pb.AppendEntriesRequest,
	start int,
	startIndex int64,
) error {
	entries, err := followerEntries(req, start, startIndex)
	if err != nil || len(entries) == 0 {
		return err
	}
	if err := node.store.AppendRaftLogs(ctx, entries); err != nil {
		return fmt.Errorf("append follower raft suffix %w", err)
	}
	node.cacheRaftLogsLocked(entries, 0)
	return nil
}

func (node *Node) replaceFollowerSuffix(
	ctx context.Context,
	req *pb.AppendEntriesRequest,
	start int,
	startIndex int64,
) error {
	entries, err := followerEntries(req, start, startIndex)
	if err != nil || len(entries) == 0 {
		return err
	}
	if err := node.store.ReplaceRaftSuffix(
		ctx,
		entries[0].LogIndex,
		entries,
	); err != nil {
		return fmt.Errorf("replace follower raft suffix %w", err)
	}
	node.cacheRaftLogsLocked(entries, entries[0].LogIndex)
	return nil

}

func followerEntries(req *pb.AppendEntriesRequest,
	start int,
	startIndex int64) ([]storage.RaftLog, error) {
	if start < 0 || start > len(req.Entries) {
		return nil, errors.New("invalid replacement start index")
	}
	entries := make([]storage.RaftLog, 0, len(req.Entries)-start)
	for _, incoming := range req.Entries[start:] {
		if incoming == nil {
			return nil, errors.New("follower entry is nil")
		}
		entries = append(entries, storage.RaftLog{
			LogIndex:         startIndex,
			Term:             incoming.Term,
			OperationType:    int32(incoming.OperationType),
			OperationPayload: append([]byte(nil), incoming.OperationPayload...),
		})
		startIndex++
	}
	return entries, nil
}
