package raft

import (
	"context"
	"errors"

	"github.com/Hardik-G1/distributed-log/internal/storage"
)

func (node *Node) stepDownForTerm(
	ctx context.Context,
	term int64,
) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	if term < 0 {
		return errors.New("term cannot be negative")
	}
	node.termMu.Lock()
	defer node.termMu.Unlock()
	node.stateMu.Lock()
	defer node.stateMu.Unlock()
	if term <= node.state.CurrentTerm {
		return nil
	}
	metadata := storage.RaftMetadata{
		CurrentTerm: term,
		VotedFor:    "",
	}
	if err := node.store.SaveRaftMetadata(
		ctx,
		metadata,
	); err != nil {
		return err
	}
	node.state.CurrentTerm = term
	node.state.Role = RoleFollower
	node.state.VotedFor = ""
	node.state.LeaderID = ""
	node.votesReceived = 0
	node.votesGranted = make(map[string]struct{})

	node.notifyCommitLocked()
	return nil
}
