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
	node.termMu.Lock()
	defer node.termMu.Unlock()
	node.stateMu.Lock()
	if term <= node.state.CurrentTerm {
		node.stateMu.Unlock()
		return nil
	}
	node.state.CurrentTerm = term
	node.state.Role = RoleFollower
	node.state.VotedFor = ""
	node.state.LeaderID = ""
	node.votesReceived = 0
	node.votesGranted = make(map[string]struct{})
	metadata := storage.RaftMetadata{
		CurrentTerm: node.state.CurrentTerm,
		VotedFor:    node.state.VotedFor,
	}
	node.stateMu.Unlock()
	return node.store.SaveRaftMetadata(
		ctx,
		metadata,
	)
}
