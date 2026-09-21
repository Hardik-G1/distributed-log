package raft

import (
	"context"

	"github.com/Hardik-G1/distributed-log/internal/storage"
)

func (node *Node) stepDownForTermLocked(
	ctx context.Context,
	term int64,
) error {
	if term <= node.state.CurrentTerm {
		return nil
	}
	node.state.CurrentTerm = term
	node.state.Role = RoleFollower
	node.state.VotedFor = ""
	node.state.LeaderID = ""
	node.votesReceived = 0

	return node.store.SaveRaftMetadata(
		ctx,
		storage.RaftMetadata{
			CurrentTerm: node.state.CurrentTerm,
			VotedFor:    node.state.VotedFor,
		},
	)
}
