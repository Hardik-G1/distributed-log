package raft

type Role uint8

const (
	RoleFollower Role = iota
	RoleCandidate
	RoleLeader
)

type PeerProgress struct {
	NextIndex  int64
	MatchIndex int64
}

type NodeState struct {
	NodeID            string
	Role              Role
	CurrentTerm       int64
	VotedFor          string
	CommitIndex       int64
	LastAppliedIndex  int64
	LeaderID          string
	LastIncludedIndex int64
	LastIncludedTerm  int64

	Peers map[string]PeerProgress //basically something to know whether others NextIndex MatchIndex when the node is a leader
}
