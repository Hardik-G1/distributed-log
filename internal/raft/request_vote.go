package raft

import (
	"context"
	"errors"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"github.com/Hardik-G1/distributed-log/internal/storage"
	"github.com/jackc/pgx/v5"
)

func (node *Node) HandleRequestVote(
	ctx context.Context, req *pb.RequestVoteRequest,
) (*pb.RequestVoteResponse, error) {
	if req == nil {
		return nil, errors.New("request vote is nil")
	}
	if req.CandidateId == "" {
		return nil, errors.New("candidate id is empty")
	}
	if req.VoteTerm < 0 || req.CandidateLogIndex < 0 || req.CandidateLastLogTerm < 0 {
		return nil, errors.New("vote request contain negative values")
	}
	lastLogIndex, lastLogTerm, err := node.lastLogInfo(ctx)
	if err != nil {
		return nil, err
	}
	node.stateMu.Lock()
	defer node.stateMu.Unlock()

	response := &pb.RequestVoteResponse{
		VoteTerm:   node.state.CurrentTerm,
		VoterId:    node.state.NodeID,
		VoteStatus: pb.Vote_VOTE_REJECTED,
	}

	//reject older terms
	if req.VoteTerm < node.state.CurrentTerm {
		return response, nil
	}

	// a new term makes this node a follower
	if req.VoteTerm > node.state.CurrentTerm {
		if err := node.stepDownForTermLocked(ctx, req.VoteTerm); err != nil {
			return nil, err
		}
	}

	response.VoteTerm = node.state.CurrentTerm

	candidateLogIsUpToDate :=
		req.CandidateLastLogTerm > lastLogTerm ||
			(req.CandidateLastLogTerm == lastLogTerm && req.CandidateLogIndex >= lastLogIndex)

	canVote := node.state.VotedFor == "" || node.state.VotedFor == req.CandidateId

	if candidateLogIsUpToDate && canVote {
		node.state.VotedFor = req.CandidateId
		metadata := storage.RaftMetadata{
			CurrentTerm: node.state.CurrentTerm,
			VotedFor:    node.state.VotedFor,
		}
		if err := node.store.SaveRaftMetadata(ctx, metadata); err != nil {
			return nil, err
		}

		response.VoteStatus = pb.Vote_VOTE_GRANTED
		node.resetElectionTimer()
	}
	return response, nil
}

func (node *Node) handleVoteResponse(
	ctx context.Context,
	response *pb.RequestVoteResponse,
	electionTerm int64,
	lastLogIndex int64,
) error {
	if response == nil {
		return nil
	}
	node.stateMu.Lock()
	defer node.stateMu.Unlock()
	if response.VoteTerm > node.state.CurrentTerm {
		err := node.stepDownForTermLocked(ctx, response.VoteTerm)
		if err != nil {
			return err
		}
		node.resetElectionTimer()
		return nil
	}
	if node.state.Role != RoleCandidate {
		return nil
	}
	if node.state.CurrentTerm != electionTerm {
		return nil
	}
	if response.VoteTerm != electionTerm {
		return nil
	}
	if response.VoteStatus != pb.Vote_VOTE_GRANTED {
		return nil
	}
	if response.VoterId == "" {
		return nil
	}
	if _, exists := node.state.Peers[response.VoterId]; !exists {
		return nil
	}
	if _, alreadyCounted := node.votesGranted[response.VoterId]; alreadyCounted {
		return nil
	}
	node.votesGranted[response.VoterId] = struct{}{}
	node.votesReceived++

	if node.votesReceived >= majority(len(node.state.Peers)+1) {
		node.becomeLeaderLocked(lastLogIndex)
	}
	return nil
}

func (node *Node) lastLogInfo(ctx context.Context) (int64, int64, error) {
	entry, err := node.store.GetLastRaftLog(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	return entry.LogIndex, entry.Term, nil

}
