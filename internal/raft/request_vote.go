package raft

import (
	"context"
	"errors"
	"time"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"github.com/Hardik-G1/distributed-log/internal/storage"
)

func (node *Node) HandleRequestVote(
	ctx context.Context, req *pb.RequestVoteRequest,
) (*pb.RequestVoteResponse, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	if req == nil {
		return nil, errors.New("request vote is nil")
	}
	if req.CandidateId == "" {
		return nil, errors.New("candidate id is empty")
	}
	if req.VoteTerm < 0 || req.CandidateLogIndex < 0 || req.CandidateLastLogTerm < 0 {
		return nil, errors.New("vote request contain negative values")
	}
	node.logMu.Lock()
	defer node.logMu.Unlock()
	lastLogIndex := node.lastLogIndex
	lastLogTerm := node.lastLogTerm

	// a new term makes this node a follower
	if err := node.stepDownForTerm(ctx, req.VoteTerm); err != nil {
		return nil, err
	}
	node.termMu.Lock()
	defer node.termMu.Unlock()
	node.stateMu.Lock()

	response := &pb.RequestVoteResponse{
		VoteTerm:   node.state.CurrentTerm,
		VoterId:    node.state.NodeID,
		VoteStatus: pb.Vote_VOTE_REJECTED,
	}

	//reject older terms
	if req.VoteTerm < node.state.CurrentTerm {
		node.stateMu.Unlock()
		return response, nil
	}

	candidateLogIsUpToDate :=
		req.CandidateLastLogTerm > lastLogTerm ||
			(req.CandidateLastLogTerm == lastLogTerm && req.CandidateLogIndex >= lastLogIndex)

	canVote := node.state.VotedFor == "" || node.state.VotedFor == req.CandidateId

	if !candidateLogIsUpToDate || !canVote {
		node.stateMu.Unlock()
		return response, nil
	}
	metadata := storage.RaftMetadata{
		CurrentTerm: node.state.CurrentTerm,
		VotedFor:    req.CandidateId,
	}
	node.stateMu.Unlock()
	if err := node.store.SaveRaftMetadata(ctx, metadata); err != nil {
		return nil, err
	}
	node.stateMu.Lock()
	node.state.VotedFor = req.CandidateId
	response.VoteStatus = pb.Vote_VOTE_GRANTED
	node.stateMu.Unlock()
	node.resetElectionTimer()
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
	currentTerm := node.state.CurrentTerm
	node.stateMu.Unlock()
	if response.VoteTerm > currentTerm {
		err := node.stepDownForTerm(ctx, response.VoteTerm)
		if err != nil {
			return err
		}
		node.resetElectionTimer()
		return nil
	}
	node.stateMu.Lock()
	defer node.stateMu.Unlock()
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

func (node *Node) HandlePreVote(
	ctx context.Context,
	req *pb.PreVoteRequest,
) (*pb.PreVoteResponse, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	if req == nil {
		return nil, errors.New("pre-vote request is nil")
	}
	if req.CandidateId == "" {
		return nil, errors.New("candidate id is empty")
	}
	if req.VoteTerm < 0 || req.CandidateLogIndex < 0 || req.CandidateLastLogTerm < 0 {
		return nil, errors.New("pre-vote request contains negative values")
	}
	node.logMu.Lock()
	defer node.logMu.Unlock()
	lastLogIndex := node.lastLogIndex
	lastLogTerm := node.lastLogTerm

	node.stateMu.Lock()
	defer node.stateMu.Unlock()
	response := &pb.PreVoteResponse{
		VoteTerm:   node.state.CurrentTerm,
		VoterId:    node.state.NodeID,
		VoteStatus: pb.Vote_VOTE_REJECTED,
	}
	if req.VoteTerm < node.state.CurrentTerm+1 ||
		node.state.Role == RoleLeader || node.installingSnapshot.Load() {
		return response, nil
	}
	if !node.lastLeaderContact.IsZero() &&
		time.Since(node.lastLeaderContact) < node.config.ElectionTimeout {
		return response, nil
	}
	candidateLogIsUpToDate := req.CandidateLastLogTerm > lastLogTerm || (req.CandidateLastLogTerm == lastLogTerm && req.CandidateLogIndex >= lastLogIndex)
	if candidateLogIsUpToDate {
		response.VoteStatus = pb.Vote_VOTE_GRANTED
	}
	return response, nil
}
