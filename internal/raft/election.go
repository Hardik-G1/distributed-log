package raft

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"time"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"github.com/Hardik-G1/distributed-log/internal/storage"
)

func (node *Node) electionLoop(ctx context.Context) {
	timer := time.NewTimer(randomElectionTimeout(
		node.config.ElectionTimeout,
	))
	defer timer.Stop()
	for {
		select {
		// Node Exits
		case <-ctx.Done():
			return
		// Valid Heartbeat arrives or Valid vote arrives
		case <-node.electionResetCh:
			resetTimer(timer, randomElectionTimeout(
				node.config.ElectionTimeout,
			))
		//Election Timeout expired or no hearbeat
		case <-timer.C:
			if err := node.startElection(ctx); err != nil {
				log.Printf("start election %v", err)
			}
			resetTimer(timer, randomElectionTimeout(
				node.config.ElectionTimeout,
			))
		}
	}
}

func (node *Node) startElection(ctx context.Context) error {
	preVoteWon, err := node.runPreVote(ctx)
	if err != nil {
		return err
	}
	if !preVoteWon || node.installingSnapshot.Load() {
		return nil
	}
	node.logMu.Lock()
	defer node.logMu.Unlock()

	lastLogIndex, lastLogTerm, err := node.lastLogInfo(ctx)
	if err != nil {
		return err
	}
	node.termMu.Lock()
	defer node.termMu.Unlock()
	node.stateMu.Lock()
	if node.state.Role == RoleLeader {
		node.stateMu.Unlock()
		return nil
	}
	if !node.lastLeaderContact.IsZero() &&
		time.Since(node.lastLeaderContact) < node.config.ElectionTimeout {
		node.stateMu.Unlock()
		return nil
	}
	transport := node.transport
	peerCount := len(node.state.Peers)
	if peerCount > 0 && transport == nil {
		node.stateMu.Unlock()
		return errors.New("peer transport is not configured")
	}
	node.state.Role = RoleCandidate
	node.state.CurrentTerm++
	node.state.VotedFor = node.state.NodeID
	node.state.LeaderID = ""
	node.votesReceived = 1
	node.votesGranted = make(map[string]struct{})
	node.votesGranted[node.state.NodeID] = struct{}{}
	metadata := storage.RaftMetadata{
		CurrentTerm: node.state.CurrentTerm,
		VotedFor:    node.state.VotedFor,
	}
	electionTerm := node.state.CurrentTerm
	candidateID := node.state.NodeID

	request := &pb.RequestVoteRequest{
		VoteTerm:             electionTerm,
		CandidateLogIndex:    lastLogIndex,
		CandidateLastLogTerm: lastLogTerm,
		CandidateId:          candidateID,
		CandidateRequestedAt: time.Now().Unix(),
	}
	peerIDs := make([]string, 0, len(node.state.Peers))
	for peerID := range node.state.Peers {
		peerIDs = append(peerIDs, peerID)
	}

	if err := node.store.SaveRaftMetadata(ctx, metadata); err != nil {
		node.stateMu.Unlock()
		return err
	}

	if node.votesReceived >= majority(len(node.state.Peers)+1) {
		node.becomeLeaderLocked(lastLogIndex)
		node.stateMu.Unlock()
		return nil
	}

	node.stateMu.Unlock()
	for _, peerID := range peerIDs {
		go node.sendVoteRequest(
			ctx,
			transport,
			peerID,
			request,
			electionTerm,
			lastLogIndex,
		)
	}
	return nil
}

func (node *Node) sendVoteRequest(
	ctx context.Context,
	transport PeerTransport,
	peerID string,
	request *pb.RequestVoteRequest,
	electionTerm int64,
	lastLogIndex int64,
) {
	response, err := transport.SendRequestVote(ctx, peerID, request)
	if err != nil {
		return
	}
	_ = node.handleVoteResponse(
		ctx,
		response,
		electionTerm,
		lastLogIndex,
	)
}

func (node *Node) becomeLeaderLocked(lastLogIndex int64) {
	node.state.Role = RoleLeader
	node.state.LeaderID = node.state.NodeID
	leaderTerm := node.state.CurrentTerm

	for peerID := range node.state.Peers {
		node.state.Peers[peerID] = PeerProgress{
			NextIndex:  lastLogIndex + 1,
			MatchIndex: 0,
		}
	}
	go node.appendLeaderNoop(node.ctx, leaderTerm)
}
func (node *Node) appendLeaderNoop(
	ctx context.Context,
	leaderTerm int64,
) {
	if ctx == nil {
		return
	}
	node.stateMu.Lock()
	stillLeader := node.state.Role == RoleLeader &&
		node.state.CurrentTerm == leaderTerm
	node.stateMu.Unlock()
	if !stillLeader {
		return
	}
	_, err := node.Propose(
		ctx,
		int32(pb.Operation_OPERATION_NOOP),
		[]byte{0},
	)
	if err != nil && !errors.Is(err, ErrNotLeader) {
		log.Printf("append leader no op %v", err)
	}
}

func majority(totalNodes int) int {
	return (totalNodes / 2) + 1
}
func (node *Node) resetElectionTimer() {
	select {
	case node.electionResetCh <- struct{}{}:
	default:
	}
}

func randomElectionTimeout(base time.Duration) time.Duration {
	if base <= 0 {
		return time.Second
	}
	jitter := time.Duration(rand.Int63n(int64(base)))
	return base + jitter
}

func resetTimer(timer *time.Timer, duration time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(duration)
}

func (node *Node) runPreVote(ctx context.Context) (bool, error) {
	if ctx == nil {
		return false, errors.New("context is nil")
	}
	if node.installingSnapshot.Load() {
		return false, nil
	}
	node.logMu.Lock()
	lastLogIndex, lastLogTerm, err := node.lastLogInfo(ctx)
	node.logMu.Unlock()
	if err != nil {
		return false, err
	}
	node.stateMu.Lock()
	if node.state.Role == RoleLeader || (!node.lastLeaderContact.IsZero() && (time.Since(node.lastLeaderContact) < node.config.ElectionTimeout)) {
		node.stateMu.Unlock()
		return false, nil
	}
	transport := node.transport
	prospectiveTerm := node.state.CurrentTerm + 1
	candidateID := node.state.NodeID
	peerIDs := make([]string, 0, len(node.state.Peers))
	for peerID := range node.state.Peers {
		peerIDs = append(peerIDs, peerID)
	}
	node.stateMu.Unlock()
	if len(peerIDs) > 0 && transport == nil {
		return false, errors.New("peer transport is not configured")
	}
	required := majority(len(peerIDs) + 1)
	granted := 1
	if granted >= required {
		return true, nil
	}
	peerSet := make(map[string]struct{}, len(peerIDs))
	for _, peerID := range peerIDs {
		peerSet[peerID] = struct{}{}
	}
	request := &pb.PreVoteRequest{
		VoteTerm:             prospectiveTerm,
		CandidateLogIndex:    lastLogIndex,
		CandidateLastLogTerm: lastLogTerm,
		CandidateId:          candidateID,
		CandidateRequestedAt: time.Now().Unix(),
	}
	type result struct {
		response *pb.PreVoteResponse
		err      error
	}
	resultCh := make(chan result, len(peerIDs))
	for _, peerID := range peerIDs {
		go func(peerID string) {
			response, err := transport.SendPreVote(
				ctx,
				peerID,
				request,
			)
			resultCh <- result{
				response: response,
				err:      err,
			}
		}(peerID)
	}
	seen := make(map[string]struct{}, len(peerIDs))

	for received := 0; received < len(peerIDs); received++ {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case result := <-resultCh:
			if result.err != nil || result.response == nil {
				continue
			}
			if result.response.VoteTerm >= prospectiveTerm {
				if err := node.stepDownForTerm(
					ctx,
					result.response.VoteTerm,
				); err != nil {
					return false, err
				}
				return false, nil
			}
			if result.response.VoteStatus != pb.Vote_VOTE_GRANTED {
				continue
			}
			if _, exists := peerSet[result.response.VoterId]; !exists {
				continue
			}
			if _, exists := seen[result.response.VoterId]; exists {
				continue
			}
			seen[result.response.VoterId] = struct{}{}
			granted++
			if granted >= required {
				return true, nil
			}
		}
	}
	return false, nil
}
