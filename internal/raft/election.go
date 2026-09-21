package raft

import (
	"context"
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
				return
			}
			resetTimer(timer, randomElectionTimeout(
				node.config.ElectionTimeout,
			))
		}
	}
}

func (node *Node) startElection(ctx context.Context) error {
	lastLogIndex, lastLogTerm, err := node.lastLogInfo(ctx)
	if err != nil {
		return err
	}

	node.stateMu.Lock()
	if node.state.Role == RoleFollower {
		node.stateMu.Unlock()
		return nil
	}
	node.state.Role = RoleCandidate
	node.state.CurrentTerm++
	node.state.VotedFor = node.state.NodeID
	node.state.LeaderID = ""
	node.votesReceived = 1
	metadata := storage.RaftMetadata{
		CurrentTerm: node.state.CurrentTerm,
		VotedFor:    node.state.VotedFor,
	}
	if err := node.store.SaveRaftMetadata(ctx, metadata); err != nil {
		return err
	}
	electionTerm := node.state.CurrentTerm
	candidateID := node.state.NodeID
	transport := node.transport
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

	for peerID := range node.state.Peers {
		node.state.Peers[peerID] = PeerProgress{
			NextIndex:  lastLogIndex + 1,
			MatchIndex: 0,
		}
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
