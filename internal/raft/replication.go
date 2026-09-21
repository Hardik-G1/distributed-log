package raft

import (
	"context"
	"errors"
	"fmt"
	"time"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"github.com/Hardik-G1/distributed-log/internal/storage"
	"github.com/jackc/pgx/v5"
)

func (node *Node) replicationLoop(ctx context.Context) {
	interval := node.config.Heartbeat
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		//exit
		case <-ctx.Done():
			return
		// interval out
		case <-ticker.C:
			node.replicateToPeers(ctx)
		}
	}
}

func (node *Node) replicateToPeers(ctx context.Context) {
	node.stateMu.Lock()
	if node.state.Role != RoleLeader || node.transport == nil {
		node.stateMu.Unlock()
		return
	}
	peerIDs := make([]string, 0, len(node.state.Peers))
	for peerID := range node.state.Peers {
		peerIDs = append(peerIDs, peerID)
	}
	transport := node.transport
	node.stateMu.Unlock()
	for _, peerID := range peerIDs {
		node.replicateToPeer(ctx, transport, peerID)
	}
}

func (node *Node) replicateToPeer(
	ctx context.Context,
	transport PeerTransport,
	peerID string,
) {
	request, leaderTerm, err := node.buildAppendEntries(
		ctx, peerID,
	)
	if err != nil || request == nil {
		return
	}
	response, err := transport.SendAppendEntries(
		ctx,
		peerID,
		request,
	)
	if err != nil {
		return
	}
	_ = node.handleAppendEntriesResponse(
		ctx,
		peerID,
		leaderTerm,
		response,
	)

}
func (node *Node) buildAppendEntries(
	ctx context.Context,
	peerID string,
) (*pb.AppendEntriesRequest, int64, error) {
	node.stateMu.Lock()
	if node.state.Role != RoleLeader {
		node.stateMu.Unlock()
		return nil, 0, nil
	}
	progress, exists := node.state.Peers[peerID]
	if !exists {
		node.stateMu.Unlock()
		return nil, 0, fmt.Errorf("unknown peer %s", peerID)
	}
	nextIndex := progress.NextIndex
	leaderTerm := node.state.CurrentTerm
	leaderID := node.state.NodeID
	leaderCommitIndex := node.state.CommitIndex
	node.stateMu.Unlock()
	if nextIndex < 1 {
		nextIndex = 1
	}
	prevLogIndex := nextIndex - 1
	prevLogTerm := int64(0)

	if prevLogIndex > 0 {
		prevEntry, err := node.store.GetRaftLog(ctx, prevLogIndex)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, 0, fmt.Errorf("log index requires a snapshot")
		}
		if err != nil {
			return nil, 0, err
		}

		prevLogTerm = prevEntry.Term
	}

	storedEntries, err := node.store.GetRaftEntriesFrom(
		ctx,
		nextIndex,
	)
	if err != nil {
		return nil, 0, err
	}
	request := &pb.AppendEntriesRequest{
		SenderId:          leaderID,
		PrevLogIndex:      prevLogIndex,
		PrevLogTerm:       prevLogTerm,
		Entries:           toProtoLogEntries(storedEntries),
		LeaderTerm:        leaderTerm,
		LeaderCommitIndex: leaderCommitIndex,
		RequestSentAt:     time.Now().Unix(),
	}
	return request, leaderTerm, nil
}

func (node *Node) handleAppendEntriesResponse(
	ctx context.Context,
	peerID string,
	sentTerm int64,
	response *pb.AppendEntriesResponse,
) error {
	if response == nil {
		return nil
	}
	node.stateMu.Lock()
	if response.CurrentTerm > node.state.CurrentTerm {
		node.state.CurrentTerm = response.CurrentTerm
		node.state.Role = RoleFollower
		node.state.VotedFor = ""
		node.state.LeaderID = ""
		node.votesReceived = 0

		err := node.store.SaveRaftMetadata(
			ctx,
			storage.RaftMetadata{
				CurrentTerm: node.state.CurrentTerm,
				VotedFor:    node.state.VotedFor,
			},
		)
		node.stateMu.Unlock()
		if err != nil {
			return err
		}
		node.resetElectionTimer()
		return nil
	}
	if node.state.Role != RoleLeader || node.state.CurrentTerm != sentTerm {
		node.stateMu.Unlock()
		return nil
	}
	progress, exists := node.state.Peers[peerID]
	if !exists {
		node.stateMu.Unlock()
		return nil
	}

	switch response.AppendStatus {
	case pb.AppendResponse_APPEND_RESPONSE_SUCCESS:
		progress.MatchIndex = max(response.MatchIndex, progress.MatchIndex)
		progress.NextIndex = progress.MatchIndex + 1
		node.state.Peers[peerID] = progress
		node.stateMu.Unlock()
		return node.advanceCommitIndex(ctx)
	case pb.AppendResponse_APPEND_RESPONSE_LOG_MISMATCH:
		if progress.NextIndex > 1 {
			progress.NextIndex--
		}
		node.state.Peers[peerID] = progress
		node.stateMu.Unlock()
		return nil
	default:
		node.stateMu.Unlock()
		return nil
	}
}

func (node *Node) advanceCommitIndex(
	ctx context.Context,
) error {
	lastLogIndex, _, err := node.lastLogInfo(ctx)
	if err != nil {
		return err
	}
	node.stateMu.Lock()
	currentTerm := node.state.CurrentTerm
	currentCommitIndex := node.state.CommitIndex

	peerProgress := make(map[string]PeerProgress)

	for peerID, progress := range node.state.Peers {
		peerProgress[peerID] = progress
	}
	node.stateMu.Unlock()

	for candidate := lastLogIndex; candidate > currentCommitIndex; candidate-- {
		entry, err := node.store.GetRaftLog(ctx, candidate)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		if entry.Term != currentTerm {
			continue
		}
		replicaCount := 1
		for _, progress := range peerProgress {
			if progress.MatchIndex >= candidate {
				replicaCount++
			}
		}
		if replicaCount >= majority(len(peerProgress)+1) {
			node.stateMu.Lock()
			if node.state.Role == RoleLeader && node.state.CurrentTerm == currentTerm && candidate > node.state.CommitIndex {
				node.state.CommitIndex = candidate
			}
			node.stateMu.Unlock()
			break

		}
	}
	return nil
}
