package raft

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"github.com/Hardik-G1/distributed-log/internal/storage"
)

const (
	maxAppendEntriesPerRPC = 1024
	snapshotChunkSize      = 64 * 1024
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
			node.replicateToPeers()
		case <-node.replicationCh:
			node.replicateToPeers()
		}
	}
}

func (node *Node) signalReplication() {
	select {
	case node.replicationCh <- struct{}{}:
	default:
	}
}

func (node *Node) replicateToPeers() {
	node.stateMu.Lock()
	if node.state.Role != RoleLeader || node.transport == nil {
		node.stateMu.Unlock()
		return
	}
	node.stateMu.Unlock()
	for _, wakeCh := range node.peerReplicationCh {
		select {
		case wakeCh <- struct{}{}:
		default:
		}
	}
}
func (node *Node) replicationPeerLoop(
	ctx context.Context,
	peerID string,
	wakeCh <-chan struct{},
) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-wakeCh:
			for node.replicateToPeer(
				ctx,
				node.transport,
				peerID,
			) {
			}
		}
	}
}

func (node *Node) replicateToPeer(
	ctx context.Context,
	transport PeerTransport,
	peerID string,
) bool {
	request, leaderTerm, err := node.buildAppendEntries(
		ctx, peerID,
	)
	if err != nil {
		if errors.Is(err, ErrSnapshotRequired) {
			return node.sendSnapshotToPeer(
				ctx,
				transport,
				peerID,
			) == nil
		}
		return false
	}
	if request == nil {
		return false
	}
	response, err := transport.SendAppendEntries(
		ctx,
		peerID,
		request,
	)
	if err != nil {
		return false
	}
	lastSentIndex := request.PrevLogIndex + int64(len(request.Entries))
	if err := node.handleAppendEntriesResponse(
		ctx,
		peerID,
		leaderTerm,
		lastSentIndex,
		response,
	); err != nil {
		return false
	}
	if response == nil {
		return false
	}
	if response.AppendStatus == pb.AppendResponse_APPEND_RESPONSE_LOG_MISMATCH {
		node.stateMu.Lock()
		progress, exists := node.state.Peers[peerID]
		node.stateMu.Unlock()
		return exists && progress.NextIndex < request.PrevLogIndex+1
	}
	return response.AppendStatus == pb.AppendResponse_APPEND_RESPONSE_SUCCESS && len(request.Entries) == maxAppendEntriesPerRPC
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
	lastIncludedIndex := node.state.LastIncludedIndex
	lastIncludedTerm := node.state.LastIncludedTerm
	node.stateMu.Unlock()
	node.logMu.Lock()
	defer node.logMu.Unlock()
	if nextIndex < 1 {
		nextIndex = 1
	}

	if nextIndex <= lastIncludedIndex {
		return nil, 0, ErrSnapshotRequired
	}
	prevLogIndex := nextIndex - 1
	prevLogTerm := int64(0)

	if prevLogIndex == lastIncludedIndex {
		prevLogTerm = lastIncludedTerm
	} else if prevLogIndex > lastIncludedIndex {
		prevEntry, cached := node.cachedRaftLogLocked(prevLogIndex)
		if !cached {
			var err error
			prevEntry, err = node.store.GetRaftLog(ctx, prevLogIndex)
			if errors.Is(err, storage.ErrNotFound) {
				return nil, 0, ErrSnapshotRequired
			}
			if err != nil {
				return nil, 0, err
			}
		}
		prevLogTerm = prevEntry.Term
	}

	entries, cached := node.cachedRaftEntriesLocked(
		nextIndex,
		maxAppendEntriesPerRPC,
	)
	if !cached {
		var err error
		entries, err = node.store.GetRaftEntriesFromLimit(ctx, nextIndex, maxAppendEntriesPerRPC)
		if err != nil {
			return nil, 0, err
		}
	}
	request := &pb.AppendEntriesRequest{
		SenderId:          leaderID,
		PrevLogIndex:      prevLogIndex,
		PrevLogTerm:       prevLogTerm,
		Entries:           toProtoLogEntries(entries),
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
	lastSentIndex int64,
	response *pb.AppendEntriesResponse,
) error {
	if response == nil {
		return nil
	}
	node.stateMu.Lock()
	currentTerm := node.state.CurrentTerm
	node.stateMu.Unlock()
	if response.CurrentTerm > currentTerm {
		err := node.stepDownForTerm(ctx, response.CurrentTerm)
		if err != nil {
			return err
		}
		node.resetElectionTimer()
		return &NotLeaderError{}
	}
	if response.CurrentTerm != sentTerm {
		return nil
	}
	node.stateMu.Lock()
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
		reportedMatchIndex := response.MatchIndex
		if reportedMatchIndex < 0 || reportedMatchIndex > lastSentIndex {
			node.stateMu.Unlock()
			return fmt.Errorf("invalid match index %d", reportedMatchIndex)
		}
		if reportedMatchIndex >= progress.MatchIndex {
			progress.MatchIndex = reportedMatchIndex
			progress.NextIndex = reportedMatchIndex + 1
			node.state.Peers[peerID] = progress
		}
		node.stateMu.Unlock()
		return node.advanceCommitIndex(ctx)
	case pb.AppendResponse_APPEND_RESPONSE_LOG_MISMATCH:
		hintedNextIndex := response.MatchIndex + 1
		if response.MatchIndex >= 0 && response.MatchIndex < lastSentIndex && hintedNextIndex != progress.NextIndex {
			progress.NextIndex = hintedNextIndex
		} else if progress.NextIndex > 1 {
			progress.NextIndex--
		}
		node.state.Peers[peerID] = progress
		node.stateMu.Unlock()
		return nil
	case pb.AppendResponse_APPEND_RESPONSE_OLD_TERM:
		node.stateMu.Unlock()
		return nil
	default:
		node.stateMu.Unlock()
		return fmt.Errorf("unknown append response status %s", response.AppendStatus.String())
	}
}

func (node *Node) advanceCommitIndex(
	ctx context.Context,
) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	node.logMu.Lock()
	lastLogIndex := node.lastLogIndex
	node.logMu.Unlock()

	node.stateMu.Lock()
	currentTerm := node.state.CurrentTerm
	currentCommitIndex := node.state.CommitIndex
	matchIndexes := make([]int64, 1, len(node.state.Peers)+1)
	matchIndexes[0] = lastLogIndex
	for _, progress := range node.state.Peers {
		matchIndexes = append(matchIndexes, progress.MatchIndex)
	}
	node.stateMu.Unlock()

	sort.Slice(matchIndexes, func(i, j int) bool {
		return matchIndexes[i] < matchIndexes[j]
	})
	quorum := majority(len(matchIndexes))
	majorityPosition := len(matchIndexes) - quorum
	majorityIndex := matchIndexes[majorityPosition]
	if majorityIndex <= currentCommitIndex {
		return nil
	}
	for candidate := majorityIndex; candidate > currentCommitIndex; candidate-- {
		node.logMu.Lock()
		entry, cached := node.cachedRaftLogLocked(candidate)
		if !cached {
			var err error
			entry, err = node.store.GetRaftLog(ctx, candidate)
			if errors.Is(err, storage.ErrNotFound) {
				node.logMu.Unlock()
				continue
			}
			if err != nil {
				node.logMu.Unlock()
				return err
			}
		}
		node.logMu.Unlock()

		if entry.Term != currentTerm {
			continue
		}
		node.stateMu.Lock()
		if node.state.Role == RoleLeader &&
			node.state.CurrentTerm == currentTerm &&
			candidate > node.state.CommitIndex {
			node.state.CommitIndex = candidate
			node.notifyCommitLocked()
		}
		node.stateMu.Unlock()
		break

	}
	return nil

}

func (node *Node) sendSnapshotToPeer(
	ctx context.Context,
	transport PeerTransport,
	peerID string,
) error {
	snapshot, data, err := node.store.LoadLatestSnapshot(ctx)
	if errors.Is(err, storage.ErrNotFound) {
		return errors.New("no snapshot is available")
	}
	if err != nil {
		return err
	}
	node.stateMu.Lock()
	if node.state.Role != RoleLeader {
		knownLeaderID := node.state.LeaderID
		node.stateMu.Unlock()
		return &NotLeaderError{LeaderID: knownLeaderID}
	}
	leaderID := node.state.NodeID
	leaderTerm := node.state.CurrentTerm
	node.stateMu.Unlock()
	sendChunk := func(
		request *pb.InstallSnapshotRequest,
		finalChunk bool,
	) error {
		response, err := transport.SendInstallSnapshot(
			ctx,
			peerID,
			request,
		)
		if err != nil {
			return err
		}
		if response == nil {
			return errors.New("empty installsnapshot response")
		}
		node.stateMu.Lock()
		currentTerm := node.state.CurrentTerm
		stillLeader := node.state.Role == RoleLeader && currentTerm == leaderTerm
		node.stateMu.Unlock()
		if response.Term > currentTerm {
			stepDownErr := node.stepDownForTerm(
				ctx, response.Term,
			)

			if stepDownErr != nil {
				return stepDownErr
			}
			node.resetElectionTimer()
			return &NotLeaderError{}
		}
		if !stillLeader {
			return &NotLeaderError{}
		}
		switch response.Status {
		case pb.SnapshotStatus_SNAPSHOT_STATUS_ACCEPTED:
			if finalChunk {
				return fmt.Errorf("final chunk was not installed %s", response.Status.String())
			}

		case pb.SnapshotStatus_SNAPSHOT_STATUS_INSTALLED:
			if !finalChunk {
				return fmt.Errorf("final chunk was not installed %s", response.Status.String())
			}
		case pb.SnapshotStatus_SNAPSHOT_STATUS_OLD_TERM,
			pb.SnapshotStatus_SNAPSHOT_STATUS_BAD_INDEX,
			pb.SnapshotStatus_SNAPSHOT_STATUS_REJECTED:
			return fmt.Errorf(
				"follower rejected snapshot %s",
				response.Status.String(),
			)
		default:
			return errors.New("unknown snapshot response status")

		}
		return nil
	}

	if len(data) == 0 {
		request := &pb.InstallSnapshotRequest{
			LeaderId:          leaderID,
			CurrentTerm:       leaderTerm,
			LastIncludedIndex: snapshot.LastIncludedIndex,
			LastIncludedTerm:  snapshot.LastIncludedTerm,
			Offset:            0,
			Data:              nil,
			Done:              true,
		}
		if err := sendChunk(request, true); err != nil {
			return err
		}
	} else {
		for offset := int64(0); offset < int64(len(data)); {
			end := offset + snapshotChunkSize
			if end > int64(len(data)) {
				end = int64(len(data))
			}
			finalChunk := end == int64(len(data))
			request := &pb.InstallSnapshotRequest{
				LeaderId:          leaderID,
				CurrentTerm:       leaderTerm,
				LastIncludedIndex: snapshot.LastIncludedIndex,
				LastIncludedTerm:  snapshot.LastIncludedTerm,
				Offset:            offset,
				Data:              data[offset:end],
				Done:              finalChunk,
			}
			if err := sendChunk(request, finalChunk); err != nil {
				return err
			}
			offset = end
		}
	}

	node.stateMu.Lock()
	defer node.stateMu.Unlock()

	if node.state.Role != RoleLeader || node.state.CurrentTerm != leaderTerm {
		return &NotLeaderError{LeaderID: node.state.LeaderID}
	}

	progress, exists := node.state.Peers[peerID]
	if !exists {
		return fmt.Errorf("unknown peer %s", peerID)
	}

	if progress.MatchIndex < snapshot.LastIncludedIndex {
		progress.MatchIndex = snapshot.LastIncludedIndex
	}
	if progress.NextIndex < snapshot.LastIncludedIndex+1 {
		progress.NextIndex = snapshot.LastIncludedIndex + 1
	}
	node.state.Peers[peerID] = progress
	return nil

}
