package raft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"github.com/Hardik-G1/distributed-log/internal/storage"
)

type incomingSnapshot struct {
	SnapshotID        string
	TempPath          string
	LastIncludedIndex int64
	LastIncludedTerm  int64
	NextOffset        int64
}

func (node *Node) HandleInstallSnapshot(
	ctx context.Context,
	req *pb.InstallSnapshotRequest,
) (*pb.InstallSnapshotResponse, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	if req == nil {
		return nil, errors.New("snapshot request is nil")
	}
	if req.LeaderId == "" {
		return nil, errors.New("leader id is empty")
	}
	if req.CurrentTerm < 0 || req.LastIncludedIndex < 0 || req.LastIncludedTerm < 0 || req.Offset < 0 {
		return nil, errors.New("snapshot request contains invalid values")
	}

	restorer, ok := node.stateMachine.(SnapshotRestorer)
	if !ok {
		return &pb.InstallSnapshotResponse{
			Term:   node.currentTerm(),
			Status: pb.SnapshotStatus_SNAPSHOT_STATUS_REJECTED,
		}, errors.New("state machine does not support snapshot restore")
	}
	if err := node.stepDownForTerm(ctx, req.CurrentTerm); err != nil {
		return nil, err
	}
	node.stateMu.Lock()

	if req.CurrentTerm < node.state.CurrentTerm {
		term := node.state.CurrentTerm
		node.stateMu.Unlock()
		return &pb.InstallSnapshotResponse{
			Term:   term,
			Status: pb.SnapshotStatus_SNAPSHOT_STATUS_OLD_TERM,
		}, nil
	}

	node.installingSnapshot.Store(true)
	defer node.installingSnapshot.Store(false)
	node.state.Role = RoleFollower
	node.state.LeaderID = req.LeaderId
	node.lastLeaderContact = time.Now()

	currentTerm := node.state.CurrentTerm
	lastIncludedIndex := node.state.LastIncludedIndex
	lastIncludedTerm := node.state.LastIncludedTerm
	node.stateMu.Unlock()
	node.resetElectionTimer()

	if req.LastIncludedIndex < lastIncludedIndex ||
		(req.LastIncludedIndex == lastIncludedIndex &&
			req.LastIncludedTerm != lastIncludedTerm) {
		return &pb.InstallSnapshotResponse{
			Term:   currentTerm,
			Status: pb.SnapshotStatus_SNAPSHOT_STATUS_BAD_INDEX,
		}, nil
	}
	node.snapshotMu.Lock()
	defer node.snapshotMu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	snapshotDirectory := node.config.SnapshotDirectory
	if snapshotDirectory == "" {
		return nil, errors.New("snapshot directory is empty")
	}

	if req.Offset == 0 {
		if node.incomingSnapshot != nil {
			_ = os.Remove(node.incomingSnapshot.TempPath)
		}
		fileMode := 0o700
		if err := os.MkdirAll(snapshotDirectory, os.FileMode(fileMode)); err != nil {
			return nil, fmt.Errorf("create snapshot directory %w", err)
		}

		snapshotID := fmt.Sprintf(
			"%s-%d-%d",
			req.LeaderId,
			req.LastIncludedIndex,
			req.LastIncludedTerm,
		)
		tempFile, err := os.CreateTemp(snapshotDirectory, ".incoming-snapshot-*")
		if err != nil {
			return nil, err
		}
		tempPath := tempFile.Name()
		if err := tempFile.Chmod(0o600); err != nil {
			_ = tempFile.Close()
			_ = os.Remove(tempPath)
			return nil, err
		}
		if err := tempFile.Close(); err != nil {
			_ = os.Remove(tempPath)
			return nil, err
		}
		node.incomingSnapshot = &incomingSnapshot{
			SnapshotID:        snapshotID,
			TempPath:          tempPath,
			LastIncludedIndex: req.LastIncludedIndex,
			LastIncludedTerm:  req.LastIncludedTerm,
			NextOffset:        0,
		}
	}
	incoming := node.incomingSnapshot
	if incoming == nil {
		return &pb.InstallSnapshotResponse{
			Term:   currentTerm,
			Status: pb.SnapshotStatus_SNAPSHOT_STATUS_BAD_INDEX,
		}, nil
	}

	if incoming.LastIncludedIndex != req.LastIncludedIndex ||
		incoming.LastIncludedTerm != req.LastIncludedTerm ||
		incoming.NextOffset != req.Offset {
		return &pb.InstallSnapshotResponse{
			Term:   currentTerm,
			Status: pb.SnapshotStatus_SNAPSHOT_STATUS_BAD_INDEX,
		}, nil
	}
	file, err := os.OpenFile(
		incoming.TempPath,
		os.O_WRONLY|os.O_APPEND,
		0o600,
	)
	if err != nil {
		return nil, err
	}
	written, writeErr := file.Write(req.Data)
	if writeErr == nil && written != len(req.Data) {
		writeErr = io.ErrShortWrite
	}

	syncErr := file.Sync()
	closeErr := file.Close()

	if writeErr != nil {
		return nil, writeErr
	}
	if syncErr != nil {
		return nil, syncErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	incoming.NextOffset += int64(written)

	if !req.Done {
		return &pb.InstallSnapshotResponse{
			Term:   currentTerm,
			Status: pb.SnapshotStatus_SNAPSHOT_STATUS_ACCEPTED,
		}, nil
	}
	node.stateMachineMu.Lock()
	defer node.stateMachineMu.Unlock()
	node.logMu.Lock()
	defer node.logMu.Unlock()

	retainSuffix := false

	switch {
	case incoming.LastIncludedIndex == lastIncludedIndex && incoming.LastIncludedTerm == lastIncludedTerm:
		retainSuffix = true
	case incoming.LastIncludedIndex > lastIncludedIndex && incoming.LastIncludedIndex <= node.lastLogIndex:
		boundaryEntry, cached := node.cachedRaftLogLocked(incoming.LastIncludedIndex)
		if !cached {
			boundaryEntry, err = node.store.GetRaftLog(ctx, incoming.LastIncludedIndex)
		}
		if err == nil && boundaryEntry.Term == incoming.LastIncludedTerm {
			retainSuffix = true
		} else if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
	}
	data, err := os.ReadFile(incoming.TempPath)
	if err != nil {
		return nil, fmt.Errorf("read incoming snapshot %w", err)
	}

	finalLocation := filepath.Join(
		snapshotDirectory,
		incoming.SnapshotID+".bin",
	)

	snapshot := storage.RaftSnapshot{
		SnapshotID:        incoming.SnapshotID,
		LastIncludedIndex: incoming.LastIncludedIndex,
		LastIncludedTerm:  incoming.LastIncludedTerm,
		Location:          finalLocation,
	}
	if err := node.store.SaveSnapshot(
		ctx,
		snapshot,
		data,
	); err != nil {
		return nil, err
	}
	if err := restorer.Restore(ctx, data); err != nil {
		return nil, err
	}
	truncateThrough := incoming.LastIncludedIndex
	if !retainSuffix && node.lastLogIndex > truncateThrough {
		truncateThrough = node.lastLogIndex
	}
	if err := node.store.DeleteRaftEntriesThrough(
		ctx,
		truncateThrough,
	); err != nil {
		return nil, err
	}
	if retainSuffix {
		node.discardCachedRaftLogsThroughLocked(incoming.LastIncludedIndex)

	} else {
		node.logCache = node.logCache[:0]
		node.logCacheBase = 0
	}
	if !retainSuffix ||
		node.lastLogIndex <= incoming.LastIncludedIndex {
		node.lastLogIndex = incoming.LastIncludedIndex
		node.lastLogTerm = incoming.LastIncludedTerm
	}
	node.stateMu.Lock()
	commitAdvanced := false
	appliedAdvanced := false
	if node.state.LastIncludedIndex < incoming.LastIncludedIndex {
		node.state.LastIncludedIndex = incoming.LastIncludedIndex
		node.state.LastIncludedTerm = incoming.LastIncludedTerm
	}
	if node.state.CommitIndex < incoming.LastIncludedIndex {
		node.state.CommitIndex = incoming.LastIncludedIndex
		commitAdvanced = true
	}
	if node.state.LastAppliedIndex < incoming.LastIncludedIndex {
		node.state.LastAppliedIndex = incoming.LastIncludedIndex
		appliedAdvanced = true
	}
	node.lastLeaderContact = time.Now()
	if commitAdvanced {
		node.notifyCommitLocked()
	}
	if appliedAdvanced {
		node.notifyAppliedLocked()
	}
	node.stateMu.Unlock()
	node.resetElectionTimer()
	_ = os.Remove(incoming.TempPath)
	node.incomingSnapshot = nil

	return &pb.InstallSnapshotResponse{
		Term:   currentTerm,
		Status: pb.SnapshotStatus_SNAPSHOT_STATUS_INSTALLED,
	}, nil
}

func (node *Node) currentTerm() int64 {
	node.stateMu.Lock()
	defer node.stateMu.Unlock()

	return node.state.CurrentTerm
}
