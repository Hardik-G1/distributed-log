package raft

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"github.com/Hardik-G1/distributed-log/internal/config"
	"github.com/Hardik-G1/distributed-log/internal/storage"
)

type testStateMachine struct{}

func (testStateMachine) Apply(context.Context, storage.RaftLog) error {
	return nil
}

type failingMetadataStore struct {
	storage.RaftLogStore
	failSave bool
}

func (store *failingMetadataStore) SaveRaftMetadata(
	ctx context.Context,
	metadata storage.RaftMetadata,
) error {
	if store.failSave {
		return errors.New("injected metadata failure")
	}
	return store.RaftLogStore.SaveRaftMetadata(ctx, metadata)
}

func newVoteTestNode(t *testing.T, store storage.RaftLogStore) *Node {
	t.Helper()
	node, err := NewNode(
		context.Background(),
		&config.ServerConfig{
			NodeID:            "node-1",
			PeerAddrs:         map[string]string{},
			Heartbeat:         100 * time.Millisecond,
			ElectionTimeout:   500 * time.Millisecond,
			SnapshotThreshold: 1000,
			SnapshotDirectory: t.TempDir(),
			ProposalBatchWait: time.Millisecond,
			ProposalBatchSize: 16,
			ProposalBatchMax:  64,
		},
		store,
		testStateMachine{},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	return node
}

func TestHandleRequestVotePersistsGrantedVote(t *testing.T) {
	ctx := context.Background()
	store, err := storage.OpenWALRaftStore(filepath.Join(t.TempDir(), "raft"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	node := newVoteTestNode(t, store)

	response, err := node.HandleRequestVote(ctx, &pb.RequestVoteRequest{
		VoteTerm: 1, CandidateId: "node-2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.VoteStatus != pb.Vote_VOTE_GRANTED {
		t.Fatalf("vote=%s, want GRANTED", response.VoteStatus)
	}
	metadata, err := store.LoadRaftMetadata(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.CurrentTerm != 1 || metadata.VotedFor != "node-2" {
		t.Fatalf("metadata=%+v", metadata)
	}

	response, err = node.HandleRequestVote(ctx, &pb.RequestVoteRequest{
		VoteTerm: 1, CandidateId: "node-3",
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.VoteStatus != pb.Vote_VOTE_REJECTED {
		t.Fatalf("second vote=%s, want REJECTED", response.VoteStatus)
	}
}

func TestHandleRequestVoteDoesNotExposeUnpersistedVote(t *testing.T) {
	ctx := context.Background()
	walStore, err := storage.OpenWALRaftStore(filepath.Join(t.TempDir(), "raft"))
	if err != nil {
		t.Fatal(err)
	}
	defer walStore.Close()
	if err := walStore.SaveRaftMetadata(ctx, storage.RaftMetadata{CurrentTerm: 1}); err != nil {
		t.Fatal(err)
	}
	store := &failingMetadataStore{RaftLogStore: walStore, failSave: true}
	node := newVoteTestNode(t, store)

	_, err = node.HandleRequestVote(ctx, &pb.RequestVoteRequest{
		VoteTerm: 1, CandidateId: "node-2",
	})
	if err == nil {
		t.Fatal("expected injected persistence failure")
	}
	node.stateMu.Lock()
	votedFor := node.state.VotedFor
	node.stateMu.Unlock()
	if votedFor != "" {
		t.Fatalf("unpersisted vote exposed in memory: %q", votedFor)
	}
}

func TestStartElectionDoesNotExposeUnpersistedTerm(t *testing.T) {
	ctx := context.Background()
	walStore, err := storage.OpenWALRaftStore(filepath.Join(t.TempDir(), "raft"))
	if err != nil {
		t.Fatal(err)
	}
	defer walStore.Close()
	if err := walStore.SaveRaftMetadata(ctx, storage.RaftMetadata{CurrentTerm: 1}); err != nil {
		t.Fatal(err)
	}
	store := &failingMetadataStore{RaftLogStore: walStore, failSave: true}
	node := newVoteTestNode(t, store)

	if err := node.startElection(ctx); err == nil {
		t.Fatal("expected injected persistence failure")
	}
	node.stateMu.Lock()
	term := node.state.CurrentTerm
	role := node.state.Role
	votedFor := node.state.VotedFor
	node.stateMu.Unlock()
	if term != 1 || role != RoleFollower || votedFor != "" {
		t.Fatalf("state changed after failed persistence: term=%d role=%v votedFor=%q", term, role, votedFor)
	}
}
