package raft

import (
	"context"
	"errors"
	"sync"

	"github.com/Hardik-G1/distributed-log/internal/config"
	"github.com/Hardik-G1/distributed-log/internal/storage"
	"github.com/jackc/pgx/v5"
)

type Node struct {
	stateMu     sync.Mutex
	lifecycleMu sync.Mutex
	config      *config.ServerConfig
	store       *storage.PostgresStore
	state       NodeState

	ctx    context.Context
	cancel context.CancelFunc
	doneCh chan struct{}

	started         bool
	stopped         bool
	votesReceived   int
	electionResetCh chan struct{}
	stateMachine    StateMachine
	transport       PeerTransport
}

func NewNode(
	ctx context.Context,
	cfg *config.ServerConfig,
	store *storage.PostgresStore,
	stateMachine StateMachine,
) (*Node, error) {
	if cfg == nil {
		return nil, errors.New("node config is nil")
	}
	if store == nil {
		return nil, errors.New("data store is nil")
	}
	if stateMachine == nil {
		return nil, errors.New("state machine is nil")
	}
	metadata, err := store.LoadRaftMetadata(ctx)
	if err != nil {
		return nil, err
	}
	lastLogIndex := int64(0)
	lastLog, err := store.GetLastRaftLog(ctx)
	switch {
	case err == nil:
		lastLogIndex = lastLog.LogIndex
	case errors.Is(err, pgx.ErrNoRows):

	default:
		return nil, err
	}
	peers := make(map[string]PeerProgress)

	for peerID := range cfg.PeerAddrs {
		if peerID == cfg.NodeID {
			continue
		}
		peers[peerID] = PeerProgress{
			NextIndex:  lastLogIndex + 1,
			MatchIndex: 0,
		}
	}

	node := &Node{
		config:          cfg,
		store:           store,
		doneCh:          make(chan struct{}),
		electionResetCh: make(chan struct{}, 1),
		stateMachine:    stateMachine,
		state: NodeState{
			NodeID:           cfg.NodeID,
			Role:             RoleFollower,
			CurrentTerm:      metadata.CurrentTerm,
			VotedFor:         metadata.VotedFor,
			CommitIndex:      0,
			LastAppliedIndex: 0,
			LeaderID:         "",
			Peers:            peers,
		},
	}
	return node, nil
}

func (node *Node) Start(parent context.Context) error {
	if parent == nil {
		return errors.New("parent context is nil")
	}
	node.lifecycleMu.Lock()
	defer node.lifecycleMu.Unlock()

	if node.started {
		return errors.New("node already started")
	}
	node.ctx, node.cancel = context.WithCancel(parent)
	node.started = true
	go node.run()

	return nil
}

func (node *Node) run() {
	defer close(node.doneCh) // 3 now it closes the doneChannel
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		node.electionLoop(node.ctx)
	}()
	go func() {
		defer wg.Done()
		node.replicationLoop(node.ctx)
	}()
	go func() {
		defer wg.Done()
		node.applyLoop(node.ctx)
	}()
	<-node.ctx.Done() // 1 waits for the context to be done
	wg.Wait()
}

func (node *Node) Stop() {
	node.lifecycleMu.Lock()
	if !node.started || node.stopped {
		node.lifecycleMu.Unlock()
		return
	}
	node.stopped = true
	cancel := node.cancel
	doneCh := node.doneCh
	node.lifecycleMu.Unlock()
	cancel() // 2 we cancel the context here so it goes to run and tells its done
	<-doneCh // 4 which tells us the stop finished
}
