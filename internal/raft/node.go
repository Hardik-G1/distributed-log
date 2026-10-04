package raft

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hardik-G1/distributed-log/internal/config"
	"github.com/Hardik-G1/distributed-log/internal/storage"
)

type Node struct {
	stateMu            sync.Mutex
	pendingMu          sync.Mutex
	termMu             sync.Mutex
	logMu              sync.Mutex
	lifecycleMu        sync.Mutex
	snapshotMu         sync.Mutex
	stateMachineMu     sync.Mutex
	installingSnapshot atomic.Bool
	lastLeaderContact  time.Time
	config             *config.ServerConfig
	store              storage.RaftLogStore
	state              NodeState

	lastLogIndex int64
	lastLogTerm  int64
	logCacheBase int64
	logCache     []storage.RaftLog

	ctx    context.Context
	cancel context.CancelFunc
	doneCh chan struct{}

	started          bool
	stopped          bool
	votesReceived    int
	votesGranted     map[string]struct{}
	pendingProposals map[string]*pendingProposal

	electionResetCh   chan struct{}
	proposalCh        chan *proposalRequest
	replicationCh     chan struct{}
	leaderNoopCh      chan struct{}
	applyCh           chan struct{}
	peerReplicationCh map[string]chan struct{}
	commitWaitCh      chan struct{}
	applyWaitCh       chan struct{}
	stateMachine      StateMachine
	transport         PeerTransport
	incomingSnapshot  *incomingSnapshot
}

type pendingProposal struct {
	done  chan struct{}
	entry storage.RaftLog
	err   error
}
type proposalRequest struct {
	operationType int32
	payload       []byte
	result        chan proposalResult
}

type proposalResult struct {
	entry storage.RaftLog
	err   error
}

func NewNode(
	ctx context.Context,
	cfg *config.ServerConfig,
	store storage.RaftLogStore,
	stateMachine StateMachine,
	transport PeerTransport,
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
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	recovered, err := recoveryRaftState(ctx, store, stateMachine)
	if err != nil {
		return nil, err
	}

	peers := make(map[string]PeerProgress)
	peerReplicationCh := make(map[string]chan struct{})
	for peerID := range cfg.PeerAddrs {
		if peerID == cfg.NodeID {
			continue
		}
		peers[peerID] = PeerProgress{
			NextIndex:  recovered.LastLogIndex + 1,
			MatchIndex: 0,
		}
		peerReplicationCh[peerID] = make(chan struct{}, 1)
	}
	if len(peers) > 0 && transport == nil {
		return nil, errors.New("peer transport is required")
	}
	node := &Node{
		config:            cfg,
		store:             store,
		doneCh:            make(chan struct{}),
		electionResetCh:   make(chan struct{}, 1),
		proposalCh:        make(chan *proposalRequest, 4096),
		replicationCh:     make(chan struct{}, 1),
		leaderNoopCh:      make(chan struct{}, 1),
		applyCh:           make(chan struct{}, 1),
		peerReplicationCh: peerReplicationCh,
		commitWaitCh:      make(chan struct{}),
		applyWaitCh:       make(chan struct{}),

		stateMachine:     stateMachine,
		transport:        transport,
		votesGranted:     make(map[string]struct{}),
		pendingProposals: make(map[string]*pendingProposal),
		lastLogIndex:     recovered.LastLogIndex,
		lastLogTerm:      recovered.LastLogTerm,

		state: NodeState{
			NodeID:            cfg.NodeID,
			Role:              RoleFollower,
			CurrentTerm:       recovered.Metadata.CurrentTerm,
			VotedFor:          recovered.Metadata.VotedFor,
			CommitIndex:       recovered.LastAppliedIndex,
			LastAppliedIndex:  recovered.LastAppliedIndex,
			LastIncludedIndex: recovered.LastIncludedIndex,
			LastIncludedTerm:  recovered.LastIncludedTerm,
			LeaderID:          "",
			Peers:             peers,
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
	defer close(node.doneCh)
	var wg sync.WaitGroup
	wg.Add(5 + len(node.peerReplicationCh))
	go func() {
		defer wg.Done()
		node.proposalLoop(node.ctx)
	}()
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
		node.leaderNoopLoop(node.ctx)
	}()
	go func() {
		defer wg.Done()
		node.applyLoop(node.ctx)
	}()
	for peerID, wakeCh := range node.peerReplicationCh {
		peerID := peerID
		wakeCh := wakeCh
		go func() {
			defer wg.Done()
			node.replicationPeerLoop(
				node.ctx,
				peerID,
				wakeCh,
			)
		}()
	}
	node.signalApply()
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
