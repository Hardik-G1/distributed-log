package config

import (
	"errors"
	"flag"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

type ServerConfig struct {
	NodeID            string
	ListenAddr        string
	PeerAddrs         map[string]string
	Heartbeat         time.Duration
	ElectionTimeout   time.Duration
	SnapshotThreshold int
	SnapshotDirectory string
	RaftDataDirectory string
	AppDataDirectory  string
	ProposalBatchWait time.Duration
	ProposalBatchSize int
	ProposalBatchMax  int
}

func parsePeers(raw string) (map[string]string, error) {
	result := make(map[string]string)
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return result, nil
	}
	items := strings.SplitSeq(raw, ",")
	for item := range items {
		item = strings.TrimSpace(item)
		nodeID, address, found := strings.Cut(item, "=")
		if !found {
			return nil, errors.New("invalid args for peer it should be nodeid=addr")
		}
		nodeID = strings.TrimSpace(nodeID)
		address = strings.TrimSpace(address)

		if nodeID == "" {
			return nil, errors.New("invalid args node id cannot be empty")
		}
		if address == "" {
			return nil, errors.New("invalid args for peer the address should not be empty")
		}
		if _, exists := result[nodeID]; exists {
			return nil, fmt.Errorf("duplicate peer %s", nodeID)
		}
		result[nodeID] = address
	}
	return result, nil
}
func ServerConfigLoad(args []string) (*ServerConfig, error) {
	flags := flag.NewFlagSet("server", flag.ContinueOnError)
	nodeID := flags.String("node-id", "", "unique server id")
	listenAddr := flags.String("listen-addr", ":5001", "grpc listen addr")
	peerAddrRaw := flags.String("peers", "", "peer address group")
	heartbeat := flags.Duration("heartbeat", 100*time.Millisecond, "raft heartbeat interval")
	electionTimeout := flags.Duration("election-timeout", 500*time.Millisecond, "election timeout")

	snapshotThreshold := flags.Int("snapshot-threshold", 10000, "the threshold at the snapshot")
	snapshotDirectory := flags.String("snapshot-directory", "", "directory for local raft snapshots")
	raftDataDirectory := flags.String("raft-data-directory", "", "directory containing the RAFT WAL")
	appDataDirectory := flags.String("app-data-directory", "", "directory containing the application state data")
	proposalBatchWait := flags.Duration("proposal-batch-wait", time.Millisecond, "max time to collect a proposal batch")
	proposalBatchSize := flags.Int("proposal-batch-size", 256, "preferred proposal batch size")
	proposalBatchMax := flags.Int("proposal-batch-max", 1024, "maximum proposal batch size")
	if err := flags.Parse(args); err != nil {
		return nil, err
	}

	*nodeID = strings.TrimSpace(*nodeID)
	*listenAddr = strings.TrimSpace(*listenAddr)

	if *nodeID == "" {
		return nil, errors.New("node id is required")
	}
	if *listenAddr == "" {
		return nil, errors.New("listen address is required")
	}
	if *heartbeat <= 0 {
		return nil, errors.New("heartbeat must be positive")
	}
	if *electionTimeout <= *heartbeat {
		return nil, errors.New(
			"election timeout must be greater than heartbeat interval",
		)
	}
	if *snapshotThreshold <= 0 {
		return nil, errors.New("snapshot threshold must be positive")
	}
	if *proposalBatchWait < 0 {
		return nil, errors.New("proposal batch wait cannot be negative")
	}
	if *proposalBatchSize <= 0 {
		return nil, errors.New("proposal batch size must be positive")
	}
	if *proposalBatchMax < *proposalBatchSize {
		return nil, errors.New(
			"proposal batch max must be at least proposal batch size",
		)
	}

	peerAddrs, err := parsePeers(*peerAddrRaw)
	if err != nil {
		return nil, err
	}
	if _, exists := peerAddrs[*nodeID]; exists {
		return nil, errors.New("node id cannot appear in its peer group")
	}

	baseDirectory := filepath.Join("data", *nodeID)

	if strings.TrimSpace(*raftDataDirectory) == "" {
		*raftDataDirectory = filepath.Join(baseDirectory, "raft")
	}
	if strings.TrimSpace(*appDataDirectory) == "" {
		*appDataDirectory = filepath.Join(baseDirectory, "application")
	}
	if strings.TrimSpace(*snapshotDirectory) == "" {
		*snapshotDirectory = filepath.Join(baseDirectory, "snapshots")
	}

	absoluteRaftDirectory, err := filepath.Abs(*raftDataDirectory)
	if err != nil {
		return nil, fmt.Errorf("resolve raft data directory: %w", err)
	}
	absoluteAppDirectory, err := filepath.Abs(*appDataDirectory)
	if err != nil {
		return nil, fmt.Errorf("resolve application data directory: %w", err)
	}
	absoluteSnapshotDirectory, err := filepath.Abs(*snapshotDirectory)
	if err != nil {
		return nil, fmt.Errorf("resolve snapshot directory: %w", err)
	}

	return &ServerConfig{
		NodeID:            *nodeID,
		ListenAddr:        *listenAddr,
		PeerAddrs:         peerAddrs,
		Heartbeat:         *heartbeat,
		ElectionTimeout:   *electionTimeout,
		SnapshotThreshold: *snapshotThreshold,
		SnapshotDirectory: absoluteSnapshotDirectory,
		RaftDataDirectory: absoluteRaftDirectory,
		AppDataDirectory:  absoluteAppDirectory,
		ProposalBatchWait: *proposalBatchWait,
		ProposalBatchSize: *proposalBatchSize,
		ProposalBatchMax:  *proposalBatchMax,
	}, nil
}
