package config

import (
	"errors"
	"flag"
	"strings"
	"time"
)

type ServerConfig struct {
	NodeID            string
	ListenAddr        string
	PeerAddrs         map[string]string
	Heartbeat         time.Duration
	ElectionTimeout   time.Duration
	PostgresDSN       string
	SnapshotThreshold int
	SnapshotDirectory string
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
		nodeID = strings.TrimSpace(nodeID)
		address = strings.TrimSpace(address)
		if !found {
			return nil, errors.New("invalid args for peer it should be nodeid=addr")
		}
		if nodeID == "" {
			return nil, errors.New("invalid args node id cannot be empty")
		}
		if address == "" {
			return nil, errors.New("invalid args for peer the address should not be empty")
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
	postgresDSN := flags.String("postgres-dsn", "localhost:6001", "the storage location")
	snapshotThreshold := flags.Int("snapshot-threshold", 10000, "the threshold at the snapshot")
	snapshotDirectory := flag.String("snapshot-directory", "data/snapshot", "directory for local raft snapshots")

	if err := flags.Parse(args); err != nil {
		return nil, err
	}

	peerAddrs, err := parsePeers(*peerAddrRaw)
	if err != nil {
		return nil, err
	}
	if *nodeID == "" {
		return nil, errors.New("node id is necessary")
	}
	if _, exists := peerAddrs[*nodeID]; exists {
		return nil, errors.New("node id could not be in the peer group")
	}
	if *electionTimeout <= *heartbeat {
		return nil, errors.New("election timeout must be greater than heartbeat interval")
	}
	if *snapshotThreshold <= 0 {
		return nil, errors.New("snapshot threshold must be positive")
	}

	return &ServerConfig{
		NodeID:            *nodeID,
		ListenAddr:        *listenAddr,
		PeerAddrs:         peerAddrs,
		Heartbeat:         *heartbeat,
		ElectionTimeout:   *electionTimeout,
		PostgresDSN:       *postgresDSN,
		SnapshotThreshold: *snapshotThreshold,
		SnapshotDirectory: *snapshotDirectory,
	}, nil
}
