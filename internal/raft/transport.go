package raft

import (
	"context"
	"errors"
	"fmt"
	"time"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	raftRPCTimeout     = 250 * time.Millisecond
	snapshotRPCTimeout = 30 * time.Second
)

type PeerTransport interface {
	SendRequestVote(ctx context.Context, peerID string, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	SendAppendEntries(ctx context.Context, peerID string, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
	SendInstallSnapshot(ctx context.Context, peerID string, request *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error)
	SendPreVote(ctx context.Context, peerID string, request *pb.PreVoteRequest) (*pb.PreVoteResponse, error)
}

type GRPCTransport struct {
	clients map[string]pb.RaftServiceClient
	conns   map[string]*grpc.ClientConn
}

func NewGRPCTransport(
	peerAddresses map[string]string,
) (*GRPCTransport, error) {
	transport := &GRPCTransport{
		clients: make(map[string]pb.RaftServiceClient),
		conns:   make(map[string]*grpc.ClientConn),
	}

	for peerID, address := range peerAddresses {
		conn, err := grpc.NewClient(
			address,
			grpc.WithTransportCredentials(
				insecure.NewCredentials(),
			),
		)
		if err != nil {
			transport.Close()
			return nil, err
		}
		transport.conns[peerID] = conn
		transport.clients[peerID] = pb.NewRaftServiceClient(conn)
	}
	return transport, nil
}

func (transport *GRPCTransport) clientForPeer(
	peerID string,
) (pb.RaftServiceClient, error) {
	client, exists := transport.clients[peerID]
	if !exists {
		return nil, fmt.Errorf("unknown peer %s", peerID)
	}
	return client, nil
}
func (transport *GRPCTransport) SendRequestVote(
	ctx context.Context,
	peerID string,
	request *pb.RequestVoteRequest,
) (*pb.RequestVoteResponse, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	client, err := transport.clientForPeer(peerID)
	if err != nil {
		return nil, err
	}
	callCtx, cancel := context.WithTimeout(
		ctx,
		raftRPCTimeout,
	)
	defer cancel()
	return client.RequestVote(callCtx, request)
}
func (transport *GRPCTransport) SendAppendEntries(
	ctx context.Context,
	peerID string,
	request *pb.AppendEntriesRequest,
) (*pb.AppendEntriesResponse, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	client, err := transport.clientForPeer(peerID)
	if err != nil {
		return nil, err
	}
	callCtx, cancel := context.WithTimeout(
		ctx,
		raftRPCTimeout,
	)
	defer cancel()
	return client.AppendEntries(callCtx, request)
}

func (transport *GRPCTransport) SendInstallSnapshot(
	ctx context.Context,
	peerID string,
	request *pb.InstallSnapshotRequest,
) (*pb.InstallSnapshotResponse, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	client, err := transport.clientForPeer(peerID)
	if err != nil {
		return nil, err
	}
	callCtx, cancel := context.WithTimeout(
		ctx,
		snapshotRPCTimeout,
	)
	defer cancel()
	return client.InstallSnapshot(callCtx, request)
}

func (transport *GRPCTransport) SendPreVote(
	ctx context.Context,
	peerID string,
	request *pb.PreVoteRequest,
) (*pb.PreVoteResponse, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	client, err := transport.clientForPeer(peerID)
	if err != nil {
		return nil, err
	}
	callCtx, cancel := context.WithTimeout(
		ctx,
		raftRPCTimeout,
	)
	defer cancel()
	return client.PreVote(callCtx, request)
}

func (transport *GRPCTransport) Close() {
	for _, conn := range transport.conns {
		_ = conn.Close()
	}
}
