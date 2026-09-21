package raft

import (
	"context"
	"fmt"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type PeerTransport interface {
	SendRequestVote(ctx context.Context, peerID string, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	SendAppendEntries(ctx context.Context, peerID string, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
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
	client, err := transport.clientForPeer(peerID)
	if err != nil {
		return nil, err
	}
	return client.RequestVote(ctx, request)
}
func (transport *GRPCTransport) SendAppendEntries(
	ctx context.Context,
	peerID string,
	request *pb.AppendEntriesRequest,
) (*pb.AppendEntriesResponse, error) {
	client, err := transport.clientForPeer(peerID)
	if err != nil {
		return nil, err
	}
	return client.AppendEntries(ctx, request)
}
func (transport *GRPCTransport) Close() {
	for _, conn := range transport.conns {
		_ = conn.Close()
	}
}
