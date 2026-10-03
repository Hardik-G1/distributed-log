package api

import (
	"context"
	"errors"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"github.com/Hardik-G1/distributed-log/internal/raft"
)

type RaftServer struct {
	pb.UnimplementedRaftServiceServer
	node *raft.Node
}

func NewRaftServer(node *raft.Node) (*RaftServer, error) {
	if node == nil {
		return nil, errors.New("raft node is nil")
	}
	return &RaftServer{
		node: node,
	}, nil
}

func (server *RaftServer) PreVote(
	ctx context.Context,
	req *pb.PreVoteRequest,
) (*pb.PreVoteResponse, error) {
	return server.node.HandlePreVote(ctx, req)
}

func (server *RaftServer) RequestVote(
	ctx context.Context,
	req *pb.RequestVoteRequest,
) (*pb.RequestVoteResponse, error) {
	return server.node.HandleRequestVote(ctx, req)
}

func (server *RaftServer) AppendEntries(
	ctx context.Context,
	req *pb.AppendEntriesRequest,
) (*pb.AppendEntriesResponse, error) {
	return server.node.HandleAppendEntries(ctx, req)
}

func (server *RaftServer) InstallSnapshot(
	ctx context.Context,
	req *pb.InstallSnapshotRequest,
) (*pb.InstallSnapshotResponse, error) {
	return server.node.HandleInstallSnapshot(ctx, req)
}
