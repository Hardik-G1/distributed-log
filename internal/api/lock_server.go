package api

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"github.com/Hardik-G1/distributed-log/internal/raft"
	"github.com/Hardik-G1/distributed-log/internal/storage"
	"google.golang.org/protobuf/proto"
)

type LockServer struct {
	pb.UnimplementedLockServiceServer
	node  *raft.Node
	store *storage.PostgresStore
}

func NewLockServer(node *raft.Node, store *storage.PostgresStore) (*LockServer, error) {
	if node == nil {
		return nil, errors.New("raft node is nil")
	}
	if store == nil {
		return nil, errors.New("store is nil")
	}
	return &LockServer{
		node:  node,
		store: store,
	}, nil
}

func (ls *LockServer) AcquireLock(ctx context.Context, req *pb.AcquireLockRequest) (*pb.AcquireLockResponse, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	if req == nil {
		return nil, errors.New("request is empty")
	}
	if req.ClientId == "" {
		return nil, errors.New("client id is empty")
	}
	if req.RequestId == "" {
		return nil, errors.New("request id is empty")
	}
	if req.ResourceId == "" {
		return nil, errors.New("resource id is empty")
	}
	payload, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal acquire lock request %w", err)
	}
	_, err = ls.node.ProposeAndWait(
		ctx,
		int32(pb.Operation_OPERATION_ACQUIRE_LOCK),
		payload,
	)
	if err != nil {
		return nil, err
	}
	processed, err := ls.store.GetProcessedRequest(
		ctx,
		req.RequestId,
		req.ClientId,
	)
	if err != nil {
		return nil, fmt.Errorf("load processed acquire-lock response %w", err)
	}
	var response pb.AcquireLockResponse
	if err := proto.Unmarshal(
		processed.Response,
		&response,
	); err != nil {
		return nil, fmt.Errorf(
			"unmarshal acquire lock response %w",
			err,
		)
	}
	return &response, nil
}
func (ls *LockServer) ReleaseLock(ctx context.Context, req *pb.ReleaseLockRequest) (*pb.ReleaseLockResponse, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	if req == nil {
		return nil, errors.New("request is empty")
	}
	if req.ClientId == "" {
		return nil, errors.New("client id is empty")
	}
	if req.RequestId == "" {
		return nil, errors.New("request id is empty")
	}
	if req.ResourceId == "" {
		return nil, errors.New("resource id is empty")
	}
	if req.LockToken == "" {
		return nil, errors.New("lock token is empty")
	}
	payload, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal release lock request %w", err)
	}
	_, err = ls.node.ProposeAndWait(
		ctx,
		int32(pb.Operation_OPERATION_RELEASE_LOCK),
		payload,
	)
	if err != nil {
		return nil, err
	}
	processed, err := ls.store.GetProcessedRequest(
		ctx,
		req.RequestId,
		req.ClientId,
	)
	if err != nil {
		return nil, fmt.Errorf("load processed release-lock response %w", err)
	}
	var response pb.ReleaseLockResponse
	if err := proto.Unmarshal(
		processed.Response,
		&response,
	); err != nil {
		return nil, fmt.Errorf(
			"unmarshal release lock response %w",
			err,
		)
	}
	return &response, nil
}
