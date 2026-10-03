package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"github.com/Hardik-G1/distributed-log/internal/raft"
	"github.com/Hardik-G1/distributed-log/internal/storage"
	"github.com/jackc/pgx/v5"
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

	var response pb.AcquireLockResponse
	found, err := ls.loadProcessedResponse(
		ctx,
		req.ClientId,
		req.RequestId,
		&response,
	)
	if err != nil {
		return nil, fmt.Errorf("load processed acquire lock response %w", err)
	}
	if found {
		return &response, nil
	}
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return nil, fmt.Errorf("generate lock token %w", err)
	}
	lockToken := hex.EncodeToString(tokenBytes)
	observedAt := time.Now().Unix()
	expiry := observedAt + 30
	command := &pb.AcquireLockCommand{
		Request:    req,
		LockToken:  lockToken,
		Expiry:     expiry,
		ObservedAt: observedAt,
	}
	payload, err := proto.Marshal(command)
	if err != nil {
		return nil, fmt.Errorf("marshal acquire lock command %w", err)
	}

	_, err = ls.node.ProposeAndWaitWithKey(
		ctx,
		proposalKey(
			pb.Operation_OPERATION_ACQUIRE_LOCK,
			req.ClientId,
			req.RequestId,
		),
		int32(pb.Operation_OPERATION_ACQUIRE_LOCK),
		payload,
	)
	if err != nil {
		return nil, err
	}
	found, err = ls.loadProcessedResponse(
		ctx,
		req.ClientId,
		req.RequestId,
		&response,
	)
	if err != nil {
		return nil, fmt.Errorf("load processed acquire lock response %w", err)
	}
	if !found {
		return nil, errors.New("proposed acquire lock response is missing after apply")
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
	var response pb.ReleaseLockResponse
	found, err := ls.loadProcessedResponse(
		ctx,
		req.ClientId,
		req.RequestId,
		&response,
	)
	if err != nil {
		return nil, fmt.Errorf("load processed release lock response %w", err)
	}
	if found {
		return &response, nil
	}
	command := &pb.ReleaseLockCommand{
		Request:    req,
		ObservedAt: time.Now().Unix(),
	}
	payload, err := proto.Marshal(command)
	if err != nil {
		return nil, fmt.Errorf("marshal release lock request %w", err)
	}
	_, err = ls.node.ProposeAndWaitWithKey(
		ctx,
		proposalKey(
			pb.Operation_OPERATION_RELEASE_LOCK,
			req.ClientId,
			req.RequestId,
		),
		int32(pb.Operation_OPERATION_RELEASE_LOCK),
		payload,
	)
	if err != nil {
		return nil, err
	}
	found, err = ls.loadProcessedResponse(
		ctx,
		req.ClientId,
		req.RequestId,
		&response,
	)
	if err != nil {
		return nil, fmt.Errorf("load processed release lock response %w", err)
	}
	if !found {
		return nil, errors.New("proposed release lock response is missing after apply")
	}
	return &response, nil

}
func (ls *LockServer) AppendLog(ctx context.Context, req *pb.AppendLogRequest) (*pb.AppendLogResponse, error) {
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
	if req.Message == "" {
		return nil, errors.New("message is empty")
	}
	var response pb.AppendLogResponse
	found, err := ls.loadProcessedResponse(
		ctx,
		req.ClientId,
		req.RequestId,
		&response,
	)
	if err != nil {
		return nil, fmt.Errorf("load processed append log response %w", err)
	}
	if found {
		return &response, nil
	}
	command := &pb.AppendLogCommand{
		Request:    req,
		ObservedAt: time.Now().Unix(),
	}
	payload, err := proto.Marshal(command)
	if err != nil {
		return nil, fmt.Errorf("marshal append log command %w", err)
	}
	_, err = ls.node.ProposeAndWaitWithKey(
		ctx,
		proposalKey(
			pb.Operation_OPERATION_APPEND_LOG,
			req.ClientId,
			req.RequestId,
		),
		int32(pb.Operation_OPERATION_APPEND_LOG),
		payload,
	)
	if err != nil {
		return nil, err
	}
	found, err = ls.loadProcessedResponse(
		ctx,
		req.ClientId,
		req.RequestId,
		&response,
	)
	if err != nil {
		return nil, fmt.Errorf("load processed append log response %w", err)
	}
	if !found {
		return nil, errors.New("proposed append log response is missing after apply")
	}
	return &response, nil
}

func (ls *LockServer) GetLogData(
	ctx context.Context,
	req *pb.GetLogDataRequest,
) (*pb.GetLogDataResponse, error) {
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
	response := &pb.GetLogDataResponse{
		RequestId:  req.RequestId,
		ResourceId: req.ResourceId,
	}
	if req.LogPosition < 0 {
		response.Status = pb.GetStatus_GET_STATUS_INVALID_POSITION
		return response, nil
	}
	if req.Length < (-1) {
		response.Status = pb.GetStatus_GET_STATUS_INVALID_LENGTH
		return response, nil
	}
	readIndex, err := ls.node.ReadIndex(ctx)
	if err != nil {
		return nil, err
	}
	if err := ls.node.WaitForApplied(ctx, readIndex); err != nil {
		return nil, err
	}
	content, err := ls.store.ReadApplicationLog(
		ctx,
		req.ResourceId,
		req.LogPosition,
		req.Length,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		response.Status = pb.GetStatus_GET_STATUS_INVALID_RESOURCE
		return response, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read application log %w", err)
	}
	response.Content = content
	response.Status = pb.GetStatus_GET_STATUS_FETCHED
	return response, nil
}

func proposalKey(
	operation pb.Operation,
	clientID string,
	requestID string,
) string {
	return fmt.Sprintf(
		"%d:%s:%s",
		operation,
		clientID,
		requestID,
	)
}

func (ls *LockServer) loadProcessedResponse(
	ctx context.Context,
	clientID string,
	requestID string,
	response proto.Message,
) (bool, error) {
	processed, err := ls.store.GetProcessedRequest(
		ctx,
		clientID,
		requestID,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := proto.Unmarshal(
		processed.Response,
		response,
	); err != nil {
		return false, err
	}
	return true, nil
}
