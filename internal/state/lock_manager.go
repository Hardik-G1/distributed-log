package state

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"github.com/Hardik-G1/distributed-log/internal/storage"
	"google.golang.org/protobuf/proto"
)

type LockStore interface {
	ApplyAcquireLock(ctx context.Context, req *pb.AcquireLockRequest) error
	SnapshotApplicationState(ctx context.Context) ([]byte, error)
	RestoreApplicationState(ctx context.Context, data []byte) error
	ApplyReleaseLock(ctx context.Context, req *pb.ReleaseLockRequest) error
}

type LockManager struct {
	store LockStore
}

func NewLockManager(store LockStore) (*LockManager, error) {
	if store == nil {
		return nil, errors.New("lock store is nil")
	}
	return &LockManager{
		store: store,
	}, nil
}

func (manager *LockManager) Apply(
	ctx context.Context,
	entry storage.RaftLog,
) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	switch pb.Operation(entry.OperationType) {
	case pb.Operation_OPERATION_ACQUIRE_LOCK:
		return manager.applyAcquireLock(ctx, entry.OperationPayload)
	case pb.Operation_OPERATION_RELEASE_LOCK:
		return manager.applyReleaseLock(ctx, entry.OperationPayload)
	case pb.Operation_OPERATION_APPEND_LOG:
		return errors.New("append log is not implemented")
	default:
		return fmt.Errorf(
			"unsupported raft operation %d",
			entry.OperationType,
		)
	}
}
func (manager *LockManager) applyAcquireLock(
	ctx context.Context,
	payload []byte,
) error {
	var request pb.AcquireLockRequest

	if err := proto.Unmarshal(payload, &request); err != nil {
		return fmt.Errorf("decode acquire lock request %w", err)
	}

	if request.ClientId == "" {
		return errors.New("client id is empty")
	}
	if request.RequestId == "" {
		return errors.New("request id is empty")
	}
	if request.ResourceId == "" {
		return errors.New("resource id is empty")
	}

	return manager.store.ApplyAcquireLock(ctx, &request)

}
func (manager *LockManager) applyReleaseLock(
	ctx context.Context,
	payload []byte,
) error {
	var request pb.ReleaseLockRequest

	if err := proto.Unmarshal(payload, &request); err != nil {
		return fmt.Errorf("decode release lock request %w", err)
	}

	if request.ClientId == "" {
		return errors.New("client id is empty")
	}
	if request.RequestId == "" {
		return errors.New("request id is empty")
	}
	if request.ResourceId == "" {
		return errors.New("resource id is empty")
	}
	if request.LockToken == "" {
		return errors.New("lock token is empty")
	}

	return manager.store.ApplyReleaseLock(ctx, &request)

}
func (manager *LockManager) Snapshot(
	ctx context.Context,
) ([]byte, error) {
	return manager.store.SnapshotApplicationState(ctx)
}
func (manager *LockManager) Restore(
	ctx context.Context,
	data []byte,
) error {
	return manager.store.RestoreApplicationState(ctx, data)
}
