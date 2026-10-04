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
	ApplyAcquireLock(ctx context.Context, req *pb.AcquireLockRequest, lockToken string, expiry int64, observedAt int64) error
	ApplyReleaseLock(ctx context.Context, req *pb.ReleaseLockRequest, observedAt int64) error
	ApplyAppendLog(ctx context.Context, req *pb.AppendLogRequest, observedAt int64) error
	SnapshotApplicationState(ctx context.Context) ([]byte, error)
	RestoreApplicationState(ctx context.Context, data []byte) error
	SyncApplicationState(context.Context) error
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
	if err := manager.applyEntry(ctx, entry); err != nil {
		return err
	}
	return manager.store.SyncApplicationState(ctx)
}

func (manager *LockManager) ApplyBatch(
	ctx context.Context,
	entries []storage.RaftLog,
) error {
	if batchStore, ok := manager.store.(interface {
		ApplyRaftBatch(context.Context, []storage.RaftLog) error
	}); ok {
		return batchStore.ApplyRaftBatch(ctx, entries)
	}
	for _, entry := range entries {
		if err := manager.applyEntry(ctx, entry); err != nil {
			return err
		}
	}
	return manager.store.SyncApplicationState(ctx)
}
func (manager *LockManager) applyEntry(
	ctx context.Context,
	entry storage.RaftLog,
) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	switch pb.Operation(entry.OperationType) {
	case pb.Operation_OPERATION_NOOP:
		return nil
	case pb.Operation_OPERATION_ACQUIRE_LOCK:
		return manager.applyAcquireLock(ctx, entry.OperationPayload)
	case pb.Operation_OPERATION_RELEASE_LOCK:
		return manager.applyReleaseLock(ctx, entry.OperationPayload)
	case pb.Operation_OPERATION_APPEND_LOG:
		return manager.applyAppendLog(ctx, entry.OperationPayload)
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
	var command pb.AcquireLockCommand

	if err := proto.Unmarshal(payload, &command); err != nil {
		return fmt.Errorf("decode acquire lock command %w", err)
	}
	request := command.GetRequest()
	if request == nil {
		return errors.New("acquire lock request is empty")
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
	if command.LockToken == "" {
		return errors.New("lock token is empty")
	}
	if command.Expiry <= command.ObservedAt {
		return errors.New("lock expiry is invalid")
	}
	return manager.store.ApplyAcquireLock(ctx, request, command.LockToken, command.Expiry, command.ObservedAt)

}
func (manager *LockManager) applyReleaseLock(
	ctx context.Context,
	payload []byte,
) error {
	var command pb.ReleaseLockCommand

	if err := proto.Unmarshal(payload, &command); err != nil {
		return fmt.Errorf("decode release lock command %w", err)
	}
	request := command.GetRequest()
	if request == nil {
		return errors.New("release lock request is empty")
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
	if command.ObservedAt <= 0 {
		return errors.New("observed time is invalid")
	}

	return manager.store.ApplyReleaseLock(ctx, request, command.ObservedAt)

}

func (manager *LockManager) applyAppendLog(
	ctx context.Context,
	payload []byte,
) error {
	var command pb.AppendLogCommand

	if err := proto.Unmarshal(payload, &command); err != nil {
		return fmt.Errorf("decode append log command %w", err)
	}
	request := command.GetRequest()
	if request == nil {
		return errors.New("append log request is empty")
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
	if request.Message == "" {
		return errors.New("message is empty")
	}
	if request.LockToken == "" {
		return errors.New("lock token is empty")
	}
	if command.ObservedAt <= 0 {
		return errors.New("observed time is invalid")
	}
	return manager.store.ApplyAppendLog(ctx, request, command.ObservedAt)

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
func (manager *LockManager) LastAppliedIndex(
	ctx context.Context,
) (int64, error) {
	provider, ok := manager.store.(interface {
		LastAppliedIndex(context.Context) (int64, error)
	})
	if !ok {
		return 0, nil
	}
	return provider.LastAppliedIndex(ctx)
}
