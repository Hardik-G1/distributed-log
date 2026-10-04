package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"google.golang.org/protobuf/proto"
)

const memoryResourceManifest = "resources.pb"

type MemoryApplicationStore struct {
	mu          sync.RWMutex
	directory   string
	closed      bool
	locks       map[string]CurrentLock
	requests    map[string]ProcessedRequest
	resources   map[string]ResourceLocation
	logs        map[string][]string
	lastApplied int64
}

func OpenMemoryApplicationStore(path string) (*MemoryApplicationStore, error) {
	if path == "" {
		return nil, errors.New("memory application directory is empty")
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return nil, fmt.Errorf("create memory application directory %w", err)
	}
	store := &MemoryApplicationStore{
		directory: path,
		locks:     make(map[string]CurrentLock),
		requests:  make(map[string]ProcessedRequest),
		resources: make(map[string]ResourceLocation),
		logs:      make(map[string][]string),
	}
	data, err := os.ReadFile(
		filepath.Join(path, memoryResourceManifest),
	)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, err
	}
	var manifest pb.MemoryApplicationStateSnapshot
	if err := proto.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("decode resource manifest %w", err)
	}
	for _, resource := range manifest.ResourceLocations {
		if resource == nil {
			continue
		}
		store.resources[resource.ResourceId] = ResourceLocation{
			ResourceID: resource.ResourceId,
			Location:   resource.Location,
		}
	}
	return store, nil
}

func (store *MemoryApplicationStore) validate(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if store == nil {
		return errors.New("memory application store is not initialised")
	}
	store.mu.RLock()
	closed := store.closed
	store.mu.RUnlock()
	if closed {
		return errors.New("memory application is closed")
	}
	return nil
}
func (store *MemoryApplicationStore) Close() error {
	if store == nil {
		return nil
	}
	store.mu.Lock()
	store.closed = true
	store.mu.Unlock()

	return nil
}

func memoryRequestKey(clientID string, requestID string) string {
	return clientID + "\x00" + requestID
}

func cloneProcessedRequest(request ProcessedRequest) ProcessedRequest {
	request.Response = append([]byte(nil), request.Response...)
	return request
}

func (store *MemoryApplicationStore) GetProcessedRequest(
	ctx context.Context,
	clientID string,
	requestID string,
) (ProcessedRequest, error) {
	if err := store.validate(ctx); err != nil {
		return ProcessedRequest{}, err
	}
	store.mu.RLock()
	request, found := store.requests[memoryRequestKey(clientID, requestID)]
	store.mu.RUnlock()
	if !found {
		return ProcessedRequest{}, ErrNotFound
	}
	return cloneProcessedRequest(request), nil
}
func (store *MemoryApplicationStore) GetResourceLocation(
	ctx context.Context,
	resourceID string,
) (ResourceLocation, error) {
	if err := store.validate(ctx); err != nil {
		return ResourceLocation{}, err
	}
	store.mu.RLock()
	resource, found := store.resources[resourceID]
	store.mu.RUnlock()
	if !found {
		return ResourceLocation{}, ErrNotFound
	}
	return resource, nil
}

func (store *MemoryApplicationStore) SaveResourceLocations(
	ctx context.Context,
	resources []ResourceLocation,
) error {
	if err := store.validate(ctx); err != nil {
		return err
	}
	store.mu.Lock()
	for _, resource := range resources {
		store.resources[resource.ResourceID] = resource
	}
	manifest := &pb.MemoryApplicationStateSnapshot{
		Version: 1,
	}
	resourceIDs := make(
		[]string,
		0,
		len(store.resources),
	)
	for resourceID := range store.resources {
		resourceIDs = append(resourceIDs, resourceID)
	}
	sort.Strings(resourceIDs)
	for _, resourceID := range resourceIDs {
		resource := store.resources[resourceID]
		manifest.ResourceLocations = append(manifest.ResourceLocations, &pb.StoredResourceLocation{
			ResourceId: resource.ResourceID,
			Location:   resource.Location,
		})
	}
	store.mu.Unlock()
	data, err := proto.Marshal(manifest)
	if err != nil {
		return err
	}
	return writeSnapshotFile(filepath.Join(store.directory, memoryResourceManifest), data)

}

func (store *MemoryApplicationStore) ApplyAcquireLock(
	ctx context.Context,
	req *pb.AcquireLockRequest,
	lockToken string,
	expiry int64,
	observedAt int64,
) error {
	if err := store.validate(ctx); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.applyAcquireLock(
		req,
		lockToken,
		expiry,
		observedAt,
	)
}

func (store *MemoryApplicationStore) applyAcquireLock(
	req *pb.AcquireLockRequest,
	lockToken string,
	expiry int64,
	observedAt int64,
) error {
	key := memoryRequestKey(req.ClientId, req.RequestId)
	if _, found := store.requests[key]; found {
		return nil
	}
	response := &pb.AcquireLockResponse{
		ResourceId: req.ResourceId,
		RequestId:  req.RequestId,
	}
	if _, found := store.resources[req.ResourceId]; !found {
		response.LockStatus = pb.LockStatus_LOCK_STATUS_INVALID_REQUEST
	} else if current, found := store.locks[req.ResourceId]; found && current.Expiry > observedAt {
		response.LockStatus = pb.LockStatus_LOCK_STATUS_BUSY
	} else {
		store.locks[req.ResourceId] = CurrentLock{
			ResourceID: req.ResourceId,
			OwnerID:    req.ClientId,
			RequestID:  req.RequestId,
			LockToken:  lockToken,
			Expiry:     expiry,
		}
		response.LockStatus = pb.LockStatus_LOCK_STATUS_APPROVED
		response.LockToken = lockToken
		response.Expiry = expiry
	}
	return store.saveResponse(
		key,
		req.ClientId,
		req.RequestId,
		req.ResourceId,
		response,
		observedAt,
	)
}

func (store *MemoryApplicationStore) ApplyReleaseLock(
	ctx context.Context,
	req *pb.ReleaseLockRequest,
	observedAt int64,
) error {
	if err := store.validate(ctx); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.applyReleaseLock(req, observedAt)
}
func (store *MemoryApplicationStore) applyReleaseLock(
	req *pb.ReleaseLockRequest,
	observedAt int64,
) error {
	key := memoryRequestKey(req.ClientId, req.RequestId)
	if _, found := store.requests[key]; found {
		return nil
	}
	response := &pb.ReleaseLockResponse{
		ResourceId: req.ResourceId,
		RequestId:  req.RequestId,
	}
	current, lockFound := store.locks[req.ResourceId]
	switch {
	case store.resources[req.ResourceId].ResourceID == "":
		response.ReleaseStatus = pb.ReleaseStatus_RELEASE_STATUS_INVALID_REQUEST
	case !lockFound || current.OwnerID != req.ClientId || current.LockToken != req.LockToken || current.Expiry <= observedAt:
		response.ReleaseStatus = pb.ReleaseStatus_RELEASE_STATUS_STALE_TOKEN
	default:
		delete(store.locks, req.ResourceId)
		response.ReleaseStatus = pb.ReleaseStatus_RELEASE_STATUS_RELEASED

	}
	return store.saveResponse(
		key,
		req.ClientId,
		req.RequestId,
		req.ResourceId,
		response,
		observedAt,
	)
}

func (store *MemoryApplicationStore) ApplyAppendLog(
	ctx context.Context,
	req *pb.AppendLogRequest,
	observedAt int64,
) error {
	if err := store.validate(ctx); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.applyAppendLog(req, observedAt)
}
func (store *MemoryApplicationStore) applyAppendLog(
	req *pb.AppendLogRequest,
	observedAt int64,
) error {
	key := memoryRequestKey(req.ClientId, req.RequestId)
	if _, found := store.requests[key]; found {
		return nil
	}
	response := &pb.AppendLogResponse{
		ResourceId: req.ResourceId,
		RequestId:  req.RequestId,
	}
	current, lockFound := store.locks[req.ResourceId]
	switch {
	case store.resources[req.ResourceId].ResourceID == "":
		response.AppendStatus = pb.AppendStatus_APPEND_STATUS_INVALID_REQUEST
	case !lockFound || current.Expiry <= observedAt:
		response.AppendStatus = pb.AppendStatus_APPEND_STATUS_EXPIRED_LOCK
	case current.OwnerID != req.ClientId || current.LockToken != req.LockToken:
		response.AppendStatus = pb.AppendStatus_APPEND_STATUS_STALE_TOKEN
	default:
		store.logs[req.ResourceId] = append(store.logs[req.ResourceId], req.Message)
		response.AppendStatus = pb.AppendStatus_APPEND_STATUS_COMMITTED

	}
	return store.saveResponse(
		key,
		req.ClientId,
		req.RequestId,
		req.ResourceId,
		response,
		observedAt,
	)
}

func (store *MemoryApplicationStore) saveResponse(
	key string,
	clientID string,
	requestID string,
	resourceID string,
	response proto.Message,
	observedAt int64,
) error {
	data, err := proto.Marshal(response)
	if err != nil {
		return err
	}
	store.requests[key] = ProcessedRequest{
		ClientID:   clientID,
		RequestID:  requestID,
		ResourceID: resourceID,
		Response:   data,
		CreatedAt:  time.Unix(observedAt, 0).UTC(),
	}
	return nil
}

func (store *MemoryApplicationStore) ApplyRaftBatch(
	ctx context.Context,
	entries []RaftLog,
) error {
	if err := store.validate(ctx); err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()

	for _, entry := range entries {
		switch pb.Operation(entry.OperationType) {
		case pb.Operation_OPERATION_ACQUIRE_LOCK:
			var command pb.AcquireLockCommand
			if err := proto.Unmarshal(entry.OperationPayload, &command); err != nil {
				return err
			}
			if command.Request == nil {
				return errors.New("acquire request is nil")
			}
			if err := store.applyAcquireLock(command.Request, command.LockToken, command.Expiry, command.ObservedAt); err != nil {
				return err
			}
		case pb.Operation_OPERATION_RELEASE_LOCK:
			var command pb.ReleaseLockCommand
			if err := proto.Unmarshal(entry.OperationPayload, &command); err != nil {
				return err
			}
			if command.Request == nil {
				return errors.New("release request is nil")
			}
			if err := store.applyReleaseLock(command.Request, command.ObservedAt); err != nil {
				return err
			}
		case pb.Operation_OPERATION_APPEND_LOG:
			var command pb.AppendLogCommand
			if err := proto.Unmarshal(entry.OperationPayload, &command); err != nil {
				return err
			}
			if command.Request == nil {
				return errors.New("append request is nil")
			}
			if err := store.applyAppendLog(command.Request, command.ObservedAt); err != nil {
				return err
			}
		case pb.Operation_OPERATION_NOOP:

		default:
			return fmt.Errorf("unsupported raft operation %d", entry.OperationType)
		}
		store.lastApplied = entry.LogIndex

	}
	return nil

}

func (store *MemoryApplicationStore) SyncApplicationState(
	ctx context.Context,
) error {
	return store.validate(ctx)
}

func (store *MemoryApplicationStore) LastAppliedIndex(
	ctx context.Context,
) (int64, error) {
	if err := store.validate(ctx); err != nil {
		return 0, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()

	return store.lastApplied, nil
}

func (store *MemoryApplicationStore) ReadApplicationLog(
	ctx context.Context,
	resourceID string,
	position int64,
	length int64,
) (string, error) {
	if err := store.validate(ctx); err != nil {
		return "", err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	if _, found := store.resources[resourceID]; !found {
		return "", ErrNotFound
	}

	messages := store.logs[resourceID]
	if position < 0 || position > int64(len(messages)) {
		return "", errors.New("position is beyond the application log")
	}
	if position == int64(len(messages)) {
		return "", nil
	}
	end := int64(len(messages))
	if length == 0 {
		end = position + 1
	} else if length > 0 && position+length < end {
		end = position + length
	}
	return strings.Join(messages[position:end], "\n"), nil
}

func (store *MemoryApplicationStore) SnapshotApplicationState(
	ctx context.Context,
) ([]byte, error) {
	if err := store.validate(ctx); err != nil {
		return nil, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	snapshot := &pb.MemoryApplicationStateSnapshot{
		Version:          1,
		LastAppliedIndex: store.lastApplied,
	}
	for _, current := range store.locks {
		snapshot.CurrentLocks = append(snapshot.CurrentLocks,
			&pb.StoredCurrentLock{
				ResourceId: current.ResourceID,
				OwnerId:    current.OwnerID,
				RequestId:  current.RequestID,
				LockToken:  current.LockToken,
				Expiry:     current.Expiry,
			},
		)
	}
	for _, request := range store.requests {
		snapshot.ProcessedRequests = append(snapshot.ProcessedRequests,
			&pb.StoredProcessedRequest{
				ClientId:          request.ClientID,
				RequestId:         request.RequestID,
				ResourceId:        request.ResourceID,
				Response:          append([]byte(nil), request.Response...),
				CreatedAtUnixNano: request.CreatedAt.UnixNano(),
			},
		)
	}
	for _, resource := range store.resources {
		snapshot.ResourceLocations = append(snapshot.ResourceLocations,
			&pb.StoredResourceLocation{
				ResourceId: resource.ResourceID,
				Location:   resource.Location,
			},
		)
	}
	for resourceID, messages := range store.logs {
		snapshot.Logs = append(snapshot.Logs,
			&pb.StoredApplicationLog{
				ResourceId: resourceID,
				Messages:   append([]string(nil), messages...),
			},
		)
	}
	return proto.Marshal(snapshot)
}

func (store *MemoryApplicationStore) RestoreApplicationState(
	ctx context.Context,
	data []byte,
) error {
	if err := store.validate(ctx); err != nil {
		return err
	}
	var snapshot pb.MemoryApplicationStateSnapshot
	if err := proto.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("decode memory application snapshot %w", err)
	}
	if snapshot.Version != 1 {
		return fmt.Errorf("unsupported memory application snapshot version %d", snapshot.Version)
	}
	store.mu.Lock()
	defer store.mu.Unlock()

	store.locks = make(map[string]CurrentLock, len(snapshot.CurrentLocks))
	store.requests = make(map[string]ProcessedRequest, len(snapshot.ProcessedRequests))
	store.resources = make(map[string]ResourceLocation, len(snapshot.ResourceLocations))
	store.logs = make(map[string][]string, len(snapshot.Logs))
	for _, current := range snapshot.CurrentLocks {
		if current == nil {
			continue
		}
		store.locks[current.ResourceId] = CurrentLock{
			ResourceID: current.ResourceId,
			OwnerID:    current.OwnerId,
			RequestID:  current.RequestId,
			LockToken:  current.LockToken,
			Expiry:     current.Expiry,
		}
	}

	for _, request := range snapshot.ProcessedRequests {
		if request == nil {
			continue
		}
		key := memoryRequestKey(request.ClientId, request.RequestId)

		store.requests[key] = ProcessedRequest{
			ClientID:   request.ClientId,
			RequestID:  request.RequestId,
			ResourceID: request.ResourceId,
			Response:   append([]byte(nil), request.Response...),
			CreatedAt:  time.Unix(0, request.CreatedAtUnixNano).UTC(),
		}

	}
	for _, resource := range snapshot.ResourceLocations {
		if resource == nil {
			continue
		}
		store.resources[resource.ResourceId] = ResourceLocation{
			ResourceID: resource.ResourceId,
			Location:   resource.Location,
		}
	}
	for _, applicationLog := range snapshot.Logs {
		if applicationLog == nil {
			continue
		}
		store.logs[applicationLog.ResourceId] = append([]string(nil), applicationLog.Messages...)
	}

	store.lastApplied = snapshot.LastAppliedIndex
	return nil

}
