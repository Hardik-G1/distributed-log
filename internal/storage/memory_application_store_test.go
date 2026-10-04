package storage

import (
	"context"
	"testing"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"google.golang.org/protobuf/proto"
)

func TestMemoryApplicationStoreLifecycleAndSnapshot(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	store, err := OpenMemoryApplicationStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	resource := ResourceLocation{ResourceID: "resource-1", Location: "unused"}
	if err := store.SaveResourceLocations(ctx, []ResourceLocation{resource}); err != nil {
		t.Fatal(err)
	}

	acquire := &pb.AcquireLockRequest{ClientId: "client-1", RequestId: "acquire-1", ResourceId: resource.ResourceID}
	if err := store.ApplyAcquireLock(ctx, acquire, "token-1", 200, 100); err != nil {
		t.Fatal(err)
	}
	processed, err := store.GetProcessedRequest(ctx, acquire.ClientId, acquire.RequestId)
	if err != nil {
		t.Fatal(err)
	}
	var acquireResponse pb.AcquireLockResponse
	if err := proto.Unmarshal(processed.Response, &acquireResponse); err != nil {
		t.Fatal(err)
	}
	if acquireResponse.LockStatus != pb.LockStatus_LOCK_STATUS_APPROVED {
		t.Fatalf("unexpected acquire status %s", acquireResponse.LockStatus)
	}

	entries := make([]RaftLog, 0, 2)
	for index, message := range []string{"one", "two"} {
		request := &pb.AppendLogRequest{
			ClientId: "client-1", RequestId: "append-" + message,
			ResourceId: resource.ResourceID, LockToken: "token-1", Message: message,
		}
		payload, err := proto.Marshal(&pb.AppendLogCommand{Request: request, ObservedAt: 101 + int64(index)})
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, RaftLog{LogIndex: int64(index + 1), Term: 1, OperationType: int32(pb.Operation_OPERATION_APPEND_LOG), OperationPayload: payload})
	}
	if err := store.ApplyRaftBatch(ctx, entries); err != nil {
		t.Fatal(err)
	}
	content, err := store.ReadApplicationLog(ctx, resource.ResourceID, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if content != "one\ntwo" {
		t.Fatalf("unexpected content %q", content)
	}

	snapshot, err := store.SnapshotApplicationState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := OpenMemoryApplicationStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if err := restored.RestoreApplicationState(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	restoredContent, err := restored.ReadApplicationLog(ctx, resource.ResourceID, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if restoredContent != content {
		t.Fatalf("restored content %q does not match %q", restoredContent, content)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenMemoryApplicationStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.GetResourceLocation(ctx, resource.ResourceID); err != nil {
		t.Fatalf("resource manifest was not persisted: %v", err)
	}
}

func TestMemoryApplicationStoreRejectsBusyAndStaleOperations(t *testing.T) {
	ctx := context.Background()
	store, err := OpenMemoryApplicationStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.SaveResourceLocations(ctx, []ResourceLocation{{
		ResourceID: "resource-1",
		Location:   "memory://resource-1",
	}}); err != nil {
		t.Fatal(err)
	}

	first := &pb.AcquireLockRequest{
		ClientId: "client-1", RequestId: "acquire-1", ResourceId: "resource-1",
	}
	if err := store.ApplyAcquireLock(ctx, first, "token-1", 200, 100); err != nil {
		t.Fatal(err)
	}
	second := &pb.AcquireLockRequest{
		ClientId: "client-2", RequestId: "acquire-2", ResourceId: "resource-1",
	}
	if err := store.ApplyAcquireLock(ctx, second, "token-2", 200, 101); err != nil {
		t.Fatal(err)
	}
	processed, err := store.GetProcessedRequest(ctx, "client-2", "acquire-2")
	if err != nil {
		t.Fatal(err)
	}
	var acquireResponse pb.AcquireLockResponse
	if err := proto.Unmarshal(processed.Response, &acquireResponse); err != nil {
		t.Fatal(err)
	}
	if acquireResponse.LockStatus != pb.LockStatus_LOCK_STATUS_BUSY {
		t.Fatalf("status=%s, want BUSY", acquireResponse.LockStatus)
	}

	appendRequest := &pb.AppendLogRequest{
		ClientId: "client-1", RequestId: "append-stale", ResourceId: "resource-1",
		LockToken: "wrong-token", Message: "must-not-append",
	}
	if err := store.ApplyAppendLog(ctx, appendRequest, 102); err != nil {
		t.Fatal(err)
	}
	processed, err = store.GetProcessedRequest(ctx, "client-1", "append-stale")
	if err != nil {
		t.Fatal(err)
	}
	var appendResponse pb.AppendLogResponse
	if err := proto.Unmarshal(processed.Response, &appendResponse); err != nil {
		t.Fatal(err)
	}
	if appendResponse.AppendStatus != pb.AppendStatus_APPEND_STATUS_STALE_TOKEN {
		t.Fatalf("status=%s, want STALE_TOKEN", appendResponse.AppendStatus)
	}

	expiredRequest := &pb.AppendLogRequest{
		ClientId: "client-1", RequestId: "append-expired", ResourceId: "resource-1",
		LockToken: "token-1", Message: "must-not-append",
	}
	if err := store.ApplyAppendLog(ctx, expiredRequest, 200); err != nil {
		t.Fatal(err)
	}
	processed, err = store.GetProcessedRequest(ctx, "client-1", "append-expired")
	if err != nil {
		t.Fatal(err)
	}
	appendResponse.Reset()
	if err := proto.Unmarshal(processed.Response, &appendResponse); err != nil {
		t.Fatal(err)
	}
	if appendResponse.AppendStatus != pb.AppendStatus_APPEND_STATUS_EXPIRED_LOCK {
		t.Fatalf("status=%s, want EXPIRED_LOCK", appendResponse.AppendStatus)
	}
}
