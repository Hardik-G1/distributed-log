package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"github.com/tidwall/wal"
	"google.golang.org/protobuf/proto"
)

const walSegmentSize = 64 << 20 //64 MiB

type WALRaftStore struct {
	mu        sync.RWMutex
	logs      *wal.Log
	metadata  *wal.Log
	snapshots *wal.Log
}

func OpenWALRaftStore(path string) (*WALRaftStore, error) {
	if path == "" {
		return nil, errors.New("raft WAL directory is empty")
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return nil, fmt.Errorf(
			"create raft WAL directory %w",
			err,
		)
	}
	options := &wal.Options{
		NoSync:           false,
		SegmentSize:      walSegmentSize,
		SegmentCacheSize: 8,
		NoCopy:           false,
		AllowEmpty:       true,
		DirPerms:         0o700,
		FilePerms:        0o600,
	}
	logs, err := wal.Open(
		filepath.Join(path, "entries"),
		options,
	)
	if err != nil {
		return nil, fmt.Errorf("open raft entry WAL %w", err)
	}
	metadata, err := wal.Open(filepath.Join(path, "metadata"), options)
	if err != nil {
		_ = logs.Close()
		return nil, fmt.Errorf("open raft metadata WAL %w", err)
	}
	snapshots, err := wal.Open(filepath.Join(path, "snapshots"), options)
	if err != nil {
		_ = metadata.Close()
		_ = logs.Close()
		return nil, fmt.Errorf("open raft snapshot WAL %w", err)
	}
	return &WALRaftStore{
		logs:      logs,
		metadata:  metadata,
		snapshots: snapshots,
	}, nil
}

func (store *WALRaftStore) Close() error {
	if store == nil {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	var result error
	for _, logStore := range []*wal.Log{
		store.snapshots,
		store.metadata,
		store.logs,
	} {
		if logStore == nil {
			continue
		}
		if err := logStore.Close(); err != nil && result == nil {
			result = err
		}
	}
	store.snapshots = nil
	store.metadata = nil
	store.logs = nil
	return result
}

func (store *WALRaftStore) validate(
	ctx context.Context,
) error {
	if err := checkRaftContext(ctx); err != nil {
		return err
	}
	if store == nil || store.logs == nil || store.metadata == nil || store.snapshots == nil {
		return errors.New("raft WAL store is not initialised")
	}
	return nil
}

func readLatestWAL(logStore *wal.Log) ([]byte, error) {
	last, err := logStore.LastIndex()
	if err != nil {
		return nil, err
	}
	if last == 0 {
		return nil, wal.ErrNotFound
	}
	return logStore.Read(last)
}

func appendControlRecord(
	logStore *wal.Log,
	data []byte,
) error {
	last, err := logStore.LastIndex()
	if err != nil {
		return err
	}
	return logStore.Write(last+1, data)
}

func (store *WALRaftStore) LoadRaftMetadata(
	ctx context.Context,
) (RaftMetadata, error) {
	if err := store.validate(ctx); err != nil {
		return RaftMetadata{}, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	data, err := readLatestWAL(store.metadata)
	if errors.Is(err, wal.ErrNotFound) {
		return RaftMetadata{}, nil
	}
	if err != nil {
		return RaftMetadata{}, err
	}
	var stored pb.StoredRaftMetadata
	if err := proto.Unmarshal(data, &stored); err != nil {
		return RaftMetadata{}, fmt.Errorf("decode raft metadata %w", err)
	}
	return RaftMetadata{CurrentTerm: stored.CurrentTerm, VotedFor: stored.VotedFor}, nil
}

func (store *WALRaftStore) SaveRaftMetadata(
	ctx context.Context,
	metadata RaftMetadata,
) error {
	if err := store.validate(ctx); err != nil {
		return err
	}
	data, err := proto.Marshal(&pb.StoredRaftMetadata{
		CurrentTerm: metadata.CurrentTerm,
		VotedFor:    metadata.VotedFor,
	})
	if err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := appendControlRecord(store.metadata, data); err != nil {
		return fmt.Errorf("append raft metadata %w", err)
	}
	return nil
}

func (store *WALRaftStore) AppendRaftLogs(
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
	last, err := store.logs.LastIndex()
	if err != nil {
		return err
	}
	expectedFirst := int64(last) + 1
	if entries[0].LogIndex != expectedFirst {
		return fmt.Errorf(
			"raft WAL append starts at %d, expected %d",
			entries[0].LogIndex,
			expectedFirst,
		)
	}
	var batch wal.Batch
	for offset, entry := range entries {
		expected := entries[0].LogIndex + int64(offset)
		if entry.LogIndex != expected {
			return fmt.Errorf("non contiguous raft WAL index %d expected %d", entry.LogIndex, expected)
		}

		encoded, err := encodeRaftLog(entry)
		if err != nil {
			return err
		}
		batch.Write(uint64(entry.LogIndex), encoded)
	}
	if err := store.logs.WriteBatch(&batch); err != nil {
		return fmt.Errorf("append raft WAL batch %w", err)
	}
	return nil
}
func (store *WALRaftStore) GetRaftLog(
	ctx context.Context,
	index int64,
) (RaftLog, error) {
	if err := store.validate(ctx); err != nil {
		return RaftLog{}, err
	}
	if index <= 0 {
		return RaftLog{}, ErrNotFound
	}
	store.mu.RLock()
	data, err := store.logs.Read(uint64(index))
	store.mu.RUnlock()
	if errors.Is(err, wal.ErrNotFound) {
		return RaftLog{}, ErrNotFound
	}
	if err != nil {
		return RaftLog{}, err
	}
	return decodeRaftLog(data)
}
func (store *WALRaftStore) GetRaftEntriesFromLimit(
	ctx context.Context,
	index int64,
	limit int,
) ([]RaftLog, error) {
	if err := store.validate(ctx); err != nil {
		return nil, err
	}
	if index < 1 {
		index = 1
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	first, err := store.logs.FirstIndex()
	if err != nil {
		return nil, err
	}
	last, err := store.logs.LastIndex()
	if err != nil {
		return nil, err
	}
	if last == 0 || uint64(index) > last {
		return []RaftLog{}, nil
	}
	start := uint64(index)
	if start < first {
		start = first
	}
	if start > last {
		return []RaftLog{}, nil
	}
	end := last
	if limit > 0 && start+uint64(limit)-1 < end {
		end = start + uint64(limit) - 1
	}
	entries := make([]RaftLog, 0, int(end-start+1))
	for current := start; current <= end; current++ {
		data, err := store.logs.Read(current)
		if err != nil {
			return nil, err
		}
		entry, err := decodeRaftLog(data)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (store *WALRaftStore) GetLastRaftLog(
	ctx context.Context,
) (RaftLog, error) {
	if err := store.validate(ctx); err != nil {
		return RaftLog{}, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	last, err := store.logs.LastIndex()
	if err != nil {
		return RaftLog{}, err
	}
	if last == 0 {
		return RaftLog{}, ErrNotFound
	}
	data, err := store.logs.Read(last)
	if errors.Is(err, wal.ErrNotFound) {
		return RaftLog{}, ErrNotFound
	}
	if err != nil {
		return RaftLog{}, err
	}
	return decodeRaftLog(data)
}

func (store *WALRaftStore) ReplaceRaftSuffix(
	ctx context.Context,
	fromIndex int64,
	entries []RaftLog,
) error {
	if err := store.validate(ctx); err != nil {
		return err
	}
	if fromIndex < 1 {
		return errors.New("raft suffix index must be positive")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	last, err := store.logs.LastIndex()
	if err != nil {
		return err
	}
	if last >= uint64(fromIndex) {
		if err := store.logs.TruncateBack(uint64(fromIndex - 1)); err != nil {
			return fmt.Errorf("truncate raft WAL suffix %w", err)
		}
	}
	if len(entries) == 0 {
		return nil
	}
	var batch wal.Batch

	for offset, entry := range entries {
		expected := fromIndex + int64(offset)
		if entry.LogIndex != expected {
			return fmt.Errorf("replacement raft WAL index %d expected %d", entry.LogIndex, expected)
		}
		encoded, err := encodeRaftLog(entry)
		if err != nil {
			return err
		}
		batch.Write(uint64(entry.LogIndex), encoded)
	}
	if err := store.logs.WriteBatch(&batch); err != nil {
		return fmt.Errorf("write replacement raaft WAL suffix %w", err)
	}
	return nil
}

func (store *WALRaftStore) DeleteRaftEntriesThrough(
	ctx context.Context,
	index int64,
) error {
	if err := store.validate(ctx); err != nil {
		return err
	}
	if index < 1 {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	first, err := store.logs.FirstIndex()
	if err != nil {
		return err
	}
	last, err := store.logs.LastIndex()
	if err != nil {
		return err
	}
	if last == 0 || uint64(index) < first {
		return nil
	}
	newFirst := uint64(index) + 1
	if newFirst > last+1 {
		newFirst = last + 1
	}
	if err := store.logs.TruncateFront(newFirst); err != nil {
		return fmt.Errorf("truncate raft WAL prefix %w", err)
	}
	return nil
}

func (store *WALRaftStore) SaveSnapshot(
	ctx context.Context,
	snapshot RaftSnapshot,
	data []byte,
) error {
	if err := store.validate(ctx); err != nil {
		return err
	}
	if snapshot.SnapshotID == "" {
		return errors.New("snapshot id is empty")
	}
	if snapshot.Location == "" {
		return errors.New("snapshot location is empty")
	}
	if snapshot.LastIncludedIndex < 0 {
		return errors.New("snapshot index is invalid")
	}
	if snapshot.LastIncludedTerm < 0 {
		return errors.New("snapshot term is invalid")
	}
	if err := writeSnapshotFile(snapshot.Location, data); err != nil {
		return err
	}
	if snapshot.CreatedAt.IsZero() {
		snapshot.CreatedAt = time.Now().UTC()
	}
	encoded, err := proto.Marshal(&pb.StoredRaftSnapshot{
		SnapshotId:        snapshot.SnapshotID,
		LastIncludedIndex: snapshot.LastIncludedIndex,
		LastIncludedTerm:  snapshot.LastIncludedTerm,
		Location:          snapshot.Location,
		CreatedAtUnixNano: snapshot.CreatedAt.UnixNano(),
	})
	if err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := appendControlRecord(
		store.snapshots,
		encoded,
	); err != nil {
		return fmt.Errorf("append raft snapshot metadata %w", err)
	}
	return nil
}

func (store *WALRaftStore) LoadLatestSnapshot(
	ctx context.Context,
) (RaftSnapshot, []byte, error) {
	if err := store.validate(ctx); err != nil {
		return RaftSnapshot{}, nil, err
	}
	store.mu.RLock()
	data, err := readLatestWAL(store.snapshots)
	store.mu.RUnlock()

	if errors.Is(err, wal.ErrNotFound) {
		return RaftSnapshot{}, nil, ErrNotFound
	}
	if err != nil {
		return RaftSnapshot{}, nil, err
	}
	var stored pb.StoredRaftSnapshot
	if err := proto.Unmarshal(
		data,
		&stored,
	); err != nil {
		return RaftSnapshot{}, nil, fmt.Errorf("decode snapshot metadata %w", err)
	}
	snapshot := RaftSnapshot{
		SnapshotID:        stored.SnapshotId,
		LastIncludedIndex: stored.LastIncludedIndex,
		LastIncludedTerm:  stored.LastIncludedTerm,
		Location:          stored.Location,
		CreatedAt:         time.Unix(0, stored.CreatedAtUnixNano).UTC(),
	}
	payload, err := os.ReadFile(snapshot.Location)
	if err != nil {
		return RaftSnapshot{}, nil, fmt.Errorf("read snapshot file %w", err)
	}
	return snapshot, payload, nil
}
