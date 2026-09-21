package storage

import "time"

type CurrentLock struct {
	ResourceID string
	OwnerID    string
	RequestID  string
	LockToken  string
	Expiry     int64
}

type ProcessedRequest struct {
	ClientID   string
	RequestID  string
	ResourceID string
	Response   []byte
	CreatedAt  time.Time
}

type ResourceLocation struct {
	ResourceID string
	Location   string
}

type RaftLog struct {
	LogIndex         int64
	Term             int64
	OperationType    int32
	OperationPayload []byte
}

type RaftMetadata struct {
	CurrentTerm int64
	VotedFor    string
}

type RaftSnapshot struct {
	SnapshotID        string
	LastIncludedIndex int64
	LastIncludedTerm  int64
	Location          string
	CreatedAt         time.Time
}
