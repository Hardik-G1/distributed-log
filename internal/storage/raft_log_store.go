package storage

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"google.golang.org/protobuf/proto"
)

type RaftLogStore interface {
	LoadRaftMetadata(context.Context) (RaftMetadata, error)
	SaveRaftMetadata(context.Context, RaftMetadata) error
	AppendRaftLogs(context.Context, []RaftLog) error
	GetRaftLog(context.Context, int64) (RaftLog, error)
	GetRaftEntriesFromLimit(context.Context, int64, int) ([]RaftLog, error)
	GetLastRaftLog(context.Context) (RaftLog, error)
	ReplaceRaftSuffix(context.Context, int64, []RaftLog) error
	DeleteRaftEntriesThrough(context.Context, int64) error
	SaveSnapshot(context.Context, RaftSnapshot, []byte) error
	LoadLatestSnapshot(context.Context) (RaftSnapshot, []byte, error)
}

func checkRaftContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	return ctx.Err()
}

func encodeRaftLog(entry RaftLog) ([]byte, error) {
	stored := &pb.StoredRaftLog{
		LogIndex:         entry.LogIndex,
		Term:             entry.Term,
		OperationType:    pb.Operation(entry.OperationType),
		OperationPayload: entry.OperationPayload,
	}
	data, err := proto.Marshal(stored)
	if err != nil {
		return nil, fmt.Errorf("encode raft log %w", err)
	}
	return data, nil
}

func decodeRaftLog(data []byte) (RaftLog, error) {
	var stored pb.StoredRaftLog
	if err := proto.Unmarshal(data, &stored); err != nil {
		return RaftLog{}, fmt.Errorf("decode raft log %w", err)
	}
	return RaftLog{
		LogIndex:         stored.LogIndex,
		Term:             stored.Term,
		OperationType:    int32(stored.OperationType),
		OperationPayload: append([]byte(nil), stored.OperationPayload...),
	}, nil
}
