package raft

import (
	"errors"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"github.com/Hardik-G1/distributed-log/internal/storage"
)

func toProtoLogEntry(entry storage.RaftLog) *pb.LogEntry {
	return &pb.LogEntry{
		Term:             entry.Term,
		OperationType:    pb.Operation(entry.OperationType),
		OperationPayload: string(entry.OperationPayload),
	}
}

func fromPrototoLogEntry(index int64, entry *pb.LogEntry) (storage.RaftLog, error) {
	if entry == nil {
		return storage.RaftLog{}, errors.New("log is nil")
	}
	return storage.RaftLog{
		LogIndex:         index,
		Term:             entry.Term,
		OperationType:    int32(entry.OperationType),
		OperationPayload: []byte(entry.OperationPayload),
	}, nil
}

func toProtoLogEntries(entries []storage.RaftLog) []*pb.LogEntry {
	result := make([]*pb.LogEntry, 0, len(entries))
	for _, entry := range entries {
		result = append(result, toProtoLogEntry(entry))
	}
	return result
}

func fromProtoLogEntries(startIndex int64, entries []*pb.LogEntry) ([]storage.RaftLog, error) {
	result := make([]storage.RaftLog, 0, len(entries))
	for offset, entry := range entries {
		index := startIndex + int64(offset)
		converted, err := fromPrototoLogEntry(index, entry)
		if err != nil {
			return nil, err
		}
		result = append(result, converted)

	}
	return result, nil
}
