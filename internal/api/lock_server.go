package api

import (
	"context"
	"errors"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
)

type LockServer struct {
	pb.UnimplementedLockServiceServer
}

func (ls *LockServer) AcquireLock(ctx context.Context, req *pb.AcquireLockRequest) (*pb.AcquireLockResponse, error) {
	return nil, errors.New("Not implemented")
}
