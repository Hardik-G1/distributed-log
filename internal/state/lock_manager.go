package state

import (
	"sync"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"github.com/Hardik-G1/distributed-log/internal/storage"
)

type LockManager struct{
	store *storage.PostgresStore
}
func Acquire(req *pb.AcquireLockRequest) (res *pb.AcquireLockResponse,error){
	validateRequest(req) // just checks the fields
	checkDuplicate(req) // just checks the duplication of request
	expireLockIfNeeded(req) // checks the if the lock on the resource has expired if expired then release any owner and give the new lock to this person
	generateLockToken(req) // just a random token with a time stamp of the expiry
	saveState(req) // save the new lock status in sqllite
}
