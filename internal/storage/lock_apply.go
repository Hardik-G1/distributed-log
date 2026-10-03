package storage

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
)

func (store *PostgresStore) ApplyAcquireLock(
	ctx context.Context,
	req *pb.AcquireLockRequest,
	lockToken string,
	expiry int64,
	observedAt int64,
) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	if store == nil || store.pool == nil {
		return errors.New("store is not initialised")
	}
	if req == nil {
		return errors.New("request is empty")
	}
	if req.ClientId == "" {
		return errors.New("client id is empty")
	}
	if req.RequestId == "" {
		return errors.New("request id is empty")
	}
	if req.ResourceId == "" {
		return errors.New("resource id is empty")
	}
	if lockToken == "" {
		return errors.New("lock token is empty")
	}
	if expiry <= observedAt {
		return errors.New("lock expiry is invalid")
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin acquire-lock transaction %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	//Idempotency check
	var previousResponse []byte
	err = tx.QueryRow(
		ctx,
		`
		SELECT response
		FROM processed_requests
		WHERE client_id=$1
		AND request_id=$2
		`,
		req.ClientId,
		req.RequestId,
	).Scan(&previousResponse)

	if err == nil {
		// request was already applied
		return nil
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("check processed request: %w", err)
	}
	// confirm the resource exists
	var resourceExists bool
	err = tx.QueryRow(
		ctx,
		`
			SELECT EXISTS(
			SELECT 1
			FROM resource_locations
			WHERE resource_id=$1
			)
		`,
		req.ResourceId,
	).Scan(&resourceExists)

	if err != nil {
		return fmt.Errorf("check resource %w", err)
	}

	if !resourceExists {
		response := &pb.AcquireLockResponse{
			ResourceId: req.ResourceId,
			LockStatus: pb.LockStatus_LOCK_STATUS_INVALID_REQUEST,
			RequestId:  req.RequestId,
		}
		return store.saveAcquireResponse(ctx, tx, req, response)
	}

	// Acquire the lock only if there is no current lock
	// or the current lock has expired
	var acquiredResourceID string

	err = tx.QueryRow(
		ctx,
		`
			INSERT INTO current_locks(
			resource_id,
			owner_id,
			request_id,
			lock_token,
			expiry
			)
			VALUES($1,$2,$3,$4,$5)
			ON CONFLICT (resource_id)
			DO UPDATE SET
				owner_id=EXCLUDED.owner_id,
				request_id=EXCLUDED.request_id,
				lock_token=EXCLUDED.lock_token,
				expiry=EXCLUDED.expiry
			WHERE current_locks.expiry<=$6
			RETURNING resource_id
		`,
		req.ResourceId,
		req.ClientId,
		req.RequestId,
		lockToken,
		expiry,
		observedAt,
	).Scan(&acquiredResourceID)

	if errors.Is(err, pgx.ErrNoRows) {
		response := &pb.AcquireLockResponse{
			ResourceId: req.ResourceId,
			LockStatus: pb.LockStatus_LOCK_STATUS_BUSY,
			RequestId:  req.RequestId,
		}
		return store.saveAcquireResponse(ctx, tx, req, response)
	}

	if err != nil {
		return fmt.Errorf("acquire lock %w", err)
	}
	response := &pb.AcquireLockResponse{
		ResourceId: req.ResourceId,
		LockStatus: pb.LockStatus_LOCK_STATUS_APPROVED,
		LockToken:  lockToken,
		Expiry:     expiry,
		RequestId:  req.RequestId,
	}
	return store.saveAcquireResponse(ctx, tx, req, response)
}

func (store *PostgresStore) saveAcquireResponse(
	ctx context.Context,
	tx pgx.Tx,
	req *pb.AcquireLockRequest,
	response *pb.AcquireLockResponse,
) error {
	data, err := proto.Marshal(response)
	if err != nil {
		return fmt.Errorf("marshal acquire request %w", err)
	}
	result, err := tx.Exec(
		ctx,
		`
			INSERT INTO processed_requests(
				resource_id,
				client_id,
				request_id,
				response
			)
			VALUES ($1,$2,$3,$4)
			ON CONFLICT (client_id,request_id)
			DO NOTHING
		`,
		req.ResourceId,
		req.ClientId,
		req.RequestId,
		data,
	)
	if err != nil {
		return fmt.Errorf("save processed response %w", err)
	}
	// if another concurrent application already stored this
	// go back rollback by the transaction
	if result.RowsAffected() == 0 {
		return nil
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit acquire-lock transaction: %w", err)
	}
	return nil
}
