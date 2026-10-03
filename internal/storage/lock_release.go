package storage

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
)

func (store *PostgresStore) ApplyReleaseLock(
	ctx context.Context,
	req *pb.ReleaseLockRequest,
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
	if req.LockToken == "" {
		return errors.New("lock token is empty")
	}
	if observedAt <= 0 {
		return errors.New("observed time is invalid")
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin release lock transaction %w", err)
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

	//means that the release already done
	if err == nil {
		return nil
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("check processed release request %w", err)
	}
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
		response := &pb.ReleaseLockResponse{
			ResourceId:    req.ResourceId,
			RequestId:     req.RequestId,
			ReleaseStatus: pb.ReleaseStatus_RELEASE_STATUS_INVALID_REQUEST,
		}
		return store.saveReleaseResponse(
			ctx,
			tx,
			req,
			response,
		)
	}
	var ownerID string
	var lockToken string
	var expiry int64

	err = tx.QueryRow(
		ctx,
		`
		SELECT owner_id,lock_token,expiry
		FROM current_locks
		WHERE resource_id=$1
		FOR UPDATE
		`,
		req.ResourceId,
	).Scan(&ownerID, &lockToken, &expiry)

	if errors.Is(err, pgx.ErrNoRows) {
		response := &pb.ReleaseLockResponse{
			ResourceId:    req.ResourceId,
			RequestId:     req.RequestId,
			ReleaseStatus: pb.ReleaseStatus_RELEASE_STATUS_STALE_TOKEN,
		}

		return store.saveReleaseResponse(
			ctx,
			tx,
			req,
			response,
		)
	}
	if err != nil {
		return fmt.Errorf("load current lock %w", err)
	}
	validOwner := ownerID == req.ClientId
	validToken := lockToken == req.LockToken
	lockActive := expiry > observedAt
	if !validOwner || !validToken || !lockActive {
		response := &pb.ReleaseLockResponse{
			ResourceId:    req.ResourceId,
			RequestId:     req.RequestId,
			ReleaseStatus: pb.ReleaseStatus_RELEASE_STATUS_STALE_TOKEN,
		}
		return store.saveReleaseResponse(
			ctx,
			tx,
			req,
			response,
		)
	}
	_, err = tx.Exec(
		ctx,
		`
		DELETE FROM current_locks
		WHERE resource_id=$1
		AND owner_id=$2
		AND lock_token=$3
		`,
		req.ResourceId,
		req.ClientId,
		req.LockToken,
	)
	if err != nil {
		return fmt.Errorf("delete current lock %w", err)
	}
	response := &pb.ReleaseLockResponse{
		ResourceId:    req.ResourceId,
		RequestId:     req.RequestId,
		ReleaseStatus: pb.ReleaseStatus_RELEASE_STATUS_RELEASED,
	}
	return store.saveReleaseResponse(
		ctx,
		tx,
		req,
		response,
	)
}

func (store *PostgresStore) saveReleaseResponse(
	ctx context.Context,
	tx pgx.Tx,
	req *pb.ReleaseLockRequest,
	response *pb.ReleaseLockResponse,
) error {
	data, err := proto.Marshal(response)
	if err != nil {
		return fmt.Errorf("marshal release response %w", err)
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
		VALUES($1,$2,$3,$4)
		ON CONFLICT (client_id,request_id)
		DO NOTHING
		`,
		req.ResourceId,
		req.ClientId,
		req.RequestId,
		data,
	)
	if err != nil {
		return fmt.Errorf("save processed release response %w", err)
	}
	// concurrent transaction already processed
	if result.RowsAffected() == 0 {
		return nil
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit release lock transaction %w", err)
	}
	return nil
}
