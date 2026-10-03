package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	pb "github.com/Hardik-G1/distributed-log/gen/distributed_log/v1"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
)

var applicationFileLocks sync.Map

func fileMutex(location string) *sync.Mutex {
	lock := &sync.Mutex{}
	actual, _ := applicationFileLocks.LoadOrStore(location, lock)
	return actual.(*sync.Mutex)
}

func (store *PostgresStore) ApplyAppendLog(
	ctx context.Context,
	req *pb.AppendLogRequest,
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
	if req.Message == "" {
		return errors.New("message is empty")
	}
	if observedAt <= 0 {
		return errors.New("observed at is invalid")
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin append transaction %w", err)
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
		return fmt.Errorf("check processed append request: %w", err)
	}

	var location string
	err = tx.QueryRow(
		ctx,
		`
			SELECT location
			FROM resource_locations
			WHERE resource_id=$1
		`,
		req.ResourceId,
	).Scan(&location)

	if errors.Is(err, pgx.ErrNoRows) {
		response := &pb.AppendLogResponse{
			RequestId:    req.RequestId,
			ResourceId:   req.ResourceId,
			AppendStatus: pb.AppendStatus_APPEND_STATUS_INVALID_REQUEST,
		}
		return store.saveAppendResponse(ctx, tx, req, response)
	}
	if err != nil {
		return fmt.Errorf("load application log location %w", err)
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
		response := &pb.AppendLogResponse{
			ResourceId:   req.ResourceId,
			RequestId:    req.RequestId,
			AppendStatus: pb.AppendStatus_APPEND_STATUS_EXPIRED_LOCK,
		}

		return store.saveAppendResponse(
			ctx,
			tx,
			req,
			response,
		)
	}
	if err != nil {
		return fmt.Errorf("load current lock in append log %w", err)
	}
	if ownerID != req.ClientId || lockToken != req.LockToken {
		response := &pb.AppendLogResponse{
			RequestId:    req.RequestId,
			ResourceId:   req.ResourceId,
			AppendStatus: pb.AppendStatus_APPEND_STATUS_EXPIRED_LOCK,
		}
		return store.saveAppendResponse(ctx, tx, req, response)
	}
	if expiry <= observedAt {
		response := &pb.AppendLogResponse{
			RequestId:    req.RequestId,
			ResourceId:   req.ResourceId,
			AppendStatus: pb.AppendStatus_APPEND_STATUS_EXPIRED_LOCK,
		}
		return store.saveAppendResponse(ctx, tx, req, response)
	}
	lock := fileMutex(location)
	lock.Lock()
	defer lock.Unlock()

	if err := os.MkdirAll(filepath.Dir(location), 0o700); err != nil {
		return fmt.Errorf("create application log directory %w", err)
	}
	file, err := os.OpenFile(
		location,
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0o600,
	)
	if err != nil {
		return fmt.Errorf("open application log %w", err)
	}
	if _, err := file.WriteString(req.Message + "\n"); err != nil {
		_ = file.Close()
		return fmt.Errorf("append application log: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync application log %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close application log %w", err)
	}
	response := &pb.AppendLogResponse{
		RequestId:    req.RequestId,
		ResourceId:   req.ResourceId,
		AppendStatus: pb.AppendStatus_APPEND_STATUS_COMMITTED,
	}
	return store.saveAppendResponse(ctx, tx, req, response)
}

func (store *PostgresStore) saveAppendResponse(
	ctx context.Context,
	tx pgx.Tx,
	req *pb.AppendLogRequest,
	response *pb.AppendLogResponse,
) error {
	data, err := proto.Marshal(response)
	if err != nil {
		return fmt.Errorf("marshal append response %w", err)
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
		return fmt.Errorf("save processed append response %w", err)
	}
	// concurrent transaction already processed
	if result.RowsAffected() == 0 {
		return nil
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit append transaction %w", err)
	}
	return nil
}

func (store *PostgresStore) ReadApplicationLog(
	ctx context.Context,
	resourceID string,
	position int64,
	length int64,
) (string, error) {
	if ctx == nil {
		return "", errors.New("context is nil")
	}
	if resourceID == "" {
		return "", errors.New("resource id is empty")
	}
	if position < 0 {
		return "", errors.New("position cannot be negative")
	}
	if length < (-1) {
		return "", errors.New("length must be -1 or non negative")
	}
	resource, err := store.GetResourceLocation(ctx, resourceID)
	if err != nil {
		return "", err
	}
	data, err := os.ReadFile(resource.Location)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read application log %w", err)
	}
	content := strings.TrimSuffix(string(data), "\n")
	if content == "" {
		if position == 0 {
			return "", nil
		}
		return "", errors.New("position is beyond the application log")
	}
	lines := strings.Split(content, "\n")
	totalLines := int64(len(lines))
	if position > totalLines {
		return "", errors.New("position is beyond the application log")
	}
	if position == totalLines {
		return "", nil
	}
	end := totalLines
	if length == 0 {
		end = position + 1
	} else if length > 0 && position+length < end {
		end = position + length
	}
	return strings.Join(lines[position:end], "\n"), nil
}
