package storage

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type dbExecutor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type PostgresStore struct {
	pool *pgxpool.Pool
}

type TxStore struct {
	db dbExecutor
}

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{
		pool: pool,
	}
}

func (store *PostgresStore) WithTransaction(ctx context.Context, fn func(*TxStore) error) error {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := fn(&TxStore{db: tx}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

func (store *PostgresStore) GetCurrentLock(ctx context.Context, resourceID string) (CurrentLock, error) {

	return getCurrentLock(ctx, store.pool, resourceID)
}
func (tx *TxStore) GetCurrentLock(ctx context.Context, resourceID string) (CurrentLock, error) {
	return getCurrentLock(ctx, tx.db, resourceID)
}

func getCurrentLock(ctx context.Context, db dbExecutor, resourceID string) (CurrentLock, error) {

	var lock CurrentLock
	err := db.QueryRow(ctx, `
		SELECT resource_id, owner_id, request_id, lock_token, expiry
	FROM current_locks
	WHERE resource_id = $1`, resourceID).Scan(&lock.ResourceID, &lock.OwnerID, &lock.RequestID, &lock.LockToken, &lock.Expiry)
	if err != nil {
		return CurrentLock{}, err
	}
	return lock, nil
}
func (store *PostgresStore) SaveCurrentLock(ctx context.Context, lock CurrentLock) error {
	return saveCurrentLock(ctx, store.pool, lock)
}
func (tx *TxStore) SaveCurrentLock(ctx context.Context, lock CurrentLock) error {
	return saveCurrentLock(ctx, tx.db, lock)
}
func saveCurrentLock(ctx context.Context, db dbExecutor, lock CurrentLock) error {
	_, err := db.Exec(ctx, `INSERT INTO current_locks(resource_id,owner_id,request_id,lock_token,expiry) VALUES($1,$2,$3,$4,$5)
			ON CONFLICT(resource_id)
		    DO UPDATE SET
				owner_id=EXCLUDED.owner_id,
				request_id=EXCLUDED.request_id,
				lock_token=EXCLUDED.lock_token,
				expiry=EXCLUDED.expiry
			`, lock.ResourceID, lock.OwnerID, lock.RequestID, lock.LockToken, lock.Expiry)
	return err
}
func (store *PostgresStore) DeleteCurrentLock(ctx context.Context, resourceID string) error {
	return deleteCurrentLock(ctx, store.pool, resourceID)
}
func (tx *TxStore) DeleteCurrentLock(ctx context.Context, resourceID string) error {
	return deleteCurrentLock(ctx, tx.db, resourceID)
}
func deleteCurrentLock(ctx context.Context, db dbExecutor, resourceID string) error {
	_, err := db.Exec(ctx, `DELETE FROM current_locks WHERE resource_id = $1`, resourceID)
	return err
}
func (store *PostgresStore) GetProcessedRequest(ctx context.Context, clientID string, requestID string) (ProcessedRequest, error) {
	return getProcessedRequest(ctx, store.pool, clientID, requestID)
}
func (tx *TxStore) GetProcessedRequest(ctx context.Context, clientID string, requestID string) (ProcessedRequest, error) {
	return getProcessedRequest(ctx, tx.db, clientID, requestID)
}
func getProcessedRequest(ctx context.Context, db dbExecutor, clientID string, requestID string) (ProcessedRequest, error) {
	var request ProcessedRequest
	err := db.QueryRow(ctx, `SELECT client_id, request_id, resource_id, response, created_at
	FROM processed_requests
	WHERE client_id = $1 AND request_id = $2`, clientID, requestID).Scan(&request.ClientID, &request.RequestID, &request.ResourceID, &request.Response, &request.CreatedAt)
	if err != nil {
		return ProcessedRequest{}, err
	}
	return request, nil
}
func (store *PostgresStore) SaveProcessedRequest(
	ctx context.Context,
	request ProcessedRequest,
) error {
	return saveProcessedRequest(ctx, store.pool, request)
}

func (tx *TxStore) SaveProcessedRequest(
	ctx context.Context,
	request ProcessedRequest,
) error {
	return saveProcessedRequest(ctx, tx.db, request)
}
func saveProcessedRequest(ctx context.Context, db dbExecutor, request ProcessedRequest) error {
	_, err := db.Exec(ctx, `INSERT INTO processed_requests(client_id,request_id,resource_id,response,created_at) VALUES($1,$2,$3,$4,$5)`, request.ClientID, request.RequestID, request.ResourceID, request.Response, request.CreatedAt)
	return err
}
func (store *PostgresStore) GetResourceLocation(
	ctx context.Context,
	resourceID string,
) (ResourceLocation, error) {
	return getResourceLocation(ctx, store.pool, resourceID)
}

func (tx *TxStore) GetResourceLocation(
	ctx context.Context,
	resourceID string,
) (ResourceLocation, error) {
	return getResourceLocation(ctx, tx.db, resourceID)
}
func getResourceLocation(ctx context.Context, db dbExecutor, resourceID string) (ResourceLocation, error) {
	var resourceLocation ResourceLocation
	err := db.QueryRow(ctx, `SELECT resource_id, location
	FROM resource_locations
	WHERE resource_id = $1`, resourceID).Scan(&resourceLocation.ResourceID, &resourceLocation.Location)
	if err != nil {
		return ResourceLocation{}, err
	}
	return resourceLocation, nil
}
func (store *PostgresStore) SaveResourceLocation(
	ctx context.Context,
	resource ResourceLocation,
) error {
	return saveResourceLocation(ctx, store.pool, resource)
}

func (tx *TxStore) SaveResourceLocation(
	ctx context.Context,
	resource ResourceLocation,
) error {
	return saveResourceLocation(ctx, tx.db, resource)
}
func saveResourceLocation(ctx context.Context, db dbExecutor, resource ResourceLocation) error {
	_, err := db.Exec(ctx, `INSERT INTO resource_locations(resource_id, location) VALUES($1,$2)`, resource.ResourceID, resource.Location)
	return err
}
