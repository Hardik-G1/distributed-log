package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

type applicationSnapshot struct {
	CurrentLocks      []CurrentLock      `json:"current_locks"`
	ProcessedRequests []ProcessedRequest `json:"processed_requests"`
	ResourceLocations []ResourceLocation `json:"resource_locations"`
}

func (store *PostgresStore) SnapshotApplicationState(
	ctx context.Context,
) ([]byte, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	if store == nil || store.pool == nil {
		return nil, errors.New("store is not initialised")
	}
	snapshot := applicationSnapshot{}
	rows, err := store.pool.Query(
		ctx,
		`
		SELECT resource_id,owner_id,request_id,lock_token,expiry
		FROM current_locks
		`,
	)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var lock CurrentLock
		if err := rows.Scan(
			&lock.ResourceID,
			&lock.OwnerID,
			&lock.RequestID,
			&lock.LockToken,
			&lock.Expiry,
		); err != nil {
			rows.Close()
			return nil, err
		}
		snapshot.CurrentLocks = append(snapshot.CurrentLocks, lock)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	rows, err = store.pool.Query(
		ctx,
		`
		SELECT client_id,request_id,resource_id,response,created_at
		FROM processed_requests
		`,
	)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var request ProcessedRequest

		if err := rows.Scan(
			&request.ClientID,
			&request.RequestID,
			&request.ResourceID,
			&request.Response,
			&request.CreatedAt,
		); err != nil {
			rows.Close()
			return nil, err
		}
		snapshot.ProcessedRequests = append(snapshot.ProcessedRequests, request)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	rows, err = store.pool.Query(
		ctx,
		`
		SELECT resource_id,location
		FROM resource_locations
		`,
	)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var location ResourceLocation

		if err := rows.Scan(
			&location.ResourceID,
			&location.Location,
		); err != nil {
			rows.Close()
			return nil, err
		}
		snapshot.ResourceLocations = append(snapshot.ResourceLocations, location)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	data, err := json.Marshal(snapshot)
	if err != nil {
		return nil, fmt.Errorf(
			"marshal application snapshot %w",
			err,
		)
	}
	return data, nil
}

func (store *PostgresStore) RestoreApplicationState(
	ctx context.Context,
	data []byte,
) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	if store == nil || store.pool == nil {
		return errors.New("store is not initialised")
	}
	var snapshot applicationSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("unmarshal application snapshot %w", err)
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	_, err = tx.Exec(
		ctx,
		`
		TRUNCATE TABLE
		current_locks,
		processed_requests,
		resource_locations
		`,
	)
	if err != nil {
		return fmt.Errorf("clear application state %w", err)
	}
	for _, lock := range snapshot.CurrentLocks {
		_, err := tx.Exec(
			ctx,
			`
			INSERT INTO current_locks(
				resource_id,
				owner_id,
				request_id,
				lock_token,
				expiry
			)
			VALUES ($1,$2,$3,$4,$5)
			`,
			lock.ResourceID,
			lock.OwnerID,
			lock.RequestID,
			lock.LockToken,
			lock.Expiry,
		)
		if err != nil {
			return err
		}
	}
	for _, request := range snapshot.ProcessedRequests {
		_, err := tx.Exec(
			ctx,
			`
			INSERT INTO processed_requests(
				client_id,
				request_id,
				resource_id,
				response,
				created_at
			)
			VALUES ($1,$2,$3,$4,$5)
			`,
			request.ClientID,
			request.RequestID,
			request.ResourceID,
			request.Response,
			request.CreatedAt,
		)
		if err != nil {
			return err
		}
	}
	for _, location := range snapshot.ResourceLocations {
		_, err := tx.Exec(
			ctx,
			`
			INSERT INTO resource_locations(
				resource_id,
				location
			)
			VALUES ($1,$2)
			`,
			location.ResourceID,
			location.Location,
		)
		if err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit application restore %w", err)
	}
	return nil
}
