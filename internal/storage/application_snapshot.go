package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type applicationSnapshot struct {
	CurrentLocks      []CurrentLock            `json:"current_locks"`
	ProcessedRequests []ProcessedRequest       `json:"processed_requests"`
	ResourceLocations []ResourceLocation       `json:"resource_locations"`
	ApplicationLogs   []applicationLogSnapshot `json:"application_logs"`
}

type applicationLogSnapshot struct {
	ResourceID string `json:"resource_id"`
	Data       []byte `json:"data"`
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
		data, err := os.ReadFile(location.Location)
		if errors.Is(err, os.ErrNotExist) {
			data = nil
		} else if err != nil {
			rows.Close()
			return nil, fmt.Errorf(
				"read application log %s %w",
				location.ResourceID,
				err,
			)
		}
		snapshot.ApplicationLogs = append(
			snapshot.ApplicationLogs,
			applicationLogSnapshot{
				ResourceID: location.ResourceID,
				Data:       data,
			},
		)
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
	localLocations := make(map[string]string)
	missingLocations := make([]ResourceLocation, 0)
	rows, err := store.pool.Query(
		ctx,
		`SELECT resource_id,location FROM resource_locations`,
	)
	if err != nil {
		return fmt.Errorf("load local resource locations %w", err)
	}
	for rows.Next() {
		var resourceID string
		var location string
		if err := rows.Scan(&resourceID, &location); err != nil {
			rows.Close()
			return fmt.Errorf("scan local resource location %w", err)
		}
		localLocations[resourceID] = location
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("read local resource locations %w", err)
	}
	rows.Close()
	for _, location := range snapshot.ResourceLocations {
		if _, exists := localLocations[location.ResourceID]; exists {
			continue
		}
		if filepath.IsAbs(location.Location) {
			return fmt.Errorf(
				"resource %s has no node-local location",
				location.ResourceID,
			)
		}
		localLocations[location.ResourceID] = location.Location
		missingLocations = append(missingLocations, location)
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
		processed_requests
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

	for _, location := range missingLocations {
		_, err := tx.Exec(
			ctx,
			`
				INSERT INTO resource_locations(
					resource_id,
					location
				)
				VALUES($1,$2)
			`,
			location.ResourceID,
			location.Location,
		)
		if err != nil {
			return err
		}
	}

	logData := make(map[string][]byte, len(snapshot.ApplicationLogs))
	for _, applicationLog := range snapshot.ApplicationLogs {
		logData[applicationLog.ResourceID] = applicationLog.Data
	}

	for resourceID, location := range localLocations {
		if err := replaceApplicationLogFile(location, logData[resourceID]); err != nil {
			return fmt.Errorf("restore application log %s %w", resourceID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit application restore %w", err)
	}
	return nil
}

func replaceApplicationLogFile(location string, data []byte) error {
	directory := filepath.Dir(location)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	tempFile, err := os.CreateTemp(directory, ".application-restore-*")
	if err != nil {
		return err
	}
	tempPath := tempFile.Name()
	defer os.Remove(tempPath)
	if err := tempFile.Chmod(0o600); err != nil {
		_ = tempFile.Close()
		return err
	}
	if _, err := tempFile.Write(data); err != nil {
		_ = tempFile.Close()
		return err
	}
	if err := tempFile.Sync(); err != nil {
		_ = tempFile.Close()
		return err
	}
	if err := tempFile.Close(); err != nil {
		return err
	}
	return os.Rename(tempPath, location)
}
