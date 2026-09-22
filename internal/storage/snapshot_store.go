package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v5"
)

func (store *PostgresStore) SaveSnapshot(
	ctx context.Context,
	snapshot RaftSnapshot,
	data []byte,
) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	if snapshot.SnapshotID == "" {
		return errors.New("snapshot id is invalid")
	}
	if snapshot.Location == "" {
		return errors.New("snapshot location is invalid")
	}
	if snapshot.LastIncludedIndex < 0 {
		return errors.New("snapshot last included index is invalid")
	}
	if snapshot.LastIncludedTerm < 0 {
		return errors.New("snapshot term is invalid")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := writeSnapshotFile(snapshot.Location, data); err != nil {
		return err
	}
	_, err := store.pool.Exec(ctx,
		`
			INSERT INTO raft_snapshots(
			snapshot_id,
			last_included_index,
			last_included_term,
			location
			)
			VALUES ($1,$2,$3,$4)
			ON CONFLICT (snapshot_id)
			DO UPDATE SET
				last_included_index=EXCLUDED.last_included_index,
				last_included_term=EXCLUDED.last_included_term,
				location=EXCLUDED.location
		`,
		snapshot.SnapshotID,
		snapshot.LastIncludedIndex,
		snapshot.LastIncludedTerm,
		snapshot.Location,
	)
	if err != nil {
		return fmt.Errorf("save snapshot error %w", err)
	}
	return nil
}

func (store *PostgresStore) LoadLatestSnapshot(
	ctx context.Context,
) (RaftSnapshot, []byte, error) {
	if ctx == nil {
		return RaftSnapshot{}, nil, errors.New("context is nil")
	}
	var snapshot RaftSnapshot

	err := store.pool.QueryRow(
		ctx,
		`
			SELECT
				snapshot_id,
				last_included_index,
				last_included_term,
				location,
				created_at
			FROM raft_snapshots
			ORDER BY last_included_index DESC, created_at DESC
			LIMIT 1
		`,
	).Scan(
		&snapshot.SnapshotID,
		&snapshot.LastIncludedIndex,
		&snapshot.LastIncludedTerm,
		&snapshot.Location,
		&snapshot.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return RaftSnapshot{}, nil, pgx.ErrNoRows
	}
	if err != nil {
		return RaftSnapshot{}, nil, err
	}
	data, err := os.ReadFile(snapshot.Location)
	if err != nil {
		return RaftSnapshot{}, nil, fmt.Errorf("read snapshot file:%w", err)
	}
	return snapshot, data, nil
}

func (store *PostgresStore) DeleteSnapshot(
	ctx context.Context,
	snapshotID string,
) error {
	if snapshotID == "" {
		return errors.New("snapshot id is empty")
	}
	var location string

	err := store.pool.QueryRow(
		ctx,
		`
			SELECT location
			FROM raft_snapshots
			WHERE snapshot_id=$1
		`,
		snapshotID,
	).Scan(&location)
	if err != nil {
		return err
	}

	_, err = store.pool.Exec(
		ctx,
		`
			DELETE FROM raft_snapshots
			WHERE snapshot_id=$1
		`,
		snapshotID,
	)
	if err != nil {
		return err
	}
	err = os.Remove(location)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func writeSnapshotFile(
	location string,
	data []byte,
) error {
	directory := filepath.Dir(location)

	permission := 0o700
	if err := os.MkdirAll(directory, os.FileMode(permission)); err != nil {
		return fmt.Errorf("create snapshot directory %w", err)
	}

	fileNamePrefix := ".snapshot-*"
	tempFile, err := os.CreateTemp(
		directory,
		fileNamePrefix,
	)
	if err != nil {
		return err
	}
	tempPath := tempFile.Name()
	defer os.Remove(tempPath)

	fileMode := 0o600
	if err := tempFile.Chmod(os.FileMode(fileMode)); err != nil {
		_ = tempFile.Close()
		return err
	}
	if _, err = tempFile.Write(data); err != nil {
		_ = tempFile.Close()
		return err
	}
	if err = tempFile.Sync(); err != nil {
		_ = tempFile.Close()
		return err
	}
	if err = tempFile.Close(); err != nil {
		return err
	}
	if err = os.Rename(tempPath, location); err != nil {
		return fmt.Errorf("finalise snapshot file %w", err)
	}
	return nil
}
