package storage

import (
	"context"
)

func (store *PostgresStore) LoadRaftMetadata(ctx context.Context) (RaftMetadata, error) {
	var metadata RaftMetadata
	err := store.pool.QueryRow(ctx,
		`SELECT current_term, voted_for from
		raft_metadata where id=1`,
	).Scan(&metadata.CurrentTerm, &metadata.VotedFor)
	if err != nil {
		return RaftMetadata{}, err
	}
	return metadata, nil
}

func (store *PostgresStore) SaveRaftMetadata(ctx context.Context, metadata RaftMetadata) error {
	_, err := store.pool.Exec(ctx, `UPDATE raft_metadata SET current_term=$1,voted_for=$2 WHERE id=1`, metadata.CurrentTerm, metadata.VotedFor)
	return err
}

func (store *PostgresStore) AppendRaftLogs(ctx context.Context, raftLogs []RaftLog) error {
	return store.WithTransaction(ctx, func(tx *TxStore) error {
		return tx.AppendRaftLogs(ctx, raftLogs)
	})
}
func (tx *TxStore) AppendRaftLogs(ctx context.Context, raftLogs []RaftLog) error {
	for _, log := range raftLogs {
		_, err := tx.db.Exec(
			ctx,
			`
			INSERT INTO raft_logs(
				log_index,
				term,
				operation_type,
				operation_payload
			)
			VALUES($1,$2,$3,$4)
			`,
			log.LogIndex,
			log.Term,
			log.OperationType,
			log.OperationPayload,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (store *PostgresStore) GetRaftLog(ctx context.Context, index int64) (RaftLog, error) {
	var log RaftLog
	err := store.pool.QueryRow(ctx,
		`
		SELECT log_index,term,operation_type,operation_payload
		FROM raft_logs
		WHERE log_index=$1
		`,
		index,
	).Scan(&log.LogIndex, &log.Term, &log.OperationType, &log.OperationPayload)
	if err != nil {
		return RaftLog{}, err
	}
	return log, nil
}

func (store *PostgresStore) GetRaftEntriesFrom(ctx context.Context, index int64) ([]RaftLog, error) {
	var logs []RaftLog
	rows, err := store.pool.Query(ctx,
		`
		SELECT log_index,term,operation_type,operation_payload
		FROM raft_logs
		WHERE log_index>=$1
		ORDER BY log_index ASC
		`,
		index,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var log RaftLog
		err := rows.Scan(&log.LogIndex, &log.Term, &log.OperationType, &log.OperationPayload)
		if err != nil {
			return nil, err
		}
		logs = append(logs, log)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return logs, nil
}

func (store *PostgresStore) GetLastRaftLog(ctx context.Context) (RaftLog, error) {
	var log RaftLog
	err := store.pool.QueryRow(ctx,
		`
		SELECT log_index,term,operation_type,operation_payload
		FROM raft_logs
		ORDER BY log_index DESC LIMIT 1
		`,
	).Scan(&log.LogIndex, &log.Term, &log.OperationType, &log.OperationPayload)
	if err != nil {
		return RaftLog{}, err
	}
	return log, nil
}

func (store *PostgresStore) ReplaceRaftSuffix(ctx context.Context, fromIndex int64, entries []RaftLog) error {
	return store.WithTransaction(ctx, func(tx *TxStore) error {
		if err := tx.DeleteRaftEntriesFrom(ctx, fromIndex); err != nil {
			return err
		}
		return tx.AppendRaftLogs(ctx, entries)
	})
}

func (tx *TxStore) DeleteRaftEntriesFrom(ctx context.Context, index int64) error {
	_, err := tx.db.Exec(ctx,
		`
		DELETE
		FROM raft_logs
		WHERE log_index>=$1
		`,
		index,
	)
	return err
}

func (tx *TxStore) DeleteRaftEntriesThrough(ctx context.Context, index int64) error {
	_, err := tx.db.Exec(ctx,
		`
		DELETE
		FROM raft_logs
		WHERE log_index<=$1
		`,
		index,
	)
	return err
}

func (store *PostgresStore) SaveSnapshotMetadata(
	ctx context.Context,
	snapshot RaftSnapshot,
) error {
	return saveSnapshotMetadata(ctx, store.pool, snapshot)
}

func (tx *TxStore) SaveSnapshotMetadata(
	ctx context.Context,
	snapshot RaftSnapshot,
) error {
	return saveSnapshotMetadata(ctx, tx.db, snapshot)
}

func saveSnapshotMetadata(ctx context.Context, db dbExecutor, snapshot RaftSnapshot) error {
	_, err := db.Exec(
		ctx,
		`INSERT INTO raft_snapshots(
			snapshot_id,
			last_included_index,
			last_included_term,
			location,
			created_at
		) VALUES($1,$2,$3,$4,$5)
		ON CONFLICT (snapshot_id)
		DO UPDATE SET
			last_included_index=EXCLUDED.last_included_index,
			last_included_term=EXCLUDED.last_included_term,
			location=EXCLUDED.location,
			created_at=EXCLUDED.created_at
		`,
		snapshot.SnapshotID,
		snapshot.LastIncludedIndex,
		snapshot.LastIncludedTerm,
		snapshot.Location,
		snapshot.CreatedAt,
	)
	return err
}

func (store *PostgresStore) GetLatestSnapshot(
	ctx context.Context,
) (RaftSnapshot, error) {
	return getLatestSnapshot(ctx, store.pool)
}

func (tx *TxStore) GetLatestSnapshot(
	ctx context.Context,
) (RaftSnapshot, error) {
	return getLatestSnapshot(ctx, tx.db)
}

func getLatestSnapshot(ctx context.Context, db dbExecutor) (RaftSnapshot, error) {
	var snapshot RaftSnapshot
	err := db.QueryRow(
		ctx,
		`SELECT snapshot_id,
			last_included_index,
			last_included_term,
			location,
			created_at
		FROM raft_snapshots
		ORDER BY last_included_index DESC
		LIMIT 1`,
	).Scan(&snapshot.SnapshotID, &snapshot.LastIncludedIndex, &snapshot.LastIncludedTerm, &snapshot.Location, &snapshot.CreatedAt)

	return snapshot, err
}

func (store *PostgresStore) DeleteOldSnapshots(
	ctx context.Context,
	keepSnapshotID string,
) error {
	_, err := store.pool.Exec(
		ctx,
		`
		DELETE FROM raft_snapshots
		WHERE snapshot_id<>$1
		`,
		keepSnapshotID,
	)
	return err
}
