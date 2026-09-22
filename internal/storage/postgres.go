package storage

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

func OpenPostgres(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}

	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if err = createTables(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func createTables(ctx context.Context, pool *pgxpool.Pool) error {
	statements :=
		[]string{
			`
		CREATE TABLE IF NOT EXISTS current_locks(
		resource_id TEXT PRIMARY KEY,
		owner_id TEXT NOT NULL,
		request_id TEXT NOT NULL,
		lock_token TEXT NOT NULL,
		expiry BIGINT NOT NULL
		)
		`,
			`
		CREATE TABLE IF NOT EXISTS processed_requests(
		resource_id TEXT NOT NULL,
		client_id TEXT NOT NULL,
		request_id TEXT NOT NULL,
		response BYTEA NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		PRIMARY KEY (client_id,request_id)
		)
		`,
			`
		CREATE TABLE IF NOT EXISTS resource_locations(
		resource_id TEXT PRIMARY KEY,
		location TEXT NOT NULL
		)
		`,
			`
		CREATE TABLE IF NOT EXISTS raft_logs(
		log_index BIGINT PRIMARY KEY,
		term BIGINT NOT NULL,
		operation_type INTEGER NOT NULL,
		operation_payload BYTEA NOT NULL
		)
		`,
			`
		CREATE TABLE IF NOT EXISTS raft_metadata(
			id SMALLINT PRIMARY KEY CHECK (id=1),
			current_term BIGINT NOT NULL,
			voted_for TEXT NOT NULL DEFAULT=''
		)
		`,
			`INSERT INTO raft_metadata(id,current_term,voted_for)
		VALUES(1,0,'')
		ON CONFLICT (id) DO NOTHING`,
			`
		CREATE TABLE IF NOT EXISTS raft_snapshots(
		snapshot_id TEXT PRIMARY KEY,
		last_included_index BIGINT NOT NULL,
		last_included_term BIGINT NOT NULL,
		location TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
		`,
		}

	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
