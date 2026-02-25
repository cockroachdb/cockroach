// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dbexec

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PGXExecutor implements Executor for PostgreSQL-compatible databases using pgx.
// This includes CockroachDB, PostgreSQL, Aurora PostgreSQL, and other compatible
// databases. The executor uses a Dialect to generate database-specific SQL.
type PGXExecutor struct {
	dialect Dialect
	cfg     Config

	mcp *workload.MultiConnPool
	sr  workload.SQLRunner

	// Statement handles for prepared statements.
	readStmt         workload.StmtHandle
	followerReadStmt workload.StmtHandle
	writeStmt        workload.StmtHandle
	deleteStmt       workload.StmtHandle
	spanStmt         workload.StmtHandle
	sfuStmt          workload.StmtHandle // SELECT ... FOR UPDATE
	sel1Stmt         workload.StmtHandle // SELECT 1
}

var _ Executor = (*PGXExecutor)(nil)

// NewPGXExecutor creates a new PGXExecutor with the given dialect.
func NewPGXExecutor(dialect Dialect) *PGXExecutor {
	return &PGXExecutor{dialect: dialect}
}

// Init prepares the executor by creating connection pools and defining
// statements via SQLRunner.
func (e *PGXExecutor) Init(ctx context.Context, cfg Config, connFlags *workload.ConnFlags) error {
	e.cfg = cfg

	// Create multi-connection pool.
	poolCfg := workload.NewMultiConnPoolCfgFromFlags(connFlags)
	poolCfg.MaxTotalConnections = connFlags.Concurrency + 1
	mcp, err := workload.NewMultiConnPool(ctx, poolCfg, cfg.URLs...)
	if err != nil {
		return errors.Wrap(err, "creating connection pool")
	}
	e.mcp = mcp

	// Define statements using the dialect.
	e.readStmt = e.sr.Define(e.dialect.ReadStmt(cfg.Table, cfg.BatchSize, cfg.Enum))
	e.followerReadStmt = e.sr.Define(e.dialect.FollowerReadStmt(cfg.Table, cfg.BatchSize, cfg.Enum))
	e.writeStmt = e.sr.Define(e.dialect.UpsertStmt(cfg.Table, cfg.BatchSize, cfg.Enum))
	e.deleteStmt = e.sr.Define(e.dialect.DeleteStmt(cfg.Table, cfg.BatchSize))
	e.spanStmt = e.sr.Define(e.dialect.SpanStmt(cfg.Table, cfg.SpanLimit))
	e.sfuStmt = e.sr.Define(e.dialect.SelectForUpdateStmt(cfg.Table, cfg.BatchSize))
	e.sel1Stmt = e.sr.Define("SELECT 1")

	// Initialize the SQLRunner with prepared statements.
	if err := e.sr.Init(ctx, e.dialect.Name(), e.mcp); err != nil {
		e.mcp.Close()
		return errors.Wrap(err, "initializing SQL runner")
	}

	return nil
}

// Read executes a batch read for the given keys.
func (e *PGXExecutor) Read(ctx context.Context, keys []interface{}) (Rows, error) {
	rows, err := e.readStmt.Query(ctx, keys...)
	if err != nil {
		return nil, err
	}
	return &pgxRowsWrapper{rows: rows}, nil
}

// FollowerRead executes a stale/replica read if supported by the dialect.
func (e *PGXExecutor) FollowerRead(ctx context.Context, keys []interface{}) (Rows, error) {
	rows, err := e.followerReadStmt.Query(ctx, keys...)
	if err != nil {
		return nil, err
	}
	return &pgxRowsWrapper{rows: rows}, nil
}

// Write upserts a batch of key-value pairs.
func (e *PGXExecutor) Write(ctx context.Context, rows []Row) error {
	args := flattenRowArgs(rows, e.cfg.Enum)
	_, err := e.writeStmt.Exec(ctx, args...)
	return err
}

// Delete removes a batch of keys.
func (e *PGXExecutor) Delete(ctx context.Context, keys []interface{}) error {
	_, err := e.deleteStmt.Exec(ctx, keys...)
	return err
}

// Span executes a spanning query starting at startKey with the given limit.
func (e *PGXExecutor) Span(ctx context.Context, startKey interface{}, limit int) (int64, error) {
	// The limit is baked into the prepared statement via SpanStmt. When
	// SpanLimit == 0, the statement is a full table scan with no arguments.
	// When SpanLimit > 0, it takes a startKey argument for the WHERE clause.
	var row pgx.Row
	if e.cfg.SpanLimit == 0 {
		row = e.spanStmt.QueryRow(ctx)
	} else {
		row = e.spanStmt.QueryRow(ctx, startKey)
	}
	var count int64
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

// BeginTx starts a transaction with the given options.
func (e *PGXExecutor) BeginTx(ctx context.Context, opts TxOptions) (Tx, error) {
	pool := e.mcp.Get()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "acquiring connection")
	}

	txOpts := pgx.TxOptions{}

	// Set transaction priority if supported and specified.
	if opts.Priority != "" && e.dialect.Capabilities().TransactionQoS {
		txOpts.BeginQuery = fmt.Sprintf("BEGIN PRIORITY %s", opts.Priority)
	}

	tx, err := conn.BeginTx(ctx, txOpts)
	if err != nil {
		conn.Release()
		return nil, errors.Wrap(err, "beginning transaction")
	}

	return &pgxTxWrapper{
		tx:       tx,
		conn:     conn,
		executor: e,
	}, nil
}

// Close releases all resources including connection pools.
func (e *PGXExecutor) Close() error {
	if e.mcp != nil {
		e.mcp.Close()
	}
	return nil
}

// Capabilities returns what features this executor supports.
func (e *PGXExecutor) Capabilities() Capabilities {
	return e.dialect.Capabilities()
}

// SetupWorker is a no-op for PGXExecutor. CRDB-specific per-worker setup
// (like transaction QoS) is not supported for generic PostgreSQL-compatible databases.
func (e *PGXExecutor) SetupWorker(ctx context.Context) error {
	return nil
}

// pgxRowsWrapper wraps pgx.Rows to implement our Rows interface.
type pgxRowsWrapper struct {
	rows pgx.Rows
}

var _ Rows = (*pgxRowsWrapper)(nil)

// Next advances to the next row.
func (r *pgxRowsWrapper) Next() bool {
	return r.rows.Next()
}

// Scan copies the columns from the current row into dest.
func (r *pgxRowsWrapper) Scan(dest ...interface{}) error {
	return r.rows.Scan(dest...)
}

// Err returns any error encountered during iteration.
func (r *pgxRowsWrapper) Err() error {
	return r.rows.Err()
}

// Close releases resources associated with the result set.
func (r *pgxRowsWrapper) Close() {
	r.rows.Close()
}

// pgxTxWrapper wraps pgx.Tx and the connection to implement Tx and ExtendedTx.
// It holds the connection so it can be released when the transaction completes,
// and a reference to the executor for prepared statement access.
type pgxTxWrapper struct {
	tx       pgx.Tx
	conn     *pgxpool.Conn
	executor *PGXExecutor
}

var _ Tx = (*pgxTxWrapper)(nil)
var _ ExtendedTx = (*pgxTxWrapper)(nil)

// Exec executes a statement that does not return rows.
func (t *pgxTxWrapper) Exec(ctx context.Context, sql string, args ...interface{}) error {
	_, err := t.tx.Exec(ctx, sql, args...)
	return err
}

// Query executes a query that returns rows.
func (t *pgxTxWrapper) Query(ctx context.Context, sql string, args ...interface{}) (Rows, error) {
	rows, err := t.tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &pgxRowsWrapper{rows: rows}, nil
}

// Commit commits the transaction.
func (t *pgxTxWrapper) Commit(ctx context.Context) error {
	err := t.tx.Commit(ctx)
	t.conn.Release()
	return err
}

// Rollback aborts the transaction.
func (t *pgxTxWrapper) Rollback(ctx context.Context) error {
	err := t.tx.Rollback(ctx)
	t.conn.Release()
	return err
}

// SelectForUpdate executes a SELECT ... FOR UPDATE query within the transaction,
// locking the selected rows until the transaction completes.
func (t *pgxTxWrapper) SelectForUpdate(ctx context.Context, keys []interface{}) (Rows, error) {
	rows, err := t.executor.sfuStmt.QueryTx(ctx, t.tx, keys...)
	if err != nil {
		return nil, err
	}
	return &pgxRowsWrapper{rows: rows}, nil
}

// Select1 executes a SELECT 1 query within the transaction. On CockroachDB,
// this prevents automatic transaction-level retries. On PostgreSQL, it is a
// harmless no-op that keeps the transaction active.
func (t *pgxTxWrapper) Select1(ctx context.Context) error {
	row := t.executor.sel1Stmt.QueryRowTx(ctx, t.tx)
	var dummy int
	return row.Scan(&dummy)
}

// Write executes an upsert within the transaction using prepared statements.
func (t *pgxTxWrapper) Write(ctx context.Context, rows []Row) error {
	args := flattenRowArgs(rows, t.executor.cfg.Enum)
	_, err := t.executor.writeStmt.ExecTx(ctx, t.tx, args...)
	return err
}
