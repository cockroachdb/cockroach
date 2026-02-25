// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dbexec

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CRDBExecutor implements Executor for CockroachDB with full feature support.
// Unlike PGXExecutor which is designed for generic PostgreSQL-compatible databases,
// CRDBExecutor provides access to CockroachDB-specific features like transaction
// QoS, SELECT FOR UPDATE, and follower reads.
//
// The executor is safe for concurrent use from multiple goroutines. It maintains
// a connection pool and prepared statements that are shared across workers.
type CRDBExecutor struct {
	dialect CockroachDialect
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

var _ Executor = (*CRDBExecutor)(nil)

// NewCRDBExecutor creates a new CRDBExecutor. Init must be called before using
// the executor.
func NewCRDBExecutor() *CRDBExecutor {
	return &CRDBExecutor{}
}

// Init prepares the executor by creating connection pools and defining
// statements via SQLRunner. This must be called before any other methods.
func (e *CRDBExecutor) Init(ctx context.Context, cfg Config, connFlags *workload.ConnFlags) error {
	e.cfg = cfg

	// Create multi-connection pool.
	poolCfg := workload.NewMultiConnPoolCfgFromFlags(connFlags)
	poolCfg.MaxTotalConnections = connFlags.Concurrency + 1
	mcp, err := workload.NewMultiConnPool(ctx, poolCfg, cfg.URLs...)
	if err != nil {
		return errors.Wrap(err, "creating connection pool")
	}
	e.mcp = mcp

	// Define statements using the CockroachDB dialect.
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

// SetupWorker performs per-worker initialization. For CRDBExecutor, QoS is set
// per-transaction in BeginTx rather than per-worker, so this is a no-op.
func (e *CRDBExecutor) SetupWorker(ctx context.Context) error {
	return nil
}

// Read executes a batch read for the given keys.
func (e *CRDBExecutor) Read(ctx context.Context, keys []interface{}) (Rows, error) {
	rows, err := e.readStmt.Query(ctx, keys...)
	if err != nil {
		return nil, err
	}
	return &pgxRowsWrapper{rows: rows}, nil
}

// FollowerRead executes a stale/replica read using CockroachDB's
// AS OF SYSTEM TIME follower_read_timestamp() feature.
func (e *CRDBExecutor) FollowerRead(ctx context.Context, keys []interface{}) (Rows, error) {
	rows, err := e.followerReadStmt.Query(ctx, keys...)
	if err != nil {
		return nil, err
	}
	return &pgxRowsWrapper{rows: rows}, nil
}

// Write upserts a batch of key-value pairs. The enum column is not included
// because it is a computed stored column in CockroachDB (AS ('v') STORED).
func (e *CRDBExecutor) Write(ctx context.Context, rows []Row) error {
	args := flattenRowArgs(rows, false)
	_, err := e.writeStmt.Exec(ctx, args...)
	return err
}

// Delete removes a batch of keys.
func (e *CRDBExecutor) Delete(ctx context.Context, keys []interface{}) error {
	_, err := e.deleteStmt.Exec(ctx, keys...)
	return err
}

// Span executes a spanning query starting at startKey with the configured limit.
// Returns the count of values scanned.
func (e *CRDBExecutor) Span(ctx context.Context, startKey interface{}, limit int) (int64, error) {
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

// BeginTx starts a transaction with the given options. For CRDBExecutor, this
// returns a crdbTxWrapper that implements both Tx and ExtendedTx interfaces,
// providing access to CockroachDB-specific transaction operations.
func (e *CRDBExecutor) BeginTx(ctx context.Context, opts TxOptions) (Tx, error) {
	pool := e.mcp.Get()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "acquiring connection")
	}

	txOpts := pgx.TxOptions{}

	// Set transaction priority if specified.
	if opts.Priority != "" {
		p := strings.ToLower(opts.Priority)
		switch p {
		case "low", "normal", "high":
			txOpts.BeginQuery = fmt.Sprintf("BEGIN PRIORITY %s", p)
		default:
			conn.Release()
			return nil, errors.Newf("invalid transaction priority: %q", opts.Priority)
		}
	}

	tx, err := conn.BeginTx(ctx, txOpts)
	if err != nil {
		conn.Release()
		return nil, errors.Wrap(err, "beginning transaction")
	}

	// Set transaction QoS if configured.
	if e.cfg.TxnQoS != "" && e.cfg.TxnQoS != "regular" {
		switch e.cfg.TxnQoS {
		case "background", "critical":
			// valid QoS values
		default:
			_ = tx.Rollback(ctx)
			conn.Release()
			return nil, errors.Newf("invalid transaction QoS: %q (must be background, regular, or critical)", e.cfg.TxnQoS)
		}
		_, err = tx.Exec(ctx, fmt.Sprintf("SET default_transaction_quality_of_service = '%s'", e.cfg.TxnQoS))
		if err != nil {
			_ = tx.Rollback(ctx)
			conn.Release()
			return nil, errors.Wrapf(err, "setting transaction QoS to %q", e.cfg.TxnQoS)
		}
	}

	return &crdbTxWrapper{
		tx:       tx,
		conn:     conn,
		executor: e,
	}, nil
}

// Close releases all resources including connection pools.
func (e *CRDBExecutor) Close() error {
	if e.mcp != nil {
		e.mcp.Close()
	}
	return nil
}

// Capabilities returns CockroachDB's full feature set. All capabilities are
// supported by CRDBExecutor.
func (e *CRDBExecutor) Capabilities() Capabilities {
	return Capabilities{
		FollowerReads:       true,
		Scatter:             true,
		Splits:              true,
		HashShardedPK:       true,
		TransactionQoS:      true,
		SelectForUpdate:     true,
		TransactionPriority: true,
		SerializationRetry:  true,
		Select1:             true,
		EnumColumn:          true,
	}
}

// crdbTxWrapper wraps a pgx transaction and provides access to prepared
// statement operations from the executor. It implements both Tx and ExtendedTx.
//
// The wrapper holds a reference to the executor to access prepared statements for
// operations like SELECT FOR UPDATE and Write within transactions.
type crdbTxWrapper struct {
	tx       pgx.Tx
	conn     *pgxpool.Conn
	executor *CRDBExecutor
}

var _ Tx = (*crdbTxWrapper)(nil)
var _ ExtendedTx = (*crdbTxWrapper)(nil)

// Exec executes a statement that does not return rows within the transaction.
func (t *crdbTxWrapper) Exec(ctx context.Context, sql string, args ...interface{}) error {
	_, err := t.tx.Exec(ctx, sql, args...)
	return err
}

// Query executes a query that returns rows within the transaction.
func (t *crdbTxWrapper) Query(ctx context.Context, sql string, args ...interface{}) (Rows, error) {
	rows, err := t.tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &pgxRowsWrapper{rows: rows}, nil
}

// Commit commits the transaction and releases the connection back to the pool.
func (t *crdbTxWrapper) Commit(ctx context.Context) error {
	err := t.tx.Commit(ctx)
	t.conn.Release()
	return err
}

// Rollback aborts the transaction and releases the connection back to the pool.
func (t *crdbTxWrapper) Rollback(ctx context.Context) error {
	err := t.tx.Rollback(ctx)
	t.conn.Release()
	return err
}

// SelectForUpdate executes a SELECT ... FOR UPDATE query within the transaction,
// locking the selected rows until the transaction completes.
func (t *crdbTxWrapper) SelectForUpdate(ctx context.Context, keys []interface{}) (Rows, error) {
	rows, err := t.executor.sfuStmt.QueryTx(ctx, t.tx, keys...)
	if err != nil {
		return nil, err
	}
	return &pgxRowsWrapper{rows: rows}, nil
}

// Select1 executes a SELECT 1 query within the transaction. This is useful
// for keeping transactions alive or as a lightweight operation in transaction
// retry loops.
func (t *crdbTxWrapper) Select1(ctx context.Context) error {
	row := t.executor.sel1Stmt.QueryRowTx(ctx, t.tx)
	var dummy int
	return row.Scan(&dummy)
}

// Write executes an upsert within the transaction using prepared statements.
// The enum column is not included because it is a computed stored column in
// CockroachDB.
func (t *crdbTxWrapper) Write(ctx context.Context, rows []Row) error {
	args := flattenRowArgs(rows, false)
	_, err := t.executor.writeStmt.ExecTx(ctx, t.tx, args...)
	return err
}
