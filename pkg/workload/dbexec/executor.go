// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package dbexec provides database-agnostic execution interfaces for workloads.
//
// This package abstracts database operations for workloads like kv to support
// multiple databases (CockroachDB, PostgreSQL, Aurora, DSQL, Spanner). Each
// database has different SQL syntax, capabilities, and connection handling,
// which are encapsulated by the Executor and Dialect interfaces.
package dbexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/workload"
)

// Row represents a key-value pair for the kv workload.
type Row struct {
	K interface{} // int64 or string depending on key type
	V []byte
	E *string // optional enum value, nil if not using enum
}

// Rows is an iterator over query results. Implementations must be safe
// to use from a single goroutine and must be closed after use.
type Rows interface {
	// Next advances to the next row, returning false when no more rows exist
	// or an error occurred.
	Next() bool

	// Scan copies the columns from the current row into dest. The number of
	// dest values must match the number of columns in the result set.
	Scan(dest ...interface{}) error

	// Err returns any error encountered during iteration.
	Err() error

	// Close releases resources associated with the result set.
	Close()
}

// Capabilities indicates which features the executor supports. This allows
// workloads to adapt their behavior based on database capabilities.
type Capabilities struct {
	// FollowerReads indicates support for stale/replica reads (e.g., CockroachDB's
	// AS OF SYSTEM TIME follower_read_timestamp()).
	FollowerReads bool

	// Scatter indicates support for the ALTER TABLE ... SCATTER command to
	// distribute ranges across the cluster.
	Scatter bool

	// Splits indicates support for the ALTER TABLE ... SPLIT AT command to
	// manually split ranges at specific keys.
	Splits bool

	// HashShardedPK indicates support for hash-sharded primary keys using
	// PRIMARY KEY USING HASH WITH (bucket_count = N).
	HashShardedPK bool

	// TransactionQoS indicates support for transaction priority/QoS settings
	// (e.g., CockroachDB's SET TRANSACTION PRIORITY).
	TransactionQoS bool

	// SelectForUpdate indicates support for SELECT ... FOR UPDATE locking.
	SelectForUpdate bool

	// TransactionPriority indicates support for BEGIN PRIORITY {low|normal|high}.
	TransactionPriority bool

	// SerializationRetry indicates the executor handles serialization failures gracefully.
	SerializationRetry bool

	// Select1 indicates the executor supports SELECT 1 as a transaction warmup
	// to prevent automatic transaction-level retries. This is a CockroachDB-
	// specific optimization used by --sel1-writes.
	Select1 bool

	// EnumColumn indicates support for enum types in schema.
	EnumColumn bool
}

// Config holds executor configuration. These values are typically derived
// from workload flags and control table schema and query generation.
type Config struct {
	// URLs contains database connection strings. Multiple URLs enable
	// connection distribution across nodes.
	URLs []string

	// Table is the name of the kv table to operate on.
	Table string

	// BatchSize is the number of rows per batch operation (read, write, delete).
	BatchSize int

	// KeyType specifies the primary key column type, either "BIGINT" or
	// "STRING(N)" where N is the string length.
	KeyType string

	// SecondaryIndex controls whether to create a secondary index on the
	// value column.
	SecondaryIndex bool

	// NumShards specifies the number of hash buckets for hash-sharded primary
	// keys. Only used when the database supports HashShardedPK.
	NumShards int

	// Concurrency is the number of concurrent workers accessing the database.
	Concurrency int

	// Enum controls whether to include an enum column in the schema.
	Enum bool

	// SpanLimit is the default LIMIT for spanning queries.
	SpanLimit int

	// TxnQoS sets transaction quality of service (background/regular/critical).
	// Only supported by CockroachDB.
	TxnQoS string
}

// TxOptions configures transaction behavior.
type TxOptions struct {
	// Priority sets the transaction priority level. Valid values are "low",
	// "normal", and "high". Not all databases support transaction priorities.
	Priority string
}

// Tx represents a database transaction. All operations within a transaction
// see a consistent snapshot and are committed atomically.
type Tx interface {
	// Exec executes a statement that does not return rows.
	Exec(ctx context.Context, sql string, args ...interface{}) error

	// Query executes a query that returns rows.
	Query(ctx context.Context, sql string, args ...interface{}) (Rows, error)

	// Commit commits the transaction. After Commit returns, the transaction
	// is no longer valid.
	Commit(ctx context.Context) error

	// Rollback aborts the transaction. After Rollback returns, the transaction
	// is no longer valid.
	Rollback(ctx context.Context) error
}

// ExtendedTx extends Tx with transactional operations that use prepared
// statements. Both CRDBExecutor and PGXExecutor implement this interface,
// enabling transactional write workflows like SELECT FOR UPDATE → delay →
// write across multiple database backends.
type ExtendedTx interface {
	Tx

	// SelectForUpdate executes SELECT ... FOR UPDATE within the transaction.
	SelectForUpdate(ctx context.Context, keys []interface{}) (Rows, error)

	// Select1 executes SELECT 1 within the transaction. On CockroachDB, this
	// prevents automatic transaction-level retries. On other databases, it is
	// a harmless no-op.
	Select1(ctx context.Context) error

	// Write executes the prepared write statement within the transaction.
	Write(ctx context.Context, rows []Row) error
}

// flattenRowArgs converts a slice of Rows into a flat argument list for
// parameterized SQL statements. If includeEnum is true, each row contributes
// (K, V, E); otherwise each row contributes (K, V).
func flattenRowArgs(rows []Row, includeEnum bool) []interface{} {
	if includeEnum {
		args := make([]interface{}, 0, len(rows)*3)
		for _, row := range rows {
			args = append(args, row.K, row.V, row.E)
		}
		return args
	}
	args := make([]interface{}, 0, len(rows)*2)
	for _, row := range rows {
		args = append(args, row.K, row.V)
	}
	return args
}

// Executor abstracts database operations for the kv workload. Each
// implementation handles connection pooling, SQL generation, and execution
// for a specific database or family of databases.
//
// Implementations must be safe for concurrent use from multiple goroutines.
type Executor interface {
	// Init prepares the executor by creating connection pools and optionally
	// preparing statements. This must be called before any other methods.
	Init(ctx context.Context, cfg Config, connFlags *workload.ConnFlags) error

	// Read executes a batch read for the given keys. The returned Rows
	// iterator yields (k, v) pairs for keys that exist.
	Read(ctx context.Context, keys []interface{}) (Rows, error)

	// FollowerRead executes a stale/replica read if supported by the database.
	// Falls back to a regular read if follower reads are not supported.
	FollowerRead(ctx context.Context, keys []interface{}) (Rows, error)

	// Write upserts a batch of key-value pairs. Existing keys are updated;
	// new keys are inserted.
	Write(ctx context.Context, rows []Row) error

	// Delete removes a batch of keys. Non-existent keys are silently ignored.
	Delete(ctx context.Context, keys []interface{}) error

	// Span executes a spanning query starting at startKey with the given limit.
	// Returns the count of values scanned.
	Span(ctx context.Context, startKey interface{}, limit int) (int64, error)

	// BeginTx starts a transaction with the given options. The returned Tx
	// must be committed or rolled back.
	BeginTx(ctx context.Context, opts TxOptions) (Tx, error)

	// Close releases all resources including connection pools. After Close
	// returns, the executor must not be used.
	Close() error

	// Capabilities returns what features this executor supports.
	Capabilities() Capabilities

	// SetupWorker performs per-worker initialization. Called once when each
	// worker starts, before any operations. Currently a no-op for all
	// executors; retained as an extension point for future executor types.
	SetupWorker(ctx context.Context) error
}
