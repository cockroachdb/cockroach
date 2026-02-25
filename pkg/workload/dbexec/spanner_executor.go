// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dbexec

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/errors"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
)

// SpannerExecutor implements Executor for Google Cloud Spanner. Unlike SQL-based
// executors, SpannerExecutor uses the Spanner mutations API for writes, which
// provides better performance and is the idiomatic way to interact with Spanner.
//
// Connection format: projects/PROJECT/instances/INSTANCE/databases/DATABASE
type SpannerExecutor struct {
	client *spanner.Client
	cfg    Config
}

// NewSpannerExecutor creates a new SpannerExecutor. Init must be called before
// using the executor.
func NewSpannerExecutor() *SpannerExecutor {
	return &SpannerExecutor{}
}

var _ Executor = (*SpannerExecutor)(nil)

// Init creates the Spanner client from the database path in cfg.URLs[0].
// The connFlags parameter is not used for Spanner as it has its own connection
// handling.
func (e *SpannerExecutor) Init(
	ctx context.Context, cfg Config, connFlags *workload.ConnFlags,
) error {
	if len(cfg.URLs) == 0 {
		return errors.New("spanner executor requires at least one URL")
	}

	// The URL should be in the format:
	// projects/PROJECT/instances/INSTANCE/databases/DATABASE
	dbPath := cfg.URLs[0]
	log.Printf("[spanner] connecting to database: %s", dbPath)
	log.Printf("[spanner] target table: %s", cfg.Table)
	client, err := spanner.NewClient(ctx, dbPath)
	if err != nil {
		return errors.Wrap(err, "creating spanner client")
	}

	e.client = client
	e.cfg = cfg

	// Validate connection by executing a simple query to check if the table exists.
	// This provides a clearer error message than failing on first write.
	if err := e.validateTable(ctx); err != nil {
		client.Close()
		return err
	}

	return nil
}

// validateTable checks that the target table exists and is accessible.
// This uses a simple read operation that will trigger session pool creation
// and validate the table exists.
func (e *SpannerExecutor) validateTable(ctx context.Context) error {
	// Don't add a timeout here - Spanner's session pool initialization can take
	// 60+ seconds on first connection. The Spanner client has its own timeouts.
	stmt := spanner.Statement{
		SQL: fmt.Sprintf("SELECT k FROM %s LIMIT 1", e.cfg.Table),
	}
	log.Printf("[spanner] validating table with query: %s", stmt.SQL)
	iter := e.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	// Try to get one row - we don't care about the result, just that the query works.
	log.Printf("[spanner] waiting for query result (session pool init may take 60+ seconds)...")
	_, err := iter.Next()
	if err != nil && !errors.Is(err, iterator.Done) {
		// Check if this is a "table not found" error.
		if spanner.ErrCode(err) == codes.NotFound ||
			strings.Contains(err.Error(), "not found") ||
			strings.Contains(err.Error(), "Table not found") ||
			strings.Contains(err.Error(), "does not exist") {
			return errors.Newf(
				"table %q does not exist in Spanner database. "+
					"Please create it manually before running the workload:\n\n"+
					"  CREATE TABLE %s (\n"+
					"    k INT64 NOT NULL,\n"+
					"    v BYTES(MAX),\n"+
					"  ) PRIMARY KEY (k)",
				e.cfg.Table, e.cfg.Table)
		}
		// For timeout errors during session creation, provide helpful context.
		if spanner.ErrCode(err) == codes.DeadlineExceeded {
			return errors.Wrapf(err,
				"timeout connecting to Spanner (session pool initialization can take 60+ seconds). "+
					"Try running again, or check network connectivity and authentication")
		}
		return errors.Wrapf(err, "validating Spanner table %q", e.cfg.Table)
	}
	log.Printf("[spanner] table %q validated successfully", e.cfg.Table)
	return nil
}

// Read executes a batch read for the given keys using Spanner's Read API.
func (e *SpannerExecutor) Read(ctx context.Context, keys []interface{}) (Rows, error) {
	keySet := spanner.KeySetFromKeys(toSpannerKeys(keys)...)
	iter := e.client.Single().Read(ctx, e.cfg.Table, keySet, []string{"k", "v"})
	return &spannerRowsWrapper{iter: iter}, nil
}

// FollowerRead executes a stale read using Spanner's exact staleness bound.
// This allows reads from any replica, reducing latency for read-heavy workloads.
func (e *SpannerExecutor) FollowerRead(ctx context.Context, keys []interface{}) (Rows, error) {
	keySet := spanner.KeySetFromKeys(toSpannerKeys(keys)...)
	// 15 seconds staleness is a reasonable default for follower reads.
	ro := e.client.ReadOnlyTransaction().WithTimestampBound(spanner.ExactStaleness(15 * time.Second))

	iter := ro.Read(ctx, e.cfg.Table, keySet, []string{"k", "v"})
	return &spannerStaleRowsWrapper{
		spannerRowsWrapper: spannerRowsWrapper{iter: iter},
		txn:                ro,
	}, nil
}

// maxMutationsPerBatch is the recommended limit for Spanner mutations per Apply call.
// Larger batches can cause timeout issues.
const maxMutationsPerBatch = 100

// Write upserts a batch of key-value pairs using Spanner mutations.
// Uses InsertOrUpdate which inserts new rows or updates existing ones.
// For large batches, mutations are split into smaller batches to avoid timeouts.
func (e *SpannerExecutor) Write(ctx context.Context, rows []Row) error {
	// For small batches, apply directly.
	if len(rows) <= maxMutationsPerBatch {
		return e.applyWriteMutations(ctx, rows)
	}

	// For large batches, split into smaller chunks.
	for i := 0; i < len(rows); i += maxMutationsPerBatch {
		end := i + maxMutationsPerBatch
		if end > len(rows) {
			end = len(rows)
		}
		if err := e.applyWriteMutations(ctx, rows[i:end]); err != nil {
			return errors.Wrapf(err, "batch [%d:%d]", i, end)
		}
	}
	return nil
}

// applyWriteMutations applies a batch of write mutations.
func (e *SpannerExecutor) applyWriteMutations(ctx context.Context, rows []Row) error {
	mutations := make([]*spanner.Mutation, len(rows))
	for i, row := range rows {
		if e.cfg.Enum {
			var enumVal interface{}
			if row.E != nil {
				enumVal = *row.E
			}
			mutations[i] = spanner.InsertOrUpdate(
				e.cfg.Table,
				[]string{"k", "v", "e"},
				[]interface{}{row.K, row.V, enumVal},
			)
		} else {
			mutations[i] = spanner.InsertOrUpdate(
				e.cfg.Table,
				[]string{"k", "v"},
				[]interface{}{row.K, row.V},
			)
		}
	}
	_, err := e.client.Apply(ctx, mutations)
	if err != nil {
		return errors.Wrap(err, "applying write mutations")
	}
	return nil
}

// Delete removes a batch of keys using Spanner mutations.
func (e *SpannerExecutor) Delete(ctx context.Context, keys []interface{}) error {
	mutations := make([]*spanner.Mutation, len(keys))
	for i, key := range keys {
		mutations[i] = spanner.Delete(e.cfg.Table, spanner.Key{key})
	}
	_, err := e.client.Apply(ctx, mutations)
	if err != nil {
		return errors.Wrap(err, "applying delete mutations")
	}
	return nil
}

// Span executes a spanning query starting at startKey with the given limit.
// Uses SQL via Spanner's Query API since this requires ordered iteration.
func (e *SpannerExecutor) Span(
	ctx context.Context, startKey interface{}, limit int,
) (int64, error) {
	var stmt spanner.Statement
	if limit == 0 {
		// Full table scan -- no WHERE clause, no parameters.
		stmt = spanner.Statement{
			SQL: fmt.Sprintf("SELECT COUNT(v) FROM %s", e.cfg.Table),
		}
	} else {
		stmt = spanner.Statement{
			SQL: fmt.Sprintf(
				"SELECT COUNT(v) FROM (SELECT v FROM %s WHERE k >= @startKey ORDER BY k LIMIT %d)",
				e.cfg.Table, limit,
			),
			Params: map[string]interface{}{
				"startKey": startKey,
			},
		}
	}

	iter := e.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		return 0, errors.Wrap(err, "executing span query")
	}

	var count int64
	if err := row.Columns(&count); err != nil {
		return 0, errors.Wrap(err, "scanning count")
	}

	return count, nil
}

// BeginTx starts a transaction that accumulates mutations.
// Spanner transactions work differently from SQL databases - mutations are
// accumulated and applied atomically on commit.
func (e *SpannerExecutor) BeginTx(ctx context.Context, opts TxOptions) (Tx, error) {
	return &spannerTxWrapper{
		client: e.client,
		table:  e.cfg.Table,
	}, nil
}

// Close releases all resources including the Spanner client.
func (e *SpannerExecutor) Close() error {
	if e.client != nil {
		e.client.Close()
	}
	return nil
}

// Capabilities returns Spanner's feature set. Spanner supports stale reads
// (follower reads) but does not support CockroachDB-specific features like
// scatter, splits, or hash-sharded primary keys. Enum columns are supported
// by storing enum values as strings.
func (e *SpannerExecutor) Capabilities() Capabilities {
	return Capabilities{
		FollowerReads:       true, // via stale reads
		Scatter:             false,
		Splits:              false, // Spanner auto-splits
		HashShardedPK:       false,
		TransactionQoS:      false,
		SelectForUpdate:     false,
		TransactionPriority: false,
		SerializationRetry:  false,
		EnumColumn:          true, // stored as strings
	}
}

// SetupWorker is a no-op for SpannerExecutor. CRDB-specific per-worker setup
// (like transaction QoS) is not supported for Spanner.
func (e *SpannerExecutor) SetupWorker(ctx context.Context) error {
	return nil
}

// toSpannerKeys converts a slice of interface{} keys to spanner.Key values.
func toSpannerKeys(keys []interface{}) []spanner.Key {
	result := make([]spanner.Key, len(keys))
	for i, k := range keys {
		result[i] = spanner.Key{k}
	}
	return result
}

// spannerRowsWrapper wraps a Spanner RowIterator to implement the Rows interface.
type spannerRowsWrapper struct {
	iter *spanner.RowIterator
	row  *spanner.Row
	err  error
}

var _ Rows = (*spannerRowsWrapper)(nil)

// Next advances to the next row.
func (w *spannerRowsWrapper) Next() bool {
	w.row, w.err = w.iter.Next()
	return w.err == nil
}

// Scan copies the columns from the current row into dest.
func (w *spannerRowsWrapper) Scan(dest ...interface{}) error {
	if w.row == nil {
		return errors.New("no current row")
	}
	return w.row.Columns(dest...)
}

// Err returns any error encountered during iteration.
// Returns nil if the error is iterator.Done (normal end of iteration).
func (w *spannerRowsWrapper) Err() error {
	if errors.Is(w.err, iterator.Done) {
		return nil
	}
	return w.err
}

// Close releases resources associated with the iterator.
func (w *spannerRowsWrapper) Close() {
	w.iter.Stop()
}

// spannerStaleRowsWrapper extends spannerRowsWrapper to also close the
// read-only transaction when the rows are closed.
type spannerStaleRowsWrapper struct {
	spannerRowsWrapper
	txn *spanner.ReadOnlyTransaction
}

var _ Rows = (*spannerStaleRowsWrapper)(nil)

// Close releases resources associated with the iterator and transaction.
func (w *spannerStaleRowsWrapper) Close() {
	w.iter.Stop()
	w.txn.Close()
}

// spannerTxWrapper implements Tx for Spanner by accumulating mutations.
// Unlike SQL transactions, Spanner mutations are buffered and applied
// atomically when Commit is called.
type spannerTxWrapper struct {
	client    *spanner.Client
	table     string
	mutations []*spanner.Mutation
}

var _ Tx = (*spannerTxWrapper)(nil)

// Exec executes a statement by converting it to mutations.
// This is a simplified implementation that only supports basic INSERT/UPDATE/DELETE.
// For complex operations, use Query or direct mutation methods.
func (t *spannerTxWrapper) Exec(ctx context.Context, sql string, args ...interface{}) error {
	// For Spanner, we expect callers to use the mutation-based Write/Delete methods
	// rather than SQL. This Exec implementation is provided for compatibility but
	// has limited functionality.
	return errors.New("Exec not fully supported in Spanner transactions; use Write/Delete methods")
}

// Query executes a query within the transaction context.
func (t *spannerTxWrapper) Query(
	ctx context.Context, sql string, args ...interface{},
) (Rows, error) {
	// Build parameters map from positional args.
	params := make(map[string]interface{})
	for i, arg := range args {
		params[fmt.Sprintf("p%d", i+1)] = arg
	}

	stmt := spanner.Statement{
		SQL:    sql,
		Params: params,
	}

	iter := t.client.Single().Query(ctx, stmt)
	return &spannerRowsWrapper{iter: iter}, nil
}

// Commit applies all accumulated mutations atomically.
func (t *spannerTxWrapper) Commit(ctx context.Context) error {
	if len(t.mutations) == 0 {
		return nil
	}
	_, err := t.client.Apply(ctx, t.mutations)
	if err != nil {
		return errors.Wrap(err, "committing spanner transaction")
	}
	t.mutations = nil
	return nil
}

// Rollback discards all accumulated mutations.
func (t *spannerTxWrapper) Rollback(ctx context.Context) error {
	t.mutations = nil
	return nil
}

// AddMutation adds a mutation to be applied on commit.
// This is a Spanner-specific method for building up transaction mutations.
func (t *spannerTxWrapper) AddMutation(m *spanner.Mutation) {
	t.mutations = append(t.mutations, m)
}
