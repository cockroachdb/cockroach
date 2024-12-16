// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package isql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/redact"
)

// DB enables clients to create and execute sql transactions from code inside
// the database. Multi-statement transactions should leverage the Txn method.
type DB interface {

	// KV returns the underlying *kv.DB.
	KV() *kv.DB

	// Txn enables callers to run transactions with a *Collection such that all
	// retrieved immutable descriptors are properly leased and all mutable
	// descriptors are handled. The function deals with verifying the two version
	// invariant and retrying when it is violated. Callers need not worry that they
	// write mutable descriptors multiple times. The call will explicitly wait for
	// the leases to drain on old versions of descriptors modified or deleted in the
	// transaction; callers do not need to call lease.WaitForOneVersion.
	// It also enables using internal executor to run sql queries in a txn manner.
	Txn(context.Context, func(context.Context, Txn) error, ...TxnOption) error

	// Executor constructs an internal executor not bound to a transaction.
	Executor(...ExecutorOption) Executor
}

// Txn is an internal sql transaction.
type Txn interface {

	// KV returns the underlying kv.Txn.
	KV() *kv.Txn

	// SessionData returns the transaction's SessionData.
	SessionData() *sessiondata.SessionData

	// GetSystemSchemaVersion exposes the schema version from the system db desc.
	GetSystemSchemaVersion(context.Context) (roachpb.Version, error)

	// Executor allows the user to execute transactional SQL statements.
	Executor
}

// Executor is meant to be used by layers below SQL in the system that
// nevertheless want to execute SQL queries (presumably against system tables).
// It is extracted in this "isql" package to avoid circular references and
// is implemented by *sql.InternalExecutor.
//
// TODO(ajwerner): Remove the txn argument from all the functions. They are
// now implicit -- if you have your hands on an isql.Txn, you know it's
// transactional. If you just have an Executor, you don't know, but you
// cannot assume one way or the other.
type Executor interface {
	// Exec executes the supplied SQL statement and returns the number of rows
	// affected (not like the full results; see QueryIterator()). If no user has
	// been previously set through SetSessionData, the statement is executed as
	// the root user.
	//
	// If txn is not nil, the statement will be executed in the respective txn.
	//
	// Exec is deprecated because it may transparently execute a query as root.
	// Use ExecEx instead.
	Exec(
		ctx context.Context, opName redact.RedactableString, txn *kv.Txn, statement string, params ...interface{},
	) (int, error)

	// ExecEx is like Exec, but allows the caller to override some session data
	// fields.
	//
	// The fields set in session that are set override the respective fields if
	// they have previously been set through SetSessionData().
	ExecEx(
		ctx context.Context,
		opName redact.RedactableString,
		txn *kv.Txn,
		o sessiondata.InternalExecutorOverride,
		stmt string,
		qargs ...interface{},
	) (int, error)

	// ExecParsed is like Exec but allows the caller to provide an already
	// parsed statement.
	ExecParsed(
		ctx context.Context,
		opName redact.RedactableString,
		txn *kv.Txn,
		o sessiondata.InternalExecutorOverride,
		parsedStmt statements.Statement[tree.Statement],
		qargs ...interface{},
	) (int, error)

	// QueryRow executes the supplied SQL statement and returns a single row, or
	// nil if no row is found, or an error if more than one row is returned.
	//
	// QueryRow is deprecated. Use QueryRowEx() instead.
	QueryRow(
		ctx context.Context, opName redact.RedactableString, txn *kv.Txn, statement string, qargs ...interface{},
	) (tree.Datums, error)

	// QueryRowEx is like QueryRow, but allows the caller to override some
	// session data fields.
	//
	// The fields set in session that are set override the respective fields if
	// they have previously been set through SetSessionData().
	QueryRowEx(
		ctx context.Context,
		opName redact.RedactableString,
		txn *kv.Txn,
		session sessiondata.InternalExecutorOverride,
		stmt string,
		qargs ...interface{},
	) (tree.Datums, error)

	// QueryRowExParsed is like QueryRowEx, but allows the caller to provide an
	// already parsed statement.
	QueryRowExParsed(
		ctx context.Context,
		opName redact.RedactableString,
		txn *kv.Txn,
		session sessiondata.InternalExecutorOverride,
		parsedStmt statements.Statement[tree.Statement],
		qargs ...interface{},
	) (tree.Datums, error)

	// QueryRowExWithCols is like QueryRowEx, additionally returning the
	// computed ResultColumns of the input query.
	QueryRowExWithCols(
		ctx context.Context,
		opName redact.RedactableString,
		txn *kv.Txn,
		session sessiondata.InternalExecutorOverride,
		stmt string,
		qargs ...interface{},
	) (tree.Datums, colinfo.ResultColumns, error)

	// QueryBuffered executes the supplied SQL statement and returns the
	// resulting rows (meaning all of them are buffered at once). If no user has
	// been previously set through SetSessionData, the statement is executed as
	// the root user.
	//
	// If txn is not nil, the statement will be executed in the respective txn.
	//
	// QueryBuffered is deprecated because it may transparently execute a query
	// as root. Use QueryBufferedEx instead.
	QueryBuffered(
		ctx context.Context,
		opName redact.RedactableString,
		txn *kv.Txn,
		stmt string,
		qargs ...interface{},
	) ([]tree.Datums, error)

	// QueryBufferedEx executes the supplied SQL statement and returns the
	// resulting rows (meaning all of them are buffered at once).
	//
	// If txn is not nil, the statement will be executed in the respective txn.
	//
	// The fields set in session that are set override the respective fields if
	// they have previously been set through SetSessionData().
	QueryBufferedEx(
		ctx context.Context,
		opName redact.RedactableString,
		txn *kv.Txn,
		session sessiondata.InternalExecutorOverride,
		stmt string,
		qargs ...interface{},
	) ([]tree.Datums, error)

	// QueryIterator executes the query, returning an iterator that can be used
	// to get the results. If the call is successful, the returned iterator
	// *must* be closed.
	//
	// QueryIterator is deprecated because it may transparently execute a query
	// as root. Use QueryIteratorEx instead.
	QueryIterator(
		ctx context.Context,
		opName redact.RedactableString,
		txn *kv.Txn,
		stmt string,
		qargs ...interface{},
	) (Rows, error)

	// QueryIteratorEx executes the query, returning an iterator that can be
	// used to get the results. If the call is successful, the returned iterator
	// *must* be closed.
	QueryIteratorEx(
		ctx context.Context,
		opName redact.RedactableString,
		txn *kv.Txn,
		session sessiondata.InternalExecutorOverride,
		stmt string,
		qargs ...interface{},
	) (Rows, error)

	// QueryBufferedExWithCols is like QueryBufferedEx, additionally returning the computed
	// ResultColumns of the input query.
	QueryBufferedExWithCols(
		ctx context.Context,
		opName redact.RedactableString,
		txn *kv.Txn,
		session sessiondata.InternalExecutorOverride,
		stmt string,
		qargs ...interface{},
	) ([]tree.Datums, colinfo.ResultColumns, error)

	// WithSyntheticDescriptors sets the synthetic descriptors before running the
	// the provided closure and resets them afterward. Used for queries/statements
	// that need to use in-memory synthetic descriptors different from descriptors
	// written to disk. These descriptors override all other descriptors on the
	// immutable resolution path.
	//
	// Warning: Not safe for concurrent use from multiple goroutines. This API is
	// flawed in that the internal executor is meant to function as a stateless
	// wrapper, and creates a new connExecutor and descs.Collection on each query/
	// statement, so these descriptors should really be specified at a per-query/
	// statement level. See #34304.
	WithSyntheticDescriptors(
		descs []catalog.Descriptor, run func() error,
	) error
}

// Rows is an iterator interface that's exposed by the internal
// executor. It provides access to the rows from a query.
type Rows interface {
	// Next advances the iterator by one row, returning false if there are no
	// more rows in this iterator or if an error is encountered (the latter is
	// then returned).
	//
	// The iterator is automatically closed when false is returned, consequent
	// calls to Next will return the same values as when the iterator was
	// closed.
	Next(context.Context) (bool, error)

	// Cur returns the row at the current position of the iterator. The row is
	// safe to hold onto (meaning that calling Next() or Close() will not
	// invalidate it).
	Cur() tree.Datums

	// RowsAffected returns the count of rows affected by the statement.
	// This is only guaranteed to be accurate after Next() has returned
	// false (no more rows).
	RowsAffected() int

	// Close closes this iterator, releasing any resources it held open. Close
	// is idempotent and *must* be called once the caller is done with the
	// iterator.
	Close() error

	// Types returns the types of the columns returned by this iterator. The
	// returned array is guaranteed to correspond 1:1 with the tree.Datums rows
	// returned by Cur().
	Types() colinfo.ResultColumns

	// HasResults returns true if there are results to the query, false otherwise.
	HasResults() bool
}
