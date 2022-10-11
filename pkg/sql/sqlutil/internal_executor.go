// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlutil

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// InternalExecutor is meant to be used by layers below SQL in the system that
// nevertheless want to execute SQL queries (presumably against system tables).
// It is extracted in this "sqlutil" package to avoid circular references and
// is implemented by *sql.InternalExecutor.
type InternalExecutor interface {
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
		ctx context.Context, opName string, txn *kv.Txn, statement string, params ...interface{},
	) (int, error)

	// ExecEx is like Exec, but allows the caller to override some session data
	// fields.
	//
	// The fields set in session that are set override the respective fields if
	// they have previously been set through SetSessionData().
	ExecEx(
		ctx context.Context,
		opName string,
		txn *kv.Txn,
		o sessiondata.InternalExecutorOverride,
		stmt string,
		qargs ...interface{},
	) (int, error)

	// QueryRow executes the supplied SQL statement and returns a single row, or
	// nil if no row is found, or an error if more that one row is returned.
	//
	// QueryRow is deprecated. Use QueryRowEx() instead.
	QueryRow(
		ctx context.Context, opName string, txn *kv.Txn, statement string, qargs ...interface{},
	) (tree.Datums, error)

	// QueryRowEx is like QueryRow, but allows the caller to override some
	// session data fields.
	//
	// The fields set in session that are set override the respective fields if
	// they have previously been set through SetSessionData().
	QueryRowEx(
		ctx context.Context,
		opName string,
		txn *kv.Txn,
		session sessiondata.InternalExecutorOverride,
		stmt string,
		qargs ...interface{},
	) (tree.Datums, error)

	// QueryRowExWithCols is like QueryRowEx, additionally returning the
	// computed ResultColumns of the input query.
	QueryRowExWithCols(
		ctx context.Context,
		opName string,
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
		opName string,
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
		opName string,
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
		opName string,
		txn *kv.Txn,
		stmt string,
		qargs ...interface{},
	) (InternalRows, error)

	// QueryIteratorEx executes the query, returning an iterator that can be
	// used to get the results. If the call is successful, the returned iterator
	// *must* be closed.
	QueryIteratorEx(
		ctx context.Context,
		opName string,
		txn *kv.Txn,
		session sessiondata.InternalExecutorOverride,
		stmt string,
		qargs ...interface{},
	) (InternalRows, error)

	// QueryBufferedExWithCols is like QueryBufferedEx, additionally returning the computed
	// ResultColumns of the input query.
	QueryBufferedExWithCols(
		ctx context.Context,
		opName string,
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

// InternalRows is an iterator interface that's exposed by the internal
// executor. It provides access to the rows from a query.
type InternalRows interface {
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

	// RowsAffected() returns the count of rows affected by the statement.
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
	//
	// WARNING: this method is safe to call anytime *after* the first call to
	// Next() (including after Close() was called).
	Types() colinfo.ResultColumns
}

// InternalExecutorFactory is an interface that allow the creation of an
// internal executor, and run sql statement without a txn with the internal
// executor.
type InternalExecutorFactory interface {
	// NewInternalExecutor constructs a new internal executor.
	// TODO (janexing): this should be deprecated soon.
	NewInternalExecutor(sd *sessiondata.SessionData) InternalExecutor
	// RunWithoutTxn is to create an internal executor without binding to a txn,
	// and run the passed function with this internal executor.
	RunWithoutTxn(ctx context.Context, run func(ctx context.Context, ie InternalExecutor) error) error

	// TxnWithExecutor enables callers to run transactions with a *Collection such that all
	// retrieved immutable descriptors are properly leased and all mutable
	// descriptors are handled. The function deals with verifying the two version
	// invariant and retrying when it is violated. Callers need not worry that they
	// write mutable descriptors multiple times. The call will explicitly wait for
	// the leases to drain on old versions of descriptors modified or deleted in the
	// transaction; callers do not need to call lease.WaitForOneVersion.
	// It also enables using internal executor to run sql queries in a txn manner.
	//
	// The passed transaction is pre-emptively anchored to the system config key on
	// the system tenant.
	TxnWithExecutor(context.Context, *kv.DB, *sessiondata.SessionData, func(context.Context, *kv.Txn, InternalExecutor) error, ...TxnOption) error
}

// TxnOption is used to configure a Txn or TxnWithExecutor.
type TxnOption interface {
	Apply(*TxnConfig)
}

// TxnConfig is the config to be set for txn.
type TxnConfig struct {
	steppingEnabled bool
}

// GetSteppingEnabled return the steppingEnabled setting from the txn config.
func (tc *TxnConfig) GetSteppingEnabled() bool {
	return tc.steppingEnabled
}

type txnOptionFn func(options *TxnConfig)

// Apply is to apply the txn config.
func (f txnOptionFn) Apply(options *TxnConfig) { f(options) }

var steppingEnabled = txnOptionFn(func(o *TxnConfig) {
	o.steppingEnabled = true
})

// SteppingEnabled creates a TxnOption to determine whether the underlying
// transaction should have stepping enabled. If stepping is enabled, the
// transaction will implicitly use lower admission priority. However, the
// user will need to remember to Step the Txn to make writes visible. The
// InternalExecutor will automatically (for better or for worse) step the
// transaction when executing each statement.
func SteppingEnabled() TxnOption {
	return steppingEnabled
}
