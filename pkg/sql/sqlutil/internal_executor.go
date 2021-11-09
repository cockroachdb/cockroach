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

// SessionBoundInternalExecutorFactory is a function that produces a "session
// bound" internal executor.
type SessionBoundInternalExecutorFactory func(
	context.Context, *sessiondata.SessionData,
) InternalExecutor
