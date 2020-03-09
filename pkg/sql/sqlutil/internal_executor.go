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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// InternalExecutor is meant to be used by layers below SQL in the system that
// nevertheless want to execute SQL queries (presumably against system tables).
// It is extracted in this "sql/util" package to avoid circular references and
// is implemented by *sql.InternalExecutor.
type InternalExecutor interface {
	// Exec executes the supplied SQL statement and returns the number of rows
	// affected (not like the results; see Query()). If no user has been previously
	// set through SetSessionData, the statement is executed as the root user.
	//
	// If txn is not nil, the statement will be executed in the respective txn.
	//
	// Exec is deprecated because it may transparently execute a query as root. Use
	// ExecEx instead.
	Exec(
		ctx context.Context, opName string, txn *kv.Txn, statement string, params ...interface{},
	) (int, error)

	// ExecEx is like Exec, but allows the caller to override some session data
	// fields.
	//
	// The fields set in session that are set override the respective fields if they
	// have previously been set through SetSessionData().
	ExecEx(
		ctx context.Context,
		opName string,
		txn *kv.Txn,
		o sqlbase.InternalExecutorSessionDataOverride,
		stmt string,
		qargs ...interface{},
	) (int, error)

	// Query executes the supplied SQL statement and returns the resulting rows.
	// If no user has been previously set through SetSessionData, the statement is
	// executed as the root user.
	//
	// If txn is not nil, the statement will be executed in the respective txn.
	//
	// Query is deprecated because it may transparently execute a query as root. Use
	// QueryEx instead.
	Query(
		ctx context.Context, opName string, txn *kv.Txn, statement string, qargs ...interface{},
	) ([]tree.Datums, error)

	// QueryEx is like Query, but allows the caller to override some session data
	// fields.
	//
	// The fields set in session that are set override the respective fields if
	// they have previously been set through SetSessionData().
	QueryEx(
		ctx context.Context,
		opName string,
		txn *kv.Txn,
		session sqlbase.InternalExecutorSessionDataOverride,
		stmt string,
		qargs ...interface{},
	) ([]tree.Datums, error)

	// QueryWithCols is like QueryEx, but it also returns the computed ResultColumns
	// of the input query.
	QueryWithCols(
		ctx context.Context, opName string, txn *kv.Txn,
		o sqlbase.InternalExecutorSessionDataOverride, statement string, qargs ...interface{},
	) ([]tree.Datums, sqlbase.ResultColumns, error)

	// QueryRow is like Query, except it returns a single row, or nil if not row is
	// found, or an error if more that one row is returned.
	//
	// QueryRow is deprecated (like Query). Use QueryRowEx() instead.
	QueryRow(
		ctx context.Context, opName string, txn *kv.Txn, statement string, qargs ...interface{},
	) (tree.Datums, error)

	// QueryRowEx is like QueryRow, but allows the caller to override some session data
	// fields.
	//
	// The fields set in session that are set override the respective fields if they
	// have previously been set through SetSessionData().
	QueryRowEx(
		ctx context.Context,
		opName string,
		txn *kv.Txn,
		session sqlbase.InternalExecutorSessionDataOverride,
		stmt string,
		qargs ...interface{},
	) (tree.Datums, error)
}

// SessionBoundInternalExecutorFactory is a function that produces a "session
// bound" internal executor.
type SessionBoundInternalExecutorFactory func(
	context.Context, *sessiondata.SessionData,
) InternalExecutor
