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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// InternalExecutor is meant to be used by layers below SQL in the system that
// nevertheless want to execute SQL queries (presumably against system tables).
// It is extracted in this "sql/util" package to avoid circular references and
// is implemented by *sql.InternalExecutor.
//
// Note that implementations might be "session bound" - meaning they inherit
// session variables from a parent session - or not.
type InternalExecutor interface {
	// Exec executes the supplied SQL statement. Statements are currently executed
	// as the root user with the system database as current database.
	//
	// If txn is not nil, the statement will be executed in the respective txn.
	//
	// Returns the number of rows affected.
	Exec(
		ctx context.Context, opName string, txn *client.Txn, statement string, params ...interface{},
	) (int, error)

	// Query executes the supplied SQL statement and returns the resulting rows.
	// The statement is executed as the root user.
	//
	// If txn is not nil, the statement will be executed in the respective txn.
	Query(
		ctx context.Context, opName string, txn *client.Txn, statement string, qargs ...interface{},
	) ([]tree.Datums, error)

	// QueryWithCols executes the supplied SQL statement and returns the resulting
	// rows and their column types.
	// The statement is executed as the root user.
	//
	// If txn is not nil, the statement will be executed in the respective txn.
	QueryWithCols(
		ctx context.Context, opName string, txn *client.Txn, statement string, qargs ...interface{},
	) ([]tree.Datums, sqlbase.ResultColumns, error)

	// QueryRow is like Query, except it returns a single row, or nil if not row is
	// found, or an error if more that one row is returned.
	QueryRow(
		ctx context.Context, opName string, txn *client.Txn, statement string, qargs ...interface{},
	) (tree.Datums, error)
}

// InternalExecutorWithUser is like an InternalExecutor but allows the client to
// specify the user for use during execution.
type InternalExecutorWithUser interface {
	InternalExecutor

	// QueryWithUser is like Query, except it changes the username to that
	// specified.
	QueryWithUser(
		ctx context.Context,
		opName string,
		txn *client.Txn,
		userName string,
		stmt string,
		qargs ...interface{},
	) ([]tree.Datums, sqlbase.ResultColumns, error)

	// ExecWithUser is like Exec, except it changes the username to that
	// specified.
	ExecWithUser(
		ctx context.Context,
		opName string,
		txn *client.Txn,
		userName string,
		stmt string,
		qargs ...interface{},
	) (int, error)
}

// SessionBoundInternalExecutorFactory is a function that produces a "session
// bound" internal executor.
type SessionBoundInternalExecutorFactory func(
	context.Context, *sessiondata.SessionData,
) InternalExecutor
