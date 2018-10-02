// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
	) ([]tree.Datums, sqlbase.ResultColumns, error)

	// QueryRow is like Query, except it returns a single row, or nil if not row is
	// found, or an error if more that one row is returned.
	QueryRow(
		ctx context.Context, opName string, txn *client.Txn, statement string, qargs ...interface{},
	) (tree.Datums, error)
}

// SessionBoundInternalExecutorFactory is a function that produces a "session
// bound" internal executor.
type SessionBoundInternalExecutorFactory func(
	context.Context, *sessiondata.SessionData,
) InternalExecutor
