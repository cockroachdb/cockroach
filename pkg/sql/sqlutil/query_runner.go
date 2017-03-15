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
//
// Author: Nikhil Benesch (nikhil.benesch@gmail.com)

package sqlutil

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

// QueryRunner abstracts the services provided by a planner object to the other
// components, both within the SQL front-end and without, that wish to execute
// SQL queries. It is extracted into sqlutil to avoid circular references.
type QueryRunner interface {
	// QueryRow executes a SQL query string where exactly 1 result row is
	// expected and returns that row.
	QueryRow(ctx context.Context, sql string, args ...interface{}) (parser.Datums, error)

	// QueryRow is like QueryRow, but executes the query as security.RootUser.
	QueryRowAsRoot(ctx context.Context, sql string, args ...interface{}) (parser.Datums, error)

	// QueryRows executes a SQL query string where multiple result rows are returned.
	QueryRows(ctx context.Context, sql string, args ...interface{}) ([]parser.Datums, error)

	// QueryRowsAsRoot is like QueryRows, but executes the query as security.RootUser.
	QueryRowsAsRoot(ctx context.Context, sql string, args ...interface{}) ([]parser.Datums, error)

	// Exec executes a SQL query string and returns the number of rows affected.
	Exec(ctx context.Context, sql string, args ...interface{}) (int, error)

	// ExecAsRoot is like Exec, but executes the query as security.RootUser.
	ExecAsRoot(ctx context.Context, sql string, args ...interface{}) (int, error)

	// Txn returns the transaction associated with the query runner.
	Txn() *client.Txn

	// Close releases the resources associated with the query runner.
	Close()
}

// QueryRunnerFactory is meant to be used by layers below SQL that cannot
// directly call sql.MakeQueryRunner due to circular references.
type QueryRunnerFactory interface {
	MakeQueryRunner(opName string, txn *client.Txn, user string) QueryRunner
}
