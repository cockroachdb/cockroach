// Copyright 2018 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package workload

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

// SQLRunner is a helper for issuing SQL statements; it supports multiple
// methods for issuing queries.
//
// Queries need to first be defined using calls to Define. Then the runner
// must be initialized, after which we can use the handles returned by Define.
//
// Sample usage:
//   sr := &workload.SQLRunner{}
//
//   sel:= sr.Define("SELECT x FROM t WHERE y = $1")
//   ins:= sr.Define("INSERT INTO t(x, y) VALUES ($1, $2)")
//
//   err := sr.Init(ctx, db, flags)
//   // [handle err]
//   defer sr.Close()
//
//   row := sel.QueryRow(1)
//   // [use row]
//
//   _, err := ins.Exec(5, 6)
//   // [handle err]
//
// A runner should typically be associated with a single worker.
type SQLRunner struct {
	initialized bool
	stmts       []*stmt
}

type stmt struct {
	sql         string
	initialized bool
	prepared    *gosql.Stmt
}

type StmtHandle struct {
	s *stmt
}

// Define creates a handle for the given statement. The handle can be used after
// Init is called.
func (sr *SQLRunner) Define(sql string) StmtHandle {
	if sr.initialized {
		panic("Define can't be called after Init")
	}
	s := &stmt{sql: sql}
	sr.stmts = append(sr.stmts, s)
	return StmtHandle{s: s}
}

// Init initializes the runner; must be called after calls to Define and before
// the StmtHandles are used.
//
// The way we issue queries is set by flags.Method:
//
//  - "prepare": we prepare the query during Init, then we reuse it. This results
//    in a Bind and Execute on the server each time we run a query.
//
//  - "noprepare": each query is issued separately. This results in Parse,
//    Bind, Execute on the server each time we run a query.
//
func (sr *SQLRunner) Init(ctx context.Context, db *gosql.DB, flags *ConnFlags) error {
	switch strings.ToLower(flags.Method) {

	case "prepare":
		for _, s := range sr.stmts {
			var err error
			s.prepared, err = db.PrepareContext(ctx, s.sql)
			if err != nil {
				return errors.Wrapf(err, "preparing %s", s.sql)
			}
		}

	case "noprepare":
		// Nothing to do.

	default:
		return fmt.Errorf("unknown method %s", flags.Method)
	}

	for _, s := range sr.stmts {
		s.initialized = true
	}
	return nil
}

// Close cleans up the SQLRunner.
func (sr *SQLRunner) Close() {
	for _, s := range sr.stmts {
		s.initialized = false
		if s.prepared != nil {
			_ = s.prepared.Close()
		}
	}

	sr.initialized = false
}

func (h StmtHandle) check() {
	if !h.s.initialized {
		panic("SQLRunner.Init not called")
	}
}

// Exec executes a query that doesn't return rows.
// See gosql.Tx.ExecContext.
func (h StmtHandle) Exec(
	ctx context.Context, tx *gosql.Tx, args ...interface{},
) (gosql.Result, error) {
	h.check()
	if h.s.prepared != nil {
		return tx.StmtContext(ctx, h.s.prepared).ExecContext(ctx, args...)
	}
	return tx.ExecContext(ctx, h.s.sql, args...)
}

// Query executes a query that returns rows.
// See gosql.Tx.QueryContext.
func (h StmtHandle) Query(
	ctx context.Context, tx *gosql.Tx, args ...interface{},
) (*gosql.Rows, error) {
	h.check()
	if h.s.prepared != nil {
		return tx.StmtContext(ctx, h.s.prepared).QueryContext(ctx, args...)
	}
	return tx.QueryContext(ctx, h.s.sql, args...)
}

// QueryRow executes a query that is expected to return at most one row.
// See gosql.Tx.QueryRowContext.
func (h StmtHandle) QueryRow(ctx context.Context, tx *gosql.Tx, args ...interface{}) *gosql.Row {
	h.check()
	if h.s.prepared != nil {
		return tx.StmtContext(ctx, h.s.prepared).QueryRowContext(ctx, args...)
	}
	return tx.QueryRowContext(ctx, h.s.sql, args...)
}
