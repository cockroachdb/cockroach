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

	"github.com/cockroachdb/cockroach/pkg/workload/internal/sanitize"
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
//   err := sr.Init(ctx, conn, flags)
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
	stmts       []*stmt
	initialized bool
	method      method
	conn        *gosql.Conn
}

type stmt struct {
	sr  *SQLRunner
	sql string
	// prepared is only used for the prepare method.
	prepared *gosql.Stmt
	// simpleFmt is only used for the simple method. It is the sql string with
	// placeholders "$1" converted to printf-style "%[1]v".
	simpleFmt string
}

type StmtHandle struct {
	s *stmt
}

type method int

const (
	prepare method = iota
	noprepare
	simple
)

var stringToMethod = map[string]method{
	"prepare":   prepare,
	"noprepare": noprepare,
	"simple":    simple,
}

// Define creates a handle for the given statement. The handle can be used after
// Init is called.
func (sr *SQLRunner) Define(sql string) StmtHandle {
	if sr.initialized {
		panic("Define can't be called after Init")
	}
	s := &stmt{sr: sr, sql: sql}
	sr.stmts = append(sr.stmts, s)
	return StmtHandle{s: s}
}

// Init initializes the runner; must be called after calls to Define and before
// the StmtHandles are used.
//
// The way we issue queries is set by flags.Method:
//
//  - "prepare": we prepare the query once during Init, then we reuse it for
//    each execution. This results in a Bind and Execute on the server each time
//    we run a query (on the given connection). Note that it's important to
//    prepare on separate connections if there are many parallel workers; this
//    avoids lock contention in the sql.Rows objects they produce. See #30811.
//
//  - "noprepare": each query is issued separately (on the given connection).
//    This results in Parse, Bind, Execute on the server each time we run a
//    query.
//
//  - "simple": each query is issued in a single string; parameters are
//    rendered inside the string. This results in a single SimpleExecute
//    request to the server for each query. Note that only a few parameter types
//    are supported.
//
func (sr *SQLRunner) Init(ctx context.Context, conn *gosql.Conn, flags *ConnFlags) error {
	if sr.initialized {
		panic("already initialized")
	}

	var ok bool
	sr.method, ok = stringToMethod[strings.ToLower(flags.Method)]
	if !ok {
		return errors.Errorf("unknown method %s", flags.Method)
	}

	if sr.method == prepare {
		for _, s := range sr.stmts {
			var err error
			s.prepared, err = conn.PrepareContext(ctx, s.sql)
			if err != nil {
				return errors.Wrapf(err, "preparing %s", s.sql)
			}
		}
	}

	sr.conn = conn
	sr.initialized = true
	return nil
}

// Close cleans up the SQLRunner.
func (sr *SQLRunner) Close() {
	for _, s := range sr.stmts {
		if s.prepared != nil {
			_ = s.prepared.Close()
			s.prepared = nil
		}
	}

	sr.initialized = false
}

func (h StmtHandle) check() {
	if !h.s.sr.initialized {
		panic("SQLRunner.Init not called")
	}
}

// Exec executes a query that doesn't return rows. The query is executed on the
// connection that was passed to SQLRunner.Init.
//
// See gosql.Conn.ExecContext / gosql.Stmt.ExecContext.
func (h StmtHandle) Exec(ctx context.Context, args ...interface{}) (gosql.Result, error) {
	h.check()
	switch h.s.sr.method {
	case prepare:
		return h.s.prepared.ExecContext(ctx, args...)

	case noprepare:
		return h.s.sr.conn.ExecContext(ctx, h.s.sql, args...)

	case simple:
		sql, err := sanitize.SanitizeSQL(h.s.sql, args...)
		if err != nil {
			panic(fmt.Sprintf("error sanitizing `%s`: %v", h.s.sql, err))
		}
		return h.s.sr.conn.ExecContext(ctx, sql)

	default:
		panic("invalid method")
	}
}

// ExecTx executes a query that doesn't return rows, in a transaction.
//
// See gosql.Tx.ExecContext.
func (h StmtHandle) ExecTx(
	ctx context.Context, tx *gosql.Tx, args ...interface{},
) (gosql.Result, error) {
	h.check()
	switch h.s.sr.method {
	case prepare:
		return tx.StmtContext(ctx, h.s.prepared).ExecContext(ctx, args...)

	case noprepare:
		return tx.ExecContext(ctx, h.s.sql, args...)

	case simple:
		sql, err := sanitize.SanitizeSQL(h.s.sql, args...)
		if err != nil {
			panic(fmt.Sprintf("error sanitizing `%s`: %v", h.s.sql, err))
		}
		return tx.ExecContext(ctx, sql)

	default:
		panic("invalid method")
	}
}

// Query executes a query that returns rows.
//
// See gosql.Conn.QueryContext / gosql.Stmt.QueryContext.
func (h StmtHandle) Query(ctx context.Context, args ...interface{}) (*gosql.Rows, error) {
	h.check()
	switch h.s.sr.method {
	case prepare:
		return h.s.prepared.QueryContext(ctx, args...)

	case noprepare:
		return h.s.sr.conn.QueryContext(ctx, h.s.sql, args...)

	case simple:
		sql, err := sanitize.SanitizeSQL(h.s.sql, args...)
		if err != nil {
			panic(fmt.Sprintf("error sanitizing `%s`: %v", h.s.sql, err))
		}
		return h.s.sr.conn.QueryContext(ctx, sql)

	default:
		panic("invalid method")
	}
}

// QueryTx executes a query that returns rows, in a transaction.
//
// See gosql.Tx.QueryContext.
func (h StmtHandle) QueryTx(
	ctx context.Context, tx *gosql.Tx, args ...interface{},
) (*gosql.Rows, error) {
	h.check()
	switch h.s.sr.method {
	case prepare:
		return tx.StmtContext(ctx, h.s.prepared).QueryContext(ctx, args...)

	case noprepare:
		return tx.QueryContext(ctx, h.s.sql, args...)

	case simple:
		sql, err := sanitize.SanitizeSQL(h.s.sql, args...)
		if err != nil {
			panic(fmt.Sprintf("error sanitizing `%s`: %v", h.s.sql, err))
		}
		return tx.QueryContext(ctx, sql)

	default:
		panic("invalid method")
	}
}

// QueryRow executes a query that is expected to return at most one row.
//
// See gosql.Conn.QueryRowContext / gosql.Stmt.QueryRowContext.
func (h StmtHandle) QueryRow(ctx context.Context, args ...interface{}) *gosql.Row {
	h.check()
	switch h.s.sr.method {
	case prepare:
		return h.s.prepared.QueryRowContext(ctx, args...)

	case noprepare:
		return h.s.sr.conn.QueryRowContext(ctx, h.s.sql, args...)

	case simple:
		sql, err := sanitize.SanitizeSQL(h.s.sql, args...)
		if err != nil {
			panic(fmt.Sprintf("error sanitizing `%s`: %v", h.s.sql, err))
		}
		return h.s.sr.conn.QueryRowContext(ctx, sql)

	default:
		panic("invalid method")
	}
}

// QueryRowTx executes a query that is expected to return at most one row, in a
// transaction.
//
// See gosql.Tx.QueryRowContext.
func (h StmtHandle) QueryRowTx(ctx context.Context, tx *gosql.Tx, args ...interface{}) *gosql.Row {
	h.check()
	switch h.s.sr.method {
	case prepare:
		return tx.StmtContext(ctx, h.s.prepared).QueryRowContext(ctx, args...)

	case noprepare:
		return tx.QueryRowContext(ctx, h.s.sql, args...)

	case simple:
		sql, err := sanitize.SanitizeSQL(h.s.sql, args...)
		if err != nil {
			panic(fmt.Sprintf("error sanitizing `%s`: %v", h.s.sql, err))
		}
		return tx.QueryRowContext(ctx, sql)

	default:
		panic("invalid method")
	}
}
