// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package workload

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx"
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
//
//   row := sel.QueryRow(1)
//   // [use row]
//
//   _, err := ins.Exec(5, 6)
//   // [handle err]
//
// A runner should typically be associated with a single worker.
type SQLRunner struct {
	// The fields below are used by Define.
	stmts []*stmt

	// The fields below are set by Init.
	initialized bool
	method      method
	mcp         *MultiConnPool
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
// The name is used for naming prepared statements. Multiple workers that use
// the same set of defined queries can and should use the same name.
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
func (sr *SQLRunner) Init(
	ctx context.Context, name string, mcp *MultiConnPool, flags *ConnFlags,
) error {
	if sr.initialized {
		panic("already initialized")
	}

	var ok bool
	sr.method, ok = stringToMethod[strings.ToLower(flags.Method)]
	if !ok {
		return errors.Errorf("unknown method %s", flags.Method)
	}

	if sr.method == prepare {
		for i, s := range sr.stmts {
			stmtName := fmt.Sprintf("%s-%d", name, i+1)
			var err error
			s.prepared, err = mcp.PrepareEx(ctx, stmtName, s.sql, nil /* opts */)
			if err != nil {
				return errors.Wrapf(err, "preparing %s", s.sql)
			}
		}
	}

	sr.mcp = mcp
	sr.initialized = true
	return nil
}

func (h StmtHandle) check() {
	if !h.s.sr.initialized {
		panic("SQLRunner.Init not called")
	}
}

var simpleProtocolOpt = &pgx.QueryExOptions{SimpleProtocol: true}

type stmt struct {
	sr  *SQLRunner
	sql string
	// prepared is only used for the prepare method.
	prepared *pgx.PreparedStatement
}

// StmtHandle is associated with a (possibly prepared) statement; created by
// SQLRunner.Define.
type StmtHandle struct {
	s *stmt
}

// Exec executes a query that doesn't return rows. The query is executed on the
// connection that was passed to SQLRunner.Init.
//
// See pgx.Conn.Exec.
func (h StmtHandle) Exec(ctx context.Context, args ...interface{}) (pgx.CommandTag, error) {
	h.check()
	p := h.s.sr.mcp.Get()
	switch h.s.sr.method {
	case prepare:
		return p.ExecEx(ctx, h.s.prepared.Name, nil /* options */, args...)

	case noprepare:
		return p.ExecEx(ctx, h.s.sql, nil /* options */, args...)

	case simple:
		return p.ExecEx(ctx, h.s.sql, simpleProtocolOpt, args...)

	default:
		panic("invalid method")
	}
}

// ExecTx executes a query that doesn't return rows, inside a transaction.
//
// See pgx.Conn.Exec.
func (h StmtHandle) ExecTx(
	ctx context.Context, tx *pgx.Tx, args ...interface{},
) (pgx.CommandTag, error) {
	h.check()
	switch h.s.sr.method {
	case prepare:
		return tx.ExecEx(ctx, h.s.prepared.Name, nil /* options */, args...)

	case noprepare:
		return tx.ExecEx(ctx, h.s.sql, nil /* options */, args...)

	case simple:
		return tx.ExecEx(ctx, h.s.sql, simpleProtocolOpt, args...)

	default:
		panic("invalid method")
	}
}

// Query executes a query that returns rows.
//
// See pgx.Conn.Query.
func (h StmtHandle) Query(ctx context.Context, args ...interface{}) (*pgx.Rows, error) {
	h.check()
	p := h.s.sr.mcp.Get()
	switch h.s.sr.method {
	case prepare:
		return p.QueryEx(ctx, h.s.prepared.Name, nil /* options */, args...)

	case noprepare:
		return p.QueryEx(ctx, h.s.sql, nil /* options */, args...)

	case simple:
		return p.QueryEx(ctx, h.s.sql, simpleProtocolOpt, args...)

	default:
		panic("invalid method")
	}
}

// QueryTx executes a query that returns rows, inside a transaction.
//
// See pgx.Tx.Query.
func (h StmtHandle) QueryTx(
	ctx context.Context, tx *pgx.Tx, args ...interface{},
) (*pgx.Rows, error) {
	h.check()
	switch h.s.sr.method {
	case prepare:
		return tx.QueryEx(ctx, h.s.prepared.Name, nil /* options */, args...)

	case noprepare:
		return tx.QueryEx(ctx, h.s.sql, nil /* options */, args...)

	case simple:
		return tx.QueryEx(ctx, h.s.sql, simpleProtocolOpt, args...)

	default:
		panic("invalid method")
	}
}

// QueryRow executes a query that is expected to return at most one row.
//
// See pgx.Conn.QueryRow.
func (h StmtHandle) QueryRow(ctx context.Context, args ...interface{}) *pgx.Row {
	h.check()
	p := h.s.sr.mcp.Get()
	switch h.s.sr.method {
	case prepare:
		return p.QueryRowEx(ctx, h.s.prepared.Name, nil /* options */, args...)

	case noprepare:
		return p.QueryRowEx(ctx, h.s.sql, nil /* options */, args...)

	case simple:
		return p.QueryRowEx(ctx, h.s.sql, simpleProtocolOpt, args...)

	default:
		panic("invalid method")
	}
}

// QueryRowTx executes a query that is expected to return at most one row,
// inside a transaction.
//
// See pgx.Conn.QueryRow.
func (h StmtHandle) QueryRowTx(ctx context.Context, tx *pgx.Tx, args ...interface{}) *pgx.Row {
	h.check()
	switch h.s.sr.method {
	case prepare:
		return tx.QueryRowEx(ctx, h.s.prepared.Name, nil /* options */, args...)

	case noprepare:
		return tx.QueryRowEx(ctx, h.s.sql, nil /* options */, args...)

	case simple:
		return tx.QueryRowEx(ctx, h.s.sql, simpleProtocolOpt, args...)

	default:
		panic("invalid method")
	}
}

// Appease the linter.
var _ = StmtHandle.QueryRow
