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
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
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
//  - "prepare": explicitly prepare the query once per connection, then we reuse
//    it for each execution. This results in a Bind and Execute on the server
//    each time we run a query (on the given connection). Note that it's
//    important to prepare on separate connections if there are many parallel
//    workers; this avoids lock contention in the sql.Rows objects they produce.
//    See #30811.
//
//  - "noprepare": each query is issued separately (on the given connection).
//    This results in Parse, Bind, Execute on the server each time we run a
//    query. The statement is an anonymous prepared statement; that is, the
//    name is the empty string.
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
			s.preparedName = stmtName
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

type stmt struct {
	sr  *SQLRunner
	sql string
	// preparedName is only used for the prepare method.
	preparedName string
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
func (h StmtHandle) Exec(ctx context.Context, args ...interface{}) (pgconn.CommandTag, error) {
	h.check()
	p := h.s.sr.mcp.Get()
	switch h.s.sr.method {
	case prepare:
		// Note that calling `Prepare` with a name that has already been prepared
		// is idempotent and short-circuits before doing any communication to the
		// server.
		var commandTag pgconn.CommandTag
		err := p.AcquireFunc(ctx, func(conn *pgxpool.Conn) error {
			if _, err := conn.Conn().Prepare(ctx, h.s.preparedName, h.s.sql); err != nil {
				return err
			}
			var connErr error
			commandTag, connErr = conn.Conn().Exec(ctx, h.s.preparedName, args...)
			return connErr
		})
		return commandTag, err

	case noprepare:
		return p.Exec(ctx, h.s.sql, args...)

	case simple:
		newArgs := []interface{}{pgx.QuerySimpleProtocol(true)}
		return p.Exec(ctx, h.s.sql, append(newArgs, args)...)

	default:
		panic("invalid method")
	}
}

// ExecTx executes a query that doesn't return rows, inside a transaction.
//
// See pgx.Conn.Exec.
func (h StmtHandle) ExecTx(
	ctx context.Context, tx pgx.Tx, args ...interface{},
) (pgconn.CommandTag, error) {
	h.check()
	switch h.s.sr.method {
	case prepare:
		// Note that calling `Prepare` with a name that has already been prepared
		// is idempotent and short-circuits before doing any communication to the
		// server.
		if _, err := tx.Prepare(ctx, h.s.preparedName, h.s.sql); err != nil {
			return nil, err
		}
		return tx.Exec(ctx, h.s.preparedName, args...)

	case noprepare:
		return tx.Exec(ctx, h.s.sql, args...)

	case simple:
		newArgs := []interface{}{pgx.QuerySimpleProtocol(true)}
		return tx.Exec(ctx, h.s.sql, append(newArgs, args)...)

	default:
		panic("invalid method")
	}
}

// Query executes a query that returns rows.
//
// See pgx.Conn.Query.
func (h StmtHandle) Query(ctx context.Context, args ...interface{}) (pgx.Rows, error) {
	h.check()
	p := h.s.sr.mcp.Get()
	switch h.s.sr.method {
	case prepare:
		// Note that calling `Prepare` with a name that has already been prepared
		// is idempotent and short-circuits before doing any communication to the
		// server.
		var rows pgx.Rows
		err := p.AcquireFunc(ctx, func(conn *pgxpool.Conn) error {
			if _, err := conn.Conn().Prepare(ctx, h.s.preparedName, h.s.sql); err != nil {
				return err
			}
			var connErr error
			rows, connErr = conn.Conn().Query(ctx, h.s.preparedName, args...)
			return connErr
		})
		return rows, err

	case noprepare:
		return p.Query(ctx, h.s.sql, args...)

	case simple:
		newArgs := []interface{}{pgx.QuerySimpleProtocol(true)}
		return p.Query(ctx, h.s.sql, append(newArgs, args)...)

	default:
		panic("invalid method")
	}
}

// QueryTx executes a query that returns rows, inside a transaction.
//
// See pgx.Tx.Query.
func (h StmtHandle) QueryTx(ctx context.Context, tx pgx.Tx, args ...interface{}) (pgx.Rows, error) {
	h.check()
	switch h.s.sr.method {
	case prepare:
		// Note that calling `Prepare` with a name that has already been prepared
		// is idempotent and short-circuits before doing any communication to the
		// server.
		if _, err := tx.Prepare(ctx, h.s.preparedName, h.s.sql); err != nil {
			return nil, err
		}
		return tx.Query(ctx, h.s.preparedName, args...)

	case noprepare:
		return tx.Query(ctx, h.s.sql, args...)

	case simple:
		newArgs := []interface{}{pgx.QuerySimpleProtocol(true)}
		return tx.Query(ctx, h.s.sql, append(newArgs, args)...)

	default:
		panic("invalid method")
	}
}

// QueryRow executes a query that is expected to return at most one row.
//
// See pgx.Conn.QueryRow.
func (h StmtHandle) QueryRow(ctx context.Context, args ...interface{}) pgx.Row {
	h.check()
	p := h.s.sr.mcp.Get()
	switch h.s.sr.method {
	case prepare:
		// Note that calling `Prepare` with a name that has already been prepared
		// is idempotent and short-circuits before doing any communication to the
		// server.
		var row pgx.Row
		err := p.AcquireFunc(ctx, func(conn *pgxpool.Conn) error {
			if _, err := conn.Conn().Prepare(ctx, h.s.preparedName, h.s.sql); err != nil {
				return err
			}
			row = conn.Conn().QueryRow(ctx, h.s.preparedName, args...)
			return nil
		})
		if err != nil {
			r := errRow{retErr: err}
			return &r
		}
		return row

	case noprepare:
		return p.QueryRow(ctx, h.s.sql, args...)

	case simple:
		newArgs := []interface{}{pgx.QuerySimpleProtocol(true)}
		return p.QueryRow(ctx, h.s.sql, append(newArgs, args)...)

	default:
		panic("invalid method")
	}
}

// QueryRowTx executes a query that is expected to return at most one row,
// inside a transaction.
//
// See pgx.Conn.QueryRow.
func (h StmtHandle) QueryRowTx(ctx context.Context, tx pgx.Tx, args ...interface{}) pgx.Row {
	h.check()
	switch h.s.sr.method {
	case prepare:
		// Note that calling `Prepare` with a name that has already been prepared
		// is idempotent and short-circuits before doing any communication to the
		// server.
		if _, err := tx.Prepare(ctx, h.s.preparedName, h.s.sql); err != nil {
			r := errRow{retErr: err}
			return &r
		}
		return tx.QueryRow(ctx, h.s.preparedName, args...)

	case noprepare:
		return tx.QueryRow(ctx, h.s.sql, args...)

	case simple:
		newArgs := []interface{}{pgx.QuerySimpleProtocol(true)}
		return tx.QueryRow(ctx, h.s.sql, append(newArgs, args)...)

	default:
		panic("invalid method")
	}
}

// errRow implements the pgx.Row interface. It's used only in the `prepare`
// mode.
type errRow struct {
	retErr error
}

var _ pgx.Row = &errRow{}

func (r *errRow) Scan(_ ...interface{}) error { return r.retErr }

// Appease the linter.
var _ = StmtHandle.QueryRow
