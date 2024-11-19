// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workload

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// SQLRunner is a helper for issuing SQL statements; it supports multiple
// methods for issuing queries.
//
// Queries need to first be defined using calls to Define. Then the runner
// must be initialized, after which we can use the handles returned by Define.
//
// Sample usage:
//
//	sr := &workload.SQLRunner{}
//
//	sel:= sr.Define("SELECT x FROM t WHERE y = $1")
//	ins:= sr.Define("INSERT INTO t(x, y) VALUES ($1, $2)")
//
//	err := sr.Init(ctx, conn, flags)
//	// [handle err]
//
//	row := sel.QueryRow(1)
//	// [use row]
//
//	_, err := ins.Exec(5, 6)
//	// [handle err]
//
// A runner should typically be associated with a single worker.
type SQLRunner struct {
	// The fields below are used by Define.
	stmts []*stmt

	// The fields below are set by Init.
	initialized bool
	mcp         *MultiConnPool
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
func (sr *SQLRunner) Init(ctx context.Context, name string, mcp *MultiConnPool) error {
	if sr.initialized {
		panic("already initialized")
	}

	switch mcp.Method() {
	case pgx.QueryExecModeCacheStatement, pgx.QueryExecModeCacheDescribe, pgx.QueryExecModeDescribeExec:
		for i, s := range sr.stmts {
			stmtName := fmt.Sprintf("%s-%d", name, i+1)
			s.preparedName = stmtName
			mcp.AddPreparedStatement(stmtName, s.sql)
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
	conn, err := h.s.sr.mcp.Get().Acquire(ctx)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	defer conn.Release()

	switch h.s.sr.mcp.Method() {
	case pgx.QueryExecModeCacheStatement, pgx.QueryExecModeCacheDescribe, pgx.QueryExecModeDescribeExec:
		return conn.Exec(ctx, h.s.preparedName, args...)

	case pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeExec:
		return conn.Exec(ctx, h.s.sql, args...)

	default:
		return pgconn.CommandTag{}, errors.Errorf("unsupported method %q", h.s.sr.mcp.Method())
	}
}

// ExecTx executes a query that doesn't return rows, inside a transaction.
//
// See pgx.Conn.Exec.
func (h StmtHandle) ExecTx(
	ctx context.Context, tx pgx.Tx, args ...interface{},
) (pgconn.CommandTag, error) {
	h.check()
	switch h.s.sr.mcp.Method() {
	case pgx.QueryExecModeCacheStatement, pgx.QueryExecModeCacheDescribe, pgx.QueryExecModeDescribeExec:
		return tx.Exec(ctx, h.s.preparedName, args...)

	case pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeExec:
		return tx.Exec(ctx, h.s.sql, args...)

	default:
		return pgconn.CommandTag{}, errors.Errorf("unsupported method %q", h.s.sr.mcp.Method())
	}
}

// Query executes a query that returns rows.
//
// See pgx.Conn.Query.
func (h StmtHandle) Query(ctx context.Context, args ...interface{}) (pgx.Rows, error) {
	h.check()
	p := h.s.sr.mcp.Get()
	switch h.s.sr.mcp.Method() {
	case pgx.QueryExecModeCacheStatement, pgx.QueryExecModeCacheDescribe, pgx.QueryExecModeDescribeExec:
		return p.Query(ctx, h.s.preparedName, args...)

	case pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeExec:
		return p.Query(ctx, h.s.sql, args...)

	default:
		return nil, errors.Errorf("unsupported method %q", h.s.sr.mcp.Method())
	}
}

// QueryTx executes a query that returns rows, inside a transaction.
//
// See pgx.Tx.Query.
func (h StmtHandle) QueryTx(ctx context.Context, tx pgx.Tx, args ...interface{}) (pgx.Rows, error) {
	h.check()
	switch h.s.sr.mcp.Method() {
	case pgx.QueryExecModeCacheStatement, pgx.QueryExecModeCacheDescribe, pgx.QueryExecModeDescribeExec:
		return tx.Query(ctx, h.s.preparedName, args...)

	case pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeExec:
		return tx.Query(ctx, h.s.sql, args...)

	default:
		return nil, errors.Errorf("unsupported method %q", h.s.sr.mcp.Method())
	}
}

// QueryRow executes a query that is expected to return at most one row.
//
// See pgx.Conn.QueryRow.
func (h StmtHandle) QueryRow(ctx context.Context, args ...interface{}) pgx.Row {
	h.check()
	p := h.s.sr.mcp.Get()
	switch h.s.sr.mcp.Method() {
	case pgx.QueryExecModeCacheStatement, pgx.QueryExecModeCacheDescribe, pgx.QueryExecModeDescribeExec:
		return p.QueryRow(ctx, h.s.preparedName, args...)

	case pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeExec:
		return p.QueryRow(ctx, h.s.sql, args...)

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
	switch h.s.sr.mcp.Method() {
	case pgx.QueryExecModeCacheStatement, pgx.QueryExecModeCacheDescribe, pgx.QueryExecModeDescribeExec:
		return tx.QueryRow(ctx, h.s.preparedName, args...)

	case pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeExec:
		return tx.QueryRow(ctx, h.s.sql, args...)

	default:
		panic("invalid method")
	}
}

// Appease the linter.
var _ = StmtHandle.QueryRow
