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

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
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
	mcp         *MultiConnPool
}

type method int

const (
	prepare method = iota
	simple
)

var stringToMethod = map[string]method{
	"prepare": prepare,
	"simple":  simple,
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
func (sr *SQLRunner) Init(ctx context.Context, mcp *MultiConnPool) error {
	if sr.initialized {
		panic("already initialized")
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
	return p.Exec(ctx, h.s.sql, args...)
}

// ExecTx executes a query that doesn't return rows, inside a transaction.
//
// See pgx.Conn.Exec.
func (h StmtHandle) ExecTx(
	ctx context.Context, tx pgx.Tx, args ...interface{},
) (pgconn.CommandTag, error) {
	h.check()
	return tx.Exec(ctx, h.s.sql, args...)
}

// Query executes a query that returns rows.
//
// See pgx.Conn.Query.
func (h StmtHandle) Query(ctx context.Context, args ...interface{}) (pgx.Rows, error) {
	h.check()
	p := h.s.sr.mcp.Get()
	return p.Query(ctx, h.s.sql, args...)
}

// QueryTx executes a query that returns rows, inside a transaction.
//
// See pgx.Tx.Query.
func (h StmtHandle) QueryTx(ctx context.Context, tx pgx.Tx, args ...interface{}) (pgx.Rows, error) {
	h.check()
	return tx.Query(ctx, h.s.sql, args...)
}

// QueryRow executes a query that is expected to return at most one row.
//
// See pgx.Conn.QueryRow.
func (h StmtHandle) QueryRow(ctx context.Context, args ...interface{}) pgx.Row {
	h.check()
	p := h.s.sr.mcp.Get()
	return p.QueryRow(ctx, h.s.sql, args...)
}

// QueryRowTx executes a query that is expected to return at most one row,
// inside a transaction.
//
// See pgx.Conn.QueryRow.
func (h StmtHandle) QueryRowTx(ctx context.Context, tx pgx.Tx, args ...interface{}) pgx.Row {
	h.check()
	return tx.QueryRow(ctx, h.s.sql, args...)
}

// Appease the linter.
var _ = StmtHandle.QueryRow
