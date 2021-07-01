// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func assertStmt(t *testing.T, cmd Command, exp string) {
	t.Helper()
	stmt, ok := cmd.(ExecStmt)
	if !ok {
		t.Fatalf("expected ExecStmt, got %T", cmd)
	}
	if stmt.AST.String() != exp {
		t.Fatalf("expected statement %s, got %s", exp, stmt)
	}
}

func assertPrepareStmt(t *testing.T, cmd Command, expName string) {
	t.Helper()
	ps, ok := cmd.(PrepareStmt)
	if !ok {
		t.Fatalf("expected PrepareStmt, got %T", cmd)
	}
	if ps.Name != expName {
		t.Fatalf("expected name %s, got %s", expName, ps.Name)
	}
}

func mustPush(ctx context.Context, t *testing.T, buf *StmtBuf, cmd Command) {
	t.Helper()
	if err := buf.Push(ctx, cmd); err != nil {
		t.Fatalf("%s", err)
	}
}

func TestStmtBuf(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s1, err := parser.ParseOne("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	s2, err := parser.ParseOne("SELECT 2")
	if err != nil {
		t.Fatal(err)
	}
	s3, err := parser.ParseOne("SELECT 3")
	if err != nil {
		t.Fatal(err)
	}
	s4, err := parser.ParseOne("SELECT 4")
	if err != nil {
		t.Fatal(err)
	}
	buf := NewStmtBuf()
	mustPush(ctx, t, buf, ExecStmt{Statement: s1})
	mustPush(ctx, t, buf, ExecStmt{Statement: s2})
	mustPush(ctx, t, buf, ExecStmt{Statement: s3})
	mustPush(ctx, t, buf, ExecStmt{Statement: s4})

	// Check that, while we don't manually advance the cursor, we keep getting the
	// same statement.
	expPos := CmdPos(0)
	for i := 0; i < 2; i++ {
		cmd, pos, err := buf.CurCmd()
		if err != nil {
			t.Fatal(err)
		}
		if pos != expPos {
			t.Fatalf("expected pos to be %d, got: %d", expPos, pos)
		}
		assertStmt(t, cmd, "SELECT 1")
	}

	buf.AdvanceOne()
	expPos++
	cmd, pos, err := buf.CurCmd()
	if err != nil {
		t.Fatal(err)
	}
	if pos != expPos {
		t.Fatalf("expected pos to be %d, got: %d", expPos, pos)
	}
	assertStmt(t, cmd, "SELECT 2")

	buf.AdvanceOne()
	expPos++
	cmd, pos, err = buf.CurCmd()
	if err != nil {
		t.Fatal(err)
	}
	if pos != expPos {
		t.Fatalf("expected pos to be %d, got: %d", expPos, pos)
	}
	assertStmt(t, cmd, "SELECT 3")

	buf.AdvanceOne()
	expPos++
	cmd, pos, err = buf.CurCmd()
	if err != nil {
		t.Fatal(err)
	}
	if pos != expPos {
		t.Fatalf("expected pos to be %d, got: %d", expPos, pos)
	}
	assertStmt(t, cmd, "SELECT 4")

	// Now rewind.
	expPos = 1
	buf.Rewind(ctx, expPos)
	cmd, pos, err = buf.CurCmd()
	if err != nil {
		t.Fatal(err)
	}
	if pos != expPos {
		t.Fatalf("expected pos to be %d, got: %d", expPos, pos)
	}
	assertStmt(t, cmd, "SELECT 2")
}

// Test that a reader blocked for an incoming statement is unblocked when that
// statement arrives.
func TestStmtBufSignal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	buf := NewStmtBuf()
	s1, err := parser.ParseOne("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = buf.Push(ctx, ExecStmt{Statement: s1})
	}()

	expPos := CmdPos(0)
	cmd, pos, err := buf.CurCmd()
	if err != nil {
		t.Fatal(err)
	}
	if pos != expPos {
		t.Fatalf("expected pos to be %d, got: %d", expPos, pos)
	}
	assertStmt(t, cmd, "SELECT 1")
}

func TestStmtBufLtrim(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	buf := NewStmtBuf()
	for i := 0; i < 5; i++ {
		stmt, err := parser.ParseOne(
			fmt.Sprintf("SELECT %d", i))
		if err != nil {
			t.Fatal(err)
		}
		mustPush(ctx, t, buf, ExecStmt{Statement: stmt})
	}
	// Advance the cursor so that we can trim.
	buf.AdvanceOne()
	buf.AdvanceOne()
	trimPos := CmdPos(2)
	buf.Ltrim(ctx, trimPos)
	if l := buf.mu.data.Len(); l != 3 {
		t.Fatalf("expected 3 left, got: %d", l)
	}
	if s := buf.mu.startPos; s != 2 {
		t.Fatalf("expected start pos 2, got: %d", s)
	}
}

// Test that, after Close() is called, buf.CurCmd() returns io.EOF even if
// there were commands queued up.
func TestStmtBufClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	buf := NewStmtBuf()
	stmt, err := parser.ParseOne("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	mustPush(ctx, t, buf, ExecStmt{Statement: stmt})
	buf.Close()

	_, _, err = buf.CurCmd()
	if err != io.EOF {
		t.Fatalf("expected EOF, got: %v", err)
	}
}

// Test that a call to Close() unblocks a CurCmd() call.
func TestStmtBufCloseUnblocksReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	buf := NewStmtBuf()

	go func() {
		buf.Close()
	}()

	_, _, err := buf.CurCmd()
	if err != io.EOF {
		t.Fatalf("expected EOF, got: %v", err)
	}
}

// Test that the buffer can hold and return other kinds of commands intermixed
// with ExecStmt.
func TestStmtBufPreparedStmt(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	buf := NewStmtBuf()
	ctx := context.Background()

	s1, err := parser.ParseOne("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	mustPush(ctx, t, buf, ExecStmt{Statement: s1})
	mustPush(ctx, t, buf, PrepareStmt{Name: "p1"})
	mustPush(ctx, t, buf, PrepareStmt{Name: "p2"})

	cmd, _, err := buf.CurCmd()
	if err != nil {
		t.Fatal(err)
	}
	assertStmt(t, cmd, "SELECT 1")

	buf.AdvanceOne()
	cmd, _, err = buf.CurCmd()
	if err != nil {
		t.Fatal(err)
	}
	assertPrepareStmt(t, cmd, "p1")

	buf.AdvanceOne()
	cmd, _, err = buf.CurCmd()
	if err != nil {
		t.Fatal(err)
	}
	assertPrepareStmt(t, cmd, "p2")

	// Rewind to the first prepared stmt.
	buf.Rewind(ctx, CmdPos(1))
	cmd, _, err = buf.CurCmd()
	if err != nil {
		t.Fatal(err)
	}
	assertPrepareStmt(t, cmd, "p1")
}

func TestStmtBufBatching(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	buf := NewStmtBuf()
	ctx := context.Background()

	s1, err := parser.ParseOne("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}

	// Start a new batch.
	mustPush(ctx, t, buf, Sync{})

	mustPush(ctx, t, buf, ExecStmt{Statement: s1})
	mustPush(ctx, t, buf, ExecStmt{Statement: s1})

	// Start a new batch.
	mustPush(ctx, t, buf, Sync{})

	mustPush(ctx, t, buf, ExecStmt{Statement: s1})
	mustPush(ctx, t, buf, ExecStmt{Statement: s1})
	mustPush(ctx, t, buf, ExecStmt{Statement: s1})

	// Start a new batch.
	mustPush(ctx, t, buf, Sync{})

	mustPush(ctx, t, buf, ExecStmt{Statement: s1})

	// Go to 2nd batch.
	if err := buf.seekToNextBatch(); err != nil {
		t.Fatal(err)
	}
	_, pos, err := buf.CurCmd()
	if err != nil {
		t.Fatal(err)
	}
	if pos != CmdPos(3) {
		t.Fatalf("expected pos to be %d, got: %d", 3, pos)
	}

	// Go to 3rd batch.
	if err := buf.seekToNextBatch(); err != nil {
		t.Fatal(err)
	}
	_, pos, err = buf.CurCmd()
	if err != nil {
		t.Fatal(err)
	}
	if pos != CmdPos(7) {
		t.Fatalf("expected pos to be %d, got: %d", 7, pos)
	}

	// Async start a 4th batch; that will unblock the seek below.
	go func() {
		mustPush(ctx, t, buf, Sync{})
		_ = buf.Push(ctx, ExecStmt{Statement: s1})
	}()

	// Go to 4th batch.
	if err := buf.seekToNextBatch(); err != nil {
		t.Fatal(err)
	}
	_, pos, err = buf.CurCmd()
	if err != nil {
		t.Fatal(err)
	}
	if pos != CmdPos(9) {
		t.Fatalf("expected pos to be %d, got: %d", 9, pos)
	}
}
