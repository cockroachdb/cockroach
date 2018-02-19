// Copyright 2017 The Cockroach Authors.
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

package sql

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func assertStmt(t *testing.T, cmd Command, exp string) {
	stmt, ok := cmd.(ExecStmt)
	if !ok {
		t.Fatalf("%s: expected ExecStmt, got %T", testutils.Caller(1), cmd)
	}
	if stmt.Stmt.String() != exp {
		t.Fatalf("%s: expected statement %s, got %s", testutils.Caller(1), exp, stmt)
	}
}

func assertPrepareStmt(t *testing.T, cmd Command, expName string) {
	ps, ok := cmd.(PrepareStmt)
	if !ok {
		t.Fatalf("%s: expected PrepareStmt, got %T", testutils.Caller(1), cmd)
	}
	if ps.Name != expName {
		t.Fatalf("%s: expected name %s, got %s", testutils.Caller(1), expName, ps.Name)
	}
}

func mustPush(ctx context.Context, t *testing.T, buf *StmtBuf, cmd Command) {
	if err := buf.Push(ctx, cmd); err != nil {
		t.Fatalf("%s: %s", testutils.Caller(1), err)
	}
}

func TestStmtBuf(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
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
	mustPush(ctx, t, buf, ExecStmt{Stmt: s1})
	mustPush(ctx, t, buf, ExecStmt{Stmt: s2})
	mustPush(ctx, t, buf, ExecStmt{Stmt: s3})
	mustPush(ctx, t, buf, ExecStmt{Stmt: s4})

	// Check that, while we don't manually advance the cursor, we keep getting the
	// same statement.
	expPos := CmdPos(0)
	for i := 0; i < 2; i++ {
		cmd, pos, err := buf.curCmd()
		if err != nil {
			t.Fatal(err)
		}
		if pos != expPos {
			t.Fatalf("expected pos to be %d, got: %d", expPos, pos)
		}
		assertStmt(t, cmd, "SELECT 1")
	}

	buf.advanceOne()
	expPos++
	cmd, pos, err := buf.curCmd()
	if err != nil {
		t.Fatal(err)
	}
	if pos != expPos {
		t.Fatalf("expected pos to be %d, got: %d", expPos, pos)
	}
	assertStmt(t, cmd, "SELECT 2")

	buf.advanceOne()
	expPos++
	cmd, pos, err = buf.curCmd()
	if err != nil {
		t.Fatal(err)
	}
	if pos != expPos {
		t.Fatalf("expected pos to be %d, got: %d", expPos, pos)
	}
	assertStmt(t, cmd, "SELECT 3")

	buf.advanceOne()
	expPos++
	cmd, pos, err = buf.curCmd()
	if err != nil {
		t.Fatal(err)
	}
	if pos != expPos {
		t.Fatalf("expected pos to be %d, got: %d", expPos, pos)
	}
	assertStmt(t, cmd, "SELECT 4")

	// Now rewind.
	expPos = 1
	buf.rewind(ctx, expPos)
	cmd, pos, err = buf.curCmd()
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

	ctx := context.TODO()
	buf := NewStmtBuf()
	s1, err := parser.ParseOne("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = buf.Push(ctx, ExecStmt{Stmt: s1})
	}()

	expPos := CmdPos(0)
	cmd, pos, err := buf.curCmd()
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

	ctx := context.TODO()
	buf := NewStmtBuf()
	for i := 0; i < 5; i++ {
		stmt, err := parser.ParseOne(
			fmt.Sprintf("SELECT %d", i))
		if err != nil {
			t.Fatal(err)
		}
		mustPush(ctx, t, buf, ExecStmt{Stmt: stmt})
	}
	// Advance the cursor so that we can trim.
	buf.advanceOne()
	buf.advanceOne()
	trimPos := CmdPos(2)
	buf.ltrim(ctx, trimPos)
	if l := len(buf.mu.data); l != 3 {
		t.Fatalf("expected 3 left, got: %d", l)
	}
	if s := buf.mu.startPos; s != 2 {
		t.Fatalf("expected start pos 2, got: %d", s)
	}
}

// Test that, after Close() is called, buf.curCmd() returns io.EOF even if
// there were commands queued up.
func TestStmtBufClose(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
	buf := NewStmtBuf()
	stmt, err := parser.ParseOne("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	mustPush(ctx, t, buf, ExecStmt{Stmt: stmt})
	buf.Close()

	_, _, err = buf.curCmd()
	if err != io.EOF {
		t.Fatalf("expected EOF, got: %v", err)
	}
}

// Test that a call to Close() unblocks a curCmd() call.
func TestStmtBufCloseUnblocksReader(t *testing.T) {
	defer leaktest.AfterTest(t)()

	buf := NewStmtBuf()

	go func() {
		buf.Close()
	}()

	_, _, err := buf.curCmd()
	if err != io.EOF {
		t.Fatalf("expected EOF, got: %v", err)
	}
}

// Test that the buffer can hold and return other kinds of commands intermixed
// with ExecStmt.
func TestStmtBufPreparedStmt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	buf := NewStmtBuf()
	ctx := context.TODO()

	s1, err := parser.ParseOne("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	mustPush(ctx, t, buf, ExecStmt{Stmt: s1})
	mustPush(ctx, t, buf, PrepareStmt{Name: "p1"})
	mustPush(ctx, t, buf, PrepareStmt{Name: "p2"})

	cmd, _, err := buf.curCmd()
	if err != nil {
		t.Fatal(err)
	}
	assertStmt(t, cmd, "SELECT 1")

	buf.advanceOne()
	cmd, _, err = buf.curCmd()
	if err != nil {
		t.Fatal(err)
	}
	assertPrepareStmt(t, cmd, "p1")

	buf.advanceOne()
	cmd, _, err = buf.curCmd()
	if err != nil {
		t.Fatal(err)
	}
	assertPrepareStmt(t, cmd, "p2")

	// Rewind to the first prepared stmt.
	buf.rewind(ctx, CmdPos(1))
	cmd, _, err = buf.curCmd()
	if err != nil {
		t.Fatal(err)
	}
	assertPrepareStmt(t, cmd, "p1")
}

func TestStmtBufBatching(t *testing.T) {
	defer leaktest.AfterTest(t)()

	buf := NewStmtBuf()
	ctx := context.TODO()

	s1, err := parser.ParseOne("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}

	// Start a new batch.
	mustPush(ctx, t, buf, Sync{})

	mustPush(ctx, t, buf, ExecStmt{Stmt: s1})
	mustPush(ctx, t, buf, ExecStmt{Stmt: s1})

	// Start a new batch.
	mustPush(ctx, t, buf, Sync{})

	mustPush(ctx, t, buf, ExecStmt{Stmt: s1})
	mustPush(ctx, t, buf, ExecStmt{Stmt: s1})
	mustPush(ctx, t, buf, ExecStmt{Stmt: s1})

	// Start a new batch.
	mustPush(ctx, t, buf, Sync{})

	mustPush(ctx, t, buf, ExecStmt{Stmt: s1})

	// Go to 2nd batch.
	if err := buf.seekToNextBatch(); err != nil {
		t.Fatal(err)
	}
	_, pos, err := buf.curCmd()
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
	_, pos, err = buf.curCmd()
	if err != nil {
		t.Fatal(err)
	}
	if pos != CmdPos(7) {
		t.Fatalf("expected pos to be %d, got: %d", 7, pos)
	}

	// Async start a 4th batch; that will unblock the seek below.
	go func() {
		mustPush(ctx, t, buf, Sync{})
		_ = buf.Push(ctx, ExecStmt{Stmt: s1})
	}()

	// Go to 4th batch.
	if err := buf.seekToNextBatch(); err != nil {
		t.Fatal(err)
	}
	_, pos, err = buf.curCmd()
	if err != nil {
		t.Fatal(err)
	}
	if pos != CmdPos(9) {
		t.Fatalf("expected pos to be %d, got: %d", 9, pos)
	}
}
