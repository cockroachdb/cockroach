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
	"fmt"
	"io"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"golang.org/x/net/context"
)

func assertStmt(t *testing.T, stmtOrPrepared queryOrPreparedStmt, exp string) {
	stmt := stmtOrPrepared.query
	if stmt == nil {
		t.Fatalf("%s: expected statement, got prepared statement", testutils.Caller(1))
	}
	if stmt.String() != exp {
		t.Fatalf("%s: expected statement %s, got %s", testutils.Caller(1), exp, stmt)
	}
}

func assertPreparedStmt(
	t *testing.T,
	stmtOrPrepared queryOrPreparedStmt,
	expPS *PreparedStatement,
	expPInfo *tree.PlaceholderInfo,
) {
	stmt := stmtOrPrepared.query
	if stmt != nil {
		t.Fatalf("%s: expected prepared statement, got statement", testutils.Caller(1))
	}
	if stmtOrPrepared.preparedStmt != expPS {
		t.Fatalf("%s: wrong prepared statement", testutils.Caller(1))
	}
	if stmtOrPrepared.pinfo != expPInfo {
		t.Fatalf("%s: wrong prepared statement", testutils.Caller(1))
	}
}

func TestStmtBuf(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
	batch1, err := parser.Parse("SELECT 1; SELECT 2; SELECT 3;")
	if err != nil {
		t.Fatal(err)
	}
	batch2, err := parser.Parse("SELECT 4; SELECT 5; SELECT 6;")
	if err != nil {
		t.Fatal(err)
	}
	buf := newStmtBuf()
	buf.push(ctx, queryStrOrPreparedStmt{queryStr: batch1})
	buf.push(ctx, queryStrOrPreparedStmt{queryStr: batch2})

	// Check that, while we don't manually advance the cursor, we keep getting the
	// same statement.
	expPos := cursorPosition{queryStrPos: 0, stmtIdx: 0}
	for i := 0; i < 2; i++ {
		stmtOrPrepared, pos, err := buf.curStmt(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if pos.compare(expPos) != 0 {
			t.Fatalf("expected pos to be %s, got: %s", expPos, pos)
		}
		assertStmt(t, stmtOrPrepared, "SELECT 1")
	}

	buf.advanceOne(ctx)
	expPos.stmtIdx = 1
	stmtOrPrepared, pos, err := buf.curStmt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pos.compare(expPos) != 0 {
		t.Fatalf("expected pos to be %s, got: %s", expPos, pos)
	}
	assertStmt(t, stmtOrPrepared, "SELECT 2")

	buf.advanceOne(ctx)
	expPos.stmtIdx = 2
	stmtOrPrepared, pos, err = buf.curStmt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pos.compare(expPos) != 0 {
		t.Fatalf("expected pos to be %s, got: %s", expPos, pos)
	}
	assertStmt(t, stmtOrPrepared, "SELECT 3")

	buf.advanceOne(ctx)
	expPos.queryStrPos = 1
	expPos.stmtIdx = 0
	stmtOrPrepared, pos, err = buf.curStmt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pos.compare(expPos) != 0 {
		t.Fatalf("expected pos to be %s, got: %s", expPos, pos)
	}
	assertStmt(t, stmtOrPrepared, "SELECT 4")

	// Now rewind to the middle of the first batch.
	expPos.queryStrPos = 0
	expPos.stmtIdx = 1
	buf.rewind(ctx, expPos)
	stmtOrPrepared, pos, err = buf.curStmt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pos.compare(expPos) != 0 {
		t.Fatalf("expected pos to be %s, got: %s", expPos, pos)
	}
	assertStmt(t, stmtOrPrepared, "SELECT 2")

	// Now seek to the beginning of the second batch.
	expPos.queryStrPos = 1
	expPos.stmtIdx = 0
	buf.seekToNextQueryStr()
	stmtOrPrepared, pos, err = buf.curStmt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pos.compare(expPos) != 0 {
		t.Fatalf("expected pos to be %s, got: %s", expPos, pos)
	}
	assertStmt(t, stmtOrPrepared, "SELECT 4")
}

// Test that a reader blocked for an incoming statement is unblocked when that
// statement arrives.
func TestStmtBufSignal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
	buf := newStmtBuf()
	batch, err := parser.Parse("SELECT 1; SELECT 2; SELECT 3;")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		buf.push(ctx, queryStrOrPreparedStmt{queryStr: batch})
	}()

	expPos := cursorPosition{queryStrPos: 0, stmtIdx: 0}
	stmtOrPrepared, pos, err := buf.curStmt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pos.compare(expPos) != 0 {
		t.Fatalf("expected pos to be %s, got: %s", expPos, pos)
	}
	assertStmt(t, stmtOrPrepared, "SELECT 1")
}

func TestStmtBufLtrim(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
	buf := newStmtBuf()
	for i := 0; i < 3; i++ {
		batch, err := parser.Parse(
			fmt.Sprintf("SELECT %d; SELECT %d;", i*2, i*2+1))
		if err != nil {
			t.Fatal(err)
		}
		buf.push(ctx, queryStrOrPreparedStmt{queryStr: batch})
	}
	// Advance the cursor so that we can trim.
	buf.seekToNextQueryStr()
	buf.seekToNextQueryStr()
	trimPos := cursorPosition{queryStrPos: 2, stmtIdx: 0}
	buf.ltrim(ctx, trimPos)
	if l := len(buf.mu.batches); l != 1 {
		t.Fatalf("expected 1 query string left, got: %d", l)
	}
	if s := buf.mu.startPos; s != 2 {
		t.Fatalf("expected start pos 2, got: %d", s)
	}
}

// Test that, after Close() is called, buf.curStmt() returns io.EOF even if
// there were statements queued up.
func TestStmtBufClose(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
	buf := newStmtBuf()
	batch, err := parser.Parse("SELECT 1;")
	if err != nil {
		t.Fatal(err)
	}
	buf.push(ctx, queryStrOrPreparedStmt{queryStr: batch})
	buf.Close()

	_, _, err = buf.curStmt(ctx)
	if err != io.EOF {
		t.Fatalf("expected EOF, got: %v", err)
	}
}

// Test that a call to Close() unblocks a curStmt() call.
func TestStmtBufCloseUnblocksReader(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
	buf := newStmtBuf()

	go func() {
		buf.Close()
	}()

	_, _, err := buf.curStmt(ctx)
	if err != io.EOF {
		t.Fatalf("expected EOF, got: %v", err)
	}
}

// Test that the buffer can hold and return prepared statements intermixed with
// query strings.
func TestPreparedStmt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	buf := newStmtBuf()
	ctx := context.TODO()

	batch, err := parser.Parse("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	buf.push(ctx, queryStrOrPreparedStmt{queryStr: batch})

	pstmt1 := new(PreparedStatement)
	pinfo1 := new(tree.PlaceholderInfo)
	buf.push(ctx, queryStrOrPreparedStmt{preparedStmt: pstmt1, pinfo: pinfo1})

	pstmt2 := new(PreparedStatement)
	pinfo2 := new(tree.PlaceholderInfo)
	buf.push(ctx, queryStrOrPreparedStmt{preparedStmt: pstmt2, pinfo: pinfo2})

	stmtOrPrepared, _, err := buf.curStmt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assertStmt(t, stmtOrPrepared, "SELECT 1")

	buf.advanceOne(ctx)
	stmtOrPrepared, _, err = buf.curStmt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assertPreparedStmt(t, stmtOrPrepared, pstmt1, pinfo1)

	buf.advanceOne(ctx)
	stmtOrPrepared, _, err = buf.curStmt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assertPreparedStmt(t, stmtOrPrepared, pstmt2, pinfo2)

	// Rewind to the first prepared stmt.
	buf.rewind(ctx, cursorPosition{queryStrPos: 1, stmtIdx: 0})
	stmtOrPrepared, _, err = buf.curStmt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assertPreparedStmt(t, stmtOrPrepared, pstmt1, pinfo1)
}
