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
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

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
	buf.push(ctx, batch1)
	buf.push(ctx, batch2)

	// Check that, while we don't manually advance the cursor, we keep getting the
	// same statement.
	expPos := cursorPosition{queryStrPos: 0, stmtIdx: 0}
	for i := 0; i < 2; i++ {
		stmt, pos, err := buf.curStmt(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if pos.compare(expPos) != 0 {
			t.Fatalf("expected pos to be %s, got: %s", expPos, pos)
		}
		if stmt.String() != "SELECT 1" {
			t.Fatalf("wrong statement: %s", stmt)
		}
	}

	buf.advanceOne(ctx)
	expPos.stmtIdx = 1
	stmt, pos, err := buf.curStmt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pos.compare(expPos) != 0 {
		t.Fatalf("expected pos to be %s, got: %s", expPos, pos)
	}
	if stmt.String() != "SELECT 2" {
		t.Fatalf("wrong statement: %s", stmt)
	}

	buf.advanceOne(ctx)
	expPos.stmtIdx = 2
	stmt, pos, err = buf.curStmt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pos.compare(expPos) != 0 {
		t.Fatalf("expected pos to be %s, got: %s", expPos, pos)
	}
	if stmt.String() != "SELECT 3" {
		t.Fatalf("wrong statement: %s", stmt)
	}

	buf.advanceOne(ctx)
	expPos.queryStrPos = 1
	expPos.stmtIdx = 0
	stmt, pos, err = buf.curStmt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pos.compare(expPos) != 0 {
		t.Fatalf("expected pos to be %s, got: %s", expPos, pos)
	}
	if stmt.String() != "SELECT 4" {
		t.Fatalf("wrong statement: %s", stmt)
	}

	// Now rewind to the middle of the first batch.
	expPos.queryStrPos = 0
	expPos.stmtIdx = 1
	buf.rewind(ctx, expPos)
	stmt, pos, err = buf.curStmt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pos.compare(expPos) != 0 {
		t.Fatalf("expected pos to be %s, got: %s", expPos, pos)
	}
	if stmt.String() != "SELECT 2" {
		t.Fatalf("wrong statement: %s", stmt)
	}

	// Now seek to the beginning of the second batch.
	expPos.queryStrPos = 1
	expPos.stmtIdx = 0
	buf.seekToNextQueryStr()
	stmt, pos, err = buf.curStmt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pos.compare(expPos) != 0 {
		t.Fatalf("expected pos to be %s, got: %s", expPos, pos)
	}
	if stmt.String() != "SELECT 4" {
		t.Fatalf("wrong statement: %s", stmt)
	}
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
		buf.push(ctx, batch)
	}()

	expPos := cursorPosition{queryStrPos: 0, stmtIdx: 0}
	stmt, pos, err := buf.curStmt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pos.compare(expPos) != 0 {
		t.Fatalf("expected pos to be %s, got: %s", expPos, pos)
	}
	if stmt.String() != "SELECT 1" {
		t.Fatalf("wrong statement: %s", stmt)
	}
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
		buf.push(ctx, batch)
	}
	// Advance the cursor so that we can trim.
	buf.seekToNextQueryStr()
	buf.seekToNextQueryStr()
	trimPos := cursorPosition{queryStrPos: 2, stmtIdx: 0}
	buf.ltrim(ctx, trimPos)
	if l := len(buf.mu.queryStrings); l != 1 {
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
	buf.push(ctx, batch)
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
