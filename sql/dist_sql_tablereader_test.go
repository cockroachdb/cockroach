// Copyright 2016 The Cockroach Authors.
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
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package sql_test

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestTableReader(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, _ := createTestServerContext()
	server, sqlDB, kvDB := setupWithContext(t, ctx)
	defer cleanup(server, sqlDB)

	if _, err := sqlDB.Exec(`
		CREATE DATABASE test;
		CREATE TABLE test.t (a INT PRIMARY KEY, b INT, c INT, d INT, INDEX bc (b, c));
		INSERT INTO test.t VALUES (1, 10, 11, 12), (2, 20, 21, 22), (3, 30, 31, 32);
		INSERT INTO test.t VALUES (4, 60, 61, 62), (5, 50, 51, 52), (6, 40, 41, 42);
	`); err != nil {
		t.Fatal(err)
	}

	td := getTableDescriptor(kvDB, "test", "t")

	ts := sql.TableReaderSpec{
		Table:         *td,
		IndexIdx:      0,
		Reverse:       false,
		Spans:         nil,
		Filter:        sql.SQLExpression{Expr: "$2 != 21"}, // c != 21
		OutputColumns: []uint32{0, 3},                      // a, d
	}

	txn := client.NewTxn(context.Background(), *kvDB)

	tr, err := sql.NewTableReader(&ts, txn, parser.EvalContext{})
	if err != nil {
		t.Fatal(err)
	}
	pErr := tr.Run()
	if pErr != nil {
		t.Fatal(pErr)
	}
	// TODO(radu): currently the table reader just prints out stuff; when it
	// will output results we will be able to verify them.
	// Expected output:
	// RESULT: 1 <skipped> 11 12
	// RESULT: 3 <skipped> 31 32
	// RESULT: 4 <skipped> 61 62
	// RESULT: 5 <skipped> 51 52
	// RESULT: 6 <skipped> 41 42

	// Read using the bc index
	var span roachpb.Span
	span.Key = roachpb.Key(sql.MakeIndexKeyPrefix(td.ID, td.Indexes[0].ID))
	span.EndKey = append(span.Key, encoding.EncodeVarintAscending(nil, 50)...)

	ts = sql.TableReaderSpec{
		Table:         *td,
		IndexIdx:      1,
		Reverse:       true,
		Spans:         []sql.TableReaderSpan{{Span: span}},
		Filter:        sql.SQLExpression{Expr: "$1 != 30"}, // b != 30
		OutputColumns: []uint32{0, 1},                      // a, c
	}
	tr, err = sql.NewTableReader(&ts, txn, parser.EvalContext{})
	if err != nil {
		t.Fatal(err)
	}
	pErr = tr.Run()
	if pErr != nil {
		t.Fatal(pErr)
	}
	// Expected output:
	// RESULT: 6 40 41 <skipped>
	// RESULT: 2 20 21 <skipped>
	// RESULT: 1 10 11 <skipped>
}
