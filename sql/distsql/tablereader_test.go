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

package distsql

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestTableReader(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	// Create a table where each row is:
	//
	//  |     a    |     b    |         sum         |         s           |
	//  |-----------------------------------------------------------------|
	//  | rowId/10 | rowId%10 | rowId/10 + rowId%10 | IntToEnglish(rowId) |

	aFn := func(row int) parser.Datum {
		return parser.NewDInt(parser.DInt(row / 10))
	}
	bFn := func(row int) parser.Datum {
		return parser.NewDInt(parser.DInt(row % 10))
	}
	sumFn := func(row int) parser.Datum {
		return parser.NewDInt(parser.DInt(row/10 + row%10))
	}

	sqlutils.CreateTable(t, sqlDB, "t",
		"a INT, b INT, sum INT, s STRING, PRIMARY KEY (a,b), INDEX bs (b,s)",
		99,
		sqlutils.ToRowFn(aFn, bFn, sumFn, sqlutils.RowEnglishFn))

	td := sqlbase.GetTableDescriptor(kvDB, "test", "t")

	makeIndexSpan := func(start, end int) TableReaderSpan {
		var span roachpb.Span
		prefix := roachpb.Key(sqlbase.MakeIndexKeyPrefix(td, td.Indexes[0].ID))
		span.Key = append(prefix, encoding.EncodeVarintAscending(nil, int64(start))...)
		span.EndKey = append(span.EndKey, prefix...)
		span.EndKey = append(span.EndKey, encoding.EncodeVarintAscending(nil, int64(end))...)
		return TableReaderSpan{Span: span}
	}

	testCases := []struct {
		spec     TableReaderSpec
		expected string
	}{
		{
			spec: TableReaderSpec{
				Filter:        Expression{Expr: "$2 < 5 AND $1 != 3"}, // sum < 5 && b != 3
				OutputColumns: []uint32{0, 1},
			},
			expected: "[[0 1] [0 2] [0 4] [1 0] [1 1] [1 2] [2 0] [2 1] [2 2] [3 0] [3 1] [4 0]]",
		},
		{
			spec: TableReaderSpec{
				Filter:        Expression{Expr: "$2 < 5 AND $1 != 3"},
				OutputColumns: []uint32{3}, // s
				HardLimit:     4,
			},
			expected: "[['one'] ['two'] ['four'] ['one-zero']]",
		},
		{
			spec: TableReaderSpec{
				IndexIdx:      1,
				Reverse:       true,
				Spans:         []TableReaderSpan{makeIndexSpan(4, 6)},
				Filter:        Expression{Expr: "$0 < 3"}, // sum < 8
				OutputColumns: []uint32{0, 1},
				SoftLimit:     1,
			},
			expected: "[[2 5] [1 5] [0 5] [2 4] [1 4] [0 4]]",
		},
	}

	for _, c := range testCases {
		ts := c.spec
		ts.Table = *td

		txn := client.NewTxn(context.Background(), *kvDB)
		flowCtx := FlowCtx{
			Context: context.Background(),
			evalCtx: &parser.EvalContext{},
			txn:     txn,
		}

		out := &RowBuffer{}
		tr, err := newTableReader(&flowCtx, &ts, out)
		if err != nil {
			t.Fatal(err)
		}
		tr.Run(nil)
		if out.err != nil {
			t.Fatal(out.err)
		}
		if !out.closed {
			t.Fatalf("output RowReceiver not closed")
		}
		if result := out.rows.String(); result != c.expected {
			t.Errorf("invalid results: %s, expected %s'", result, c.expected)
		}
	}
}
