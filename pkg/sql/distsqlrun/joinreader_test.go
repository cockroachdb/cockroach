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

package distsqlrun

import (
	"errors"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestJoinReader(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

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

	testCases := []struct {
		post     PostProcessSpec
		input    [][]parser.Datum
		expected string
	}{
		{
			post: PostProcessSpec{
				OutputColumns: []uint32{0, 1, 2},
			},
			input: [][]parser.Datum{
				{aFn(2), bFn(2)},
				{aFn(5), bFn(5)},
				{aFn(10), bFn(10)},
				{aFn(15), bFn(15)},
			},
			expected: "[[0 2 2] [0 5 5] [1 0 1] [1 5 6]]",
		},
		{
			post: PostProcessSpec{
				Filter:        Expression{Expr: "@3 <= 5"}, // sum <= 5
				OutputColumns: []uint32{3},
			},
			input: [][]parser.Datum{
				{aFn(1), bFn(1)},
				{aFn(25), bFn(25)},
				{aFn(5), bFn(5)},
				{aFn(21), bFn(21)},
				{aFn(34), bFn(34)},
				{aFn(13), bFn(13)},
				{aFn(51), bFn(51)},
				{aFn(50), bFn(50)},
			},
			expected: "[['one'] ['five'] ['two-one'] ['one-three'] ['five-zero']]",
		},
	}
	for _, c := range testCases {
		flowCtx := FlowCtx{
			evalCtx:  parser.EvalContext{},
			txnProto: &roachpb.Transaction{},
			clientDB: kvDB,
		}

		in := &RowBuffer{}
		for _, row := range c.input {
			encRow := make(sqlbase.EncDatumRow, len(row))
			for i, d := range row {
				encRow[i] = sqlbase.DatumToEncDatum(sqlbase.ColumnType{Kind: sqlbase.ColumnType_INT}, d)
			}
			if status := in.Push(encRow, ProducerMetadata{}); status != NeedMoreRows {
				t.Fatalf("unexpected response: %d", status)
			}
		}

		out := &RowBuffer{}
		jr, err := newJoinReader(&flowCtx, &JoinReaderSpec{Table: *td}, in, &c.post, out)
		if err != nil {
			t.Fatal(err)
		}

		jr.Run(context.Background(), nil)

		if !in.Done {
			t.Fatal("joinReader didn't consume all the rows")
		}
		if !out.ProducerClosed {
			t.Fatalf("output RowReceiver not closed")
		}

		var res sqlbase.EncDatumRows
		for {
			row, meta := out.Next()
			if !meta.Empty() {
				t.Fatalf("unexpected metadata: %v", meta)
			}
			if row == nil {
				break
			}
			res = append(res, row)
		}

		if result := res.String(); result != c.expected {
			t.Errorf("invalid results: %s, expected %s'", result, c.expected)
		}
	}
}

// TestJoinReaderDrain tests various scenarios in which a joinReader's consumer
// is closed.
func TestJoinReaderDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	sqlutils.CreateTable(
		t,
		sqlDB,
		"t",
		"a INT, PRIMARY KEY (a)",
		1, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn),
	)
	td := sqlbase.GetTableDescriptor(kvDB, "test", "t")

	flowCtx := FlowCtx{
		evalCtx:  parser.EvalContext{},
		txnProto: &roachpb.Transaction{},
		clientDB: kvDB,
	}

	encRow := make(sqlbase.EncDatumRow, 1)
	encRow[0] = sqlbase.DatumToEncDatum(
		sqlbase.ColumnType{Kind: sqlbase.ColumnType_INT},
		parser.NewDInt(1),
	)

	ctx := context.Background()

	// ConsumerClosed verifies that when a joinReader's consumer is closed, the
	// joinReader finishes gracefully.
	t.Run("ConsumerClosed", func(t *testing.T) {
		in := &RowBuffer{}
		if status := in.Push(encRow, ProducerMetadata{}); status != NeedMoreRows {
			t.Fatalf("unexpected response: %d", status)
		}

		out := &RowBuffer{}
		out.ConsumerClosed()
		jr, err := newJoinReader(&flowCtx, &JoinReaderSpec{Table: *td}, in, &PostProcessSpec{}, out)
		if err != nil {
			t.Fatal(err)
		}
		jr.Run(ctx, nil)
	})

	// ConsumerDone verifies that the producer drains properly by checking that
	// metadata coming from the producer is still read when ConsumerDone is
	// called on the consumer.
	t.Run("ConsumerDone", func(t *testing.T) {
		expectedMetaErr := errors.New("dummy")
		in := &RowBuffer{}
		if status := in.Push(encRow, ProducerMetadata{Err: expectedMetaErr}); status != NeedMoreRows {
			t.Fatalf("unexpected response: %d", status)
		}

		out := &RowBuffer{}
		out.ConsumerDone()
		jr, err := newJoinReader(&flowCtx, &JoinReaderSpec{Table: *td}, in, &PostProcessSpec{}, out)
		if err != nil {
			t.Fatal(err)
		}
		jr.Run(ctx, nil)
		row, meta := out.Next()
		if row != nil {
			t.Fatalf("row was pushed unexpectedly: %s", row)
		}
		if meta.Err != expectedMetaErr {
			t.Fatalf("unexpected error in metadata: %v", meta.Err)
		}
	})
}
