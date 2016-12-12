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
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)

package distsql

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"

	"golang.org/x/net/context"
)

func TestEvaluator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	columnTypeInt := &sqlbase.ColumnType{Kind: sqlbase.ColumnType_INT}
	columnTypeBool := &sqlbase.ColumnType{Kind: sqlbase.ColumnType_BOOL}
	v := [15]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(*columnTypeInt, parser.NewDInt(parser.DInt(i)))
	}

	b := [2]sqlbase.EncDatum{}
	b[0] = sqlbase.DatumToEncDatum(*columnTypeBool, parser.DBoolTrue)
	b[1] = sqlbase.DatumToEncDatum(*columnTypeBool, parser.DBoolFalse)

	nullInt := sqlbase.DatumToEncDatum(*columnTypeInt, parser.DNull)

	testCases := []struct {
		spec     EvaluatorSpec
		input    sqlbase.EncDatumRows
		expected sqlbase.EncDatumRows
	}{
		{
			spec: EvaluatorSpec{
				Exprs: []Expression{{Expr: "@2"}, {Expr: "(((@1)))"}},
			},
			input: sqlbase.EncDatumRows{
				{v[1], v[2]},
				{v[3], v[4]},
				{v[6], v[2]},
				{v[7], v[2]},
				{v[8], v[4]},
				{nullInt, nullInt},
			},
			expected: sqlbase.EncDatumRows{
				{v[2], v[1]},
				{v[4], v[3]},
				{v[2], v[6]},
				{v[2], v[7]},
				{v[4], v[8]},
				{nullInt, nullInt},
			},
		}, {
			spec: EvaluatorSpec{
				Exprs: []Expression{
					{Expr: "@1 + @2"},
					{Expr: "@1 - @2"},
					{Expr: "@1 >= 8"},
				},
			},
			input: sqlbase.EncDatumRows{
				{v[10], v[0]},
				{v[9], v[1]},
				{v[8], v[2]},
				{v[7], v[3]},
				{v[6], v[4]},
			},
			expected: sqlbase.EncDatumRows{
				{v[10], v[10], b[0]},
				{v[10], v[8], b[0]},
				{v[10], v[6], b[0]},
				{v[10], v[4], b[1]},
				{v[10], v[2], b[1]},
			},
		}, {
			spec: EvaluatorSpec{
				Exprs: []Expression{
					{Expr: "@1 AND @1"},
					{Expr: "@1 AND @2"},
					{Expr: "NOT @1"},
				},
			},
			input: sqlbase.EncDatumRows{
				{b[0], b[1]},
			},
			expected: sqlbase.EncDatumRows{
				{b[0], b[1], b[1]},
			},
		},
		{
			spec: EvaluatorSpec{
				Exprs: []Expression{{Expr: "1"}},
			},
			input: sqlbase.EncDatumRows{
				{v[1], v[2]},
				{v[3], v[4]},
				{v[6], v[2]},
				{v[7], v[2]},
				{v[8], v[4]},
			},
			expected: sqlbase.EncDatumRows{
				{v[1]},
				{v[1]},
				{v[1]},
				{v[1]},
				{v[1]},
			},
		},
	}

	for _, c := range testCases {
		es := c.spec

		in := &RowBuffer{rows: c.input}
		out := &RowBuffer{}

		flowCtx := FlowCtx{
			Context: context.Background(),
			evalCtx: &parser.EvalContext{},
		}

		ev, err := newEvaluator(&flowCtx, &es, in, out)
		if err != nil {
			t.Fatal(err)
		}

		ev.Run(nil)
		if out.err != nil {
			t.Fatal(out.err)
		}
		if !out.closed {
			t.Fatalf("output RowReceiver not closed")
		}

		if result := out.rows.String(); result != c.expected.String() {
			t.Errorf("invalid results: %s, expected %s'", result, c.expected.String())
		}
	}
}
