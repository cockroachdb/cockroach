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
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"golang.org/x/net/context"
)

// TODO(irfansharif): Add tests to verify the following aggregation functions:
//      AVG
//      BOOL_AND
//      BOOL_OR
//      CONCAT_AGG
//      STDDEV
//      VARIANCE
// TODO(irfansharif): Replicate sql/testdata and TestLogic for distsql, this kind of manual
// case-by-case testing is error prone making it very easy to miss edge cases.
func TestAggregator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := [15]sqlbase.EncDatum{}
	for i := range v {
		v[i].SetDatum(sqlbase.ColumnType_INT, parser.NewDInt(parser.DInt(i)))
	}

	testCases := []struct {
		spec     AggregatorSpec
		input    sqlbase.EncDatumRows
		expected sqlbase.EncDatumRows
	}{
		{
			// SELECT $1, COUNT($0), GROUP BY $1.
			spec: AggregatorSpec{
				Types:     []sqlbase.ColumnType_Kind{sqlbase.ColumnType_INT, sqlbase.ColumnType_INT},
				GroupCols: []uint32{1},
				Exprs: []AggregatorSpec_Expr{
					{
						Func:   AggregatorSpec_IDENT,
						ColIdx: 1,
					},
					{
						Func:   AggregatorSpec_COUNT,
						ColIdx: 0,
					},
				},
			},
			input: sqlbase.EncDatumRows{
				{v[1], v[2]},
				{v[3], v[4]},
				{v[6], v[2]},
				{v[7], v[2]},
				{v[8], v[4]},
			},
			expected: sqlbase.EncDatumRows{
				{v[4], v[2]},
				{v[2], v[3]},
			},
		}, {
			// SELECT $1, SUM($0), GROUP BY $1.
			spec: AggregatorSpec{
				Types:     []sqlbase.ColumnType_Kind{sqlbase.ColumnType_INT, sqlbase.ColumnType_INT},
				GroupCols: []uint32{1},
				Exprs: []AggregatorSpec_Expr{
					{
						Func:   AggregatorSpec_IDENT,
						ColIdx: 1,
					},
					{
						Func:   AggregatorSpec_SUM,
						ColIdx: 0,
					},
				},
			},
			input: sqlbase.EncDatumRows{
				{v[1], v[2]},
				{v[3], v[4]},
				{v[6], v[2]},
				{v[7], v[2]},
				{v[8], v[4]},
			},
			expected: sqlbase.EncDatumRows{
				{v[2], v[14]},
				{v[4], v[11]},
			},
		}, {
			// SELECT COUNT($0), SUM($0), GROUP BY [] (empty group key).
			spec: AggregatorSpec{
				Types: []sqlbase.ColumnType_Kind{sqlbase.ColumnType_INT, sqlbase.ColumnType_INT},
				Exprs: []AggregatorSpec_Expr{
					{
						Func:   AggregatorSpec_COUNT,
						ColIdx: 0,
					},
					{
						Func:   AggregatorSpec_SUM,
						ColIdx: 0,
					},
				},
			},
			input: sqlbase.EncDatumRows{
				{v[1], v[2]},
				{v[1], v[4]},
				{v[3], v[2]},
				{v[4], v[2]},
				{v[5], v[4]},
			},
			expected: sqlbase.EncDatumRows{
				{v[5], v[14]},
			},
		},
		{
			// SELECT SUM DISTINCT ($0), GROUP BY [] (empty group key).
			spec: AggregatorSpec{
				Types: []sqlbase.ColumnType_Kind{sqlbase.ColumnType_INT, sqlbase.ColumnType_INT},
				Exprs: []AggregatorSpec_Expr{
					{
						Func:     AggregatorSpec_SUM,
						Distinct: true,
						ColIdx:   0,
					},
				},
			},
			input: sqlbase.EncDatumRows{
				{v[2]},
				{v[4]},
				{v[2]},
				{v[2]},
				{v[4]},
			},
			expected: sqlbase.EncDatumRows{
				{v[6]},
			},
		},
		{
			// SELECT $0, GROUP BY [] (empty group key).
			spec: AggregatorSpec{
				Types: []sqlbase.ColumnType_Kind{sqlbase.ColumnType_INT, sqlbase.ColumnType_INT},
				Exprs: []AggregatorSpec_Expr{
					{
						Func:   AggregatorSpec_IDENT,
						ColIdx: 0,
					},
				},
			},
			input: sqlbase.EncDatumRows{
				{v[1]},
				{v[1]},
				{v[1]},
			},
			expected: sqlbase.EncDatumRows{
				{v[1]},
			},
		}, {
			// SELECT MAX($0), MIN($1), COUNT($1), COUNT DISTINCT ($1), GROUP BY [] (empty group key).
			spec: AggregatorSpec{
				Types: []sqlbase.ColumnType_Kind{sqlbase.ColumnType_INT, sqlbase.ColumnType_INT},
				Exprs: []AggregatorSpec_Expr{
					{
						Func:   AggregatorSpec_MAX,
						ColIdx: 0,
					},
					{
						Func:   AggregatorSpec_MIN,
						ColIdx: 1,
					},
					{
						Func:   AggregatorSpec_COUNT,
						ColIdx: 1,
					},
					{
						Func:     AggregatorSpec_COUNT,
						Distinct: true,
						ColIdx:   1,
					},
				},
			},
			input: sqlbase.EncDatumRows{
				{v[2], v[2]},
				{v[1], v[4]},
				{v[3], v[2]},
				{v[4], v[2]},
				{v[5], v[4]},
			},
			expected: sqlbase.EncDatumRows{
				{v[5], v[2], v[5], v[2]},
			},
		},
	}

	for _, c := range testCases {
		ags := c.spec

		in := &RowBuffer{rows: c.input}
		out := &RowBuffer{}

		flowCtx := FlowCtx{
			Context: context.Background(),
			evalCtx: &parser.EvalContext{},
		}

		ag, err := newAggregator(&flowCtx, &ags, in, out)
		if err != nil {
			t.Fatal(err)
		}

		ag.Run(nil)

		var expected []string
		for _, row := range c.expected {
			expected = append(expected, row.String())
		}
		sort.Strings(expected)
		expStr := strings.Join(expected, "")

		var rets []string
		for {
			row, err := out.NextRow()
			if err != nil {
				t.Fatal(err)
			}
			if row == nil {
				break
			}
			rets = append(rets, row.String())
		}
		sort.Strings(rets)
		retStr := strings.Join(rets, "")

		if expStr != retStr {
			t.Errorf("invalid results; expected:\n   %s\ngot:\n   %s",
				expStr, retStr)
		}
	}
}
