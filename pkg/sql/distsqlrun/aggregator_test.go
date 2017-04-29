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

package distsqlrun

import (
	"math"
	"sort"
	"strings"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TODO(irfansharif): Add tests to verify the following aggregation functions:
//      AVG
//      BOOL_AND
//      BOOL_OR
//      CONCAT_AGG
//      STDDEV
//      VARIANCE
func TestAggregator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	columnTypeInt := sqlbase.ColumnType{Kind: sqlbase.ColumnType_INT}
	v := [15]sqlbase.EncDatum{}
	null := sqlbase.EncDatum{Datum: parser.DNull}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(columnTypeInt, parser.NewDInt(parser.DInt(i)))
	}

	testCases := []struct {
		spec     AggregatorSpec
		input    sqlbase.EncDatumRows
		expected sqlbase.EncDatumRows
	}{
		{
			// SELECT MIN(@0), MAX(@0), COUNT(@0), AVG(@0), SUM(@0), STDDEV(@0),
			// VARIANCE(@0) GROUP BY [] (no rows).
			spec: AggregatorSpec{
				Aggregations: []AggregatorSpec_Aggregation{
					{
						Func:   AggregatorSpec_MIN,
						ColIdx: 0,
					},
					{
						Func:   AggregatorSpec_MAX,
						ColIdx: 0,
					},
					{
						Func:   AggregatorSpec_COUNT,
						ColIdx: 0,
					},
					{
						Func:   AggregatorSpec_AVG,
						ColIdx: 0,
					},
					{
						Func:   AggregatorSpec_SUM,
						ColIdx: 0,
					},
					{
						Func:   AggregatorSpec_STDDEV,
						ColIdx: 0,
					},
					{
						Func:   AggregatorSpec_VARIANCE,
						ColIdx: 0,
					},
				},
			},
			input: sqlbase.EncDatumRows{},
			expected: sqlbase.EncDatumRows{
				{null, null, v[0], null, null, null, null},
			},
		},
		{
			// SELECT @2, COUNT(@1), GROUP BY @2.
			spec: AggregatorSpec{
				GroupCols: []uint32{1},
				Aggregations: []AggregatorSpec_Aggregation{
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
				{v[3], null},
				{v[6], v[2]},
				{v[7], v[2]},
				{v[8], v[4]},
			},
			expected: sqlbase.EncDatumRows{
				{null, v[1]},
				{v[4], v[1]},
				{v[2], v[3]},
			},
		},
		{
			// SELECT @2, COUNT(@1), GROUP BY @2.
			spec: AggregatorSpec{
				GroupCols: []uint32{1},
				Aggregations: []AggregatorSpec_Aggregation{
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
			// SELECT @2, SUM(@1), GROUP BY @2.
			spec: AggregatorSpec{
				GroupCols: []uint32{1},
				Aggregations: []AggregatorSpec_Aggregation{
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
			// SELECT COUNT(@1), SUM(@1), GROUP BY [] (empty group key).
			spec: AggregatorSpec{
				Aggregations: []AggregatorSpec_Aggregation{
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
			// SELECT SUM DISTINCT (@1), GROUP BY [] (empty group key).
			spec: AggregatorSpec{
				Aggregations: []AggregatorSpec_Aggregation{
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
			// SELECT @1, GROUP BY [] (empty group key).
			spec: AggregatorSpec{
				Aggregations: []AggregatorSpec_Aggregation{
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
			// SELECT MAX(@1), MIN(@2), COUNT(@2), COUNT DISTINCT (@2), GROUP BY [] (empty group key).
			spec: AggregatorSpec{
				Aggregations: []AggregatorSpec_Aggregation{
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

		var types []sqlbase.ColumnType
		if len(c.input) == 0 {
			types = []sqlbase.ColumnType{columnTypeInt}
		}
		in := NewRowBuffer(types, c.input, RowBufferArgs{})
		out := &RowBuffer{}

		monitor := mon.MakeUnlimitedMonitor(context.Background(), "test", nil, nil, math.MaxInt64)
		flowCtx := FlowCtx{
			evalCtx: parser.EvalContext{Mon: &monitor},
		}

		ag, err := newAggregator(&flowCtx, &ags, in, &PostProcessSpec{}, out)
		if err != nil {
			t.Fatal(err)
		}

		ag.Run(context.Background(), nil)

		var expected []string
		for _, row := range c.expected {
			expected = append(expected, row.String())
		}
		sort.Strings(expected)
		expStr := strings.Join(expected, "")

		var rets []string
		for {
			row, meta := out.Next()
			if !meta.Empty() {
				t.Fatalf("unexpected metadata: %v", meta)
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
		monitor.Stop(context.Background())
	}
}
