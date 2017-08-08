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

package distsqlrun

import (
	"sort"
	"strings"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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

	columnTypeInt := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
	v := [15]sqlbase.EncDatum{}
	null := sqlbase.EncDatum{Datum: parser.DNull}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(columnTypeInt, parser.NewDInt(parser.DInt(i)))
	}

	columnTypeBool := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BOOL}
	boolTrue := sqlbase.DatumToEncDatum(columnTypeBool, parser.DBoolTrue)
	boolFalse := sqlbase.DatumToEncDatum(columnTypeBool, parser.DBoolFalse)
	boolNULL := sqlbase.DatumToEncDatum(columnTypeBool, parser.DNull)

	colPtr := func(idx uint32) *uint32 { return &idx }

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
						ColIdx: []uint32{0},
					},
					{
						Func:   AggregatorSpec_MAX,
						ColIdx: []uint32{0},
					},
					{
						Func:   AggregatorSpec_COUNT,
						ColIdx: []uint32{0},
					},
					{
						Func:   AggregatorSpec_AVG,
						ColIdx: []uint32{0},
					},
					{
						Func:   AggregatorSpec_SUM,
						ColIdx: []uint32{0},
					},
					{
						Func:   AggregatorSpec_STDDEV,
						ColIdx: []uint32{0},
					},
					{
						Func:   AggregatorSpec_VARIANCE,
						ColIdx: []uint32{0},
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
						ColIdx: []uint32{1},
					},
					{
						Func:   AggregatorSpec_COUNT,
						ColIdx: []uint32{0},
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
						ColIdx: []uint32{1},
					},
					{
						Func:   AggregatorSpec_COUNT,
						ColIdx: []uint32{0},
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
						ColIdx: []uint32{1},
					},
					{
						Func:   AggregatorSpec_SUM,
						ColIdx: []uint32{0},
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
						ColIdx: []uint32{0},
					},
					{
						Func:   AggregatorSpec_SUM,
						ColIdx: []uint32{0},
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
						ColIdx:   []uint32{0},
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
						ColIdx: []uint32{0},
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
						ColIdx: []uint32{0},
					},
					{
						Func:   AggregatorSpec_MIN,
						ColIdx: []uint32{1},
					},
					{
						Func:   AggregatorSpec_COUNT,
						ColIdx: []uint32{1},
					},
					{
						Func:     AggregatorSpec_COUNT,
						Distinct: true,
						ColIdx:   []uint32{1},
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
		}, {
			// SELECT MAX(@1) FILTER @2, COUNT(@3) FILTER @4, COUNT_ROWS FILTER @4
			spec: AggregatorSpec{
				Aggregations: []AggregatorSpec_Aggregation{
					{
						Func:         AggregatorSpec_MAX,
						ColIdx:       []uint32{0},
						FilterColIdx: colPtr(1),
					},
					{
						Func:         AggregatorSpec_COUNT,
						ColIdx:       []uint32{2},
						FilterColIdx: colPtr(3),
					},
					{
						Func:         AggregatorSpec_COUNT_ROWS,
						FilterColIdx: colPtr(3),
					},
				},
			},
			input: sqlbase.EncDatumRows{
				{v[1], boolTrue, v[1], boolTrue},
				{v[5], boolFalse, v[1], boolFalse},
				{v[2], boolTrue, v[1], boolNULL},
				{v[3], boolNULL, v[1], boolTrue},
				{v[2], boolTrue, v[1], boolTrue},
			},
			expected: sqlbase.EncDatumRows{
				{v[2], v[3], v[3]},
			},
		},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			ags := c.spec

			var types []sqlbase.ColumnType
			if len(c.input) == 0 {
				types = []sqlbase.ColumnType{columnTypeInt}
			}
			in := NewRowBuffer(types, c.input, RowBufferArgs{})
			out := &RowBuffer{}
			evalCtx := parser.MakeTestingEvalContext()
			defer evalCtx.Stop(context.Background())
			flowCtx := FlowCtx{
				Settings: cluster.MakeClusterSettings(),
				EvalCtx:  evalCtx,
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
		})
	}
}
