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
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"

	"golang.org/x/net/context"
)

func TestSorter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	columnTypeInt := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
	v := [6]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(columnTypeInt, parser.NewDInt(parser.DInt(i)))
	}

	asc := encoding.Ascending
	desc := encoding.Descending

	testCases := []struct {
		name     string
		spec     SorterSpec
		post     PostProcessSpec
		input    sqlbase.EncDatumRows
		expected sqlbase.EncDatumRows
	}{
		{
			name: "SortAll",
			// No specified input ordering and unspecified limit.
			spec: SorterSpec{
				OutputOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: asc},
						{ColIdx: 1, Direction: desc},
						{ColIdx: 2, Direction: asc},
					}),
			},
			input: sqlbase.EncDatumRows{
				{v[1], v[0], v[4]},
				{v[3], v[4], v[1]},
				{v[4], v[4], v[4]},
				{v[3], v[2], v[0]},
				{v[4], v[4], v[5]},
				{v[3], v[3], v[0]},
				{v[0], v[0], v[0]},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], v[0], v[0]},
				{v[1], v[0], v[4]},
				{v[3], v[4], v[1]},
				{v[3], v[3], v[0]},
				{v[3], v[2], v[0]},
				{v[4], v[4], v[4]},
				{v[4], v[4], v[5]},
			},
		}, {
			name: "SortLimit",
			// No specified input ordering but specified limit.
			spec: SorterSpec{
				OutputOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: asc},
						{ColIdx: 1, Direction: asc},
						{ColIdx: 2, Direction: asc},
					}),
			},
			post: PostProcessSpec{Limit: 4},
			input: sqlbase.EncDatumRows{
				{v[3], v[3], v[0]},
				{v[3], v[4], v[1]},
				{v[1], v[0], v[4]},
				{v[0], v[0], v[0]},
				{v[4], v[4], v[4]},
				{v[4], v[4], v[5]},
				{v[3], v[2], v[0]},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], v[0], v[0]},
				{v[1], v[0], v[4]},
				{v[3], v[2], v[0]},
				{v[3], v[3], v[0]},
			},
		}, {
			name: "SortMatchOrderingNoLimit",
			// Specified match ordering length but no specified limit.
			spec: SorterSpec{
				OrderingMatchLen: 2,
				OutputOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: asc},
						{ColIdx: 1, Direction: asc},
						{ColIdx: 2, Direction: asc},
					}),
			},
			input: sqlbase.EncDatumRows{
				{v[0], v[1], v[2]},
				{v[0], v[1], v[0]},
				{v[1], v[0], v[5]},
				{v[1], v[1], v[5]},
				{v[1], v[1], v[4]},
				{v[3], v[4], v[3]},
				{v[3], v[4], v[2]},
				{v[3], v[5], v[1]},
				{v[4], v[4], v[5]},
				{v[4], v[4], v[4]},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], v[1], v[0]},
				{v[0], v[1], v[2]},
				{v[1], v[0], v[5]},
				{v[1], v[1], v[4]},
				{v[1], v[1], v[5]},
				{v[3], v[4], v[2]},
				{v[3], v[4], v[3]},
				{v[3], v[5], v[1]},
				{v[4], v[4], v[4]},
				{v[4], v[4], v[5]},
			},
		}, {
			name: "SortInputOrderingNoLimit",
			// Specified input ordering but no specified limit.
			spec: SorterSpec{
				OrderingMatchLen: 2,
				OutputOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 1, Direction: asc},
						{ColIdx: 2, Direction: asc},
						{ColIdx: 3, Direction: asc},
					}),
			},
			input: sqlbase.EncDatumRows{
				{v[1], v[1], v[2], v[5]},
				{v[0], v[1], v[2], v[4]},
				{v[0], v[1], v[2], v[3]},
				{v[1], v[1], v[2], v[2]},
				{v[1], v[2], v[2], v[5]},
				{v[0], v[2], v[2], v[4]},
				{v[0], v[2], v[2], v[3]},
				{v[1], v[2], v[2], v[2]},
			},
			expected: sqlbase.EncDatumRows{
				{v[1], v[1], v[2], v[2]},
				{v[0], v[1], v[2], v[3]},
				{v[0], v[1], v[2], v[4]},
				{v[1], v[1], v[2], v[5]},
				{v[1], v[2], v[2], v[2]},
				{v[0], v[2], v[2], v[3]},
				{v[0], v[2], v[2], v[4]},
				{v[1], v[2], v[2], v[5]},
			},
		},
	}

	rocksDB, err := newTestingRocksDB()
	if err != nil {
		t.Fatal(err)
	}
	defer closeRocks(rocksDB)

	ctx := context.Background()
	for _, forceDisk := range []bool{false, true} {
		for _, c := range testCases {
			t.Run(c.name, func(t *testing.T) {
				types := make([]sqlbase.ColumnType, len(c.input[0]))
				for i := range types {
					types[i] = c.input[0][i].Type
				}
				in := NewRowBuffer(types, c.input, RowBufferArgs{})
				out := &RowBuffer{}
				evalCtx := parser.MakeTestingEvalContext()
				defer evalCtx.Stop(ctx)
				flowCtx := FlowCtx{
					evalCtx: evalCtx,
				}

				s, err := newSorter(
					&flowCtx,
					&c.spec,
					in,
					&c.post,
					out,
					rocksDB, /* localStorage */
					0,       /* localStoragePrefix */
				)
				if err != nil {
					t.Fatal(err)
				}
				s.testingKnobForceDisk = forceDisk
				s.Run(ctx, nil)
				if !out.ProducerClosed {
					t.Fatalf("output RowReceiver not closed")
				}

				var retRows sqlbase.EncDatumRows
				for {
					row, meta := out.Next()
					if !meta.Empty() {
						t.Fatalf("unexpected metadata: %v", meta)
					}
					if row == nil {
						break
					}
					retRows = append(retRows, row)
				}

				expStr := c.expected.String()
				retStr := retRows.String()
				if expStr != retStr {
					t.Errorf("invalid results; expected:\n   %s\ngot:\n   %s",
						expStr, retStr)
				}
			})
		}
	}
}

// BenchmarkSortAll times how long it takes to sort an input of varying length.
func BenchmarkSortAll(b *testing.B) {
	ctx := context.Background()
	evalCtx := parser.MakeTestingEvalContext()
	defer evalCtx.Stop(ctx)
	flowCtx := FlowCtx{
		evalCtx: evalCtx,
	}

	// One column integer rows.
	columnTypeInt := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
	types := []sqlbase.ColumnType{columnTypeInt}
	rng := rand.New(rand.NewSource(int64(timeutil.Now().UnixNano())))

	spec := SorterSpec{
		OutputOrdering: convertToSpecOrdering(sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}),
	}
	post := PostProcessSpec{}

	for _, inputSize := range []int{0, 1 << 2, 1 << 4, 1 << 8, 1 << 12, 1 << 16} {
		input := make(sqlbase.EncDatumRows, inputSize)
		for i := range input {
			input[i] = sqlbase.EncDatumRow{
				sqlbase.DatumToEncDatum(columnTypeInt, parser.NewDInt(parser.DInt(rng.Int()))),
			}
		}
		rowSource := NewRepeatableRowSource(types, input)
		s, err := newSorter(
			&flowCtx,
			&spec,
			rowSource,
			&post,
			&RowDisposer{},
			nil, /* localStorage */
			0,   /* localStoragePrefix */
		)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("InputSize%d", inputSize), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				s.Run(ctx, nil)
				rowSource.Reset()
			}
		})
	}
}

// BenchmarkSortLimit times how long it takes to sort a fixed size input with
// varying limits.
func BenchmarkSortLimit(b *testing.B) {
	ctx := context.Background()
	evalCtx := parser.MakeTestingEvalContext()
	defer evalCtx.Stop(ctx)
	flowCtx := FlowCtx{
		evalCtx: evalCtx,
	}

	// One column integer rows.
	columnTypeInt := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
	types := []sqlbase.ColumnType{columnTypeInt}
	rng := rand.New(rand.NewSource(int64(timeutil.Now().UnixNano())))

	spec := SorterSpec{
		OutputOrdering: convertToSpecOrdering(sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}),
	}

	inputSize := 1 << 16
	input := make(sqlbase.EncDatumRows, inputSize)
	for i := range input {
		input[i] = sqlbase.EncDatumRow{
			sqlbase.DatumToEncDatum(columnTypeInt, parser.NewDInt(parser.DInt(rng.Int()))),
		}
	}
	rowSource := NewRepeatableRowSource(types, input)

	for _, limit := range []uint64{1, 1 << 2, 1 << 4, 1 << 8, 1 << 12, 1 << 16} {
		s, err := newSorter(
			&flowCtx,
			&spec,
			rowSource,
			&PostProcessSpec{Limit: limit},
			&RowDisposer{},
			nil, /* localStorage */
			0,   /* localStoragePrefix */
		)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("InputSize%dLimit%d", inputSize, limit), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				s.Run(ctx, nil)
				rowSource.Reset()
			}
		})
	}
}
