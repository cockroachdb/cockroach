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
	"context"
	"testing"

	"github.com/axiomhq/hyperloglog"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// runSampler runs the sampler aggregator on numRows and returns numSamples rows.
func runSampler(t *testing.T, numRows, numSamples int) []int {
	rows := make([]sqlbase.EncDatumRow, numRows)
	for i := range rows {
		rows[i] = sqlbase.EncDatumRow{intEncDatum(i)}
	}
	in := NewRowBuffer(oneIntCol, rows, RowBufferArgs{})
	outTypes := []sqlbase.ColumnType{
		intType, // original column
		intType, // rank
		intType, // sketch index
		intType, // num rows
		intType, // null vals
		{SemanticType: sqlbase.ColumnType_BYTES},
	}

	out := NewRowBuffer(outTypes, nil /* rows */, RowBufferArgs{})

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())
	flowCtx := FlowCtx{
		Ctx:      context.Background(),
		Settings: st,
		EvalCtx:  evalCtx,
	}

	spec := &SamplerSpec{SampleSize: uint32(numSamples)}
	p, err := newSamplerProcessor(&flowCtx, spec, in, &PostProcessSpec{}, out)
	if err != nil {
		t.Fatal(err)
	}
	p.Run(nil)

	// Verify we have numSamples distinct rows.
	res := make([]int, 0, numSamples)
	seen := make(map[tree.DInt]bool)
	n := 0
	for {
		row := out.NextNoMeta(t)
		if row == nil {
			break
		}
		for i := 2; i < len(outTypes); i++ {
			if !row[i].IsNull() {
				t.Fatalf("expected NULL on column %d, got %s", i, row[i].Datum)
			}
		}
		v := *row[0].Datum.(*tree.DInt)
		if seen[v] {
			t.Fatalf("duplicate row %d", v)
		}
		seen[v] = true
		res = append(res, int(v))
		n++
	}
	if n != numSamples {
		t.Fatalf("expected %d rows, got %d", numSamples, n)
	}
	return res
}

func TestSampler(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// We run many samplings and record the frequencies.
	numRows := 100
	numSamples := 20
	numRuns := 1000
	delta := 0.5

	freq := make([]int, numRows)
	for r := 0; r < numRuns; r++ {
		for _, v := range runSampler(t, numRows, numSamples) {
			freq[v]++
		}
	}

	// The expected frequency of each row is f = numRuns * (numSamples / numRows).
	f := float64(numRuns) * float64(numSamples) / float64(numRows)

	// Verify that no frequency is outside of the range (f / (1+delta), f * (1+delta));
	// the probability of a given row violating this is subject to the Chernoff
	// bound which decreases exponentially (with exponent f).
	for i := range freq {
		if float64(freq[i]) < f/(1+delta) || float64(freq[i]) > f*(1+delta) {
			t.Errorf("frequency %d out of bound (expected value %f)", freq[i], f)
		}
	}
}

func TestSamplerSketch(t *testing.T) {
	defer leaktest.AfterTest(t)()

	inputRows := [][]int{
		{1, 1},
		{2, 2},
		{1, 3},
		{2, 4},
		{1, 5},
		{2, 6},
		{1, 7},
		{2, 8},
		{-1, 1},
		{-1, 3},
		{1, -1},
	}
	cardinalities := []int{2, 8}
	numNulls := []int{2, 1}

	rows := genEncDatumRowsInt(inputRows)
	in := NewRowBuffer(twoIntCols, rows, RowBufferArgs{})
	outTypes := []sqlbase.ColumnType{
		intType, // original column
		intType, // original column
		intType, // rank
		intType, // sketch index
		intType, // num rows
		intType, // null vals
		{SemanticType: sqlbase.ColumnType_BYTES}, // sketch data
	}

	out := NewRowBuffer(outTypes, nil /* rows */, RowBufferArgs{})

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())
	flowCtx := FlowCtx{
		Ctx:      context.Background(),
		Settings: st,
		EvalCtx:  evalCtx,
	}

	spec := &SamplerSpec{
		SampleSize: uint32(1),
		Sketches: []SketchSpec{
			{
				SketchType: SketchType_HLL_PLUS_PLUS_V1,
				Columns:    []uint32{0},
			},
			{
				SketchType: SketchType_HLL_PLUS_PLUS_V1,
				Columns:    []uint32{1},
			},
		},
	}
	p, err := newSamplerProcessor(&flowCtx, spec, in, &PostProcessSpec{}, out)
	if err != nil {
		t.Fatal(err)
	}
	p.Run(nil)

	rows = out.GetRowsNoMeta(t)
	// We expect one sampled row and two sketch rows.
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %v\n", rows.String(outTypes))
	}
	rows = rows[1:]

	for sketchIdx, r := range rows {
		// First three columns are for sampled rows.
		for i := 0; i < 3; i++ {
			if !r[i].IsNull() {
				t.Errorf("expected NULL on column %d, got %s", i, r[i].Datum)
			}
		}
		if v := int(*r[3].Datum.(*tree.DInt)); v != sketchIdx {
			t.Errorf("expected sketch index %d, got %d", sketchIdx, v)
		}
		if v := int(*r[4].Datum.(*tree.DInt)); v != len(inputRows) {
			t.Errorf("expected numRows %d, got %d", len(inputRows), v)
		}
		if v := int(*r[5].Datum.(*tree.DInt)); v != numNulls[sketchIdx] {
			t.Errorf("expected numNulls %d, got %d", numNulls[sketchIdx], v)
		}
		data := []byte(*r[6].Datum.(*tree.DBytes))
		var s hyperloglog.Sketch
		if err := s.UnmarshalBinary(data); err != nil {
			t.Fatal(err)
		}
		// HLL++ should be exact on small datasets.
		if v := int(s.Estimate()); v != cardinalities[sketchIdx] {
			t.Errorf("expected cardinality %d, got %d", cardinalities[sketchIdx], v)
		}
	}
}
