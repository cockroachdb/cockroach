// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"
	"fmt"
	"testing"

	"github.com/axiomhq/hyperloglog"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// runSampler runs the sampler aggregator on numRows and returns numSamples rows.
func runSampler(
	t *testing.T, numRows, numSamples int, memLimitBytes int64, expectOutOfMemory bool,
) []int {
	rows := make([]sqlbase.EncDatumRow, numRows)
	for i := range rows {
		rows[i] = sqlbase.EncDatumRow{sqlbase.IntEncDatum(i)}
	}
	in := distsqlutils.NewRowBuffer(sqlbase.OneIntCol, rows, distsqlutils.RowBufferArgs{})
	outTypes := []*types.T{
		types.Int, // original column
		types.Int, // rank
		types.Int, // sketch index
		types.Int, // num rows
		types.Int, // null vals
		types.Bytes,
	}

	out := distsqlutils.NewRowBuffer(outTypes, nil /* rows */, distsqlutils.RowBufferArgs{})

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())
	flowCtx := execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}
	// Override the default memory limit. If memLimitBytes is small but
	// non-zero, the processor will hit this limit and disable sampling.
	flowCtx.Cfg.TestingKnobs.MemoryLimitBytes = memLimitBytes

	spec := &execinfrapb.SamplerSpec{
		Sketches: []execinfrapb.SketchSpec{
			{
				SketchType:        execinfrapb.SketchType_HLL_PLUS_PLUS_V1,
				Columns:           []uint32{0},
				GenerateHistogram: true,
			},
		},
		SampleSize: uint32(numSamples),
	}
	p, err := newSamplerProcessor(
		&flowCtx, 0 /* processorID */, spec, in, &execinfrapb.PostProcessSpec{}, out,
	)
	if err != nil {
		t.Fatal(err)
	}
	p.Run(context.Background())

	// Verify we have numSamples distinct rows.
	res := make([]int, 0, numSamples)
	seen := make(map[tree.DInt]bool)
	histogramDisabled := false
	n := 0
	for {
		row, meta := out.Next()
		if meta != nil {
			if meta.SamplerProgress == nil {
				t.Fatalf("unexpected metadata: %v", meta)
			}
			if meta.SamplerProgress.HistogramDisabled {
				histogramDisabled = true
			}
			continue
		} else if row == nil {
			break
		}
		if row[0].IsNull() {
			// This is a sketch row.
			continue
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
	if expectOutOfMemory {
		if !histogramDisabled {
			t.Fatal("expected processor to disable histogram collection")
		}
	} else if n != numSamples {
		t.Fatalf("expected %d rows, got %d", numSamples, n)
	}
	return res
}

func TestSampler(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// We run many samplings and record the frequencies.
	numRows := 100
	numSamples := 20
	minRuns := 200
	maxRuns := 5000
	delta := 0.5

	freq := make([]int, numRows)
	var err error
	// Instead of doing maxRuns and checking at the end, we do minRuns at a time
	// and exit early. This speeds up the test.
	for r := 0; r < maxRuns; r += minRuns {
		for i := 0; i < minRuns; i++ {
			for _, v := range runSampler(
				t, numRows, numSamples, 0 /* memLimitBytes */, false, /* expectOutOfMemory */
			) {
				freq[v]++
			}
		}

		// The expected frequency of each row is f = numRuns * (numSamples / numRows).
		f := float64(r) * float64(numSamples) / float64(numRows)

		// Verify that no frequency is outside of the range (f / (1+delta), f * (1+delta));
		// the probability of a given row violating this is subject to the Chernoff
		// bound which decreases exponentially (with exponent f).
		err = nil
		for i := range freq {
			if float64(freq[i]) < f/(1+delta) || float64(freq[i]) > f*(1+delta) {
				err = fmt.Errorf("frequency %d out of bound (expected value %f)", freq[i], f)
				break
			}
		}
		if err == nil {
			return
		}
	}
	t.Error(err)
}

func TestSamplerMemoryLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	numRows := 100
	numSamples := 20

	runSampler(t, numRows, numSamples, 0 /* memLimitBytes */, false /* expectOutOfMemory */)
	runSampler(t, numRows, numSamples, 1 /* memLimitBytes */, true /* expectOutOfMemory */)
	runSampler(t, numRows, numSamples, 20 /* memLimitBytes */, true /* expectOutOfMemory */)
	runSampler(t, numRows, numSamples, 20*1024 /* memLimitBytes */, false /* expectOutOfMemory */)
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
		{2, 8},
		{-1, 1},
		{-1, -1},
	}
	cardinalities := []int{3, 9, 12}
	numNulls := []int{4, 2, 1}

	rows := sqlbase.GenEncDatumRowsInt(inputRows)
	in := distsqlutils.NewRowBuffer(sqlbase.TwoIntCols, rows, distsqlutils.RowBufferArgs{})
	outTypes := []*types.T{
		types.Int,   // original column
		types.Int,   // original column
		types.Int,   // rank
		types.Int,   // sketch index
		types.Int,   // num rows
		types.Int,   // null vals
		types.Bytes, // sketch data
	}

	out := distsqlutils.NewRowBuffer(outTypes, nil /* rows */, distsqlutils.RowBufferArgs{})

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())
	flowCtx := execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}

	spec := &execinfrapb.SamplerSpec{
		SampleSize: uint32(1),
		Sketches: []execinfrapb.SketchSpec{
			{
				SketchType: execinfrapb.SketchType_HLL_PLUS_PLUS_V1,
				Columns:    []uint32{0},
			},
			{
				SketchType: execinfrapb.SketchType_HLL_PLUS_PLUS_V1,
				Columns:    []uint32{1},
			},
			{
				SketchType: execinfrapb.SketchType_HLL_PLUS_PLUS_V1,
				Columns:    []uint32{0, 1},
			},
		},
	}
	p, err := newSamplerProcessor(&flowCtx, 0 /* processorID */, spec, in, &execinfrapb.PostProcessSpec{}, out)
	if err != nil {
		t.Fatal(err)
	}
	p.Run(context.Background())

	// Collect the rows, excluding metadata.
	rows = rows[:0]
	for {
		row, meta := out.Next()
		if meta != nil {
			if meta.SamplerProgress == nil {
				t.Fatalf("unexpected metadata: %v", meta)
			}
			continue
		} else if row == nil {
			break
		}
		rows = append(rows, row)
	}

	// We expect one sampled row and three sketch rows.
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %v\n", rows.String(outTypes))
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
