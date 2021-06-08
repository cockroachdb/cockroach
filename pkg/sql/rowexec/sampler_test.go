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
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// runSampler runs the samplerProcessor on numRows and returns numSamples rows.
func runSampler(
	t *testing.T,
	numRows, numSamples, minNumSamples, expectedSamples int,
	memLimitBytes int64,
	expectOutOfMemory bool,
) []int {
	rows := make([]rowenc.EncDatumRow, numRows)
	for i := range rows {
		rows[i] = rowenc.EncDatumRow{randgen.IntEncDatum(i)}
	}
	in := distsqlutils.NewRowBuffer(types.OneIntCol, rows, distsqlutils.RowBufferArgs{})
	outTypes := []*types.T{
		types.Int, // original column
		types.Int, // rank
		types.Int, // sketch index
		types.Int, // num rows
		types.Int, // null vals
		types.Bytes,
	}
	const (
		valCol = iota
		rankCol
		sketchIndexCol
		numRowsCol
		numNullsCol
		sketchDataCol
	)
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
		SampleSize:    uint32(numSamples),
		MinSampleSize: uint32(minNumSamples),
	}
	p, err := newSamplerProcessor(
		&flowCtx, 0 /* processorID */, spec, in, &execinfrapb.PostProcessSpec{}, out,
	)
	if err != nil {
		t.Fatal(err)
	}
	p.Run(context.Background())

	// Verify we have expectedSamples distinct rows.
	res := make([]int, 0, numSamples)
	seen := make(map[tree.DInt]bool)
	histogramDisabled := false
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

		if row[valCol].IsNull() {
			// This is a sketch row.
			continue
		}
		for i := sketchIndexCol; i < len(outTypes); i++ {
			if i != numRowsCol && !row[i].IsNull() {
				t.Fatalf("expected NULL on column %d, got %s", i, row[i].Datum)
			}
		}
		v := *row[valCol].Datum.(*tree.DInt)
		if seen[v] {
			t.Fatalf("duplicate row %d", v)
		}
		seen[v] = true
		res = append(res, int(v))
	}
	if expectOutOfMemory {
		if !histogramDisabled {
			t.Fatal("expected processor to disable histogram collection")
		}
	} else if histogramDisabled {
		t.Fatal("processor unexpectedly disabled histogram collection")
	} else if len(res) != expectedSamples {
		t.Fatalf("expected %d rows, got %d", expectedSamples, len(res))
	}
	return res
}

// checkSamplerDistribution runs the sampler many times over the same input and
// verifies that the distribution of samples approaches a uniform distribution.
func checkSamplerDistribution(
	t *testing.T,
	numRows, numSamples, minNumSamples, expectedSamples int,
	memLimitBytes int64,
	expectOutOfMemory bool,
) {
	minRuns := 200
	maxRuns := 5000
	delta := 0.5
	totalSamples := 0

	freq := make([]int, numRows)
	var err error
	// Instead of doing maxRuns and checking at the end, we do minRuns at a time
	// and exit as soon as the distribution is uniform enough. This speeds up the
	// test.
	for r := 0; r < maxRuns; r += minRuns {
		for i := 0; i < minRuns; i++ {
			for _, v := range runSampler(
				t, numRows, numSamples, minNumSamples, expectedSamples, memLimitBytes, expectOutOfMemory,
			) {
				freq[v]++
				totalSamples++
			}
		}

		// The expected frequency of each row is totalSamples / numRows.
		f := float64(totalSamples) / float64(numRows)

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
	// The distribution failed to become uniform enough after maxRuns.
	t.Error(err)
}

type testCase struct {
	numRows, numSamples, minNumSamples, expectedSamples int
	memLimit                                            int64
	expectOutOfMemory                                   bool
}

func TestSampler(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []testCase{
		// Check distribution when capacity is held steady.
		{100, 20, 20, 20, 0, false},
		// Check distribution when capacity shrinks due to hitting memory limit.
		{1000, 200, 20, 64, 1 << 14, false},
	} {
		checkSamplerDistribution(
			t, tc.numRows, tc.numSamples, tc.minNumSamples, tc.expectedSamples, tc.memLimit,
			tc.expectOutOfMemory,
		)
	}
}

func TestSamplerMemoryLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []testCase{
		// While holding numSamples and minNumSamples fixed, increase the memory
		// limit until we no longer hit it.
		{100, 20, 2, 20, 0, false},
		{100, 20, 2, 0, 1 << 0, true},
		{100, 20, 2, 0, 1 << 1, true},
		{100, 20, 2, 0, 1 << 4, true},
		{100, 20, 2, 0, 1 << 8, true},
		{100, 20, 2, 20, 1 << 14, false},

		// Same as above, but 10x larger population to check dynamic shrinking.
		{1000, 200, 20, 0, 1 << 13, true},
		{1000, 200, 20, 64, 1 << 14, false},
		{1000, 200, 20, 200, 1 << 15, false},

		// While holding the memory limit fixed, decrease minNumSamples until we no
		// longer hit the memory limit.
		{1000, 200, 100, 0, 1 << 14, true},
		{1000, 200, 75, 0, 1 << 14, true},
		{1000, 200, 50, 64, 1 << 14, false},
	} {
		runSampler(
			t, tc.numRows, tc.numSamples, tc.minNumSamples, tc.expectedSamples, tc.memLimit,
			tc.expectOutOfMemory,
		)
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
		{2, 8},
		{-1, 1},
		{-1, -1},
	}
	cardinalities := []int{3, 9, 12}
	numNulls := []int{4, 2, 1}

	rows := randgen.GenEncDatumRowsInt(inputRows)
	in := distsqlutils.NewRowBuffer(types.TwoIntCols, rows, distsqlutils.RowBufferArgs{})
	outTypes := []*types.T{
		types.Int,   // original column
		types.Int,   // original column
		types.Int,   // rank
		types.Int,   // sketch index
		types.Int,   // num rows
		types.Int,   // null vals
		types.Bytes, // sketch data
	}
	const (
		valCol0 = iota
		valCol1
		rankCol
		sketchIndexCol
		numRowsCol
		numNullsCol
		sketchDataCol
	)
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
		for i := valCol0; i < sketchIndexCol; i++ {
			if !r[i].IsNull() {
				t.Errorf("expected NULL on column %d, got %s", i, r[i].Datum)
			}
		}
		if v := int(*r[sketchIndexCol].Datum.(*tree.DInt)); v != sketchIdx {
			t.Errorf("expected sketch index %d, got %d", sketchIdx, v)
		}
		if v := int(*r[numRowsCol].Datum.(*tree.DInt)); v != len(inputRows) {
			t.Errorf("expected numRows %d, got %d", len(inputRows), v)
		}
		if v := int(*r[numNullsCol].Datum.(*tree.DInt)); v != numNulls[sketchIdx] {
			t.Errorf("expected numNulls %d, got %d", numNulls[sketchIdx], v)
		}
		data := []byte(*r[sketchDataCol].Datum.(*tree.DBytes))
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
