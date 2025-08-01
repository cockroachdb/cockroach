// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec

import (
	"context"
	"fmt"
	"testing"

	"github.com/axiomhq/hyperloglog"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execversion"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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
		types.Int, // size
		types.Bytes,
	}
	const (
		valCol = iota
		rankCol
		sketchIndexCol
		numRowsCol
		numNullsCol
		sizeCol
		sketchDataCol
	)
	out := distsqlutils.NewRowBuffer(outTypes, nil /* rows */, distsqlutils.RowBufferArgs{})

	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())
	flowCtx := execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
		Mon:     evalCtx.TestingMon,
	}
	// Override the default memory limit. If memLimitBytes is small but
	// non-zero, the processor will hit this limit and disable sampling.
	flowCtx.Cfg.TestingKnobs.MemoryLimitBytes = memLimitBytes

	ctx := execversion.TestingWithLatestCtx
	spec := &execinfrapb.SamplerSpec{
		Sketches: []execinfrapb.SketchSpec{
			{
				Columns:           []uint32{0},
				GenerateHistogram: true,
			},
		},
		SampleSize:    uint32(numSamples),
		MinSampleSize: uint32(minNumSamples),
	}
	p, err := newSamplerProcessor(
		ctx, &flowCtx, 0 /* processorID */, spec, in, &execinfrapb.PostProcessSpec{},
	)
	if err != nil {
		t.Fatal(err)
	}
	p.Run(ctx, out)

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
	defer log.Scope(t).Close(t)

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
	defer log.Scope(t).Close(t)

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
	defer log.Scope(t).Close(t)

	testCases := []struct {
		typs                []*types.T
		inputRows           interface{}
		cardinalities       []int
		valEncCardinalities []int
		numNulls            []int
		size                []int
		keySize             []int
		valueSize           []int
	}{
		{
			typs: types.TwoIntCols,
			inputRows: [][]int{
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
			},
			cardinalities: []int{3, 9, 12},
			numNulls:      []int{4, 2, 1},
			size:          []int{80, 96, 176},
			keySize:       []int{14, 14, 28},
			valueSize:     []int{24, 26, 50},
		},
		{
			typs: []*types.T{types.String, types.String},
			inputRows: [][]string{
				{"123", "1"},
				{"12345", "12345678"},
				{"", "1234"},
				{"1234", "123456"},
				{"1234", "123456789"},
			},
			cardinalities: []int{4, 5, 5},
			numNulls:      []int{1, 0, 0},
			size:          []int{80, 108, 188},
			keySize:       []int{29, 43, 72},
			valueSize:     []int{25, 38, 63},
		},
		{
			// The und-u-ks-level2 collation make the strings case-insensitive.
			typs: []*types.T{
				types.MakeCollatedType(types.String, "und-u-ks-level2"),
				types.MakeCollatedType(types.String, "und-u-ks-level2"),
			},
			inputRows: [][]string{
				{"foo", "bar"},
				{"FOO", "BAR"},
				{"", ""},
				{"bar", "bAr"},
				{"bar", "baR"},
			},
			cardinalities: []int{3, 2, 3},
			numNulls:      []int{1, 1, 1},
			size:          []int{352, 352, 704},
			keySize:       []int{89, 89, 178},
			valueSize:     []int{21, 21, 42},
		},
		{
			typs: []*types.T{types.Bytes, types.Bytes},
			inputRows: [][][]byte{
				{[]byte("\x16\x03123"), []byte("\x16\x011")},
				{[]byte("\x16\x0512345"), []byte("\x16\b12345678")},
				{[]byte("\x16\x00"), []byte("\x16\x041234")},
				{[]byte("\x16\x041234"), []byte("\x16\x06123456")},
				{[]byte("\x16\x041234"), []byte("\x16\t123456789")},
			},
			cardinalities: []int{4, 5, 5},
			numNulls:      []int{0, 0, 0},
			size:          []int{106, 118, 224},
			keySize:       []int{26, 38, 64},
			valueSize:     []int{26, 38, 64},
		},
		{
			typs: []*types.T{types.Float, types.Float},
			inputRows: [][]string{
				{"1.0", "1.0"},
				{"1.000", "1.00"},
				{"0", "0"},
				{"-0", "-0"},
			},
			cardinalities: []int{2, 2, 2},
			// Composite, value encoded datums may have different encodings for
			// semantically equivalent types, so the cardinality is not always
			// 100% accurate.
			valEncCardinalities: []int{3, 3, 3},
			numNulls:            []int{0, 0, 0},
			size:                []int{32, 32, 64},
			keySize:             []int{20, 20, 40},
			valueSize:           []int{36, 36, 72},
		},
	}

	for _, enc := range []catenumpb.DatumEncoding{
		randgen.DatumEncoding_NONE,
		catenumpb.DatumEncoding_ASCENDING_KEY,
		catenumpb.DatumEncoding_VALUE,
	} {
		t.Run(enc.String(), func(t *testing.T) {
			for _, tc := range testCases {
				// The output types don't need to match because they are never
				// used by the output row buffer. We only need the length to
				// match the number of output columns. There are 8 columns: 2
				// datums from the test case, a rank, a sketch index, a row
				// count, a null value count, a size, and sketch data.
				outTypes := make([]*types.T, 8)
				var rows rowenc.EncDatumRows
				inputLen := 0
				switch inputRows := tc.inputRows.(type) {
				case [][]int:
					inputLen = len(inputRows)
					rows = randgen.GenEncDatumRowsInt(inputRows, enc)
				case [][]float64:
				case [][]string:
					inputLen = len(inputRows)
					switch tc.typs[0].Family() {
					case types.StringFamily:
						rows = randgen.GenEncDatumRowsString(inputRows, enc)
					case types.CollatedStringFamily:
						rows = randgen.GenEncDatumRowsCollatedString(inputRows, tc.typs[0], enc)
					case types.FloatFamily:
						inputLen = len(inputRows)
						rows = randgen.GenEncDatumRowsFloat(inputRows, enc)
					}
				case [][][]byte:
					inputLen = len(inputRows)
					rows = randgen.GenEncDatumRowsBytes(inputRows, enc)
				default:
					panic(errors.AssertionFailedf("Type %T not supported for inputRows", t))
				}

				const (
					valCol0 = iota
					valCol1
					rankCol
					sketchIndexCol
					numRowsCol
					numNullsCol
					sizeCol
					sketchDataCol
				)
				in := distsqlutils.NewRowBuffer(tc.typs, rows, distsqlutils.RowBufferArgs{})
				out := distsqlutils.NewRowBuffer(outTypes, nil /* rows */, distsqlutils.RowBufferArgs{})

				st := cluster.MakeTestingClusterSettings()
				evalCtx := eval.MakeTestingEvalContext(st)
				defer evalCtx.Stop(context.Background())
				flowCtx := execinfra.FlowCtx{
					Cfg: &execinfra.ServerConfig{
						Settings: st,
					},
					EvalCtx: &evalCtx,
					Mon:     evalCtx.TestingMon,
				}

				ctx := execversion.TestingWithLatestCtx
				spec := &execinfrapb.SamplerSpec{
					SampleSize: uint32(1),
					Sketches: []execinfrapb.SketchSpec{
						{
							Columns: []uint32{
								0,
							},
						},
						{
							Columns: []uint32{
								1,
							},
						},
						{
							Columns: []uint32{
								0, 1,
							},
						},
					},
				}
				p, err := newSamplerProcessor(ctx, &flowCtx, 0 /* processorID */, spec, in, &execinfrapb.PostProcessSpec{})
				if err != nil {
					t.Fatal(err)
				}
				p.Run(ctx, out)

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
					if v := int(*r[numRowsCol].Datum.(*tree.DInt)); v != inputLen {
						t.Errorf("expected numRows %d, got %d", inputLen, v)
					}
					if v := int(*r[numNullsCol].Datum.(*tree.DInt)); v != tc.numNulls[sketchIdx] {
						t.Errorf("expected numNulls %d, got %d", tc.numNulls[sketchIdx], v)
					}
					size := int(*r[sizeCol].Datum.(*tree.DInt))
					var expectedSize int
					switch enc {
					case randgen.DatumEncoding_NONE:
						expectedSize = tc.size[sketchIdx]
					case catenumpb.DatumEncoding_ASCENDING_KEY:
						expectedSize = tc.keySize[sketchIdx]
					case catenumpb.DatumEncoding_VALUE:
						expectedSize = tc.valueSize[sketchIdx]
					}
					if size != expectedSize {
						t.Errorf("expected size %d, got %d", expectedSize, size)
					}
					data := []byte(*r[sketchDataCol].Datum.(*tree.DBytes))
					var s hyperloglog.Sketch
					if err := s.UnmarshalBinary(data); err != nil {
						t.Fatal(err)
					}
					if enc == catenumpb.DatumEncoding_VALUE && tc.valEncCardinalities != nil {
						// Value encoding cardinalities may be approximate for
						// composite types. Check against the expected value
						// encoded cardinalities instead.
						if v := int(s.Estimate()); v != tc.valEncCardinalities[sketchIdx] {
							t.Errorf("expected cardinality %d, got %d",
								tc.valEncCardinalities[sketchIdx], v)
						}
					} else {
						// HLL++ should be exact on small datasets.
						if v := int(s.Estimate()); v != tc.cardinalities[sketchIdx] {
							t.Errorf("expected cardinality %d, got %d",
								tc.cardinalities[sketchIdx], v)
						}
					}
				}
			}
		})
	}
}
