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
	gosql "database/sql"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

type resultBucket struct {
	numEq, numRange, upper int
}

type result struct {
	tableID                            int
	name, colIDs                       string
	rowCount, distinctCount, nullCount int
	buckets                            []resultBucket
}

// runSampleAggregator runs a full distsql sampling flow on a canned set of
// input rows, and checks that the generated stats match what we expect.
func runSampleAggregator(
	t *testing.T,
	server serverutils.TestServerInterface,
	sqlDB *gosql.DB,
	kvDB *kv.DB,
	st *cluster.Settings,
	evalCtx *tree.EvalContext,
	memLimitBytes int64,
	expectOutOfMemory bool,
	childNumSamples, childMinNumSamples uint32,
	aggNumSamples, aggMinNumSamples uint32,
	maxBuckets, expectedMaxBuckets uint32,
	inputRows [][]int,
	expected []result,
) {
	flowCtx := execinfra.FlowCtx{
		EvalCtx: evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
			DB:       kvDB,
			Executor: server.InternalExecutor().(sqlutil.InternalExecutor),
			Gossip:   gossip.MakeOptionalGossip(server.GossipI().(*gossip.Gossip)),
		},
	}
	// Override the default memory limit. If memLimitBytes is small but
	// non-zero, the processor will hit this limit and disable sampling.
	flowCtx.Cfg.TestingKnobs.MemoryLimitBytes = memLimitBytes

	// We randomly distribute the input rows between multiple Samplers and
	// aggregate the results.
	numSamplers := 3

	samplerOutTypes := []*types.T{
		types.Int,   // original column
		types.Int,   // original column
		types.Int,   // rank
		types.Int,   // sketch index
		types.Int,   // num rows
		types.Int,   // null vals
		types.Bytes, // sketch data
		types.Int,   // inverted index column
		types.Bytes, // inverted index data
	}

	sketchSpecs := []execinfrapb.SketchSpec{
		{
			SketchType:        execinfrapb.SketchType_HLL_PLUS_PLUS_V1,
			Columns:           []uint32{0},
			GenerateHistogram: false,
			StatName:          "a",
		},
		{
			SketchType:          execinfrapb.SketchType_HLL_PLUS_PLUS_V1,
			Columns:             []uint32{1},
			GenerateHistogram:   true,
			HistogramMaxBuckets: maxBuckets,
		},
	}

	rng, _ := randutil.NewPseudoRand()
	rowPartitions := make([][][]int, numSamplers)
	for _, row := range inputRows {
		j := rng.Intn(numSamplers)
		rowPartitions[j] = append(rowPartitions[j], row)
	}

	outputs := make([]*distsqlutils.RowBuffer, numSamplers)
	for i := 0; i < numSamplers; i++ {
		rows := randgen.GenEncDatumRowsInt(rowPartitions[i])
		in := distsqlutils.NewRowBuffer(types.TwoIntCols, rows, distsqlutils.RowBufferArgs{})
		outputs[i] = distsqlutils.NewRowBuffer(samplerOutTypes, nil /* rows */, distsqlutils.RowBufferArgs{})

		spec := &execinfrapb.SamplerSpec{
			SampleSize:    childNumSamples,
			MinSampleSize: childMinNumSamples,
			Sketches:      sketchSpecs,
		}
		p, err := newSamplerProcessor(
			&flowCtx, 0 /* processorID */, spec, in, &execinfrapb.PostProcessSpec{}, outputs[i],
		)
		if err != nil {
			t.Fatal(err)
		}
		p.Run(context.Background())
	}
	// Randomly interleave the output rows from the samplers into a single buffer.
	samplerResults := distsqlutils.NewRowBuffer(samplerOutTypes, nil /* rows */, distsqlutils.RowBufferArgs{})
	for len(outputs) > 0 {
		i := rng.Intn(len(outputs))
		row, meta := outputs[i].Next()
		if meta != nil {
			if meta.SamplerProgress == nil {
				t.Fatalf("unexpected metadata: %v", meta)
			}
		} else if row == nil {
			outputs = append(outputs[:i], outputs[i+1:]...)
		} else {
			samplerResults.Push(row, nil /* meta */)
		}
	}

	// Now run the sample aggregator.
	finalOut := distsqlutils.NewRowBuffer([]*types.T{}, nil /* rows*/, distsqlutils.RowBufferArgs{})
	spec := &execinfrapb.SampleAggregatorSpec{
		SampleSize:       aggNumSamples,
		MinSampleSize:    aggMinNumSamples,
		Sketches:         sketchSpecs,
		SampledColumnIDs: []descpb.ColumnID{100, 101},
		TableID:          13,
	}

	agg, err := newSampleAggregator(
		&flowCtx, 0 /* processorID */, spec, samplerResults, &execinfrapb.PostProcessSpec{}, finalOut,
	)
	if err != nil {
		t.Fatal(err)
	}
	agg.Run(context.Background())
	// Make sure there was no error.
	finalOut.GetRowsNoMeta(t)
	r := sqlutils.MakeSQLRunner(sqlDB)

	rows := r.Query(t, `
	  SELECT "tableID",
					 "name",
					 "columnIDs",
					 "rowCount",
					 "distinctCount",
					 "nullCount",
					 histogram
	  FROM system.table_statistics
  `)
	defer rows.Close()

	for _, exp := range expected {
		if !rows.Next() {
			t.Fatal("fewer rows than expected")
		}
		if expectOutOfMemory {
			exp.buckets = nil
		}

		var histData []byte
		var name gosql.NullString
		var r result
		if err := rows.Scan(
			&r.tableID, &name, &r.colIDs, &r.rowCount, &r.distinctCount, &r.nullCount, &histData,
		); err != nil {
			t.Fatal(err)
		}
		if name.Valid {
			r.name = name.String
		} else {
			r.name = "<NULL>"
		}

		if len(histData) > 0 {
			var h stats.HistogramData
			if err := protoutil.Unmarshal(histData, &h); err != nil {
				t.Fatal(err)
			}

			for _, b := range h.Buckets {
				ed, _, err := rowenc.EncDatumFromBuffer(
					types.Int, descpb.DatumEncoding_ASCENDING_KEY, b.UpperBound,
				)
				if err != nil {
					t.Fatal(err)
				}
				var d rowenc.DatumAlloc
				if err := ed.EnsureDecoded(types.Int, &d); err != nil {
					t.Fatal(err)
				}
				r.buckets = append(r.buckets, resultBucket{
					numEq:    int(b.NumEq),
					numRange: int(b.NumRange),
					upper:    int(*ed.Datum.(*tree.DInt)),
				})
			}

			// If we collected fewer samples than rows, the generated histogram will
			// be nondeterministic. Rather than checking for an exact match, verify
			// some properties and then ignore it.
			if childNumSamples < uint32(len(inputRows)) || aggNumSamples < uint32(len(inputRows)) {
				if uint32(len(r.buckets)) > expectedMaxBuckets {
					t.Errorf(
						"Expected at most %d buckets, got %d:\n  %v", expectedMaxBuckets, len(r.buckets), r,
					)
				}
				var count int
				for _, bucket := range r.buckets {
					count += bucket.numEq + bucket.numRange
				}
				if count != r.rowCount-r.nullCount {
					t.Errorf(
						"Expected %d rows counted in histogram, got %d:\n  %v",
						r.rowCount-r.nullCount, count, r,
					)
				}
				r.buckets = nil
				exp.buckets = nil
			}
		} else if len(exp.buckets) > 0 {
			t.Error("no histogram")
		}

		if !reflect.DeepEqual(exp, r) {
			t.Errorf("Expected:\n  %v\ngot:\n  %v", exp, r)
		}
	}
	if rows.Next() {
		t.Fatal("more rows than expected")
	}
}

func TestSampleAggregator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	server, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer server.Stopper().Stop(context.Background())

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())

	type sampAggTestCase struct {
		memLimitBytes                       int64
		expectOutOfMemory                   bool
		childNumSamples, childMinNumSamples uint32
		aggNumSamples, aggMinNumSamples     uint32
		maxBuckets, expectedMaxBuckets      uint32
		inputRows                           [][]int
		expected                            []result
	}

	inputRowsA := [][]int{
		{-1, 1},
		{1, 1},
		{2, 2},
		{1, 3},
		{2, 4},
		{1, 5},
		{2, 6},
		{1, 7},
		{2, 8},
		{-1, 3},
		{1, -1},
	}

	expectedA := []result{
		{
			tableID:       13,
			name:          "a",
			colIDs:        "{100}",
			rowCount:      11,
			distinctCount: 3,
			nullCount:     2,
		},
		{
			tableID:       13,
			name:          "<NULL>",
			colIDs:        "{101}",
			rowCount:      11,
			distinctCount: 9,
			nullCount:     1,
			buckets: []resultBucket{
				{numEq: 2, numRange: 0, upper: 1},
				{numEq: 2, numRange: 1, upper: 3},
				{numEq: 1, numRange: 1, upper: 5},
				{numEq: 1, numRange: 2, upper: 8},
			},
		},
	}

	var inputRowsB [][]int
	for i := 0; i < 1000; i++ {
		inputRowsB = append(inputRowsB, []int{i, i})
	}

	expectedB := []result{
		{
			tableID:       13,
			name:          "a",
			colIDs:        "{100}",
			rowCount:      1000,
			distinctCount: 1000,
			nullCount:     0,
		},
		{
			tableID:       13,
			name:          "<NULL>",
			colIDs:        "{101}",
			rowCount:      1000,
			distinctCount: 1000,
			nullCount:     0,
			buckets: []resultBucket{
				// The "B" cases will always sample fewer than 100% of rows, so the
				// expected histogram just needs to have one dummy bucket and will not
				// actually be checked.
				{numEq: 0, numRange: 0, upper: 0},
			},
		},
	}

	for _, tc := range []sampAggTestCase{
		// Sample all rows, check that stats match expected results exactly except
		// with histograms disabled in cases when stats collection hits the memory
		// limit.
		{0, false, 100, 100, 100, 100, 4, 4, inputRowsA, expectedA},
		{1 << 0, true, 100, 100, 100, 100, 4, 4, inputRowsA, expectedA},
		{1 << 4, true, 100, 100, 100, 100, 4, 4, inputRowsA, expectedA},
		{1 << 15, false, 100, 100, 100, 100, 4, 4, inputRowsA, expectedA},

		// Sample some rows, check that stats match expected results except for
		// histograms, which are nondeterministic. Only check the number of buckets
		// and the number of rows counted by the histogram. This also tests that
		// sampleAggregator can dynamically shrink capacity if fed from a
		// lower-capacity samplerProcessor.
		{0, false, 2, 2, 2, 2, 2, 2, inputRowsA, expectedA},
		{0, false, 2, 2, 2, 2, 4, 2, inputRowsA, expectedA},
		{0, false, 100, 100, 2, 2, 4, 2, inputRowsA, expectedA},
		{0, false, 2, 2, 100, 100, 4, 2, inputRowsA, expectedA},

		// Sample some rows with dynamic shrinking due to memory limits. Check that
		// stats match and that histograms have the right number of buckets and
		// number of rows.
		{1 << 15, false, 200, 20, 200, 20, 200, 50, inputRowsB, expectedB},
		{1 << 16, false, 200, 20, 200, 20, 200, 200, inputRowsB, expectedB},
	} {
		runSampleAggregator(
			t, server, sqlDB, kvDB, st, &evalCtx, tc.memLimitBytes, tc.expectOutOfMemory,
			tc.childNumSamples, tc.childMinNumSamples, tc.aggNumSamples, tc.aggMinNumSamples,
			tc.maxBuckets, tc.expectedMaxBuckets, tc.inputRows, tc.expected,
		)
	}
}
