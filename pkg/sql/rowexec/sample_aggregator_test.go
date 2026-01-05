// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec

import (
	"context"
	gosql "database/sql"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execversion"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
)

type resultBucket struct {
	numEq, numRange, upper int
}

type result struct {
	tableID                                     int
	name, colIDs                                string
	rowCount, distinctCount, nullCount, avgSize int
	buckets                                     []resultBucket
}

// runSampleAggregator runs a full distsql sampling flow on a canned set of
// input rows, and checks that the generated stats match what we expect.
func runSampleAggregator(
	t *testing.T,
	server serverutils.ApplicationLayerInterface,
	sqlDB *gosql.DB,
	st *cluster.Settings,
	evalCtx *eval.Context,
	memLimitBytes int64,
	expectOutOfMemory bool,
	childNumSamples, childMinNumSamples uint32,
	aggNumSamples, aggMinNumSamples uint32,
	maxBuckets, expectedMaxBuckets uint32,
	inputRows interface{},
	expected []result,
) {
	flowCtx := execinfra.FlowCtx{
		EvalCtx: evalCtx,
		Mon:     evalCtx.TestingMon,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
			DB:       server.InternalDB().(descs.DB),
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
		types.Int,   // size
		types.Bytes, // sketch data
		types.Int,   // inverted index column
		types.Bytes, // inverted index data
	}

	sketchSpecs := []execinfrapb.SketchSpec{
		{
			Columns:           []uint32{0},
			GenerateHistogram: false,
			StatName:          "a",
		},
		{
			Columns:             []uint32{1},
			GenerateHistogram:   true,
			HistogramMaxBuckets: maxBuckets,
		},
	}

	rng, _ := randutil.NewTestRand()

	// Randomly partition the inputRows for each sampler and encode them as
	// EncDatum in a row buffer so the samplers can ingest them. Since encoding
	// inputRows depends on their type, we handle each type separately.
	in := make([]*distsqlutils.RowBuffer, numSamplers)
	inLen := 0
	histEncType := types.Int
	switch t := inputRows.(type) {
	case [][]int:
		rowPartitions := make([][][]int, numSamplers)
		inLen = len(inputRows.([][]int))
		for _, row := range inputRows.([][]int) {
			j := rng.Intn(numSamplers)
			rowPartitions[j] = append(rowPartitions[j], row)
		}
		for i := 0; i < numSamplers; i++ {
			rows := randgen.GenEncDatumRowsInt(rowPartitions[i], randgen.DatumEncoding_NONE)
			in[i] = distsqlutils.NewRowBuffer(types.TwoIntCols, rows, distsqlutils.RowBufferArgs{})
		}

	case [][]string:
		rowPartitions := make([][][]string, numSamplers)
		inLen = len(inputRows.([][]string))
		histEncType = types.String
		for _, row := range inputRows.([][]string) {
			j := rng.Intn(numSamplers)
			rowPartitions[j] = append(rowPartitions[j], row)
		}
		for i := 0; i < numSamplers; i++ {
			rows := randgen.GenEncDatumRowsString(rowPartitions[i], randgen.DatumEncoding_NONE)
			in[i] = distsqlutils.NewRowBuffer([]*types.T{types.String, types.String}, rows, distsqlutils.RowBufferArgs{})
		}
		// Override original columns in samplerOutTypes.
		samplerOutTypes[0] = types.String
		samplerOutTypes[1] = types.String

	default:
		panic(errors.AssertionFailedf("Type %T not supported for inputRows", t))
	}

	ctx := execversion.TestingWithLatestCtx
	outputs := make([]*distsqlutils.RowBuffer, numSamplers)
	for i := 0; i < numSamplers; i++ {
		outputs[i] = distsqlutils.NewRowBuffer(samplerOutTypes, nil /* rows */, distsqlutils.RowBufferArgs{})

		spec := &execinfrapb.SamplerSpec{
			SampleSize:    childNumSamples,
			MinSampleSize: childMinNumSamples,
			Sketches:      sketchSpecs,
		}
		p, err := newSamplerProcessor(
			ctx, &flowCtx, 0 /* processorID */, spec, in[i], &execinfrapb.PostProcessSpec{},
		)
		if err != nil {
			t.Fatal(err)
		}
		p.Run(ctx, outputs[i])
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
		ctx, &flowCtx, 0 /* processorID */, spec, samplerResults, &execinfrapb.PostProcessSpec{},
	)
	if err != nil {
		t.Fatal(err)
	}
	agg.Run(ctx, finalOut)
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
					 "avgSize",
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
			&r.tableID, &name, &r.colIDs, &r.rowCount, &r.distinctCount, &r.nullCount, &r.avgSize, &histData,
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

			var a tree.DatumAlloc
			for _, b := range h.Buckets {
				datum, err := stats.DecodeUpperBound(h.Version, histEncType, &a, b.UpperBound)
				if err != nil {
					t.Fatal(err)
				}
				r.buckets = append(r.buckets, resultBucket{
					numEq:    int(b.NumEq),
					numRange: int(b.NumRange),
					upper:    int(*datum.(*tree.DInt)),
				})
			}

			// If we collected fewer samples than rows, the generated histogram will
			// be nondeterministic. Rather than checking for an exact match, verify
			// some properties and then ignore it.
			if childNumSamples < uint32(inLen) || aggNumSamples < uint32(inLen) {
				if uint32(len(r.buckets)) > expectedMaxBuckets {
					t.Errorf(
						"Expected at most %d buckets, got %d:\n  %v", expectedMaxBuckets, len(r.buckets), r,
					)
				}
				var count int
				for _, bucket := range r.buckets {
					count += bucket.numEq + bucket.numRange
				}
				targetCount := r.rowCount - r.nullCount
				// Due to rounding errors, we may be within +/- 1 of the target.
				if count < targetCount-1 && count > targetCount+1 {
					t.Errorf(
						"Expected %d rows counted in histogram, got %d:\n  %v",
						targetCount, count, r,
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
	defer log.Scope(t).Close(t)

	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

	st := s.ClusterSettings()
	evalCtx := eval.MakeTestingEvalContextWithCodec(s.Codec(), st)
	defer evalCtx.Stop(context.Background())

	type sampAggTestCase struct {
		memLimitBytes                       int64
		expectOutOfMemory                   bool
		childNumSamples, childMinNumSamples uint32
		aggNumSamples, aggMinNumSamples     uint32
		maxBuckets, expectedMaxBuckets      uint32
		inputRows                           interface{}
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
			avgSize:       7,
		},
		{
			tableID:       13,
			name:          "<NULL>",
			colIDs:        "{101}",
			rowCount:      11,
			distinctCount: 9,
			nullCount:     1,
			avgSize:       8,
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
			avgSize:       8,
		},
		{
			tableID:       13,
			name:          "<NULL>",
			colIDs:        "{101}",
			rowCount:      1000,
			distinctCount: 1000,
			nullCount:     0,
			avgSize:       8,
			buckets: []resultBucket{
				// The "B" cases will always sample fewer than 100% of rows, so the
				// expected histogram just needs to have one dummy bucket and will not
				// actually be checked.
				{numEq: 0, numRange: 0, upper: 0},
			},
		},
	}

	inputRowsC := [][]string{
		{"123", "1"},
		{"12345", "12345678"},
		{"", "1234"},
		{"1234", "123456"},
		{"1234", "123456789"},
	}
	expectedC := []result{
		{
			tableID:       13,
			name:          "a",
			colIDs:        "{100}",
			rowCount:      5,
			distinctCount: 4,
			nullCount:     1,
			avgSize:       16,
		},
		{
			tableID:       13,
			name:          "<NULL>",
			colIDs:        "{101}",
			rowCount:      5,
			distinctCount: 5,
			nullCount:     0,
			avgSize:       22,
		},
	}

	for i, tc := range []sampAggTestCase{
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
		{0, false, 2, 2, 2, 2, 2, 4, inputRowsA, expectedA},
		{0, false, 2, 2, 2, 2, 4, 4, inputRowsA, expectedA},
		{0, false, 100, 100, 2, 2, 4, 4, inputRowsA, expectedA},
		{0, false, 2, 2, 100, 100, 4, 4, inputRowsA, expectedA},

		// Sample some rows with dynamic shrinking due to memory limits. Check that
		// stats match and that histograms have the right number of buckets and
		// number of rows.
		{1 << 15, false, 200, 20, 200, 20, 200, 52, inputRowsB, expectedB},
		{1 << 16, false, 200, 20, 200, 20, 200, 202, inputRowsB, expectedB},

		// Sample rows with variable length columns. Don't take samples since
		// strings are not currently supported in the test.
		{0, false, 0, 0, 0, 0, 4, 4, inputRowsC, expectedC},
	} {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			runSampleAggregator(
				t, s, sqlDB, st, &evalCtx, tc.memLimitBytes, tc.expectOutOfMemory,
				tc.childNumSamples, tc.childMinNumSamples, tc.aggNumSamples, tc.aggMinNumSamples,
				tc.maxBuckets, tc.expectedMaxBuckets, tc.inputRows, tc.expected,
			)
		})
	}
}

// TestPanicDeadlock verifies that the defer-based cleanup fix prevents the deadlock
// using the real sampleAggregator processor.
//
// The original deadlock scenario (issue #160337):
// 1. A processor panics during execution (without deferred cleanup)
// 2. Producer goroutines are blocked sending to it via RowChannel
// 3. The panic is recovered but the processor never calls ConsumerClosed()
// 4. Producers remain stuck on channel sends indefinitely
// 5. Wait() blocks forever waiting for producer goroutines
// 6. Cleanup is never called, so UnregisterFlow never happens
// 7. Drain waits forever for the flow to unregister
//
// The fix uses defer to ensure ConsumerClosed() is called even on panic,
// which drains the channel and unblocks stuck producers.
func TestPanicDeadlock(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderStress(t, "test has a 10-second timeout to detect deadlock")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up minimal infrastructure for sampleAggregator
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	monitor := mon.NewMonitor(mon.Options{
		Name:     mon.MakeName("test"),
		Settings: st,
	})
	monitor.Start(ctx, nil, mon.NewStandaloneBudget(1<<30))
	defer monitor.Stop(ctx)

	// Set up testing knob to inject panic after first row
	var rowsSeen int
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Mon:     monitor,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
			TestingKnobs: execinfra.TestingKnobs{
				SampleAggregatorTestingKnobRowHook: func() {
					rowsSeen++
					if rowsSeen >= 1 {
						panic("sampleAggregator test: injected panic")
					}
				},
			},
		},
	}

	// SampleAggregator expects sampler output format: original columns + 8 metadata columns
	// Sampler adds: rank, sketch_idx, num_rows, num_nulls, size, sketch_data, inv_col_idx, inv_idx_key
	samplerOutTypes := []*types.T{
		types.Int,   // original column (the data being sampled)
		types.Int,   // rank
		types.Int,   // sketch index
		types.Int,   // num rows
		types.Int,   // num nulls
		types.Int,   // size
		types.Bytes, // sketch data
		types.Int,   // inverted column index
		types.Bytes, // inverted index key
	}

	// Use unbuffered channel to ensure blocking happens immediately
	rowChan := &execinfra.RowChannel{}
	rowChan.InitWithBufSizeAndNumSenders(samplerOutTypes, 0 /* unbuffered */, 1 /* numSenders */)
	rowChan.Start(ctx)

	// Create the real sampleAggregator
	spec := &execinfrapb.SampleAggregatorSpec{
		SampleSize:    100,
		MinSampleSize: 10,
		Sketches: []execinfrapb.SketchSpec{
			{
				Columns:           []uint32{0},
				GenerateHistogram: false,
				StatName:          "test",
			},
		},
	}
	post := &execinfrapb.PostProcessSpec{}

	proc, err := newSampleAggregator(ctx, flowCtx, 0 /* processorID */, spec, rowChan, post)
	if err != nil {
		t.Fatal(err)
	}

	// Create output channel (sampleAggregator outputs stats results)
	outputChan := &execinfra.RowChannel{}
	outputChan.InitWithBufSizeAndNumSenders([]*types.T{types.Bytes}, 10, 1)
	outputChan.Start(ctx)

	// Track producer goroutine
	var producerWg sync.WaitGroup
	producerWg.Add(1)

	// Start producer goroutine that sends rows to the processor
	go func() {
		defer producerWg.Done()
		defer rowChan.ProducerDone()

		// Create a sampler-format row with all 9 columns
		row := rowenc.EncDatumRow{
			rowenc.DatumToEncDatumUnsafe(types.Int, tree.NewDInt(1)),      // original column
			rowenc.DatumToEncDatumUnsafe(types.Int, tree.NewDInt(0)),      // rank
			rowenc.DatumToEncDatumUnsafe(types.Int, tree.NewDInt(0)),      // sketch index
			rowenc.DatumToEncDatumUnsafe(types.Int, tree.NewDInt(1)),      // num rows
			rowenc.DatumToEncDatumUnsafe(types.Int, tree.NewDInt(0)),      // num nulls
			rowenc.DatumToEncDatumUnsafe(types.Int, tree.NewDInt(0)),      // size
			rowenc.DatumToEncDatumUnsafe(types.Bytes, tree.NewDBytes("")), // sketch data
			rowenc.DatumToEncDatumUnsafe(types.Int, tree.NewDInt(-1)),     // inverted column index (-1 = not used)
			rowenc.DatumToEncDatumUnsafe(types.Bytes, tree.NewDBytes("")), // inverted index key
		}

		// Send multiple rows. The processor will panic after reading the first one.
		// WITH FIX: The defer in sampleAggregator.Run() calls ConsumerClosed() which drains the channel
		// WITHOUT FIX: Producer would stay blocked forever
		for i := 0; i < 5; i++ {
			rowChan.Push(row, nil)
		}
	}()

	// Run the processor in a separate goroutine (simulates flow.Run)
	processorDone := make(chan bool)
	var processorPanic interface{}

	go func() {
		defer func() {
			// Simulates Wait() catching the panic
			processorPanic = recover()
			if processorPanic != nil {
				// Simulates ctxCancel() being called
				cancel()
			}
			close(processorDone)
		}()

		proc.Run(ctx, outputChan)
	}()

	// Wait for processor to panic and exit
	<-processorDone

	if processorPanic == nil {
		t.Fatal("expected processor to panic, but it didn't")
	}

	// Now try to wait for producer goroutine with a timeout
	// WITH THE FIX: This should complete quickly because the defer calls
	//               ConsumerClosed(), which drains the channel and unblocks the producer
	producersDone := make(chan bool)
	go func() {
		producerWg.Wait()
		close(producersDone)
	}()

	select {
	case <-producersDone:
		// SUCCESS: Producer finished (this is what we want with the fix)
		t.Log("Producer finished successfully after panic - fix is working")
	case <-time.After(10 * time.Second):
		// FAILURE: Producer is deadlocked (the fix is not working)
		t.Fatal("DEADLOCK: Producer goroutine is still blocked 10 seconds after panic. " +
			"The defer-based fix in sampleAggregator is not working correctly.")
	}
}
