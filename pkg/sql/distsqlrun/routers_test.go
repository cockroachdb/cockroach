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
	"bytes"
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

// setupRouter creates and starts a router. Returns the router and a WaitGroup
// that tracks the lifetime of the background router goroutines.
func setupRouter(
	t testing.TB,
	evalCtx *tree.EvalContext,
	spec OutputRouterSpec,
	inputTypes []sqlbase.ColumnType,
	streams []RowReceiver,
) (router, *sync.WaitGroup) {
	r, err := makeRouter(&spec, streams)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.TODO()
	flowCtx := FlowCtx{Settings: cluster.MakeTestingClusterSettings(), EvalCtx: evalCtx}
	r.init(ctx, &flowCtx, inputTypes)
	wg := &sync.WaitGroup{}
	r.start(ctx, wg, nil /* ctxCancel */)
	return r, wg
}

func TestRouters(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numCols = 6
	const numRows = 200

	rng, _ := randutil.NewPseudoRand()
	alloc := &sqlbase.DatumAlloc{}
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())

	// Generate tables of possible values for each column; we have fewer possible
	// values than rows to guarantee many occurrences of each value.
	vals, types := sqlbase.RandSortingEncDatumSlices(rng, numCols, numRows/10)

	testCases := []struct {
		spec       OutputRouterSpec
		numBuckets int
	}{
		{
			spec:       OutputRouterSpec{Type: OutputRouterSpec_BY_HASH, HashColumns: []uint32{0}},
			numBuckets: 4,
		},
		{
			spec:       OutputRouterSpec{Type: OutputRouterSpec_BY_HASH, HashColumns: []uint32{0}},
			numBuckets: 2,
		},
		{
			spec:       OutputRouterSpec{Type: OutputRouterSpec_BY_HASH, HashColumns: []uint32{3}},
			numBuckets: 4,
		},
		{
			spec:       OutputRouterSpec{Type: OutputRouterSpec_BY_HASH, HashColumns: []uint32{1, 3}},
			numBuckets: 4,
		},
		{
			spec:       OutputRouterSpec{Type: OutputRouterSpec_BY_HASH, HashColumns: []uint32{5, 2}},
			numBuckets: 3,
		},
		{
			spec:       OutputRouterSpec{Type: OutputRouterSpec_BY_HASH, HashColumns: []uint32{0, 1, 2, 3, 4}},
			numBuckets: 5,
		},
		{
			spec:       OutputRouterSpec{Type: OutputRouterSpec_MIRROR},
			numBuckets: 2,
		},
		{
			spec:       OutputRouterSpec{Type: OutputRouterSpec_MIRROR},
			numBuckets: 3,
		},
		{
			spec:       OutputRouterSpec{Type: OutputRouterSpec_MIRROR},
			numBuckets: 4,
		},
		{
			spec:       OutputRouterSpec{Type: OutputRouterSpec_BY_RANGE, RangeRouterSpec: testRangeRouterSpec},
			numBuckets: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.spec.Type.String(), func(t *testing.T) {
			bufs := make([]*RowBuffer, tc.numBuckets)
			recvs := make([]RowReceiver, tc.numBuckets)
			tc.spec.Streams = make([]StreamEndpointSpec, tc.numBuckets)
			for i := 0; i < tc.numBuckets; i++ {
				bufs[i] = &RowBuffer{}
				recvs[i] = bufs[i]
				tc.spec.Streams[i] = StreamEndpointSpec{StreamID: StreamID(i)}
			}

			r, wg := setupRouter(t, evalCtx, tc.spec, types, recvs)

			for i := 0; i < numRows; i++ {
				row := make(sqlbase.EncDatumRow, numCols)
				for j := 0; j < numCols; j++ {
					row[j] = vals[j][rng.Intn(len(vals[j]))]
				}
				if status := r.Push(row, nil /* meta */); status != NeedMoreRows {
					t.Fatalf("unexpected status: %d", status)
				}
			}
			r.ProducerDone()
			wg.Wait()

			rows := make([]sqlbase.EncDatumRows, len(bufs))
			for i, b := range bufs {
				if !b.ProducerClosed {
					t.Fatalf("bucket not closed: %d", i)
				}
				rows[i] = b.GetRowsNoMeta(t)
			}

			switch tc.spec.Type {
			case OutputRouterSpec_BY_HASH:
				for bIdx := range rows {
					for _, row := range rows[bIdx] {
						// Verify there are no rows that
						//  - have the same values with this row on all the hashColumns, and
						//  - ended up in a different bucket
						for b2Idx, r2 := range rows {
							if b2Idx == bIdx {
								continue
							}
							for _, row2 := range r2 {
								equal := true
								for _, c := range tc.spec.HashColumns {
									cmp, err := row[c].Compare(&types[c], alloc, evalCtx, &row2[c])
									if err != nil {
										t.Fatal(err)
									}
									if cmp != 0 {
										equal = false
										break
									}
								}
								if equal {
									t.Errorf(
										"rows %s and %s in different buckets", row.String(types), row2.String(types),
									)
								}
							}
						}
					}
				}

			case OutputRouterSpec_MIRROR:
				// Verify each row is sent to each of the output streams.
				for bIdx, r := range rows {
					if bIdx == 0 {
						continue
					}
					if len(rows[bIdx]) != len(rows[0]) {
						t.Errorf("buckets %d and %d have different number of rows", 0, bIdx)
					}

					// Verify that the i-th row is the same across all buffers.
					for i, row := range r {
						row2 := rows[0][i]

						equal := true
						for j, c := range row {
							cmp, err := c.Compare(&types[j], alloc, evalCtx, &row2[j])
							if err != nil {
								t.Fatal(err)
							}
							if cmp != 0 {
								equal = false
								break
							}
						}
						if !equal {
							t.Errorf(
								"rows %s and %s found in one bucket and not the other",
								row.String(types), row2.String(types),
							)
						}
					}
				}

			case OutputRouterSpec_BY_RANGE:
				// Verify each row is in the correct output stream.
				enc := testRangeRouterSpec.Encodings[0]
				var alloc sqlbase.DatumAlloc
				for bIdx := range rows {
					for _, row := range rows[bIdx] {
						data, err := row[enc.Column].Encode(&types[enc.Column], &alloc, enc.Encoding, nil)
						if err != nil {
							t.Fatal(err)
						}
						span := testRangeRouterSpec.Spans[bIdx]
						if bytes.Compare(span.Start, data) > 0 || bytes.Compare(span.End, data) <= 0 {
							t.Errorf("%s in wrong span: %v", data, span)
						}
					}
				}

			default:
				t.Fatalf("unknown router type %d", tc.spec.Type)
			}
		})
	}
}

const testRangeRouterSpanBreak byte = (encoding.IntMax + encoding.IntMin) / 2

var (
	testRangeRouterSpec = OutputRouterSpec_RangeRouterSpec{
		Spans: []OutputRouterSpec_RangeRouterSpec_Span{
			{
				Start:  []byte{0x00},
				End:    []byte{testRangeRouterSpanBreak},
				Stream: 0,
			},
			{
				Start:  []byte{testRangeRouterSpanBreak},
				End:    []byte(keys.MaxKey),
				Stream: 1,
			},
		},
		Encodings: []OutputRouterSpec_RangeRouterSpec_ColumnEncoding{
			{
				Column:   0,
				Encoding: sqlbase.DatumEncoding_ASCENDING_KEY,
			},
		},
	}
)

// Test that the correct status is returned to producers: NeedMoreRows should be
// returned while there's at least one consumer that's not draining, then
// DrainRequested should be returned while there's at least one consumer that's
// not closed, and ConsumerClosed should be returned afterwards.
func TestConsumerStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()

	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())

	testCases := []struct {
		name string
		spec OutputRouterSpec
	}{
		{
			name: "MirrorRouter",
			spec: OutputRouterSpec{Type: OutputRouterSpec_MIRROR},
		},
		{
			name: "HashRouter",
			spec: OutputRouterSpec{Type: OutputRouterSpec_BY_HASH, HashColumns: []uint32{0}},
		},
		{
			name: "RangeRouter",
			spec: OutputRouterSpec{Type: OutputRouterSpec_BY_RANGE, RangeRouterSpec: testRangeRouterSpec},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bufs := make([]*RowBuffer, 2)
			recvs := make([]RowReceiver, 2)
			tc.spec.Streams = make([]StreamEndpointSpec, 2)
			for i := 0; i < 2; i++ {
				bufs[i] = &RowBuffer{}
				recvs[i] = bufs[i]
				tc.spec.Streams[i] = StreamEndpointSpec{StreamID: StreamID(i)}
			}

			colTypes := []sqlbase.ColumnType{{SemanticType: sqlbase.ColumnType_INT}}
			router, wg := setupRouter(t, evalCtx, tc.spec, colTypes, recvs)

			// row0 will be a row that the router sends to the first stream, row1 to
			// the 2nd stream.
			var row0, row1 sqlbase.EncDatumRow
			switch r := router.(type) {
			case *hashRouter:
				var err error
				row0, err = preimageAttack(colTypes, r, 0, len(bufs))
				if err != nil {
					t.Fatal(err)
				}
				row1, err = preimageAttack(colTypes, r, 1, len(bufs))
				if err != nil {
					t.Fatal(err)
				}
			case *rangeRouter:
				// Use 0 and MaxInt32 to route rows based on testRangeRouterSpec's spans.
				d := tree.NewDInt(0)
				row0 = sqlbase.EncDatumRow{sqlbase.DatumToEncDatum(colTypes[0], d)}
				d = tree.NewDInt(math.MaxInt32)
				row1 = sqlbase.EncDatumRow{sqlbase.DatumToEncDatum(colTypes[0], d)}
			default:
				rng, _ := randutil.NewPseudoRand()
				vals := sqlbase.RandEncDatumRowsOfTypes(rng, 1 /* numRows */, colTypes)
				row0 = vals[0]
				row1 = row0
			}

			// Push a row and expect NeedMoreRows.
			consumerStatus := router.Push(row0, nil /* meta */)
			if consumerStatus != NeedMoreRows {
				t.Fatalf("expected status %d, got: %d", NeedMoreRows, consumerStatus)
			}

			// Start draining stream 0. Keep expecting NeedMoreRows, regardless on
			// which stream we send.
			bufs[0].ConsumerDone()
			consumerStatus = router.Push(row0, nil /* meta */)
			if consumerStatus != NeedMoreRows {
				t.Fatalf("expected status %d, got: %d", NeedMoreRows, consumerStatus)
			}
			consumerStatus = router.Push(row1, nil /* meta */)
			if consumerStatus != NeedMoreRows {
				t.Fatalf("expected status %d, got: %d", NeedMoreRows, consumerStatus)
			}

			// Close stream 0. Continue to expect NeedMoreRows.
			bufs[0].ConsumerClosed()
			consumerStatus = router.Push(row0, nil /* meta */)
			if consumerStatus != NeedMoreRows {
				t.Fatalf("expected status %d, got: %d", NeedMoreRows, consumerStatus)
			}
			consumerStatus = router.Push(row1, nil /* meta */)
			if consumerStatus != NeedMoreRows {
				t.Fatalf("expected status %d, got: %d", NeedMoreRows, consumerStatus)
			}

			// Start draining stream 1. Now that all streams are draining, expect
			// DrainRequested.
			bufs[1].ConsumerDone()
			testutils.SucceedsSoon(t, func() error {
				status := router.Push(row1, nil /* meta */)
				if status != DrainRequested {
					return fmt.Errorf("expected status %d, got: %d", DrainRequested, consumerStatus)
				}
				return nil
			})

			// Close stream 1. Everything's closed now, but the routers currently
			// only detect this when trying to send metadata - so we still expect
			// DrainRequested.
			bufs[1].ConsumerClosed()
			consumerStatus = router.Push(row1, nil /* meta */)
			if consumerStatus != DrainRequested {
				t.Fatalf("expected status %d, got: %d", DrainRequested, consumerStatus)
			}

			// Attempt to send some metadata. This will cause the router to observe
			// that everything's closed now.
			testutils.SucceedsSoon(t, func() error {
				consumerStatus := router.Push(
					nil /* row */, &ProducerMetadata{Err: errors.Errorf("test error")},
				)
				if consumerStatus != ConsumerClosed {
					return fmt.Errorf("expected status %d, got: %d", ConsumerClosed, consumerStatus)
				}
				return nil
			})
			router.ProducerDone()
			wg.Wait()
		})
	}
}

// preimageAttack finds a row that hashes to a particular output stream. It's
// assumed that hr is configured for rows with one column.
func preimageAttack(
	colTypes []sqlbase.ColumnType, hr *hashRouter, streamIdx int, numStreams int,
) (sqlbase.EncDatumRow, error) {
	rng, _ := randutil.NewPseudoRand()
	for {
		vals := sqlbase.RandEncDatumRowOfTypes(rng, colTypes)
		curStreamIdx, err := hr.computeDestination(vals)
		if err != nil {
			return nil, err
		}
		if curStreamIdx == streamIdx {
			return vals, nil
		}
	}
}

// Test that metadata records get forwarded by routers. Regardless of the type
// of router, the records are supposed to be forwarded on the first output
// stream that's not closed.
func TestMetadataIsForwarded(t *testing.T) {
	defer leaktest.AfterTest(t)()

	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())

	testCases := []struct {
		name string
		spec OutputRouterSpec
	}{
		{
			name: "MirrorRouter",
			spec: OutputRouterSpec{Type: OutputRouterSpec_MIRROR},
		},
		{
			name: "HashRouter",
			spec: OutputRouterSpec{Type: OutputRouterSpec_BY_HASH, HashColumns: []uint32{0}},
		},
		{
			name: "RangeRouter",
			spec: OutputRouterSpec{Type: OutputRouterSpec_BY_RANGE, RangeRouterSpec: testRangeRouterSpec},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			chans := make([]RowChannel, 2)
			recvs := make([]RowReceiver, 2)
			tc.spec.Streams = make([]StreamEndpointSpec, 2)
			for i := 0; i < 2; i++ {
				chans[i].initWithBufSizeAndNumSenders(nil /* no column types */, 1, 1)
				recvs[i] = &chans[i]
				tc.spec.Streams[i] = StreamEndpointSpec{StreamID: StreamID(i)}
			}
			router, wg := setupRouter(t, evalCtx, tc.spec, nil /* no columns */, recvs)

			err1 := errors.Errorf("test error 1")
			err2 := errors.Errorf("test error 2")
			err3 := errors.Errorf("test error 3")
			err4 := errors.Errorf("test error 4")

			// Push metadata; it should go to stream 0.
			for i := 0; i < 10; i++ {
				consumerStatus := router.Push(nil /* row */, &ProducerMetadata{Err: err1})
				if consumerStatus != NeedMoreRows {
					t.Fatalf("expected status %d, got: %d", NeedMoreRows, consumerStatus)
				}
				_, meta := chans[0].Next()
				if meta.Err != err1 {
					t.Fatalf("unexpected meta.Err %v, expected %s", meta.Err, err1)
				}
			}

			chans[0].ConsumerDone()
			// Push metadata; it should still go to stream 0.
			for i := 0; i < 10; i++ {
				consumerStatus := router.Push(nil /* row */, &ProducerMetadata{Err: err2})
				if consumerStatus != NeedMoreRows {
					t.Fatalf("expected status %d, got: %d", NeedMoreRows, consumerStatus)
				}
				_, meta := chans[0].Next()
				if meta.Err != err2 {
					t.Fatalf("unexpected meta.Err %v, expected %s", meta.Err, err2)
				}
			}

			chans[0].ConsumerClosed()

			// Metadata should switch to going to stream 1 once the new status is
			// observed.
			testutils.SucceedsSoon(t, func() error {
				consumerStatus := router.Push(nil /* row */, &ProducerMetadata{Err: err3})
				if consumerStatus != NeedMoreRows {
					t.Fatalf("expected status %d, got: %d", NeedMoreRows, consumerStatus)
				}
				// Receive on stream 1 if there is a message waiting. Metadata may still
				// try to go to 0 for a little while.
				select {
				case d := <-chans[1].C:
					if d.Meta.Err != err3 {
						t.Fatalf("unexpected meta.Err %v, expected %s", d.Meta.Err, err3)
					}
					return nil
				default:
					return errors.Errorf("no metadata on stream 1")
				}
			})

			chans[1].ConsumerClosed()

			// Start drain the channels in the background.
			for i := range chans {
				go drainRowChannel(&chans[i])
			}

			testutils.SucceedsSoon(t, func() error {
				consumerStatus := router.Push(nil /* row */, &ProducerMetadata{Err: err4})
				if consumerStatus != ConsumerClosed {
					return fmt.Errorf("expected status %d, got: %d", ConsumerClosed, consumerStatus)
				}
				return nil
			})

			router.ProducerDone()

			wg.Wait()
		})
	}
}

func drainRowChannel(rc *RowChannel) {
	for {
		row, meta := rc.Next()
		if row == nil && meta == nil {
			return
		}
	}
}

// TestRouterBlocks verifies that routers block if all their consumers are
// blocked.
func TestRouterBlocks(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name string
		spec OutputRouterSpec
	}{
		{
			name: "MirrorRouter",
			spec: OutputRouterSpec{Type: OutputRouterSpec_MIRROR},
		},
		{
			name: "HashRouter",
			spec: OutputRouterSpec{Type: OutputRouterSpec_BY_HASH, HashColumns: []uint32{0}},
		},
		{
			name: "RangeRouter",
			spec: OutputRouterSpec{Type: OutputRouterSpec_BY_RANGE, RangeRouterSpec: testRangeRouterSpec},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			colTypes := []sqlbase.ColumnType{{SemanticType: sqlbase.ColumnType_INT}}
			chans := make([]RowChannel, 2)
			recvs := make([]RowReceiver, 2)
			tc.spec.Streams = make([]StreamEndpointSpec, 2)
			for i := 0; i < 2; i++ {
				chans[i].initWithBufSizeAndNumSenders(colTypes, 1, 1)
				recvs[i] = &chans[i]
				tc.spec.Streams[i] = StreamEndpointSpec{StreamID: StreamID(i)}
			}
			router, err := makeRouter(&tc.spec, recvs)
			if err != nil {
				t.Fatal(err)
			}
			st := cluster.MakeTestingClusterSettings()
			ctx := context.TODO()
			evalCtx := tree.MakeTestingEvalContext(st)
			flowCtx := FlowCtx{Settings: st, EvalCtx: &evalCtx}
			router.init(ctx, &flowCtx, colTypes)
			var wg sync.WaitGroup
			router.start(ctx, &wg, nil /* ctxCancel */)

			// Set up a goroutine that tries to send rows until the stop channel
			// is closed.
			wg.Add(1)
			var numRowsSent uint32
			stop := make(chan struct{})
			go func() {
				rng, _ := randutil.NewPseudoRand()
			Loop:
				for {
					select {
					case <-stop:
						break Loop
					default:
						row := sqlbase.RandEncDatumRowOfTypes(rng, colTypes)
						status := router.Push(row, nil /* meta */)
						if status != NeedMoreRows {
							break Loop
						}
						atomic.AddUint32(&numRowsSent, 1)
					}
				}
				router.ProducerDone()
				wg.Done()
			}()

			// We are not reading from the row channels; the router should become
			// blocked after trying to send a row to each stream. We sample the number
			// of rows sent and verify that it stops increasing.
			var lastVal uint32
			iterationsWithNoChange := 0
			const itDuration = time.Millisecond
			const timeout = 5 * time.Second
			for i := 0; ; i++ {
				if i > int(timeout/itDuration) {
					t.Fatalf("the number of rows sent still increasing after %s", timeout)
				}
				time.Sleep(itDuration)
				val := atomic.LoadUint32(&numRowsSent)
				// If we see a ridiculously high value, exit early.
				if val > 1000000 {
					t.Fatalf("pushed too many rows (%d)", val)
				}
				if val != lastVal {
					lastVal = val
					iterationsWithNoChange = 0
					continue
				}
				iterationsWithNoChange++
				if iterationsWithNoChange > 5 {
					break
				}
			}
			close(stop)

			// Drain the channels.
			for i := range chans {
				go drainRowChannel(&chans[i])
			}
			wg.Wait()
		})
	}
}

// TestRouterDiskSpill verifies that router outputs spill to disk when a memory
// limit is reached. It also verifies that stats are properly recorded in this
// scenario.
func TestRouterDiskSpill(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numRows = 200
	const numCols = 1

	var (
		rowChan RowChannel
		rb      routerBase
		wg      sync.WaitGroup
	)

	// Enable stats recording.
	tracer := tracing.NewTracer()
	sp := tracer.StartSpan("root", tracing.Recordable)
	tracing.StartRecording(sp, tracing.SnowballRecording)
	ctx := opentracing.ContextWithSpan(context.Background(), sp)

	st := cluster.MakeTestingClusterSettings()
	diskMonitor := mon.MakeMonitor(
		"test-disk",
		mon.DiskResource,
		nil, /* curCount */
		nil, /* maxHist */
		-1,  /* increment: use default block size */
		math.MaxInt64,
		st,
	)
	diskMonitor.Start(ctx, nil /* pool */, mon.MakeStandaloneBudget(math.MaxInt64))
	defer diskMonitor.Stop(ctx)
	tempEngine, err := engine.NewTempEngine(base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()
	// monitor is the custom memory monitor used in this test. The increment is
	// set to 1 for fine-grained memory allocations and the limit is set to half
	// the number of rows that wil eventually be added to the underlying
	// rowContainer. This is a bytes value that will ensure we fall back to disk
	// but use memory for at least a couple of rows.
	monitor := mon.MakeMonitorWithLimit(
		"test-monitor",
		mon.MemoryResource,
		(numRows-routerRowBufSize)/2, /* limit */
		nil,           /* curCount */
		nil,           /* maxHist */
		1,             /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	evalCtx := tree.MakeTestingEvalContextWithMon(st, &monitor)
	defer evalCtx.Stop(ctx)
	flowCtx := FlowCtx{
		Settings:    st,
		EvalCtx:     &evalCtx,
		TempStorage: tempEngine,
		diskMonitor: &diskMonitor,
	}
	alloc := &sqlbase.DatumAlloc{}

	var spec OutputRouterSpec
	spec.Streams = make([]StreamEndpointSpec, 1)
	// Initialize the RowChannel with the minimal buffer size so as to block
	// writes to the channel (after the first one).
	rowChan.initWithBufSizeAndNumSenders(oneIntCol, 1 /* chanBufSize */, 1 /* numSenders */)
	rb.setupStreams(&spec, []RowReceiver{&rowChan})
	rb.init(ctx, &flowCtx, oneIntCol)
	rb.start(ctx, &wg, nil /* ctxCancel */)

	rows := makeIntRows(numRows, numCols)
	// output is the sole router output in this test.
	output := &rb.outputs[0]
	errChan := make(chan error)

	go func() {
		for _, row := range rows {
			output.mu.Lock()
			err := output.addRowLocked(ctx, row)
			output.mu.Unlock()
			if err != nil {
				errChan <- err
			}
		}
		rb.ProducerDone()
		wg.Wait()
		close(errChan)
	}()

	testutils.SucceedsSoon(t, func() error {
		output.mu.Lock()
		spilled := output.mu.rowContainer.Spilled()
		output.mu.Unlock()
		if !spilled {
			return errors.New("did not spill to disk")
		}
		return nil
	})

	metaSeen := false
	for i := 0; ; i++ {
		row, meta := rowChan.Next()
		if meta != nil {
			// Check that router output stats were recorded as expected.
			if metaSeen {
				t.Fatal("expected only one meta, encountered multiple")
			}
			metaSeen = true
			if len(meta.TraceData) != 1 {
				t.Fatalf("expected one recorded span, found %d", len(meta.TraceData))
			}
			span := meta.TraceData[0]
			getIntTagValue := func(key string) int {
				strValue, ok := span.Tags[key]
				if !ok {
					t.Errorf("missing tag: %s", key)
				}
				intValue, err := strconv.Atoi(strValue)
				if err != nil {
					t.Error(err)
				}
				return intValue
			}
			rowsRouted := getIntTagValue("cockroach.stat.routeroutput.rows_routed")
			memMax := getIntTagValue("cockroach.stat.routeroutput.mem.max")
			diskMax := getIntTagValue("cockroach.stat.routeroutput.disk.max")
			if rowsRouted != numRows {
				t.Errorf("expected %d rows routed, got %d", numRows, rowsRouted)
			}
			if memMax <= 0 {
				t.Errorf("expected memMax > 0, got %d", memMax)
			}
			if diskMax <= 0 {
				t.Errorf("expected memMax > 0, got %d", diskMax)
			}
			continue
		}
		if row == nil {
			break
		}
		// Verify correct order (should be the order in which we added rows).
		for j, c := range row {
			if cmp, err := c.Compare(&intType, alloc, flowCtx.EvalCtx, &rows[i][j]); err != nil {
				t.Fatal(err)
			} else if cmp != 0 {
				t.Fatalf(
					"order violated on row %d, expected %v got %v",
					i,
					rows[i].String(oneIntCol),
					row.String(oneIntCol),
				)
			}
		}
	}
	if !metaSeen {
		t.Error("expected trace metadata, found none")
	}

	// Make sure the goroutine adding rows is done.
	if err := <-errChan; err != nil {
		t.Fatal(err)
	}
}

func TestRangeRouterInit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		spec OutputRouterSpec_RangeRouterSpec
		err  string
	}{
		{
			spec: testRangeRouterSpec,
		},
		{
			spec: OutputRouterSpec_RangeRouterSpec{
				Spans: []OutputRouterSpec_RangeRouterSpec_Span{
					{
						Start:  []byte{testRangeRouterSpanBreak},
						End:    []byte{0xff},
						Stream: 0,
					},
					{
						Start:  []byte{0x00},
						End:    []byte{testRangeRouterSpanBreak},
						Stream: 1,
					},
				},
				Encodings: testRangeRouterSpec.Encodings,
			},
			err: "not after previous span",
		},
		{
			spec: OutputRouterSpec_RangeRouterSpec{
				Spans: testRangeRouterSpec.Spans,
			},
			err: "missing encodings",
		},
	}

	for i, tc := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			spec := OutputRouterSpec{
				Type:            OutputRouterSpec_BY_RANGE,
				RangeRouterSpec: tc.spec,
			}
			colTypes := []sqlbase.ColumnType{{SemanticType: sqlbase.ColumnType_INT}}
			chans := make([]RowChannel, 2)
			recvs := make([]RowReceiver, 2)
			spec.Streams = make([]StreamEndpointSpec, 2)
			for i := 0; i < 2; i++ {
				chans[i].initWithBufSizeAndNumSenders(colTypes, 1, 1)
				recvs[i] = &chans[i]
				spec.Streams[i] = StreamEndpointSpec{StreamID: StreamID(i)}
			}
			_, err := makeRouter(&spec, recvs)
			if !testutils.IsError(err, tc.err) {
				t.Fatalf("got %v, expected %v", err, tc.err)
			}
		})
	}
}

func BenchmarkRouter(b *testing.B) {
	numCols := 1
	numRows := 1 << 16
	colTypes := makeIntCols(numCols)

	ctx := context.Background()
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())

	input := NewRepeatableRowSource(oneIntCol, makeIntRows(numRows, numCols))

	for _, spec := range []OutputRouterSpec{
		{
			Type: OutputRouterSpec_BY_RANGE,
			RangeRouterSpec: OutputRouterSpec_RangeRouterSpec{
				Spans:     testRangeRouterSpec.Spans,
				Encodings: testRangeRouterSpec.Encodings,
			},
		},
		{
			Type:        OutputRouterSpec_BY_HASH,
			HashColumns: []uint32{0},
		},
		{
			Type: OutputRouterSpec_MIRROR,
		},
	} {
		b.Run(spec.Type.String(), func(b *testing.B) {
			for _, nOutputs := range []int{2, 4, 8} {
				chans := make([]RowChannel, nOutputs)
				recvs := make([]RowReceiver, nOutputs)
				spec.Streams = make([]StreamEndpointSpec, nOutputs)
				b.Run(fmt.Sprintf("outputs=%d", nOutputs), func(b *testing.B) {
					b.SetBytes(int64(nOutputs * numCols * numRows * 8))
					for i := 0; i < b.N; i++ {
						input.Reset()
						for i := 0; i < nOutputs; i++ {
							chans[i].InitWithNumSenders(colTypes, 1)
							recvs[i] = &chans[i]
							spec.Streams[i] = StreamEndpointSpec{StreamID: StreamID(i)}
						}
						r, wg := setupRouter(b, evalCtx, spec, colTypes, recvs)
						for i := range chans {
							go drainRowChannel(&chans[i])
						}
						Run(ctx, input, r)
						r.ProducerDone()
						wg.Wait()
					}
				})
			}
		})
	}
}
