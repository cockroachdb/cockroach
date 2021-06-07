// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowflow

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

// setupRouter creates and starts a router. Returns the router and a WaitGroup
// that tracks the lifetime of the background router goroutines.
func setupRouter(
	t testing.TB,
	st *cluster.Settings,
	evalCtx *tree.EvalContext,
	diskMonitor *mon.BytesMonitor,
	spec execinfrapb.OutputRouterSpec,
	inputTypes []*types.T,
	streams []execinfra.RowReceiver,
) (router, *sync.WaitGroup) {
	r, err := makeRouter(&spec, streams)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	flowCtx := execinfra.FlowCtx{
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
		EvalCtx:     evalCtx,
		DiskMonitor: diskMonitor,
	}
	r.init(ctx, &flowCtx, inputTypes)
	wg := &sync.WaitGroup{}
	r.Start(ctx, wg, nil /* flowCtxCancel */)
	return r, wg
}

func TestRouters(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numCols = 6
	const numRows = 200

	rng, _ := randutil.NewPseudoRand()
	alloc := &rowenc.DatumAlloc{}
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.NewTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)

	// Generate tables of possible values for each column; we have fewer possible
	// values than rows to guarantee many occurrences of each value.
	vals, types := randgen.RandSortingEncDatumSlices(rng, numCols, numRows/10)

	testCases := []struct {
		spec       execinfrapb.OutputRouterSpec
		numBuckets int
	}{
		{
			spec:       execinfrapb.OutputRouterSpec{Type: execinfrapb.OutputRouterSpec_BY_HASH, HashColumns: []uint32{0}},
			numBuckets: 4,
		},
		{
			spec:       execinfrapb.OutputRouterSpec{Type: execinfrapb.OutputRouterSpec_BY_HASH, HashColumns: []uint32{0}},
			numBuckets: 2,
		},
		{
			spec:       execinfrapb.OutputRouterSpec{Type: execinfrapb.OutputRouterSpec_BY_HASH, HashColumns: []uint32{3}},
			numBuckets: 4,
		},
		{
			spec:       execinfrapb.OutputRouterSpec{Type: execinfrapb.OutputRouterSpec_BY_HASH, HashColumns: []uint32{1, 3}},
			numBuckets: 4,
		},
		{
			spec:       execinfrapb.OutputRouterSpec{Type: execinfrapb.OutputRouterSpec_BY_HASH, HashColumns: []uint32{5, 2}},
			numBuckets: 3,
		},
		{
			spec:       execinfrapb.OutputRouterSpec{Type: execinfrapb.OutputRouterSpec_BY_HASH, HashColumns: []uint32{0, 1, 2, 3, 4}},
			numBuckets: 5,
		},
		{
			spec:       execinfrapb.OutputRouterSpec{Type: execinfrapb.OutputRouterSpec_MIRROR},
			numBuckets: 2,
		},
		{
			spec:       execinfrapb.OutputRouterSpec{Type: execinfrapb.OutputRouterSpec_MIRROR},
			numBuckets: 3,
		},
		{
			spec:       execinfrapb.OutputRouterSpec{Type: execinfrapb.OutputRouterSpec_MIRROR},
			numBuckets: 4,
		},
		{
			spec:       execinfrapb.OutputRouterSpec{Type: execinfrapb.OutputRouterSpec_BY_RANGE, RangeRouterSpec: testRangeRouterSpec},
			numBuckets: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.spec.Type.String(), func(t *testing.T) {
			bufs := make([]*distsqlutils.RowBuffer, tc.numBuckets)
			recvs := make([]execinfra.RowReceiver, tc.numBuckets)
			tc.spec.Streams = make([]execinfrapb.StreamEndpointSpec, tc.numBuckets)
			for i := 0; i < tc.numBuckets; i++ {
				bufs[i] = &distsqlutils.RowBuffer{}
				recvs[i] = bufs[i]
				tc.spec.Streams[i] = execinfrapb.StreamEndpointSpec{StreamID: execinfrapb.StreamID(i)}
			}

			r, wg := setupRouter(t, st, evalCtx, diskMonitor, tc.spec, types, recvs)

			for i := 0; i < numRows; i++ {
				row := make(rowenc.EncDatumRow, numCols)
				for j := 0; j < numCols; j++ {
					row[j] = vals[j][rng.Intn(len(vals[j]))]
				}
				if status := r.Push(row, nil /* meta */); status != execinfra.NeedMoreRows {
					t.Fatalf("unexpected status: %d", status)
				}
			}
			r.ProducerDone()
			wg.Wait()

			rows := make([]rowenc.EncDatumRows, len(bufs))
			for i, b := range bufs {
				if !b.ProducerClosed() {
					t.Fatalf("bucket not closed: %d", i)
				}
				rows[i] = b.GetRowsNoMeta(t)
			}

			switch tc.spec.Type {
			case execinfrapb.OutputRouterSpec_BY_HASH:
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
									cmp, err := row[c].Compare(types[c], alloc, evalCtx, &row2[c])
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

			case execinfrapb.OutputRouterSpec_MIRROR:
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
							cmp, err := c.Compare(types[j], alloc, evalCtx, &row2[j])
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

			case execinfrapb.OutputRouterSpec_BY_RANGE:
				// Verify each row is in the correct output stream.
				enc := testRangeRouterSpec.Encodings[0]
				var alloc rowenc.DatumAlloc
				for bIdx := range rows {
					for _, row := range rows[bIdx] {
						data, err := row[enc.Column].Encode(types[enc.Column], &alloc, enc.Encoding, nil)
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
	testRangeRouterSpec = execinfrapb.OutputRouterSpec_RangeRouterSpec{
		Spans: []execinfrapb.OutputRouterSpec_RangeRouterSpec_Span{
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
		Encodings: []execinfrapb.OutputRouterSpec_RangeRouterSpec_ColumnEncoding{
			{
				Column:   0,
				Encoding: descpb.DatumEncoding_ASCENDING_KEY,
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

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.NewTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)

	testCases := []struct {
		name string
		spec execinfrapb.OutputRouterSpec
	}{
		{
			name: "MirrorRouter",
			spec: execinfrapb.OutputRouterSpec{Type: execinfrapb.OutputRouterSpec_MIRROR},
		},
		{
			name: "HashRouter",
			spec: execinfrapb.OutputRouterSpec{Type: execinfrapb.OutputRouterSpec_BY_HASH, HashColumns: []uint32{0}},
		},
		{
			name: "RangeRouter",
			spec: execinfrapb.OutputRouterSpec{Type: execinfrapb.OutputRouterSpec_BY_RANGE, RangeRouterSpec: testRangeRouterSpec},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bufs := make([]*distsqlutils.RowBuffer, 2)
			recvs := make([]execinfra.RowReceiver, 2)
			tc.spec.Streams = make([]execinfrapb.StreamEndpointSpec, 2)
			for i := 0; i < 2; i++ {
				bufs[i] = &distsqlutils.RowBuffer{}
				recvs[i] = bufs[i]
				tc.spec.Streams[i] = execinfrapb.StreamEndpointSpec{StreamID: execinfrapb.StreamID(i)}
			}

			colTypes := []*types.T{types.Int}
			router, wg := setupRouter(t, st, evalCtx, diskMonitor, tc.spec, colTypes, recvs)

			// row0 will be a row that the router sends to the first stream, row1 to
			// the 2nd stream.
			var row0, row1 rowenc.EncDatumRow
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
				row0 = rowenc.EncDatumRow{rowenc.DatumToEncDatum(colTypes[0], d)}
				d = tree.NewDInt(math.MaxInt32)
				row1 = rowenc.EncDatumRow{rowenc.DatumToEncDatum(colTypes[0], d)}
			default:
				rng, _ := randutil.NewPseudoRand()
				vals := randgen.RandEncDatumRowsOfTypes(rng, 1 /* numRows */, colTypes)
				row0 = vals[0]
				row1 = row0
			}

			// Push a row and expect NeedMoreRows.
			consumerStatus := router.Push(row0, nil /* meta */)
			if consumerStatus != execinfra.NeedMoreRows {
				t.Fatalf("expected status %d, got: %d", execinfra.NeedMoreRows, consumerStatus)
			}

			// Start draining stream 0. Keep expecting NeedMoreRows, regardless on
			// which stream we send.
			bufs[0].ConsumerDone()
			consumerStatus = router.Push(row0, nil /* meta */)
			if consumerStatus != execinfra.NeedMoreRows {
				t.Fatalf("expected status %d, got: %d", execinfra.NeedMoreRows, consumerStatus)
			}
			consumerStatus = router.Push(row1, nil /* meta */)
			if consumerStatus != execinfra.NeedMoreRows {
				t.Fatalf("expected status %d, got: %d", execinfra.NeedMoreRows, consumerStatus)
			}

			// Close stream 0. Continue to expect NeedMoreRows.
			bufs[0].ConsumerClosed()
			consumerStatus = router.Push(row0, nil /* meta */)
			if consumerStatus != execinfra.NeedMoreRows {
				t.Fatalf("expected status %d, got: %d", execinfra.NeedMoreRows, consumerStatus)
			}
			consumerStatus = router.Push(row1, nil /* meta */)
			if consumerStatus != execinfra.NeedMoreRows {
				t.Fatalf("expected status %d, got: %d", execinfra.NeedMoreRows, consumerStatus)
			}

			// Start draining stream 1. Now that all streams are draining, expect
			// DrainRequested.
			bufs[1].ConsumerDone()
			testutils.SucceedsSoon(t, func() error {
				status := router.Push(row1, nil /* meta */)
				if status != execinfra.DrainRequested {
					return fmt.Errorf("expected status %d, got: %d", execinfra.DrainRequested, consumerStatus)
				}
				return nil
			})

			// Close stream 1. Everything's closed now, but the routers currently
			// only detect this when trying to send metadata - so we still expect
			// DrainRequested.
			bufs[1].ConsumerClosed()
			consumerStatus = router.Push(row1, nil /* meta */)
			if consumerStatus != execinfra.DrainRequested {
				t.Fatalf("expected status %d, got: %d", execinfra.DrainRequested, consumerStatus)
			}

			// Attempt to send some metadata. This will cause the router to observe
			// that everything's closed now.
			testutils.SucceedsSoon(t, func() error {
				consumerStatus := router.Push(
					nil /* row */, &execinfrapb.ProducerMetadata{Err: errors.Errorf("test error")},
				)
				if consumerStatus != execinfra.ConsumerClosed {
					return fmt.Errorf("expected status %d, got: %d", execinfra.ConsumerClosed, consumerStatus)
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
	colTypes []*types.T, hr *hashRouter, streamIdx int, numStreams int,
) (rowenc.EncDatumRow, error) {
	rng, _ := randutil.NewPseudoRand()
	for {
		vals := randgen.RandEncDatumRowOfTypes(rng, colTypes)
		curStreamIdx, err := hr.computeDestination(vals)
		if err != nil {
			return nil, err
		}
		if curStreamIdx == streamIdx {
			return vals, nil
		}
	}
}

// Test that metadata records get forwarded by routers. Depending on the type
// of the metadata, it might need to be forward to either one or all non-closed
// streams (regardless of the type of the router).
func TestMetadataIsForwarded(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.NewTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)

	testCases := []struct {
		name string
		spec execinfrapb.OutputRouterSpec
	}{
		{
			name: "MirrorRouter",
			spec: execinfrapb.OutputRouterSpec{Type: execinfrapb.OutputRouterSpec_MIRROR},
		},
		{
			name: "HashRouter",
			spec: execinfrapb.OutputRouterSpec{Type: execinfrapb.OutputRouterSpec_BY_HASH, HashColumns: []uint32{0}},
		},
		{
			name: "RangeRouter",
			spec: execinfrapb.OutputRouterSpec{Type: execinfrapb.OutputRouterSpec_BY_RANGE, RangeRouterSpec: testRangeRouterSpec},
		},
	}

	// metaConfigs describe different configuration of metadata handling. It
	// assumes the test is working in 4 stages with the following events:
	// - stage 1 is finished with ConsumerDone() call on stream 0
	// - stage 2 is finished with ConsumerClosed() call on stream 0 (at this
	// point only stream 1 is non-closed)
	// - stage 3 is finished with ConsumerClosed() call on stream 1 (at this
	// point all streams are closed).
	metaConfigs := []struct {
		name    string
		getMeta func(stage int) *execinfrapb.ProducerMetadata
		// getReceiverStreamIDs returns the streamIDs of streams that are
		// expected to receive the metadata on the given stage.
		getReceiverStreamIDs func(stage int) []int
		assertExpected       func(streamID int, meta *execinfrapb.ProducerMetadata, stage int)
	}{
		{
			name: "error",
			getMeta: func(stage int) *execinfrapb.ProducerMetadata {
				return &execinfrapb.ProducerMetadata{
					Err: errors.Errorf("test error %d", stage),
				}
			},
			getReceiverStreamIDs: func(stage int) []int {
				switch stage {
				case 1, 2:
					// Errors are propagated to all non-closed streams.
					return []int{0, 1}
				default:
					// Stream 0 is closed after stage 2, so now only stream 1
					// is expected to receive metadata.
					return []int{1}
				}
			},
			assertExpected: func(streamID int, meta *execinfrapb.ProducerMetadata, stage int) {
				expected := errors.Errorf("test error %d", stage)
				if !errors.Is(meta.Err, expected) {
					t.Fatalf("stream %d: unexpected meta.Err %v, expected %s", streamID, meta.Err, expected)
				}
			},
		},
		{
			name: "non-error",
			getMeta: func(stage int) *execinfrapb.ProducerMetadata {
				return &execinfrapb.ProducerMetadata{
					RowNum: &execinfrapb.RemoteProducerMetadata_RowNum{RowNum: int32(stage)},
				}
			},
			getReceiverStreamIDs: func(stage int) []int {
				switch stage {
				case 1, 2:
					return []int{0}
				default:
					return []int{1}
				}
			},
			assertExpected: func(streamID int, meta *execinfrapb.ProducerMetadata, stage int) {
				if meta.RowNum.RowNum != int32(stage) {
					t.Fatalf("streamID %d: unexpected meta %v, expected RowNum=%d in stage %d", streamID, meta, stage, stage)
				}
			},
		},
	}

	for _, tc := range testCases {
		for _, metaConfig := range metaConfigs {
			t.Run(fmt.Sprintf("%s/%s", tc.name, metaConfig.name), func(t *testing.T) {
				chans := make([]execinfra.RowChannel, 2)
				recvs := make([]execinfra.RowReceiver, 2)
				tc.spec.Streams = make([]execinfrapb.StreamEndpointSpec, 2)
				for i := 0; i < 2; i++ {
					chans[i].InitWithBufSizeAndNumSenders(nil /* no column types */, 1, 1)
					recvs[i] = &chans[i]
					tc.spec.Streams[i] = execinfrapb.StreamEndpointSpec{StreamID: execinfrapb.StreamID(i)}
				}
				router, wg := setupRouter(t, st, evalCtx, diskMonitor, tc.spec, nil /* no columns */, recvs)

				stage := 1
				for i := 0; i < 10; i++ {
					consumerStatus := router.Push(nil /* row */, metaConfig.getMeta(stage))
					if consumerStatus != execinfra.NeedMoreRows {
						t.Fatalf("expected status %d, got: %d", execinfra.NeedMoreRows, consumerStatus)
					}
					for _, streamID := range metaConfig.getReceiverStreamIDs(stage) {
						_, meta := chans[streamID].Next()
						metaConfig.assertExpected(streamID, meta, stage)
					}
				}
				chans[0].ConsumerDone()

				stage = 2
				for i := 0; i < 10; i++ {
					consumerStatus := router.Push(nil /* row */, metaConfig.getMeta(stage))
					if consumerStatus != execinfra.NeedMoreRows {
						t.Fatalf("expected status %d, got: %d", execinfra.NeedMoreRows, consumerStatus)
					}
					for _, streamID := range metaConfig.getReceiverStreamIDs(stage) {
						_, meta := chans[streamID].Next()
						metaConfig.assertExpected(streamID, meta, stage)
					}
				}
				chans[0].ConsumerClosed()

				stage = 3
				testutils.SucceedsSoon(t, func() error {
					consumerStatus := router.Push(nil /* row */, metaConfig.getMeta(stage))
					if consumerStatus != execinfra.NeedMoreRows {
						t.Fatalf("expected status %d, got: %d", execinfra.NeedMoreRows, consumerStatus)
					}
					// Receive on stream 1 if there is a message waiting. Metadata may still
					// try to go to 0 for a little while.
					select {
					case d := <-chans[1].C:
						metaConfig.assertExpected(1 /* streamID */, d.Meta, stage)
						return nil
					default:
						return errors.Errorf("no metadata on stream 1")
					}
				})
				chans[1].ConsumerClosed()

				stage = 4
				// Start drain the channels in the background.
				for i := range chans {
					go drainRowChannel(&chans[i])
				}
				testutils.SucceedsSoon(t, func() error {
					consumerStatus := router.Push(nil /* row */, metaConfig.getMeta(stage))
					if consumerStatus != execinfra.ConsumerClosed {
						return fmt.Errorf("expected status %d, got: %d", execinfra.ConsumerClosed, consumerStatus)
					}
					return nil
				})

				router.ProducerDone()

				wg.Wait()
			})
		}
	}
}

func drainRowChannel(rc *execinfra.RowChannel) {
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
		spec execinfrapb.OutputRouterSpec
	}{
		{
			name: "MirrorRouter",
			spec: execinfrapb.OutputRouterSpec{Type: execinfrapb.OutputRouterSpec_MIRROR},
		},
		{
			name: "HashRouter",
			spec: execinfrapb.OutputRouterSpec{Type: execinfrapb.OutputRouterSpec_BY_HASH, HashColumns: []uint32{0}},
		},
		{
			name: "RangeRouter",
			spec: execinfrapb.OutputRouterSpec{Type: execinfrapb.OutputRouterSpec_BY_RANGE, RangeRouterSpec: testRangeRouterSpec},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			colTypes := []*types.T{types.Int}
			chans := make([]execinfra.RowChannel, 2)
			recvs := make([]execinfra.RowReceiver, 2)
			tc.spec.Streams = make([]execinfrapb.StreamEndpointSpec, 2)
			for i := 0; i < 2; i++ {
				chans[i].InitWithBufSizeAndNumSenders(colTypes, 1, 1)
				recvs[i] = &chans[i]
				tc.spec.Streams[i] = execinfrapb.StreamEndpointSpec{StreamID: execinfrapb.StreamID(i)}
			}
			router, err := makeRouter(&tc.spec, recvs)
			if err != nil {
				t.Fatal(err)
			}
			st := cluster.MakeTestingClusterSettings()
			ctx := context.Background()
			evalCtx := tree.MakeTestingEvalContext(st)
			defer evalCtx.Stop(ctx)
			diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
			defer diskMonitor.Stop(ctx)
			flowCtx := execinfra.FlowCtx{
				Cfg: &execinfra.ServerConfig{
					Settings: st,
				},
				EvalCtx:     &evalCtx,
				DiskMonitor: diskMonitor,
			}
			router.init(ctx, &flowCtx, colTypes)
			var wg sync.WaitGroup
			router.Start(ctx, &wg, nil /* flowCtxCancel */)

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
						row := randgen.RandEncDatumRowOfTypes(rng, colTypes)
						status := router.Push(row, nil /* meta */)
						if status != execinfra.NeedMoreRows {
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

	// Enable stats recording.
	tracer := tracing.NewTracer()
	sp := tracer.StartSpan("root", tracing.WithForceRealSpan())
	sp.SetVerbose(true)
	ctx := tracing.ContextWithSpan(context.Background(), sp)

	st := cluster.MakeTestingClusterSettings()
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)
	tempEngine, _, err := storage.NewTempEngine(ctx, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()
	// monitor is the custom memory monitor used in this test. The increment is
	// set to 1 for fine-grained memory allocations and the limit is set to half
	// the number of rows that will eventually be added to the underlying
	// rowContainer. This is a bytes value that will ensure we fall back to disk
	// but use memory for at least a couple of rows.
	monitor := mon.NewMonitorWithLimit(
		"test-monitor",
		mon.MemoryResource,
		(numRows-routerRowBufSize)/2, /* limit */
		nil,                          /* curCount */
		nil,                          /* maxHist */
		1,                            /* increment */
		math.MaxInt64,                /* noteworthy */
		st,
	)
	evalCtx := tree.MakeTestingEvalContextWithMon(st, monitor)
	defer evalCtx.Stop(ctx)
	flowCtx := execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings:    st,
			TempStorage: tempEngine,
		},
		DiskMonitor: diskMonitor,
	}
	alloc := &rowenc.DatumAlloc{}

	extraMemMonitor := execinfra.NewTestMemMonitor(ctx, st)
	defer extraMemMonitor.Stop(ctx)
	// memErrorWhenConsumingRows indicates whether we expect an OOM error to
	// occur when we're consuming rows from the row channel. By default, it
	// will occur because routerOutput derives a memory monitor for the row
	// buffer from evalCtx.Mon which has a limit, and we're going to consume
	// rows after the spilling has occurred (meaning that evalCtx.Mon reached
	// its limit). In order for this to not happen we will create a separate
	// memory account.
	for _, memErrorWhenConsumingRows := range []bool{false, true} {
		var (
			rowChan execinfra.RowChannel
			rb      routerBase
			wg      sync.WaitGroup
			spec    execinfrapb.OutputRouterSpec
		)
		spec.Streams = make([]execinfrapb.StreamEndpointSpec, 1)
		// Initialize the RowChannel with the minimal buffer size so as to block
		// writes to the channel (after the first one).
		rowChan.InitWithBufSizeAndNumSenders(types.OneIntCol, 1 /* chanBufSize */, 1 /* numSenders */)
		rb.setupStreams(&spec, []execinfra.RowReceiver{&rowChan})
		rb.init(ctx, &flowCtx, types.OneIntCol)
		// output is the sole router output in this test.
		output := &rb.outputs[0]
		if !memErrorWhenConsumingRows {
			separateAcc := extraMemMonitor.MakeBoundAccount()
			// NOTE: we need to close the memory account that routerBase
			// created in init for the output since we're overriding it.
			output.rowBufToPushFromAcc.Close(ctx)
			output.rowBufToPushFromAcc = &separateAcc
		}
		rb.Start(ctx, &wg, nil /* ctxCancel */)

		rows := randgen.MakeIntRows(numRows, numCols)
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

		errMetaSeen := false
		traceMetaSeen := false
		for i := 0; ; i++ {
			row, meta := rowChan.Next()
			if meta != nil {
				if memErrorWhenConsumingRows {
					if meta.Err != nil {
						errMetaSeen = true
					}
				} else {
					// Check that router output stats were recorded as expected.
					if traceMetaSeen {
						t.Fatal("expected only one trace meta, encountered multiple")
					}
					if len(meta.TraceData) != 1 {
						t.Fatalf("expected one recorded span, found %d", len(meta.TraceData))
					}
					traceMetaSeen = true
					span := meta.TraceData[0]
					var stats execinfrapb.ComponentStats
					var err error
					var unmarshalled bool
					span.Structured(func(any *pbtypes.Any, _ time.Time) {
						if !pbtypes.Is(any, &stats) {
							return
						}
						if err = pbtypes.UnmarshalAny(any, &stats); err != nil {
							return
						}
						unmarshalled = true
					})
					require.NoError(t, err)
					require.True(t, unmarshalled)
					require.True(t, stats.Inputs[0].NumTuples.HasValue())
					require.True(t, stats.Exec.MaxAllocatedMem.HasValue())
					require.True(t, stats.Exec.MaxAllocatedDisk.HasValue())
					rowsRouted := stats.Inputs[0].NumTuples.Value()
					memMax := stats.Exec.MaxAllocatedMem.Value()
					diskMax := stats.Exec.MaxAllocatedDisk.Value()
					if rowsRouted != numRows {
						t.Errorf("expected %d rows routed, got %d", numRows, rowsRouted)
					}
					if memMax == 0 {
						t.Errorf("expected memMax > 0, got %d", memMax)
					}
					if diskMax == 0 {
						t.Errorf("expected diskMax > 0, got %d", diskMax)
					}
				}
				continue
			}
			if row == nil {
				break
			}
			// Verify correct order (should be the order in which we added rows).
			for j, c := range row {
				if cmp, err := c.Compare(types.Int, alloc, flowCtx.EvalCtx, &rows[i][j]); err != nil {
					t.Fatal(err)
				} else if cmp != 0 {
					t.Fatalf(
						"order violated on row %d, expected %v got %v",
						i,
						rows[i].String(types.OneIntCol),
						row.String(types.OneIntCol),
					)
				}
			}
		}
		if memErrorWhenConsumingRows {
			if !errMetaSeen {
				t.Fatalf("expected memory error when consuming rows")
			}
		} else {
			if !traceMetaSeen {
				t.Error("expected trace metadata, found none")
			}
		}

		// Make sure the goroutine adding rows is done.
		if err := <-errChan; err != nil {
			t.Fatal(err)
		}
	}
}

func TestRangeRouterInit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		spec execinfrapb.OutputRouterSpec_RangeRouterSpec
		err  string
	}{
		{
			spec: testRangeRouterSpec,
		},
		{
			spec: execinfrapb.OutputRouterSpec_RangeRouterSpec{
				Spans: []execinfrapb.OutputRouterSpec_RangeRouterSpec_Span{
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
			spec: execinfrapb.OutputRouterSpec_RangeRouterSpec{
				Spans: testRangeRouterSpec.Spans,
			},
			err: "missing encodings",
		},
	}

	for i, tc := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			spec := execinfrapb.OutputRouterSpec{
				Type:            execinfrapb.OutputRouterSpec_BY_RANGE,
				RangeRouterSpec: tc.spec,
			}
			colTypes := []*types.T{types.Int}
			chans := make([]execinfra.RowChannel, 2)
			recvs := make([]execinfra.RowReceiver, 2)
			spec.Streams = make([]execinfrapb.StreamEndpointSpec, 2)
			for i := 0; i < 2; i++ {
				chans[i].InitWithBufSizeAndNumSenders(colTypes, 1, 1)
				recvs[i] = &chans[i]
				spec.Streams[i] = execinfrapb.StreamEndpointSpec{StreamID: execinfrapb.StreamID(i)}
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
	colTypes := types.MakeIntCols(numCols)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.NewTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)

	input := execinfra.NewRepeatableRowSource(types.OneIntCol, randgen.MakeIntRows(numRows, numCols))

	for _, spec := range []execinfrapb.OutputRouterSpec{
		{
			Type: execinfrapb.OutputRouterSpec_BY_RANGE,
			RangeRouterSpec: execinfrapb.OutputRouterSpec_RangeRouterSpec{
				Spans:     testRangeRouterSpec.Spans,
				Encodings: testRangeRouterSpec.Encodings,
			},
		},
		{
			Type:        execinfrapb.OutputRouterSpec_BY_HASH,
			HashColumns: []uint32{0},
		},
		{
			Type: execinfrapb.OutputRouterSpec_MIRROR,
		},
	} {
		b.Run(spec.Type.String(), func(b *testing.B) {
			for _, nOutputs := range []int{2, 4, 8} {
				chans := make([]execinfra.RowChannel, nOutputs)
				recvs := make([]execinfra.RowReceiver, nOutputs)
				spec.Streams = make([]execinfrapb.StreamEndpointSpec, nOutputs)
				b.Run(fmt.Sprintf("outputs=%d", nOutputs), func(b *testing.B) {
					b.SetBytes(int64(nOutputs * numCols * numRows * 8))
					for i := 0; i < b.N; i++ {
						input.Reset()
						for i := 0; i < nOutputs; i++ {
							chans[i].InitWithNumSenders(colTypes, 1)
							recvs[i] = &chans[i]
							spec.Streams[i] = execinfrapb.StreamEndpointSpec{StreamID: execinfrapb.StreamID(i)}
						}
						r, wg := setupRouter(b, st, evalCtx, diskMonitor, spec, colTypes, recvs)
						for i := range chans {
							go drainRowChannel(&chans[i])
						}
						execinfra.Run(ctx, input, r)
						r.ProducerDone()
						wg.Wait()
					}
				})
			}
		})
	}
}
