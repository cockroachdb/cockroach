// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// memoryTestCase is a helper struct for a test with memory limits.
type memoryTestCase struct {
	// bytes is the memory limit.
	bytes int64
	// skipExpSpillCheck specifies whether expSpill should be checked to assert
	// that expected spilling behavior happened. This is true if bytes was
	// randomly generated.
	skipExpSpillCheck bool
	// expSpill specifies whether a spill is expected or not. Should be ignored if
	// skipExpSpillCheck is true.
	expSpill bool
}

// getDiskqueueCfgAndMemoryTestCases is a test helper that creates an in-memory
// DiskQueueCfg that can be used to create a new DiskQueue. A cleanup function
// is also returned as well as some default memory limits that are useful to
// test with: 0 for an immediate spill, a random memory limit up to 64 MiB, and
// 1GiB, which shouldn't result in a spill.
// Note that not all tests will check for a spill, it is enough that some
// deterministic tests do so for the simple cases.
// TODO(asubiotto): We might want to also return a verify() function that will
//  check for leftover files.
func getDiskQueueCfgAndMemoryTestCases(
	t *testing.T, rng *rand.Rand,
) (colcontainer.DiskQueueCfg, func(), []memoryTestCase) {
	t.Helper()
	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)

	return queueCfg, cleanup, []memoryTestCase{
		{bytes: 0, expSpill: true},
		{bytes: 1 + rng.Int63n(64<<20 /* 64 MiB */), skipExpSpillCheck: true},
		{bytes: 1 << 30 /* 1 GiB */, expSpill: false},
	}
}

// getDataAndFullSelection is a test helper that generates tuples representing
// a batch with single int64 column where each element is its ordinal and an
// accompanying selection vector that selects every index in tuples.
func getDataAndFullSelection() (tuples, []*types.T, []int) {
	data := make(tuples, coldata.BatchSize())
	fullSelection := make([]int, coldata.BatchSize())
	for i := range data {
		data[i] = tuple{i}
		fullSelection[i] = i
	}
	return data, []*types.T{types.Int}, fullSelection
}

func TestRouterOutputAddBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	data, typs, fullSelection := getDataAndFullSelection()

	// Since the actual data doesn't matter, we will just be reusing data for each
	// test case.
	testCases := []struct {
		inputBatchSize   int
		outputBatchSize  int
		blockedThreshold int
		// selection determines which indices to add to the router output as well
		// as how many elements from data are compared to the output.
		selection []int
		name      string
	}{
		{
			inputBatchSize:   coldata.BatchSize(),
			outputBatchSize:  coldata.BatchSize(),
			blockedThreshold: getDefaultRouterOutputBlockedThreshold(),
			selection:        fullSelection,
			name:             "OneBatch",
		},
		{
			inputBatchSize:   coldata.BatchSize(),
			outputBatchSize:  4,
			blockedThreshold: getDefaultRouterOutputBlockedThreshold(),
			selection:        fullSelection,
			name:             "OneBatchGTOutputSize",
		},
		{
			inputBatchSize:   4,
			outputBatchSize:  coldata.BatchSize(),
			blockedThreshold: getDefaultRouterOutputBlockedThreshold(),
			selection:        fullSelection,
			name:             "MultipleInputBatchesLTOutputSize",
		},
		{
			inputBatchSize:   coldata.BatchSize(),
			outputBatchSize:  coldata.BatchSize(),
			blockedThreshold: getDefaultRouterOutputBlockedThreshold(),
			selection:        fullSelection[:len(fullSelection)/4],
			name:             "QuarterSelection",
		},
	}

	// unblockEventsChan is purposefully unbuffered; the router output should never write to it
	// in this test.
	unblockEventsChan := make(chan struct{})

	rng, _ := randutil.NewPseudoRand()
	queueCfg, cleanup, memoryTestCases := getDiskQueueCfgAndMemoryTestCases(t, rng)
	defer cleanup()

	for _, tc := range testCases {
		if len(tc.selection) == 0 {
			// No data to work with, probably due to a low coldata.BatchSize.
			continue
		}
		for _, mtc := range memoryTestCases {
			t.Run(fmt.Sprintf("%s/memoryLimit=%s", tc.name, humanizeutil.IBytes(mtc.bytes)), func(t *testing.T) {
				// Clear the testAllocator for use.
				testAllocator.ReleaseMemory(testAllocator.Used())
				o := newRouterOutputOpWithBlockedThresholdAndBatchSize(testAllocator, typs, unblockEventsChan, mtc.bytes, queueCfg, colexecbase.NewTestingSemaphore(2), tc.blockedThreshold, tc.outputBatchSize, testDiskAcc)
				in := newOpTestInput(tc.inputBatchSize, data, nil /* typs */)
				out := newOpTestOutput(o, data[:len(tc.selection)])
				in.Init()
				for {
					b := in.Next(ctx)
					o.addBatch(ctx, b, tc.selection)
					if b.Length() == 0 {
						break
					}
				}
				if err := out.Verify(); err != nil {
					t.Fatal(err)
				}

				// The output should never block. This assumes test cases never send more
				// than defaultRouterOutputBlockedThreshold values.
				select {
				case b := <-unblockEventsChan:
					t.Fatalf("unexpected output state change blocked: %t", b)
				default:
				}

				if !mtc.skipExpSpillCheck {
					require.Equal(t, mtc.expSpill, o.mu.data.spilled())
				}
			})
		}
	}
}

func TestRouterOutputNext(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	data, typs, fullSelection := getDataAndFullSelection()

	testCases := []struct {
		unblockEvent func(in colexecbase.Operator, o *routerOutputOp)
		expected     tuples
		name         string
	}{
		{
			// ReaderWaitsForData verifies that a reader blocks in Next(ctx) until there
			// is data available.
			unblockEvent: func(in colexecbase.Operator, o *routerOutputOp) {
				for {
					b := in.Next(ctx)
					o.addBatch(ctx, b, fullSelection)
					if b.Length() == 0 {
						break
					}
				}
			},
			expected: data,
			name:     "ReaderWaitsForData",
		},
		{
			// ReaderWaitsForZeroBatch verifies that a reader blocking on Next will
			// also get unblocked with no data other than the zero batch.
			unblockEvent: func(_ colexecbase.Operator, o *routerOutputOp) {
				o.addBatch(ctx, coldata.ZeroBatch, nil /* selection */)
			},
			expected: tuples{},
			name:     "ReaderWaitsForZeroBatch",
		},
		{
			// CancelUnblocksReader verifies that calling cancel on an output unblocks
			// a reader.
			unblockEvent: func(_ colexecbase.Operator, o *routerOutputOp) {
				o.cancel(ctx)
			},
			expected: tuples{},
			name:     "CancelUnblocksReader",
		},
	}

	// unblockedEventsChan is purposefully unbuffered; the router output should
	// never write to it in this test.
	unblockedEventsChan := make(chan struct{})

	rng, _ := randutil.NewPseudoRand()
	queueCfg, cleanup, memoryTestCases := getDiskQueueCfgAndMemoryTestCases(t, rng)
	defer cleanup()

	for _, mtc := range memoryTestCases {
		for _, tc := range testCases {
			t.Run(fmt.Sprintf("%s/memoryLimit=%s", tc.name, humanizeutil.IBytes(mtc.bytes)), func(t *testing.T) {
				var wg sync.WaitGroup
				batchChan := make(chan coldata.Batch)
				if queueCfg.FS == nil {
					t.Fatal("FS was nil")
				}
				o := newRouterOutputOp(testAllocator, typs, unblockedEventsChan, mtc.bytes, queueCfg, colexecbase.NewTestingSemaphore(2), testDiskAcc)
				in := newOpTestInput(coldata.BatchSize(), data, nil /* typs */)
				in.Init()
				wg.Add(1)
				go func() {
					for {
						b := o.Next(ctx)
						batchChan <- b
						if b.Length() == 0 {
							break
						}
					}
					wg.Done()
				}()

				// Sleep a long enough amount of time to make sure that if Next didn't block
				// above, we have a good chance of reading a batch.
				time.Sleep(time.Millisecond)
				select {
				case <-batchChan:
					t.Fatal("expected reader goroutine to block when no data ready")
				default:
				}

				tc.unblockEvent(in, o)

				// Should have data available, pushed by our reader goroutine.
				batches := colexecbase.NewBatchBuffer()
				out := newOpTestOutput(batches, tc.expected)
				for {
					b := <-batchChan
					batches.Add(b, typs)
					if b.Length() == 0 {
						break
					}
				}
				if err := out.Verify(); err != nil {
					t.Fatal(err)
				}
				wg.Wait()

				select {
				case <-unblockedEventsChan:
					t.Fatal("unexpected output state change")
				default:
				}
			})
		}

		t.Run(fmt.Sprintf("NextAfterZeroBatchDoesntBlock/memoryLimit=%s", humanizeutil.IBytes(mtc.bytes)), func(t *testing.T) {
			o := newRouterOutputOp(testAllocator, typs, unblockedEventsChan, mtc.bytes, queueCfg, colexecbase.NewTestingSemaphore(2), testDiskAcc)
			o.addBatch(ctx, coldata.ZeroBatch, fullSelection)
			o.Next(ctx)
			o.Next(ctx)
			select {
			case <-unblockedEventsChan:
				t.Fatal("unexpected output state change")
			default:
			}
		})

		t.Run(fmt.Sprintf("AddBatchDoesntBlockWhenOutputIsBlocked/memoryLimit=%s", humanizeutil.IBytes(mtc.bytes)), func(t *testing.T) {
			var (
				smallBatchSize = 8
				blockThreshold = smallBatchSize / 2
			)

			if len(fullSelection) <= smallBatchSize {
				// If a full batch is smaller than our small batch size, reduce it, since
				// this test relies on multiple batches returned from the input.
				smallBatchSize = 2
				if smallBatchSize >= minBatchSize {
					// Sanity check.
					t.Fatalf("smallBatchSize=%d still too large (must be less than minBatchSize=%d)", smallBatchSize, minBatchSize)
				}
				blockThreshold = 1
			}

			// Use a smaller selection than the batch size; it increases test coverage.
			selection := fullSelection[:blockThreshold]

			expected := make(tuples, 0, len(data))
			for i := 0; i < len(data); i += smallBatchSize {
				for k := 0; k < blockThreshold && i+k < len(data); k++ {
					expected = append(expected, data[i+k])
				}
			}

			ch := make(chan struct{}, 2)
			o := newRouterOutputOpWithBlockedThresholdAndBatchSize(testAllocator, typs, ch, mtc.bytes, queueCfg, colexecbase.NewTestingSemaphore(2), blockThreshold, coldata.BatchSize(), testDiskAcc)
			in := newOpTestInput(smallBatchSize, data, nil /* typs */)
			out := newOpTestOutput(o, expected)
			in.Init()

			b := in.Next(ctx)
			// Make sure the output doesn't consider itself blocked. We're right at the
			// limit but not over.
			if o.addBatch(ctx, b, selection) {
				t.Fatal("unexpectedly blocked")
			}
			b = in.Next(ctx)
			// This addBatch call should now block the output.
			if !o.addBatch(ctx, b, selection) {
				t.Fatal("unexpectedly still unblocked")
			}

			// Add the rest of the data.
			for {
				b = in.Next(ctx)
				if o.addBatch(ctx, b, selection) {
					t.Fatal("should only return true when switching from unblocked to blocked")
				}
				if b.Length() == 0 {
					break
				}
			}

			// Unblock the output.
			if err := out.Verify(); err != nil {
				t.Fatal(err)
			}

			// Verify that an unblock event is sent on the channel. This test will fail
			// with a timeout on a channel read if not.
			<-ch
		})
	}
}

func TestRouterOutputRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	rng, _ := randutil.NewPseudoRand()

	var (
		maxValues        = coldata.BatchSize() * 4
		blockedThreshold = 1 + rng.Intn(maxValues-1)
		outputSize       = 1 + rng.Intn(maxValues-1)
	)

	typs := []*types.T{types.Int, types.Int}

	dataLen := 1 + rng.Intn(maxValues-1)
	data := make(tuples, dataLen)
	for i := range data {
		data[i] = make(tuple, len(typs))
		for j := range typs {
			data[i][j] = rng.Int63()
		}
	}

	queueCfg, cleanup, memoryTestCases := getDiskQueueCfgAndMemoryTestCases(t, rng)
	defer cleanup()

	testName := fmt.Sprintf(
		"blockedThreshold=%d/outputSize=%d/totalInputSize=%d", blockedThreshold, outputSize, len(data),
	)
	for _, mtc := range memoryTestCases {
		t.Run(fmt.Sprintf("%s/memoryLimit=%s", testName, humanizeutil.IBytes(mtc.bytes)), func(t *testing.T) {
			runTestsWithFn(t, []tuples{data}, nil /* typs */, func(t *testing.T, inputs []colexecbase.Operator) {
				var wg sync.WaitGroup
				unblockedEventsChans := make(chan struct{}, 2)
				o := newRouterOutputOpWithBlockedThresholdAndBatchSize(testAllocator, typs, unblockedEventsChans, mtc.bytes, queueCfg, colexecbase.NewTestingSemaphore(2), blockedThreshold, outputSize, testDiskAcc)
				inputs[0].Init()

				expected := make(tuples, 0, len(data))

				// canceled is a boolean that specifies whether the output was canceled.
				// If this is the case, the output should not be verified.
				canceled := false

				// Producer.
				errCh := make(chan error)
				wg.Add(1)
				go func() {
					defer wg.Done()
					lastBlockedState := false
					for {
						b := inputs[0].Next(ctx)
						selection := b.Selection()
						if selection == nil {
							selection = coldatatestutils.RandomSel(rng, b.Length(), rng.Float64())
						}

						selection = selection[:b.Length()]

						for _, i := range selection {
							expected = append(expected, make(tuple, len(typs)))
							for j := range typs {
								expected[len(expected)-1][j] = b.ColVec(j).Int64()[i]
							}
						}

						if o.addBatch(ctx, b, selection) {
							if lastBlockedState {
								// We might have missed an unblock event during the last loop.
								select {
								case <-unblockedEventsChans:
								default:
									errCh <- errors.New("output returned state change to blocked when already blocked")
								}
							}
							lastBlockedState = true
						}

						if rng.Float64() < 0.1 {
							o.cancel(ctx)
							canceled = true
							errCh <- nil
							return
						}

						// Read any state changes.
						for moreToRead := true; moreToRead; {
							select {
							case <-unblockedEventsChans:
								if !lastBlockedState {
									errCh <- errors.New("received unblocked state change when output is already unblocked")
								}
								lastBlockedState = false
							default:
								moreToRead = false
							}
						}

						if b.Length() == 0 {
							errCh <- nil
							return
						}
					}
				}()

				actual := colexecbase.NewBatchBuffer()

				// Consumer.
				wg.Add(1)
				go func() {
					for {
						b := o.Next(ctx)
						actual.Add(coldatatestutils.CopyBatch(b, typs, testColumnFactory), typs)
						if b.Length() == 0 {
							wg.Done()
							return
						}
					}
				}()

				if err := <-errCh; err != nil {
					t.Fatal(err)
				}

				wg.Wait()

				if canceled {
					return
				}

				if err := newOpTestOutput(actual, expected).Verify(); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

type callbackRouterOutput struct {
	colexecbase.ZeroInputNode
	addBatchCb func(coldata.Batch, []int) bool
	cancelCb   func()
}

var _ routerOutput = callbackRouterOutput{}

func (o callbackRouterOutput) addBatch(
	ctx context.Context, batch coldata.Batch, selection []int,
) bool {
	if o.addBatchCb != nil {
		return o.addBatchCb(batch, selection)
	}
	return false
}

func (o callbackRouterOutput) cancel(context.Context) {
	if o.cancelCb != nil {
		o.cancelCb()
	}
}

func (o callbackRouterOutput) drain() []execinfrapb.ProducerMetadata {
	return nil
}

func TestHashRouterComputesDestination(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	// We have precomputed expectedNumVals only for the default batch size, so we
	// will override it if a different value is set.
	const expectedBatchSize = 1024
	batchSize := coldata.BatchSize()
	if batchSize != expectedBatchSize {
		require.NoError(t, coldata.SetBatchSizeForTests(expectedBatchSize))
		defer func(batchSize int) { require.NoError(t, coldata.SetBatchSizeForTests(batchSize)) }(batchSize)
		batchSize = expectedBatchSize
	}
	data := make(tuples, batchSize)
	valsYetToSee := make(map[int64]struct{})
	for i := range data {
		data[i] = tuple{i}
		valsYetToSee[int64(i)] = struct{}{}
	}

	in := newOpTestInput(batchSize, data, nil /* typs */)
	in.Init()

	var (
		// expectedNumVals is the number of expected values the output at the
		// corresponding index in outputs receives. This should not change between
		// runs of tests unless the underlying hash algorithm changes. If it does,
		// distributed hash routing will not produce correct results.
		expectedNumVals = []int{273, 252, 287, 212}
		numOutputs      = 4
		valsPushed      = make([]int, numOutputs)
		typs            = []*types.T{types.Int}
	)

	outputs := make([]routerOutput, numOutputs)
	for i := range outputs {
		// Capture the index.
		outputIdx := i
		outputs[i] = callbackRouterOutput{
			addBatchCb: func(batch coldata.Batch, sel []int) bool {
				for _, j := range sel {
					key := batch.ColVec(0).Int64()[j]
					if _, ok := valsYetToSee[key]; !ok {
						t.Fatalf("pushed alread seen value to router output: %d", key)
					}
					delete(valsYetToSee, key)
					valsPushed[outputIdx]++
				}
				return false
			},
			cancelCb: func() {
				t.Fatalf(
					"output %d canceled, outputs should not be canceled during normal operation", outputIdx,
				)
			},
		}
	}

	r := newHashRouterWithOutputs(in, typs, []uint32{0}, nil /* ch */, outputs)
	for r.processNextBatch(ctx) {
	}

	if len(valsYetToSee) != 0 {
		t.Fatalf("hash router failed to push values: %v", valsYetToSee)
	}

	for i, expected := range expectedNumVals {
		if valsPushed[i] != expected {
			t.Fatalf("num val slices differ at output %d, expected: %v actual: %v", i, expectedNumVals, valsPushed)
		}
	}
}

func TestHashRouterCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	outputs := make([]routerOutput, 4)
	numCancels := int64(0)
	numAddBatches := int64(0)
	for i := range outputs {
		// We'll just be checking canceled.
		outputs[i] = callbackRouterOutput{
			addBatchCb: func(_ coldata.Batch, _ []int) bool {
				atomic.AddInt64(&numAddBatches, 1)
				return false
			},
			cancelCb: func() { atomic.AddInt64(&numCancels, 1) },
		}
	}

	typs := []*types.T{types.Int}
	// Never-ending input of 0s.
	batch := testAllocator.NewMemBatch(typs)
	batch.SetLength(coldata.BatchSize())
	in := colexecbase.NewRepeatableBatchSource(testAllocator, batch, typs)

	unbufferedCh := make(chan struct{})
	r := newHashRouterWithOutputs(in, typs, []uint32{0}, unbufferedCh, outputs)

	t.Run("BeforeRun", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		r.Run(ctx)

		if numCancels != int64(len(outputs)) {
			t.Fatalf("expected %d canceled outputs, actual %d", len(outputs), numCancels)
		}

		if numAddBatches != 0 {
			t.Fatalf("detected %d addBatch calls but expected 0", numAddBatches)
		}

		meta := r.DrainMeta(ctx)
		require.Equal(t, 1, len(meta))
		require.True(t, testutils.IsError(meta[0].Err, "context canceled"), meta[0].Err)
	})

	testCases := []struct {
		blocked bool
		name    string
	}{
		{
			blocked: false,
			name:    "DuringRun",
		},
		{
			blocked: true,
			name:    "WhileWaitingForUnblock",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			numCancels = 0
			numAddBatches = 0

			ctx, cancel := context.WithCancel(context.Background())

			if tc.blocked {
				r.numBlockedOutputs = len(outputs)
				defer func() {
					r.numBlockedOutputs = 0
				}()
			}

			routerMeta := make(chan []execinfrapb.ProducerMetadata)
			go func() {
				r.Run(ctx)
				routerMeta <- r.DrainMeta(ctx)
				close(routerMeta)
			}()

			time.Sleep(time.Millisecond)
			if tc.blocked {
				// Make sure no addBatches happened.
				if n := atomic.LoadInt64(&numAddBatches); n != 0 {
					t.Fatalf("expected router to be blocked, but detected %d addBatch calls", n)
				}
			}
			select {
			case <-routerMeta:
				t.Fatal("hash router goroutine unexpectedly done")
			default:
			}
			cancel()
			meta := <-routerMeta
			require.Equal(t, 1, len(meta))
			require.True(t, testutils.IsError(meta[0].Err, "canceled"), meta[0].Err)

			if numCancels != int64(len(outputs)) {
				t.Fatalf("expected %d canceled outputs, actual %d", len(outputs), numCancels)
			}
		})
	}
}

func TestHashRouterOneOutput(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	rng, _ := randutil.NewPseudoRand()

	sel := coldatatestutils.RandomSel(rng, coldata.BatchSize(), rng.Float64())

	data, typs, _ := getDataAndFullSelection()

	expected := make(tuples, 0, len(data))
	for _, i := range sel {
		expected = append(expected, data[i])
	}

	queueCfg, cleanup, memoryTestCases := getDiskQueueCfgAndMemoryTestCases(t, rng)
	defer cleanup()

	for _, mtc := range memoryTestCases {
		t.Run(fmt.Sprintf("memoryLimit=%s", humanizeutil.IBytes(mtc.bytes)), func(t *testing.T) {
			// Clear the testAllocator for use.
			testAllocator.ReleaseMemory(testAllocator.Used())
			diskAcc := testDiskMonitor.MakeBoundAccount()
			defer diskAcc.Close(ctx)
			r, routerOutputs := NewHashRouter(
				[]*colmem.Allocator{testAllocator}, newOpFixedSelTestInput(sel, len(sel), data, typs),
				typs, []uint32{0}, mtc.bytes, queueCfg, colexecbase.NewTestingSemaphore(2),
				[]*mon.BoundAccount{&diskAcc},
			)

			if len(routerOutputs) != 1 {
				t.Fatalf("expected 1 router output but got %d", len(routerOutputs))
			}

			o := newOpTestOutput(routerOutputs[0], expected)

			ro := routerOutputs[0].(*routerOutputOp)
			// Set alwaysFlush so that data is always flushed to the spillingQueue.
			ro.testingKnobs.alwaysFlush = true

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				r.Run(ctx)
				wg.Done()
			}()

			if err := o.Verify(); err != nil {
				t.Fatal(err)
			}
			wg.Wait()
			// Expect no metadata, this should be a successful run.
			unexpectedMetadata := r.DrainMeta(ctx)
			if len(unexpectedMetadata) != 0 {
				t.Fatalf("unexpected metadata when draining HashRouter: %+v", unexpectedMetadata)
			}
			if !mtc.skipExpSpillCheck {
				// If len(sel) == 0, no items will have been enqueued so override an
				// expected spill if this is the case.
				mtc.expSpill = mtc.expSpill && len(sel) != 0
				require.Equal(t, mtc.expSpill, ro.mu.data.spilled())
			}
		})
	}
}

func TestHashRouterRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	rng, _ := randutil.NewPseudoRand()

	var (
		maxValues        = coldata.BatchSize() * 4
		maxOutputs       = 128
		blockedThreshold = 1 + rng.Intn(maxValues-1)
		outputSize       = 1 + rng.Intn(maxValues-1)
		numOutputs       = 1 + rng.Intn(maxOutputs-1)
	)

	typs := []*types.T{types.Int, types.Int}
	dataLen := 1 + rng.Intn(maxValues-1)
	data := make(tuples, dataLen)
	for i := range data {
		data[i] = make(tuple, len(typs))
		for j := range typs {
			data[i][j] = rng.Int63()
		}
	}

	hashCols := make([]uint32, 0, len(typs))
	hashCols = append(hashCols, 0)
	for i := 1; i < cap(hashCols); i++ {
		if rng.Float64() < 0.5 {
			hashCols = append(hashCols, uint32(i))
		}
	}

	// cancel determines whether we test cancellation.
	cancel := false
	if rng.Float64() < 0.25 {
		cancel = true
	}

	testName := fmt.Sprintf(
		"numOutputs=%d/blockedThreshold=%d/outputSize=%d/totalInputSize=%d/hashCols=%v/cancel=%t",
		numOutputs,
		blockedThreshold,
		outputSize,
		len(data),
		hashCols,
		cancel,
	)

	queueCfg, cleanup, memoryTestCases := getDiskQueueCfgAndMemoryTestCases(t, rng)
	defer cleanup()

	// expectedDistribution is set after the first run and used to verify that the
	// distribution of results does not change between runs, as we are sending the
	// same data to the same number of outputs.
	var expectedDistribution []int
	for _, mtc := range memoryTestCases {
		t.Run(fmt.Sprintf(testName+"/memoryLimit=%s", humanizeutil.IBytes(mtc.bytes)), func(t *testing.T) {
			runTestsWithFn(t, []tuples{data}, nil /* typs */, func(t *testing.T, inputs []colexecbase.Operator) {
				unblockEventsChan := make(chan struct{}, 2*numOutputs)
				outputs := make([]routerOutput, numOutputs)
				outputsAsOps := make([]colexecbase.Operator, numOutputs)
				memoryLimitPerOutput := mtc.bytes / int64(len(outputs))
				for i := range outputs {
					// Create separate monitoring infrastructure as well as
					// an allocator for each output as router outputs run
					// concurrently.
					acc := testMemMonitor.MakeBoundAccount()
					defer acc.Close(ctx)
					diskAcc := testDiskMonitor.MakeBoundAccount()
					defer diskAcc.Close(ctx)
					allocator := colmem.NewAllocator(ctx, &acc, testColumnFactory)
					op := newRouterOutputOpWithBlockedThresholdAndBatchSize(allocator, typs, unblockEventsChan, memoryLimitPerOutput, queueCfg, colexecbase.NewTestingSemaphore(len(outputs)*2), blockedThreshold, outputSize, &diskAcc)
					outputs[i] = op
					outputsAsOps[i] = op
				}

				r := newHashRouterWithOutputs(
					inputs[0], typs, hashCols, unblockEventsChan, outputs,
				)

				var (
					results uint64
					wg      sync.WaitGroup
				)
				resultsByOp := make([]int, len(outputsAsOps))
				wg.Add(len(outputsAsOps))
				for i := range outputsAsOps {
					go func(i int) {
						for {
							b := outputsAsOps[i].Next(ctx)
							if b.Length() == 0 {
								break
							}
							atomic.AddUint64(&results, uint64(b.Length()))
							resultsByOp[i] += b.Length()
						}
						wg.Done()
					}(i)
				}

				ctx, cancelFunc := context.WithCancel(context.Background())
				wg.Add(1)
				go func() {
					r.Run(ctx)
					wg.Done()
				}()

				if cancel {
					// Sleep between 0 and ~5 milliseconds.
					time.Sleep(time.Microsecond * time.Duration(rng.Intn(5000)))
					cancelFunc()
				} else {
					// Satisfy linter context leak error.
					defer cancelFunc()
				}

				// Ensure all goroutines end. If a test fails with a hang here it is most
				// likely due to a cancellation bug.
				wg.Wait()
				if !cancel {
					// Expect no metadata, this should be a successful run.
					unexpectedMetadata := r.DrainMeta(ctx)
					if len(unexpectedMetadata) != 0 {
						t.Fatalf("unexpected metadata when draining HashRouter: %+v", unexpectedMetadata)
					}
					// Only do output verification if no cancellation happened.
					if actualTotal := atomic.LoadUint64(&results); actualTotal != uint64(len(data)) {
						t.Fatalf("unexpected number of results %d, expected %d", actualTotal, len(data))
					}
					if expectedDistribution == nil {
						expectedDistribution = resultsByOp
						return
					}
					for i, numVals := range expectedDistribution {
						if numVals != resultsByOp[i] {
							t.Fatalf(
								"distribution of results changed compared to first run at output %d. expected: %v, actual: %v",
								i,
								expectedDistribution,
								resultsByOp,
							)
						}
					}
				}
			})
		})
	}
}

func BenchmarkHashRouter(b *testing.B) {
	defer leaktest.AfterTest(b)()
	ctx := context.Background()

	// Use only one type. Note: the more types you use, the more you inflate the
	// numbers.
	typs := []*types.T{types.Int}
	batch := testAllocator.NewMemBatch(typs)
	batch.SetLength(coldata.BatchSize())
	input := colexecbase.NewRepeatableBatchSource(testAllocator, batch, typs)

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(b, true /* inMem */)
	defer cleanup()

	var wg sync.WaitGroup
	for _, numOutputs := range []int{2, 4, 8, 16} {
		for _, numInputBatches := range []int{2, 4, 8, 16} {
			b.Run(fmt.Sprintf("numOutputs=%d/numInputBatches=%d", numOutputs, numInputBatches), func(b *testing.B) {
				allocators := make([]*colmem.Allocator, numOutputs)
				diskAccounts := make([]*mon.BoundAccount, numOutputs)
				for i := range allocators {
					acc := testMemMonitor.MakeBoundAccount()
					allocators[i] = colmem.NewAllocator(ctx, &acc, testColumnFactory)
					defer acc.Close(ctx)
					diskAcc := testDiskMonitor.MakeBoundAccount()
					diskAccounts[i] = &diskAcc
					defer diskAcc.Close(ctx)
				}
				r, outputs := NewHashRouter(allocators, input, typs, []uint32{0}, 64<<20, queueCfg, &colexecbase.TestingSemaphore{}, diskAccounts)
				b.SetBytes(8 * int64(coldata.BatchSize()) * int64(numInputBatches))
				// We expect distribution to not change. This is a sanity check that
				// we're resetting properly.
				var expectedDistribution []int
				actualDistribution := make([]int, len(outputs))
				// zeroDistribution just allows us to reset actualDistribution with a
				// copy.
				zeroDistribution := make([]int, len(outputs))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					input.ResetBatchesToReturn(numInputBatches)
					r.resetForBenchmarks(ctx)
					wg.Add(len(outputs))
					for j := range outputs {
						go func(j int) {
							for {
								oBatch := outputs[j].Next(ctx)
								actualDistribution[j] += oBatch.Length()
								if oBatch.Length() == 0 {
									break
								}
							}
							wg.Done()
						}(j)
					}
					r.Run(ctx)
					wg.Wait()
					// sum sanity checks that we are actually pushing as many values as we
					// expect.
					sum := 0
					for i := range actualDistribution {
						sum += actualDistribution[i]
					}
					if sum != numInputBatches*coldata.BatchSize() {
						b.Fatalf("unexpected sum %d, expected %d", sum, numInputBatches*coldata.BatchSize())
					}
					if expectedDistribution == nil {
						expectedDistribution = make([]int, len(actualDistribution))
						copy(expectedDistribution, actualDistribution)
					} else {
						for j := range expectedDistribution {
							if expectedDistribution[j] != actualDistribution[j] {
								b.Fatalf(
									"not resetting properly expected distribution: %v, actual distribution: %v",
									expectedDistribution,
									actualDistribution,
								)
							}
						}
					}
					copy(actualDistribution, zeroDistribution)
				}
			})
		}
	}
}
