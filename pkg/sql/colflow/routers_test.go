// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colflow

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type testUtils struct {
	// testAllocator is an Allocator with an unlimited budget for use in tests.
	testAllocator     *colmem.Allocator
	testColumnFactory coldata.ColumnFactory

	// testMemMonitor and testMemAcc are a test monitor with an unlimited budget
	// and a memory account bound to it for use in tests.
	testMemMonitor *mon.BytesMonitor
	testMemAcc     *mon.BoundAccount

	// testDiskMonitor and testDiskmAcc are a test monitor with an unlimited budget
	// and a disk account bound to it for use in tests.
	testDiskMonitor *mon.BytesMonitor
	testDiskAcc     *mon.BoundAccount
}

func newTestUtils(ctx context.Context) *testUtils {
	st := cluster.MakeTestingClusterSettings()
	testMemMonitor := execinfra.NewTestMemMonitor(ctx, st)
	memAcc := testMemMonitor.MakeBoundAccount()
	evalCtx := tree.MakeTestingEvalContext(st)
	testColumnFactory := coldataext.NewExtendedColumnFactory(&evalCtx)
	testAllocator := colmem.NewAllocator(ctx, &memAcc, testColumnFactory)
	testDiskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	diskAcc := testDiskMonitor.MakeBoundAccount()
	return &testUtils{
		testAllocator:     testAllocator,
		testColumnFactory: testColumnFactory,
		testMemMonitor:    testMemMonitor,
		testMemAcc:        &memAcc,
		testDiskMonitor:   testDiskMonitor,
		testDiskAcc:       &diskAcc,
	}
}

func (t *testUtils) cleanup(ctx context.Context) {
	t.testMemAcc.Close(ctx)
	t.testMemMonitor.Stop(ctx)
	t.testDiskAcc.Close(ctx)
	t.testDiskMonitor.Stop(ctx)
}

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

// getDiskQueueCfgAndMemoryTestCases is a test helper that creates an in-memory
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
func getDataAndFullSelection() (colexectestutils.Tuples, []*types.T, []int) {
	data := make(colexectestutils.Tuples, coldata.BatchSize())
	fullSelection := make([]int, coldata.BatchSize())
	for i := range data {
		data[i] = colexectestutils.Tuple{i}
		fullSelection[i] = i
	}
	return data, []*types.T{types.Int}, fullSelection
}

// pushSelectionIntoBatch updates b in-place to have the provided selection
// vector while setting the length to minimum of b's length and len(selection).
// Zero-length batch is not modified.
func pushSelectionIntoBatch(b coldata.Batch, selection []int) {
	if b.Length() > 0 {
		b.SetSelection(true)
		copy(b.Selection(), selection)
		if len(selection) < b.Length() {
			b.SetLength(len(selection))
		}
	}
}

func TestRouterOutputAddBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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
	tu := newTestUtils(ctx)
	defer tu.cleanup(ctx)

	for _, tc := range testCases {
		if len(tc.selection) == 0 {
			// No data to work with, probably due to a low coldata.BatchSize.
			continue
		}
		for _, mtc := range memoryTestCases {
			t.Run(fmt.Sprintf("%s/memoryLimit=%s", tc.name, humanizeutil.IBytes(mtc.bytes)), func(t *testing.T) {
				// Clear the testAllocator for use.
				tu.testAllocator.ReleaseMemory(tu.testAllocator.Used())
				o := newRouterOutputOp(
					routerOutputOpArgs{
						types:               typs,
						unlimitedAllocator:  tu.testAllocator,
						memoryLimit:         mtc.bytes,
						diskAcc:             tu.testDiskAcc,
						cfg:                 queueCfg,
						fdSemaphore:         colexecop.NewTestingSemaphore(2),
						unblockedEventsChan: unblockEventsChan,
						testingKnobs: routerOutputOpTestingKnobs{
							blockedThreshold: tc.blockedThreshold,
						},
					},
				)
				in := colexectestutils.NewOpTestInput(tu.testAllocator, tc.inputBatchSize, data, nil)
				out := colexectestutils.NewOpTestOutput(o, data[:len(tc.selection)])
				in.Init(ctx)
				for {
					b := in.Next()
					pushSelectionIntoBatch(b, tc.selection)
					o.addBatch(ctx, b)
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
					require.Equal(t, mtc.expSpill, o.mu.data.Spilled())
				}
			})
		}
	}
}

func TestRouterOutputNext(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	data, typs, fullSelection := getDataAndFullSelection()

	testCases := []struct {
		unblockEvent func(in colexecop.Operator, o *routerOutputOp)
		expected     colexectestutils.Tuples
		name         string
	}{
		{
			// ReaderWaitsForData verifies that a reader blocks in Next() until there
			// is data available.
			unblockEvent: func(in colexecop.Operator, o *routerOutputOp) {
				for {
					b := in.Next()
					pushSelectionIntoBatch(b, fullSelection)
					o.addBatch(ctx, b)
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
			unblockEvent: func(_ colexecop.Operator, o *routerOutputOp) {
				o.addBatch(ctx, coldata.ZeroBatch)
			},
			expected: colexectestutils.Tuples{},
			name:     "ReaderWaitsForZeroBatch",
		},
		{
			// CancelUnblocksReader verifies that calling cancel on an output unblocks
			// a reader.
			unblockEvent: func(_ colexecop.Operator, o *routerOutputOp) {
				o.cancel(ctx, nil /* err */)
			},
			expected: colexectestutils.Tuples{},
			name:     "CancelUnblocksReader",
		},
	}

	// unblockedEventsChan is purposefully unbuffered; the router output should
	// never write to it in this test.
	unblockedEventsChan := make(chan struct{})

	rng, _ := randutil.NewPseudoRand()
	queueCfg, cleanup, memoryTestCases := getDiskQueueCfgAndMemoryTestCases(t, rng)
	defer cleanup()
	tu := newTestUtils(ctx)
	defer tu.cleanup(ctx)

	for _, mtc := range memoryTestCases {
		for _, tc := range testCases {
			t.Run(fmt.Sprintf("%s/memoryLimit=%s", tc.name, humanizeutil.IBytes(mtc.bytes)), func(t *testing.T) {
				var wg sync.WaitGroup
				batchChan := make(chan coldata.Batch)
				if queueCfg.FS == nil {
					t.Fatal("FS was nil")
				}
				o := newRouterOutputOp(
					routerOutputOpArgs{
						types:               typs,
						unlimitedAllocator:  tu.testAllocator,
						memoryLimit:         mtc.bytes,
						diskAcc:             tu.testDiskAcc,
						cfg:                 queueCfg,
						fdSemaphore:         colexecop.NewTestingSemaphore(2),
						unblockedEventsChan: unblockedEventsChan,
					},
				)
				o.Init(ctx)
				in := colexectestutils.NewOpTestInput(tu.testAllocator, coldata.BatchSize(), data, nil)
				in.Init(ctx)
				wg.Add(1)
				go func() {
					for {
						b := o.Next()
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
				batches := colexecop.NewBatchBuffer()
				out := colexectestutils.NewOpTestOutput(batches, tc.expected)
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
			o := newRouterOutputOp(
				routerOutputOpArgs{
					types:               typs,
					unlimitedAllocator:  tu.testAllocator,
					memoryLimit:         mtc.bytes,
					diskAcc:             tu.testDiskAcc,
					cfg:                 queueCfg,
					fdSemaphore:         colexecop.NewTestingSemaphore(2),
					unblockedEventsChan: unblockedEventsChan,
				},
			)
			o.addBatch(ctx, coldata.ZeroBatch)
			o.Init(ctx)
			o.Next()
			o.Next()
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
				// this test relies on multiple batches returned from the Input.
				smallBatchSize = 2
				if smallBatchSize >= colexectestutils.MinBatchSize {
					// Sanity check.
					t.Fatalf("smallBatchSize=%d still too large (must be less than minBatchSize=%d)", smallBatchSize, colexectestutils.MinBatchSize)
				}
				blockThreshold = 1
			}

			// Use a smaller selection than the batch size; it increases test coverage.
			selection := fullSelection[:blockThreshold]

			expected := make(colexectestutils.Tuples, 0, len(data))
			for i := 0; i < len(data); i += smallBatchSize {
				for k := 0; k < blockThreshold && i+k < len(data); k++ {
					expected = append(expected, data[i+k])
				}
			}

			ch := make(chan struct{}, 2)
			o := newRouterOutputOp(
				routerOutputOpArgs{
					types:               typs,
					unlimitedAllocator:  tu.testAllocator,
					memoryLimit:         mtc.bytes,
					diskAcc:             tu.testDiskAcc,
					cfg:                 queueCfg,
					fdSemaphore:         colexecop.NewTestingSemaphore(2),
					unblockedEventsChan: ch,
					testingKnobs: routerOutputOpTestingKnobs{
						blockedThreshold: blockThreshold,
					},
				},
			)
			o.Init(ctx)
			in := colexectestutils.NewOpTestInput(tu.testAllocator, smallBatchSize, data, nil)
			out := colexectestutils.NewOpTestOutput(o, expected)
			in.Init(ctx)

			b := in.Next()
			// Make sure the output doesn't consider itself blocked. We're right at the
			// limit but not over.
			pushSelectionIntoBatch(b, selection)
			if o.addBatch(ctx, b) {
				t.Fatal("unexpectedly blocked")
			}
			b = in.Next()
			// This addBatch call should now block the output.
			pushSelectionIntoBatch(b, selection)
			if !o.addBatch(ctx, b) {
				t.Fatal("unexpectedly still unblocked")
			}

			// Add the rest of the data.
			for {
				b = in.Next()
				pushSelectionIntoBatch(b, selection)
				if o.addBatch(ctx, b) {
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
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	rng, _ := randutil.NewPseudoRand()

	var (
		maxValues        = coldata.BatchSize() * 4
		blockedThreshold = 1 + rng.Intn(maxValues-1)
	)

	typs := []*types.T{types.Int, types.Int}

	dataLen := 1 + rng.Intn(maxValues-1)
	data := make(colexectestutils.Tuples, dataLen)
	for i := range data {
		data[i] = make(colexectestutils.Tuple, len(typs))
		for j := range typs {
			data[i][j] = rng.Int63()
		}
	}

	queueCfg, cleanup, memoryTestCases := getDiskQueueCfgAndMemoryTestCases(t, rng)
	defer cleanup()
	tu := newTestUtils(ctx)
	defer tu.cleanup(ctx)

	testName := fmt.Sprintf(
		"blockedThreshold=%d/totalInputSize=%d", blockedThreshold, len(data),
	)
	for _, mtc := range memoryTestCases {
		t.Run(fmt.Sprintf("%s/memoryLimit=%s", testName, humanizeutil.IBytes(mtc.bytes)), func(t *testing.T) {
			colexectestutils.RunTestsWithFn(t, tu.testAllocator, []colexectestutils.Tuples{data}, nil, func(t *testing.T, inputs []colexecop.Operator) {
				var wg sync.WaitGroup
				unblockedEventsChans := make(chan struct{}, 2)
				o := newRouterOutputOp(
					routerOutputOpArgs{
						types:               typs,
						unlimitedAllocator:  tu.testAllocator,
						memoryLimit:         mtc.bytes,
						diskAcc:             tu.testDiskAcc,
						cfg:                 queueCfg,
						fdSemaphore:         colexecop.NewTestingSemaphore(2),
						unblockedEventsChan: unblockedEventsChans,
						testingKnobs: routerOutputOpTestingKnobs{
							blockedThreshold: blockedThreshold,
						},
					},
				)
				inputs[0].Init(ctx)

				expected := make(colexectestutils.Tuples, 0, len(data))

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
						b := inputs[0].Next()
						selection := b.Selection()
						if selection == nil {
							selection = coldatatestutils.RandomSel(rng, b.Length(), rng.Float64())
						}

						selection = selection[:b.Length()]

						for _, i := range selection {
							expected = append(expected, make(colexectestutils.Tuple, len(typs)))
							for j := range typs {
								expected[len(expected)-1][j] = b.ColVec(j).Int64()[i]
							}
						}

						pushSelectionIntoBatch(b, selection)
						if o.addBatch(ctx, b) {
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
							o.cancel(ctx, nil /* err */)
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

				actual := colexecop.NewBatchBuffer()

				// Consumer.
				wg.Add(1)
				go func() {
					o.Init(ctx)
					for {
						b := o.Next()
						actual.Add(coldatatestutils.CopyBatch(b, typs, tu.testColumnFactory), typs)
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

				if err := colexectestutils.NewOpTestOutput(actual, expected).Verify(); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

type callbackRouterOutput struct {
	colexecop.ZeroInputNode
	addBatchCb   func(coldata.Batch) bool
	cancelCb     func()
	forwardedErr error
}

var _ routerOutput = &callbackRouterOutput{}

func (o *callbackRouterOutput) initWithHashRouter(*HashRouter) {}

func (o *callbackRouterOutput) addBatch(_ context.Context, batch coldata.Batch) bool {
	if o.addBatchCb != nil {
		return o.addBatchCb(batch)
	}
	return false
}

func (o *callbackRouterOutput) cancel(context.Context, error) {
	if o.cancelCb != nil {
		o.cancelCb()
	}
}

func (o *callbackRouterOutput) forwardErr(err error) {
	o.forwardedErr = err
}

func (o *callbackRouterOutput) resetForTests(context.Context) {
	o.forwardedErr = nil
}

func TestHashRouterComputesDestination(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tu := newTestUtils(ctx)
	defer tu.cleanup(ctx)

	// We have precomputed expectedNumVals only for the default batch size, so we
	// will override it if a different value is set.
	const expectedBatchSize = 1024
	batchSize := coldata.BatchSize()
	if batchSize != expectedBatchSize {
		require.NoError(t, coldata.SetBatchSizeForTests(expectedBatchSize))
		defer func(batchSize int) { require.NoError(t, coldata.SetBatchSizeForTests(batchSize)) }(batchSize)
		batchSize = expectedBatchSize
	}
	data := make(colexectestutils.Tuples, batchSize)
	valsYetToSee := make(map[int64]struct{})
	for i := range data {
		data[i] = colexectestutils.Tuple{i}
		valsYetToSee[int64(i)] = struct{}{}
	}

	in := colexectestutils.NewOpTestInput(tu.testAllocator, batchSize, data, nil)
	in.Init(ctx)

	var (
		// expectedNumVals is the number of expected values the output at the
		// corresponding index in outputs receives. This should not change between
		// runs of tests unless the underlying hash algorithm changes. If it does,
		// distributed hash routing will not produce correct results.
		expectedNumVals = []int{273, 252, 287, 212}
		numOutputs      = 4
		valsPushed      = make([]int, numOutputs)
	)

	outputs := make([]routerOutput, numOutputs)
	for i := range outputs {
		// Capture the index.
		outputIdx := i
		outputs[i] = &callbackRouterOutput{
			addBatchCb: func(batch coldata.Batch) bool {
				for _, j := range batch.Selection()[:batch.Length()] {
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

	r := newHashRouterWithOutputs(
		colexecargs.OpWithMetaInfo{Root: in},
		[]uint32{0}, /* hashCols */
		nil,         /* unblockEventsChan */
		outputs,
	)
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
	defer log.Scope(t).Close(t)

	outputs := make([]*callbackRouterOutput, 4)
	routerOutputs := make([]routerOutput, 4)
	numCancels := int64(0)
	numAddBatches := int64(0)
	for i := range outputs {
		outputs[i] = &callbackRouterOutput{
			addBatchCb: func(_ coldata.Batch) bool {
				atomic.AddInt64(&numAddBatches, 1)
				return false
			},
			cancelCb: func() { atomic.AddInt64(&numCancels, 1) },
		}
		routerOutputs[i] = outputs[i]
	}

	typs := []*types.T{types.Int}
	tu := newTestUtils(context.Background())
	defer tu.cleanup(context.Background())
	// Never-ending input of 0s.
	batch := tu.testAllocator.NewMemBatchWithMaxCapacity(typs)
	batch.SetLength(coldata.BatchSize())
	in := colexecop.NewRepeatableBatchSource(tu.testAllocator, batch, typs)

	unbufferedCh := make(chan struct{})
	r := newHashRouterWithOutputs(
		colexecargs.OpWithMetaInfo{Root: in},
		[]uint32{0}, /* hashCols */
		unbufferedCh,
		routerOutputs,
	)

	t.Run("BeforeRun", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		r.resetForTests(ctx)
		cancel()
		r.Run(ctx)

		if numCancels != int64(len(outputs)) {
			t.Fatalf("expected %d canceled outputs, actual %d", len(outputs), numCancels)
		}

		if numAddBatches != 0 {
			t.Fatalf("detected %d addBatch calls but expected 0", numAddBatches)
		}
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
			r.resetForTests(ctx)

			if tc.blocked {
				r.numBlockedOutputs = len(outputs)
				defer func() {
					r.numBlockedOutputs = 0
				}()
			}

			doneCh := make(chan struct{})
			go func() {
				r.Run(ctx)
				close(doneCh)
			}()

			time.Sleep(time.Millisecond)
			if tc.blocked {
				// Make sure no addBatches happened.
				if n := atomic.LoadInt64(&numAddBatches); n != 0 {
					t.Fatalf("expected router to be blocked, but detected %d addBatch calls", n)
				}
			}
			select {
			case <-doneCh:
				t.Fatal("hash router goroutine unexpectedly done")
			default:
			}
			cancel()
			<-doneCh

			if numCancels != int64(len(outputs)) {
				t.Fatalf("expected %d canceled outputs, actual %d", len(outputs), numCancels)
			}
		})
	}
}

func TestHashRouterOneOutput(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	rng, _ := randutil.NewPseudoRand()

	sel := coldatatestutils.RandomSel(rng, coldata.BatchSize(), rng.Float64())

	data, typs, _ := getDataAndFullSelection()

	expected := make(colexectestutils.Tuples, 0, len(data))
	for _, i := range sel {
		expected = append(expected, data[i])
	}

	queueCfg, cleanup, memoryTestCases := getDiskQueueCfgAndMemoryTestCases(t, rng)
	defer cleanup()
	tu := newTestUtils(ctx)
	defer tu.cleanup(ctx)

	for _, mtc := range memoryTestCases {
		t.Run(fmt.Sprintf("memoryLimit=%s", humanizeutil.IBytes(mtc.bytes)), func(t *testing.T) {
			// Clear the testAllocator for use.
			tu.testAllocator.ReleaseMemory(tu.testAllocator.Used())
			diskAcc := tu.testDiskMonitor.MakeBoundAccount()
			defer diskAcc.Close(ctx)
			r, routerOutputs := NewHashRouter(
				[]*colmem.Allocator{tu.testAllocator},
				colexecargs.OpWithMetaInfo{
					Root: colexectestutils.NewOpFixedSelTestInput(tu.testAllocator, sel, len(sel), data, typs),
				},
				typs,
				[]uint32{0}, /* hashCols */
				mtc.bytes,
				queueCfg,
				colexecop.NewTestingSemaphore(2),
				[]*mon.BoundAccount{&diskAcc},
			)

			if len(routerOutputs) != 1 {
				t.Fatalf("expected 1 router output but got %d", len(routerOutputs))
			}

			o := colexectestutils.NewOpTestOutput(routerOutputs[0], expected)

			ro := routerOutputs[0].(*routerOutputOp)

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
			unexpectedMetadata := ro.DrainMeta()
			if len(unexpectedMetadata) != 0 {
				t.Fatalf("unexpected metadata when draining HashRouter output: %+v", unexpectedMetadata)
			}
			if !mtc.skipExpSpillCheck {
				// If len(sel) == 0, no items will have been enqueued so override an
				// expected spill if this is the case.
				mtc.expSpill = mtc.expSpill && len(sel) != 0
				require.Equal(t, mtc.expSpill, ro.mu.data.Spilled())
			}
		})
	}
}

func TestHashRouterRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	rng, _ := randutil.NewPseudoRand()

	var (
		maxValues        = coldata.BatchSize() * 4
		maxOutputs       = 128
		blockedThreshold = 1 + rng.Intn(maxValues-1)
		numOutputs       = 1 + rng.Intn(maxOutputs-1)
	)

	typs := []*types.T{types.Int, types.Int}
	dataLen := 1 + rng.Intn(maxValues-1)
	data := make(colexectestutils.Tuples, dataLen)
	for i := range data {
		data[i] = make(colexectestutils.Tuple, len(typs))
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

	type hashRouterTerminationScenario int
	const (
		// hashRouterGracefulTermination is a normal termination scenario where the
		// input stream is fully consumed and all outputs are drained normally.
		hashRouterGracefulTermination hashRouterTerminationScenario = iota
		// hashRouterPrematureDrainMeta is a termination scenario where one or more
		// outputs are drained before all output is pushed to it.
		hashRouterPrematureDrainMeta
		// hashRouterContextCanceled is a termination scenario where the caller of
		// Run suddenly cancels the provided context.
		hashRouterContextCanceled
		// hashRouterOutputErrorOnAddBatch is a termination scenario in which the
		// router output encounters an error when a batch is pushed to it.
		hashRouterOutputErrorOnAddBatch
		// hashRouterOutputErrorOnNext is a termination scenario in which the
		// router output encounters an error when a batch is read from it.
		hashRouterOutputErrorOnNext
		// hashRouterChaos is a fun scenario in which maybe a bit of everything
		// happens.
		hashRouterChaos
		// hashRouterMaxTerminationScenario should be kept at the end of this list
		// for the rng to generate a valid termination scenario.
		hashRouterMaxTerminationScenario
	)
	terminationScenario := hashRouterTerminationScenario(rng.Intn(int(hashRouterMaxTerminationScenario)))
	// isTerminationScenario is a helper function that returns true with a
	// given probability if the termination scenario is equivalent to the given
	// scenario. This can be used before performing any disruptive behavior in
	// order to randomize its occurrence.
	isTerminationScenario := func(rng *rand.Rand, probability float64, ts hashRouterTerminationScenario) bool {
		return rng.Float64() < probability && (terminationScenario == ts || terminationScenario == hashRouterChaos)
	}
	const (
		addBatchErrMsg = "test induced addBatch error"
		nextErrMsg     = "test induced Next error"
	)

	testName := fmt.Sprintf(
		"terminationScenario=%d/numOutputs=%d/blockedThreshold=%d/totalInputSize=%d/hashCols=%v",
		terminationScenario,
		numOutputs,
		blockedThreshold,
		len(data),
		hashCols,
	)

	queueCfg, cleanup, memoryTestCases := getDiskQueueCfgAndMemoryTestCases(t, rng)
	defer cleanup()
	tu := newTestUtils(ctx)
	defer tu.cleanup(ctx)

	// expectedDistribution is set after the first run and used to verify that the
	// distribution of results does not change between runs, as we are sending the
	// same data to the same number of outputs.
	var expectedDistribution []int
	for _, mtc := range memoryTestCases {
		t.Run(fmt.Sprintf(testName+"/memoryLimit=%s", humanizeutil.IBytes(mtc.bytes)), func(t *testing.T) {
			colexectestutils.RunTestsWithFn(t, tu.testAllocator, []colexectestutils.Tuples{data}, nil, func(t *testing.T, inputs []colexecop.Operator) {
				unblockEventsChan := make(chan struct{}, 2*numOutputs)
				outputs := make([]routerOutput, numOutputs)
				outputsAsOps := make([]colexecop.DrainableOperator, numOutputs)
				memoryLimitPerOutput := mtc.bytes / int64(len(outputs))
				for i := range outputs {
					// Create separate monitoring infrastructure as well as
					// an allocator for each output as router outputs run
					// concurrently.
					acc := tu.testMemMonitor.MakeBoundAccount()
					defer acc.Close(ctx)
					diskAcc := tu.testDiskMonitor.MakeBoundAccount()
					defer diskAcc.Close(ctx)
					allocator := colmem.NewAllocator(ctx, &acc, tu.testColumnFactory)
					op := newRouterOutputOp(
						routerOutputOpArgs{
							types:               typs,
							unlimitedAllocator:  allocator,
							memoryLimit:         memoryLimitPerOutput,
							diskAcc:             &diskAcc,
							cfg:                 queueCfg,
							fdSemaphore:         colexecop.NewTestingSemaphore(len(outputs) * 2),
							unblockedEventsChan: unblockEventsChan,
							testingKnobs: routerOutputOpTestingKnobs{
								blockedThreshold: blockedThreshold,
							},
						},
					)

					injectErrorScenario := terminationScenario
					if terminationScenario == hashRouterChaos {
						switch rng.Intn(2) {
						case 0:
							injectErrorScenario = hashRouterOutputErrorOnAddBatch
						case 1:
							injectErrorScenario = hashRouterOutputErrorOnNext
						default:
						}
					}

					switch injectErrorScenario {
					case hashRouterOutputErrorOnAddBatch:
						op.testingKnobs.addBatchTestInducedErrorCb = func() error {
							addBatchRng, _ := randutil.NewPseudoRand()
							// Sleep between 0 and ~5 milliseconds.
							time.Sleep(time.Microsecond * time.Duration(addBatchRng.Intn(5000)))
							return errors.New(addBatchErrMsg)
						}
					case hashRouterOutputErrorOnNext:
						op.testingKnobs.nextTestInducedErrorCb = func() error {
							nextRng, _ := randutil.NewPseudoRand()
							// Sleep between 0 and ~5 milliseconds.
							time.Sleep(time.Microsecond * time.Duration(nextRng.Intn(5000)))
							return errors.New(nextErrMsg)
						}
					}
					outputs[i] = op
					outputsAsOps[i] = op
				}

				const hashRouterMetadataMsg = "hash router test metadata"
				r := newHashRouterWithOutputs(
					colexecargs.OpWithMetaInfo{
						Root: inputs[0],
						MetadataSources: []colexecop.MetadataSource{
							colexectestutils.CallbackMetadataSource{
								DrainMetaCb: func() []execinfrapb.ProducerMetadata {
									return []execinfrapb.ProducerMetadata{{Err: errors.New(hashRouterMetadataMsg)}}
								},
							},
						},
					},
					hashCols,
					unblockEventsChan,
					outputs,
				)

				var (
					results uint64
					wg      sync.WaitGroup
				)
				resultsByOp := make(
					[]struct {
						numResults int
						err        error
					},
					len(outputsAsOps),
				)
				wg.Add(len(outputsAsOps))
				metadataMu := struct {
					syncutil.Mutex
					metadata [][]execinfrapb.ProducerMetadata
				}{}
				for i := range outputsAsOps {
					go func(i int) {
						outputRng, _ := randutil.NewPseudoRand()
						outputsAsOps[i].Init(ctx)
						for {
							var b coldata.Batch
							err := colexecerror.CatchVectorizedRuntimeError(func() {
								b = outputsAsOps[i].Next()
							})
							if err != nil || b.Length() == 0 || isTerminationScenario(outputRng, 0.5, hashRouterPrematureDrainMeta) {
								resultsByOp[i].err = err
								metadataMu.Lock()
								if meta := outputsAsOps[i].DrainMeta(); meta != nil {
									metadataMu.metadata = append(metadataMu.metadata, meta)
								}
								metadataMu.Unlock()
								break
							}
							atomic.AddUint64(&results, uint64(b.Length()))
							resultsByOp[i].numResults += b.Length()
						}
						wg.Done()
					}(i)
				}

				ctx, cancelFunc := context.WithCancel(context.Background())
				wg.Add(1)
				go func() {
					if terminationScenario == hashRouterContextCanceled || terminationScenario == hashRouterChaos {
						// cancel the context before using it so the HashRouter does not
						// finish Run before it is canceled.
						cancelFunc()
					} else {
						// Satisfy linter context leak error.
						defer cancelFunc()
					}
					r.Run(ctx)
					wg.Done()
				}()

				wg.Wait()
				// The waitGroup protects metadataMu from concurrent access, so there's
				// no need to lock the mutex here.
				metadata := metadataMu.metadata
				checkMetadata := func(t *testing.T, expectedErrMsgs []string) {
					t.Helper()
					require.Equal(t, 1, len(metadata), "one output (the last to exit) should return metadata")

					require.Equal(t, len(expectedErrMsgs), len(metadata[0]), "unexpected number of metadata messages")
					var actualErrMsgs []string
					for _, meta := range metadata[0] {
						require.NotNil(t, meta.Err)
						actualErrMsgs = append(actualErrMsgs, meta.Err.Error())
					}
					sort.Strings(actualErrMsgs)
					sort.Strings(expectedErrMsgs)
					for i := range actualErrMsgs {
						require.Contains(
							t, actualErrMsgs[i], expectedErrMsgs[i], "expected and actual metadata mismatch at index %d: expected: %v actual: %v", i, expectedErrMsgs, actualErrMsgs,
						)
					}
				}
				requireNoErrors := func(t *testing.T) {
					t.Helper()
					for i := range resultsByOp {
						require.NoError(t, resultsByOp[i].err)
					}
				}
				requireErrFromEachOutput := func(t *testing.T, err error) {
					t.Helper()
					if err == nil {
						t.Fatal("use requireNoErrors instead")
					}
					for i := range resultsByOp {
						if resultsByOp[i].err == nil {
							t.Fatalf("unexpectedly no error from %d output", i)
						}
						require.True(t, testutils.IsError(resultsByOp[i].err, err.Error()), "unexpected error %v", resultsByOp[i].err)
					}
				}

				switch terminationScenario {
				case hashRouterGracefulTermination, hashRouterPrematureDrainMeta:
					requireNoErrors(t)
					checkMetadata(t, []string{hashRouterMetadataMsg})

					if terminationScenario == hashRouterPrematureDrainMeta {
						// Don't check output results if an output is prematurely drained
						// since they differ from run to run.
						break
					}

					// Do output verification.
					if actualTotal := atomic.LoadUint64(&results); actualTotal != uint64(len(data)) {
						t.Fatalf("unexpected number of results %d, expected %d", actualTotal, len(data))
					}
					if expectedDistribution == nil {
						expectedDistribution = make([]int, len(resultsByOp))
						for i := range resultsByOp {
							expectedDistribution[i] = resultsByOp[i].numResults
						}
						return
					}
					for i, numVals := range expectedDistribution {
						if numVals != resultsByOp[i].numResults {
							t.Fatalf(
								"distribution of results changed compared to first run at output %d. expected: %v, actual: %v",
								i,
								expectedDistribution,
								resultsByOp,
							)
						}
					}
				case hashRouterContextCanceled:
					requireErrFromEachOutput(t, context.Canceled)
					checkMetadata(t, []string{hashRouterMetadataMsg})
				case hashRouterOutputErrorOnAddBatch:
					requireErrFromEachOutput(t, errors.New(addBatchErrMsg))
					checkMetadata(t, []string{hashRouterMetadataMsg})
				case hashRouterOutputErrorOnNext:
					// If an error is encountered in Next, it is returned to the caller,
					// not as metadata by the HashRouter.
					checkMetadata(t, []string{hashRouterMetadataMsg})
					numNextErrs := 0
					for i := range resultsByOp {
						if err := resultsByOp[i].err; err != nil {
							require.Contains(t, err.Error(), nextErrMsg)
							numNextErrs++
						}
					}
					require.GreaterOrEqual(t, numNextErrs, 1, "expected at least one test-induced next error msg")
				case hashRouterChaos:
					// Metadata must contain at least the hashRouterMetadataMsg and may
					// contain metadata from the other cases.
					require.Equal(t, 1, len(metadataMu.metadata), "one output (the last to exit) should return metadata")
					matchedHashRouterMetadataMsg := false
					metadata := metadataMu.metadata[0]
					for _, meta := range metadata {
						require.NotNil(t, meta.Err)
						if strings.Contains(meta.Err.Error(), hashRouterMetadataMsg) {
							matchedHashRouterMetadataMsg = true
							continue
						}

						matched := false
						for _, errMsg := range []string{
							context.Canceled.Error(),
							addBatchErrMsg,
							nextErrMsg,
						} {
							if strings.Contains(meta.Err.Error(), errMsg) {
								matched = true
								break
							}
						}
						require.True(t, matched, "unexpected metadata: %v", meta.Err)
					}

					require.True(t, matchedHashRouterMetadataMsg, "hashRouterChaos metadata must contain hashRouterMetadataMsg")
				default:
					t.Fatalf("unhandled terminationScenario: %d", terminationScenario)
				}
			})
		})
	}
}

func BenchmarkHashRouter(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	tu := newTestUtils(ctx)
	defer tu.cleanup(ctx)

	// Use only one type. Note: the more types you use, the more you inflate the
	// numbers.
	typs := []*types.T{types.Int}
	batch := tu.testAllocator.NewMemBatchWithMaxCapacity(typs)
	batch.SetLength(coldata.BatchSize())
	input := colexecop.NewRepeatableBatchSource(tu.testAllocator, batch, typs)

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(b, true /* inMem */)
	defer cleanup()

	var wg sync.WaitGroup
	for _, numOutputs := range []int{2, 4, 8, 16} {
		for _, numInputBatches := range []int{2, 4, 8, 16} {
			b.Run(fmt.Sprintf("numOutputs=%d/numInputBatches=%d", numOutputs, numInputBatches), func(b *testing.B) {
				allocators := make([]*colmem.Allocator, numOutputs)
				diskAccounts := make([]*mon.BoundAccount, numOutputs)
				for i := range allocators {
					acc := tu.testMemMonitor.MakeBoundAccount()
					allocators[i] = colmem.NewAllocator(ctx, &acc, tu.testColumnFactory)
					defer acc.Close(ctx)
					diskAcc := tu.testDiskMonitor.MakeBoundAccount()
					diskAccounts[i] = &diskAcc
					defer diskAcc.Close(ctx)
				}
				r, outputs := NewHashRouter(
					allocators,
					colexecargs.OpWithMetaInfo{Root: input},
					typs,
					[]uint32{0}, /* hashCols */
					64<<20,      /* memoryLimit */
					queueCfg,
					&colexecop.TestingSemaphore{},
					diskAccounts,
				)
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
					r.resetForTests(ctx)
					wg.Add(len(outputs))
					for j := range outputs {
						go func(j int) {
							outputs[j].Init(ctx)
							for {
								oBatch := outputs[j].Next()
								actualDistribution[j] += oBatch.Length()
								if oBatch.Length() == 0 {
									_ = outputs[j].DrainMeta()
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
					// Sanity check the spilling queues' memory management.
					for i := range outputs {
						sq := outputs[i].(*routerOutputOp).mu.data
						if sq.Spilled() {
							b.Fatal("unexpectedly spilling queue spilled to disk")
						}
						if sq.MemoryUsage() > 0 {
							b.Fatal("unexpectedly spilling queue's allocator has non-zero usage")
						}
					}
				}
			})
		}
	}
}
