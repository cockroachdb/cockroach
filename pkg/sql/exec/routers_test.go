// Copyright 2019 The Cockroach Authors.
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

package exec

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// getDataAndFullSelection is a test helper that generates tuples representing
// a one-column types.Int64 batch where each element is its ordinal and an
// accompanying selection vector that selects every index in tuples.
func getDataAndFullSelection() (tuples, []uint16) {
	data := make(tuples, coldata.BatchSize)
	fullSelection := make([]uint16, coldata.BatchSize)
	for i := range data {
		data[i] = tuple{i}
		fullSelection[i] = uint16(i)
	}
	return data, fullSelection
}

func TestRouterOutputAddBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()

	data, fullSelection := getDataAndFullSelection()

	// Since the actual data doesn't matter, we will just be reusing data for each
	// test case.
	testCases := []struct {
		inputBatchSize   uint16
		outputBatchSize  int
		blockedThreshold int
		// selection determines which indices to add to the router output as well
		// as how many elements from data are compared to the output.
		selection []uint16
		name      string
	}{
		{
			inputBatchSize:   coldata.BatchSize,
			outputBatchSize:  coldata.BatchSize,
			blockedThreshold: defaultRouterOutputBlockedThreshold,
			selection:        fullSelection,
			name:             "OneBatch",
		},
		{
			inputBatchSize:   coldata.BatchSize,
			outputBatchSize:  4,
			blockedThreshold: defaultRouterOutputBlockedThreshold,
			selection:        fullSelection,
			name:             "OneBatchGTOutputSize",
		},
		{
			inputBatchSize:   4,
			outputBatchSize:  coldata.BatchSize,
			blockedThreshold: defaultRouterOutputBlockedThreshold,
			selection:        fullSelection,
			name:             "MultipleInputBatchesLTOutputSize",
		},
		{
			inputBatchSize:   coldata.BatchSize,
			outputBatchSize:  coldata.BatchSize,
			blockedThreshold: defaultRouterOutputBlockedThreshold,
			selection:        fullSelection[:len(fullSelection)/4],
			name:             "QuarterSelection",
		},
	}

	// ch is purposefully unbuffered; the router output should never write to it
	// in this test.
	ch := make(chan bool)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			o := newRouterOutputOpWithBlockedThresholdAndBatchSize(
				[]types.T{types.Int64}, ch, tc.blockedThreshold, tc.outputBatchSize,
			)
			in := newOpTestInput(tc.inputBatchSize, data)
			out := newOpTestOutput(o, []int{0}, data[:len(tc.selection)])
			in.Init()
			for {
				b := in.Next()
				selection := tc.selection
				if uint16(len(selection)) > b.Length() && b.Length() != 0 {
					// addBatch has the reasonable assumption that a selection vector is
					// never larger than an accompanying batch. This case just implies
					// full selection, so truncate the selection vector to the appropriate
					// size.
					selection = selection[:tc.inputBatchSize]
				}
				o.addBatch(b, selection)
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
			case b := <-ch:
				t.Fatalf("unexpected output state change blocked: %t", b)
			default:
			}
		})
	}
}

func TestRouterOutputNext(t *testing.T) {
	defer leaktest.AfterTest(t)()

	data, fullSelection := getDataAndFullSelection()

	testCases := []struct {
		unblockEvent func(in Operator, o *routerOutputOp)
		expected     tuples
		name         string
	}{
		{
			// ReaderWaitsForData verifies that a reader blocks in Next() until there
			// is data available.
			unblockEvent: func(in Operator, o *routerOutputOp) {
				for {
					b := in.Next()
					o.addBatch(b, fullSelection)
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
			unblockEvent: func(_ Operator, o *routerOutputOp) {
				o.addBatch(o.zeroBatch, nil /* selection */)
			},
			expected: tuples{},
			name:     "ReaderWaitsForZeroBatch",
		},
		{
			// CancelUnblocksReader verifies that calling cancel on an output unblocks
			// a reader.
			unblockEvent: func(_ Operator, o *routerOutputOp) {
				o.cancel()
			},
			expected: tuples{},
			name:     "CancelUnblocksReader",
		},
	}

	// blockedStateChangeChan is purposefully unbuffered; the router output should
	// never write to it in this test.
	blockedStateChangeChan := make(chan bool)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var wg sync.WaitGroup
			batchChan := make(chan coldata.Batch)
			o := newRouterOutputOp([]types.T{types.Int64}, blockedStateChangeChan)
			in := newOpTestInput(coldata.BatchSize, data)
			in.Init()
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
			batches := newBatchBuffer()
			out := newOpTestOutput(batches, []int{0}, tc.expected)
			for {
				b := <-batchChan
				batches.add(b)
				if b.Length() == 0 {
					break
				}
			}
			if err := out.Verify(); err != nil {
				t.Fatal(err)
			}
			wg.Wait()

			select {
			case s := <-blockedStateChangeChan:
				t.Fatalf("unexpected output state change blocked: %t", s)
			default:
			}
		})
	}

	t.Run("NextAfterZeroBatchDoesntBlock", func(t *testing.T) {
		o := newRouterOutputOp([]types.T{types.Int64}, blockedStateChangeChan)
		o.addBatch(o.zeroBatch, fullSelection)
		o.Next()
		o.Next()
		select {
		case s := <-blockedStateChangeChan:
			t.Fatalf("unexpected output state change blocked: %t", s)
		default:
		}
	})

	t.Run("AddBatchDoesntBlockWhenOutputIsBlocked", func(t *testing.T) {
		const (
			smallBatchSize = 8
			blockThreshold = smallBatchSize / 2
		)

		// Use a smaller selection than the batch size; it increases test coverage.
		selection := fullSelection[:blockThreshold]

		expected := make(tuples, len(data)/(smallBatchSize/blockThreshold))
		for i, j := 0, 0; i < len(data) && j < len(expected); i, j = i+smallBatchSize, j+blockThreshold {
			for k := 0; k < blockThreshold; k++ {
				expected[j+k] = data[i+k]
			}
		}

		ch := make(chan bool, 2)
		o := newRouterOutputOpWithBlockedThresholdAndBatchSize(
			[]types.T{types.Int64}, ch, blockThreshold, coldata.BatchSize,
		)
		in := newOpTestInput(smallBatchSize, data)
		out := newOpTestOutput(o, []int{0}, expected)
		in.Init()

		b := in.Next()
		o.addBatch(b, selection)
		// Make sure the output doesn't consider itself blocked. We're right at the
		// limit but not over.
		select {
		case s := <-ch:
			t.Fatalf("unexpected output state change blocked: %t", s)
		default:
		}
		b = in.Next()
		o.addBatch(b, selection)
		// Now we are blocked.
		if s := <-ch; !s {
			t.Fatal("output unexpectedly returned unblocked state")
		}

		// Add the rest of the data.
		for {
			b = in.Next()
			o.addBatch(b, selection)
			if b.Length() == 0 {
				break
			}
		}

		// Verify that no more state changes happened.
		select {
		case s := <-ch:
			t.Fatalf("unexpected output state change blocked: %t", s)
		default:
		}

		// Unblock the output.
		if err := out.Verify(); err != nil {
			t.Fatal(err)
		}

		// Verify that the state changed to unblocked.
		if s := <-ch; s {
			t.Fatal("output unexpectedly returned blocked state")
		}
	})
}

func TestRouterOutputRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng, _ := randutil.NewPseudoRand()

	const maxValues = coldata.BatchSize * 4
	var (
		blockedThreshold = rng.Intn(maxValues)
		outputSize       = rng.Intn(maxValues)
	)

	types := []types.T{types.Int64, types.Int64}

	dataLen := rng.Intn(maxValues)
	if dataLen == 0 {
		dataLen = 1
	}
	data := make(tuples, dataLen)
	for i := range data {
		data[i] = make(tuple, len(types))
		for j := range types {
			data[i][j] = rng.Int63()
		}
	}

	testName := fmt.Sprintf("blockedThreshold=%d/outputSize=%d/totalInputSize=%d", blockedThreshold, outputSize, len(data))
	t.Run(testName, func(t *testing.T) {
		runTests(t, []tuples{data}, func(t *testing.T, inputs []Operator) {
			var wg sync.WaitGroup
			ch := make(chan bool, 2)
			o := newRouterOutputOpWithBlockedThresholdAndBatchSize(
				types, ch, blockedThreshold, outputSize,
			)
			inputs[0].Init()

			expected := make(tuples, 0, len(data))

			// Producer.
			errCh := make(chan error)
			go func() {
				lastBlockedState := false
				for {
					b := inputs[0].Next()
					selection := b.Selection()
					// batchSize is the actual length of the selection vector.
					batchSize := b.Length()
					if selection == nil {
						selection, batchSize = generateSelectionVector(b.Length(), rng.Float64())
					}

					// TODO(asubiotto): The router output assumes a selection vector is
					// never larger than a batch. Should it panic in this case?
					selection = selection[:batchSize]

					// Note: an operator should only assume the first batch.Length()
					// elements of a selection vector are valid.
					for _, i := range selection {
						expected = append(expected, make(tuple, len(types)))
						for j := range types {
							expected[len(expected)-1][j] = b.ColVec(j).Int64()[i]
						}
					}

					o.addBatch(b, selection)

					// Read any state changes. Since we're the only goroutine with the
					// power to block this output, we expect to have to read from this
					// channel at most twice: once a blocked event caused by addBatch
					// above and an unblock event caused by a read happening between
					// addBatch and here.
					i := 0
					for channelDrained := false; !channelDrained; {
						unexpectedStateErr := func(s bool) error {
							return fmt.Errorf(
								"expected output state change to toggle to %t but got same as last time %t",
								!s,
								s,
							)
						}
						select {
						case s := <-ch:
							if s == lastBlockedState {
								errCh <- unexpectedStateErr(lastBlockedState)
								return
							}
							i++
							lastBlockedState = s
						default:
							if lastBlockedState == true {
								// We blocked the output with the batch but read too early for
								// the unblock event. This is a case in which if we go to the
								// next iteration, we might read 3 state changes next time.
								// Consider the case in which we continue to the next iteration
								// without reading from the channel and the reader enqueues an
								// unblock event. We could call addBatch and enqueue a new
								// block event and before we get to read from the channel, the
								// reader might enqueue another unblock event, so we will end
								// up reading three events from this channel.
								// TODO(asubiotto): Is the hashRouter safe from this? If it is,
								// add explanation to comment.
								s := <-ch
								if s == lastBlockedState {
									errCh <- unexpectedStateErr(lastBlockedState)
									return
								}
								lastBlockedState = s
							}
							channelDrained = true
						}
					}

					if i > 2 {
						errCh <- fmt.Errorf("read %d state changes from output but expected a maximum of 2", i)
						return
					}
					if b.Length() == 0 {
						errCh <- nil
						return
					}
				}
			}()

			actual := newBatchBuffer()

			// Consumer.
			wg.Add(1)
			go func() {
				for {
					b := o.Next()
					actual.add(b)
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

			cols := make([]int, len(types))
			for i := range types {
				cols[i] = i
			}
			if err := newOpTestOutput(actual, cols, expected).Verify(); err != nil {
				t.Fatal(err)
			}
		})
	})
}

type callbackRouterOutput struct {
	addBatchCb func(coldata.Batch, []uint16)
	cancelCb   func()
}

var _ routerOutput = callbackRouterOutput{}

func (o callbackRouterOutput) addBatch(batch coldata.Batch, sel []uint16) {
	if o.addBatchCb != nil {
		o.addBatchCb(batch, sel)
	}
}

func (o callbackRouterOutput) cancel() {
	if o.cancelCb != nil {
		o.cancelCb()
	}
}

func TestHashRouterComputesDestination(t *testing.T) {
	defer leaktest.AfterTest(t)()

	data := make(tuples, coldata.BatchSize)
	valsYetToSee := make(map[int64]struct{})
	for i := range data {
		data[i] = tuple{i}
		valsYetToSee[int64(i)] = struct{}{}
	}

	in := newOpTestInput(coldata.BatchSize, data)
	in.Init()

	var (
		// expectedNumVals is the number of expected values the output at the
		// corresponding index in outputs receives. This should not change between
		// runs of tests unless the underlying hash algorithm changes. If it does,
		// distributed hash routing will not produce correct results.
		expectedNumVals = []int{273, 252, 287, 212}
		valsPushed      = make([]int, len(expectedNumVals))
	)

	outputs := make([]routerOutput, len(expectedNumVals))
	for i := range outputs {
		// Capture the index.
		outputIdx := i
		outputs[i] = callbackRouterOutput{
			addBatchCb: func(batch coldata.Batch, sel []uint16) {
				for _, j := range sel {
					key := batch.ColVec(0).Int64()[j]
					if _, ok := valsYetToSee[key]; !ok {
						t.Fatalf("pushed alread seen value to router output: %d", key)
					}
					delete(valsYetToSee, key)
					valsPushed[outputIdx]++
				}
			},
			cancelCb: func() {
				t.Fatalf(
					"output %d canceled, outputs should not be canceled during normal operation", outputIdx,
				)
			},
		}
	}

	r := newHashRouterWithOutputs(in, []types.T{types.Int64}, []int{0}, nil /* ch */, outputs)
	for r.processNextBatch() {
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
			addBatchCb: func(_ coldata.Batch, _ []uint16) { atomic.AddInt64(&numAddBatches, 1) },
			cancelCb:   func() { atomic.AddInt64(&numCancels, 1) },
		}
	}

	// Never-ending input of 0s.
	batch := coldata.NewMemBatch([]types.T{types.Int64})
	batch.SetLength(coldata.BatchSize)
	in := newRepeatableBatchSource(batch)

	unbufferedCh := make(chan bool)
	r := newHashRouterWithOutputs(in, []types.T{types.Int64}, []int{0}, unbufferedCh, outputs)

	t.Run("BeforeRun", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		r.run(ctx)

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
			name: "DuringRun",
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

			doneCh := make(chan struct{})
			go func() {
				r.run(ctx)
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

func TestHashRouterRandom(t *testing.T) {
	// TODO(asubiotto): Write
	return
}
