// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvstreamer

import (
	"container/heap"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/diskmap"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// resultsBuffer encapsulates the logic of handling the Results created by the
// asynchronous requests. The implementations are concurrency-safe.
type resultsBuffer interface {
	///////////////////////////////////////////////////////////////////////////
	//                                                                       //
	//    Methods that should be called by the Streamer's user goroutine.    //
	//                                                                       //
	///////////////////////////////////////////////////////////////////////////

	// init prepares the buffer for the next batch of responses.
	// numExpectedResponses specifies how many complete responses are expected
	// to be added to the buffer.
	//
	// It will return an error if:
	// - the buffer hasn't seen all of the complete responses since the previous
	// init() call (i.e. no pipelining is allowed);
	// - the buffer is not empty (i.e. not all results have been retrieved);
	// - there are some unreleased results.
	init(_ context.Context, numExpectedResponses int) error

	// get returns all the Results that the buffer can send to the client at the
	// moment. The boolean indicates whether all expected Results have been
	// returned. Must be called without holding the budget's mutex.
	get(context.Context) (_ []Result, allComplete bool, _ error)

	// wait blocks until there is at least one Result available to be returned
	// to the client.
	wait()

	// releaseOne decrements the number of unreleased Results by one.
	releaseOne()

	// close releases all of the resources associated with the buffer.
	close(context.Context)

	///////////////////////////////////////////////////////////////////////////
	//                                                                       //
	//            Methods that should be called by the goroutines            //
	//            evaluating the requests asynchronously.                    //
	//                                                                       //
	///////////////////////////////////////////////////////////////////////////

	// add adds the provided Results into the buffer. If any Results are
	// available to be returned to the client and there is a goroutine blocked
	// in wait(), the goroutine is woken up.
	add([]Result)

	///////////////////////////////////////////////////////////////////////////
	//                                                                       //
	//   Methods that should be called by the worker coordinator goroutine.  //
	//                                                                       //
	///////////////////////////////////////////////////////////////////////////

	// spill returns true if the resultsBuffer is able to spill already buffered
	// results to disk so that atLeastBytes (or more) bytes are returned to the
	// budget.
	//
	// Only buffered results with higher values of priority (and less "urgency")
	// are spilled. In other words, if a buffered result's priority has higher
	// value than the provided spillingPriority and the result is currently kept
	// in-memory, the result is spilled to disk and its memory reservation is
	// released. The decision to spill or not a particular buffered result is
	// made on a per result basis, independently of other results. The iteration
	// over the buffered results stops once either there are no more candidates
	// for spilling (i.e. all in-memory results have lower priority values than
	// spillingPriority) or enough budget has been freed up.
	//
	// If an error is returned, it should be propagated to the Streamer's
	// client.
	//
	// It is assumed that the budget's mutex is already being held.
	spill(_ context.Context, atLeastBytes int64, spillingPriority int) (bool, error)

	// error returns the first error encountered by the buffer.
	error() error

	///////////////////////////////////////////////////////////////////////////
	//                                                                       //
	//              Methods that can be called by any goroutine.             //
	//                                                                       //
	///////////////////////////////////////////////////////////////////////////

	// numUnreleased returns the number of unreleased Results.
	numUnreleased() int
	// setError sets the error on the buffer (if it hasn't been previously set)
	// and wakes up a goroutine if there is one blocked in wait().
	setError(error)
}

type resultsBufferBase struct {
	budget *budget
	syncutil.Mutex
	// numExpectedResponses tracks the number of complete responses that the
	// results buffer expects to process until the next call to init().
	numExpectedResponses int
	// numCompleteResponses tracks the number of complete Results added into the
	// buffer since the last init().
	numCompleteResponses int
	// numUnreleasedResults tracks the number of Results that have already been
	// created but haven't been Release()'d yet.
	numUnreleasedResults int
	// hasResults is used in wait() to block until there are some results to be
	// picked up.
	hasResults chan struct{}
	err        error
}

func newResultsBufferBase(budget *budget) *resultsBufferBase {
	return &resultsBufferBase{
		budget:     budget,
		hasResults: make(chan struct{}, 1),
	}
}

func (b *resultsBufferBase) initLocked(isEmpty bool, numExpectedResponses int) error {
	b.Mutex.AssertHeld()
	if b.numExpectedResponses != b.numCompleteResponses {
		b.setErrorLocked(errors.AssertionFailedf("Enqueue is called before the previous requests have been completed"))
		return b.err
	}
	if !isEmpty {
		b.setErrorLocked(errors.AssertionFailedf("Enqueue is called before the results of the previous requests have been retrieved"))
		return b.err
	}
	if b.numUnreleasedResults > 0 {
		b.setErrorLocked(errors.AssertionFailedf("unexpectedly there are some unreleased Results"))
		return b.err
	}
	b.numExpectedResponses = numExpectedResponses
	b.numCompleteResponses = 0
	return nil
}

func (b *resultsBufferBase) findCompleteResponses(results []Result) {
	for i := range results {
		if results[i].GetResp != nil || results[i].scanComplete {
			b.numCompleteResponses++
		}
	}
}

// signal non-blockingly sends on hasResults channel.
func (b *resultsBufferBase) signal() {
	select {
	case b.hasResults <- struct{}{}:
	default:
	}
}

func (b *resultsBufferBase) wait() {
	<-b.hasResults
}

func (b *resultsBufferBase) numUnreleased() int {
	b.Lock()
	defer b.Unlock()
	return b.numUnreleasedResults
}

func (b *resultsBufferBase) releaseOne() {
	b.Lock()
	defer b.Unlock()
	b.numUnreleasedResults--
}

func (b *resultsBufferBase) setError(err error) {
	b.Lock()
	defer b.Unlock()
	b.setErrorLocked(err)
}

func (b *resultsBufferBase) setErrorLocked(err error) {
	b.Mutex.AssertHeld()
	if b.err == nil {
		b.err = err
	}
	b.signal()
}

func (b *resultsBufferBase) error() error {
	b.Lock()
	defer b.Unlock()
	return b.err
}

func resultsToString(results []Result, printSubRequestIdx bool) string {
	result := "results for positions "
	for i, r := range results {
		if i > 0 {
			result += ", "
		}
		result += fmt.Sprintf("%d", r.Position)
		if printSubRequestIdx {
			result += fmt.Sprintf(" (%d)", r.subRequestIdx)
		}
	}
	return result
}

// outOfOrderResultsBuffer is a resultsBuffer that returns the Results in an
// arbitrary order (namely in the same order as the Results are added).
type outOfOrderResultsBuffer struct {
	*resultsBufferBase
	results []Result
}

var _ resultsBuffer = &outOfOrderResultsBuffer{}

func newOutOfOrderResultsBuffer(budget *budget) resultsBuffer {
	return &outOfOrderResultsBuffer{resultsBufferBase: newResultsBufferBase(budget)}
}

func (b *outOfOrderResultsBuffer) init(_ context.Context, numExpectedResponses int) error {
	b.Lock()
	defer b.Unlock()
	if err := b.initLocked(len(b.results) == 0 /* isEmpty */, numExpectedResponses); err != nil {
		b.setErrorLocked(err)
		return err
	}
	return nil
}

func (b *outOfOrderResultsBuffer) add(results []Result) {
	b.Lock()
	defer b.Unlock()
	b.results = append(b.results, results...)
	b.findCompleteResponses(results)
	b.numUnreleasedResults += len(results)
	b.signal()
}

func (b *outOfOrderResultsBuffer) get(context.Context) ([]Result, bool, error) {
	b.Lock()
	defer b.Unlock()
	results := b.results
	b.results = nil
	allComplete := b.numCompleteResponses == b.numExpectedResponses
	return results, allComplete, b.err
}

func (b *outOfOrderResultsBuffer) spill(context.Context, int64, int) (bool, error) {
	// There is nothing to spill in the OutOfOrder mode, but we'll assert that
	// the budget's mutex is held.
	b.budget.mu.AssertHeld()
	return false, nil
}

func (b *outOfOrderResultsBuffer) close(context.Context) {
	// Note that only the client's goroutine can be blocked waiting for the
	// results, and close() is called only by the same goroutine, so signaling
	// isn't necessary. However, we choose to be safe and do it anyway.
	b.signal()
}

// ResultDiskBuffer encapsulates the logic for spilling Results to temporary
// disk storage.
//
// At the moment, we use facilities of the SQL layer for this purpose, so we
// introduced this interface to avoid the dependency on SQL.
type ResultDiskBuffer interface {
	// Serialize writes the given Result into the disk buffer. An integer
	// identifying the position of the Result is returned.
	Serialize(context.Context, *Result) (resultID int, _ error)
	// Deserialize updates the Result in-place according to the
	// previously-serialized Result identified by resultID.
	Deserialize(_ context.Context, _ *Result, resultID int) error
	// Reset reset the buffer for reuse.
	Reset(context.Context) error
	// Close closes the buffer and releases all of its resources.
	Close(context.Context)
}

// TestResultDiskBufferConstructor constructs a ResultDiskBuffer for tests. It
// is injected to be rowcontainer.NewKVStreamerResultDiskBuffer in order to
// avoid an import cycle.
var TestResultDiskBufferConstructor func(diskmap.Factory, *mon.BytesMonitor) ResultDiskBuffer

// inOrderResultsBuffer is a resultsBuffer that returns the Results in the same
// order as the original requests were Enqueued into the Streamer (in other
// words, in non-decreasing fashion of Result.Position). Internally, it
// maintains a min heap over Result.Position values, keeps track of the current
// head-of-the-line position, and might spill some of the Results to disk when
// asked.
//
// The Results that are buffered but cannot be returned to the client are not
// considered "unreleased" - we think of them as "buffered". This matters so
// that the Streamer could issue the head-of-the-line request with
// headOfLine=true even if there are some buffered results.
type inOrderResultsBuffer struct {
	*resultsBufferBase
	// headOfLinePosition is the Position value of the next Result to be
	// returned.
	headOfLinePosition int
	// headOfLineSubRequestIdx is the sub-request ordinal of the next Result to
	// be returned. This value only matters when the original Scan request spans
	// multiple ranges - in such a scenario, multiple Results are created. For
	// Get requests and for single-range Scan requests this will always stay at
	// zero.
	headOfLineSubRequestIdx int32
	// buffered contains all buffered Results, regardless of whether they are
	// stored in-memory or on disk.
	buffered []inOrderBufferedResult

	diskBuffer ResultDiskBuffer

	// addCounter tracks the number of times add() has been called. See
	// inOrderBufferedResult.addEpoch for why this is needed.
	addCounter int

	// singleRowLookup is the value of Hints.SingleRowLookup. Only used for
	// debug messages.
	// TODO(yuzefovich): remove this when debug messages are removed.
	singleRowLookup bool
}

var _ resultsBuffer = &inOrderResultsBuffer{}
var _ heap.Interface = &inOrderResultsBuffer{}

func newInOrderResultsBuffer(
	budget *budget, diskBuffer ResultDiskBuffer, singleRowLookup bool,
) resultsBuffer {
	if diskBuffer == nil {
		panic(errors.AssertionFailedf("diskBuffer is nil"))
	}
	return &inOrderResultsBuffer{
		resultsBufferBase: newResultsBufferBase(budget),
		diskBuffer:        diskBuffer,
		singleRowLookup:   singleRowLookup,
	}
}

func (b *inOrderResultsBuffer) Len() int {
	return len(b.buffered)
}

func (b *inOrderResultsBuffer) Less(i, j int) bool {
	posI, posJ := b.buffered[i].Position, b.buffered[j].Position
	subI, subJ := b.buffered[i].subRequestIdx, b.buffered[j].subRequestIdx
	epochI, epochJ := b.buffered[i].addEpoch, b.buffered[j].addEpoch
	return posI < posJ || // results for different requests
		(posI == posJ && subI < subJ) || // results for the same Scan request, but for different ranges
		(posI == posJ && subI == subJ && epochI < epochJ) // results for the same Scan request, for the same range
}

func (b *inOrderResultsBuffer) Swap(i, j int) {
	b.buffered[i], b.buffered[j] = b.buffered[j], b.buffered[i]
}

func (b *inOrderResultsBuffer) Push(x interface{}) {
	b.buffered = append(b.buffered, x.(inOrderBufferedResult))
}

func (b *inOrderResultsBuffer) Pop() interface{} {
	x := b.buffered[len(b.buffered)-1]
	b.buffered = b.buffered[:len(b.buffered)-1]
	return x
}

func (b *inOrderResultsBuffer) init(ctx context.Context, numExpectedResponses int) error {
	b.Lock()
	defer b.Unlock()
	if err := b.initLocked(len(b.buffered) == 0 /* isEmpty */, numExpectedResponses); err != nil {
		b.setErrorLocked(err)
		return err
	}
	b.headOfLinePosition = 0
	b.headOfLineSubRequestIdx = 0
	b.addCounter = 0
	if err := b.diskBuffer.Reset(ctx); err != nil {
		b.setErrorLocked(err)
		return err
	}
	return nil
}

func (b *inOrderResultsBuffer) add(results []Result) {
	b.Lock()
	defer b.Unlock()
	// Note that we don't increase b.numUnreleasedResults because all these
	// results are "buffered".
	b.findCompleteResponses(results)
	foundHeadOfLine := false
	for _, r := range results {
		if debug {
			subRequestIdx := ""
			if !b.singleRowLookup {
				subRequestIdx = fmt.Sprintf(" (%d)", r.subRequestIdx)
			}
			fmt.Printf("adding a result for position %d%s of size %d\n", r.Position, subRequestIdx, r.memoryTok.toRelease)
		}
		// All the Results have already been registered with the budget, so
		// we're keeping them in-memory.
		heap.Push(b, inOrderBufferedResult{Result: r, onDisk: false, addEpoch: b.addCounter})
		if r.Position == b.headOfLinePosition && r.subRequestIdx == b.headOfLineSubRequestIdx {
			foundHeadOfLine = true
		}
	}
	if foundHeadOfLine {
		if debug {
			fmt.Println("found head-of-the-line")
		}
		b.signal()
	}
	b.addCounter++
}

func (b *inOrderResultsBuffer) get(ctx context.Context) ([]Result, bool, error) {
	// Whenever a result is picked up from disk, we need to make the memory
	// reservation for it, so we acquire the budget's mutex.
	b.budget.mu.Lock()
	defer b.budget.mu.Unlock()
	b.Lock()
	defer b.Unlock()
	var res []Result
	if debug {
		fmt.Printf("attempting to get results, current headOfLinePosition = %d\n", b.headOfLinePosition)
	}
	for len(b.buffered) > 0 && b.buffered[0].Position == b.headOfLinePosition && b.buffered[0].subRequestIdx == b.headOfLineSubRequestIdx {
		result, toConsume, err := b.buffered[0].get(ctx, b)
		if err != nil {
			b.setErrorLocked(err)
			return nil, false, err
		}
		if toConsume > 0 {
			if err = b.budget.consumeLocked(ctx, toConsume, len(res) == 0 /* allowDebt */); err != nil {
				if len(res) > 0 {
					// Most likely this error means that we'd put the budget in
					// debt if we kept this result in-memory. However, there are
					// already some results to return to the client, so we'll
					// return them and attempt to proceed with the current
					// result the next time the client asks.

					// The buffered result has been updated in-place to hold the
					// deserialized Result, but since we didn't have the budget
					// to keep it in-memory, we need to spill it. Note that we
					// don't need to update the disk buffer since the
					// serialized Result is still stored with the same resultID,
					// we only need to make sure to lose the references to
					// Get/Scan responses.
					b.buffered[0].spill(b.buffered[0].diskResultID)
					break
				}
				b.setErrorLocked(err)
				return nil, false, err
			}
		}
		res = append(res, result)
		heap.Remove(b, 0)
		if result.subRequestDone {
			// Only advance the sub-request index if we're done with all parts
			// of the sub-request.
			b.headOfLineSubRequestIdx++
		}
		if result.GetResp != nil || result.scanComplete {
			// If the current Result is complete, then we need to advance the
			// head-of-the-line position.
			b.headOfLinePosition++
			b.headOfLineSubRequestIdx = 0
		}
	}
	// Now all the Results in res are no longer "buffered" and become
	// "unreleased", so we increment the corresponding tracker.
	b.numUnreleasedResults += len(res)
	if debug {
		if len(res) > 0 {
			printSubRequestIdx := !b.singleRowLookup
			fmt.Printf("returning %s to the client, headOfLinePosition is now %d\n", resultsToString(res, printSubRequestIdx), b.headOfLinePosition)
		}
	}
	// All requests are complete IFF we have received the complete responses for
	// all requests and there no buffered Results.
	allComplete := b.numCompleteResponses == b.numExpectedResponses && len(b.buffered) == 0
	return res, allComplete, b.err
}

func (b *inOrderResultsBuffer) stringLocked() string {
	b.Mutex.AssertHeld()
	result := "buffered for "
	for i := range b.buffered {
		if i > 0 {
			result += ", "
		}
		var onDiskInfo string
		if b.buffered[i].onDisk {
			onDiskInfo = " (on disk)"
		}
		var subRequestIdx string
		if !b.singleRowLookup {
			subRequestIdx = fmt.Sprintf(" (%d)", b.buffered[i].subRequestIdx)
		}
		result += fmt.Sprintf(
			"[%d%s]%s: size %d", b.buffered[i].Position, subRequestIdx,
			onDiskInfo, b.buffered[i].memoryTok.toRelease,
		)
	}
	return result
}

func (b *inOrderResultsBuffer) spill(
	ctx context.Context, atLeastBytes int64, spillingPriority int,
) (spilled bool, _ error) {
	b.budget.mu.AssertHeld()
	b.Lock()
	defer b.Unlock()
	if buildutil.CrdbTestBuild {
		// In test builds, if we didn't succeed in freeing up the budget, assert
		// that all buffered results with higher priority value have been
		// spilled.
		defer func() {
			if !spilled {
				for i := range b.buffered {
					if b.buffered[i].Position > spillingPriority && !b.buffered[i].onDisk {
						panic(errors.AssertionFailedf(
							"unexpectedly result for position %d wasn't spilled, spilling priority %d\n%s\n",
							b.buffered[i].Position, spillingPriority, b.stringLocked()),
						)
					}
				}
			}
		}()
	}
	if len(b.buffered) == 0 {
		return false, nil
	}
	// Spill some results to disk.
	if debug {
		fmt.Printf(
			"want to spill at least %d bytes with priority %d\t%s\n",
			atLeastBytes, spillingPriority, b.stringLocked(),
		)
	}
	// Iterate in reverse order so that the results with higher priority values
	// are spilled first (this could matter if the query has a LIMIT).
	for idx := len(b.buffered) - 1; idx >= 0; idx-- {
		if r := &b.buffered[idx]; !r.onDisk && r.Position > spillingPriority {
			if debug {
				fmt.Printf(
					"spilling a result for position %d (%d) which will free up %d bytes\n",
					r.Position, r.subRequestIdx, r.memoryTok.toRelease,
				)
			}
			diskResultID, err := b.diskBuffer.Serialize(ctx, &b.buffered[idx].Result)
			if err != nil {
				b.setErrorLocked(err)
				return false, err
			}
			r.spill(diskResultID)
			b.budget.releaseLocked(ctx, r.memoryTok.toRelease)
			atLeastBytes -= r.memoryTok.toRelease
			if atLeastBytes <= 0 {
				if debug {
					fmt.Println("the spill was successful")
				}
				return true, nil
			}
		}
	}
	return false, nil
}

func (b *inOrderResultsBuffer) close(ctx context.Context) {
	b.Lock()
	defer b.Unlock()
	b.diskBuffer.Close(ctx)
	// Note that only the client's goroutine can be blocked waiting for the
	// results, and close() is called only by the same goroutine, so signaling
	// isn't necessary. However, we choose to be safe and do it anyway.
	b.signal()
}

// inOrderBufferedResult describes a single Result for InOrder mode, regardless
// of where it is stored (in-memory or on disk).
type inOrderBufferedResult struct {
	// If onDisk is true, then only Result.Position, Result.memoryTok,
	// Result.subRequestIdx, Result.subRequestDone, and Result.scanComplete are
	// set.
	Result
	// addEpoch indicates the value of addCounter variable when this result was
	// added to the buffer. This "epoch" allows us to order correctly two
	// partial Results that came for the same original Scan request from the
	// same range when one of the Results was the "ResumeSpan" Result to
	// another.
	//
	// Consider the following example: we have the original Scan request as
	// Scan(a, c) which goes to a single range [a, c) containing keys 'a' and
	// 'b'. Imagine that the Scan response can only contain a single key, so we
	// first get a partial ScanResponse('a') with ResumeSpan(b, c), and then we
	// get a partial ScanResponse('b') with an empty ResumeSpan. The first
	// response will be added to the buffer when addCounter is 0, so its "epoch"
	// is 0 whereas the second response is added during "epoch" 1 - thus, we
	// can correctly return 'a' before 'b' although the priority and
	// subRequestIdx of two Results are the same.
	addEpoch int
	// If onDisk is true, then the serialized Result is stored on disk in the
	// ResultDiskBuffer, identified by diskResultID.
	onDisk       bool
	diskResultID int
}

// spill updates r to represent a result that has been spilled to disk and is
// identified by the provided ordinal in the disk buffer.
func (r *inOrderBufferedResult) spill(diskResultID int) {
	isScanComplete := r.scanComplete
	*r = inOrderBufferedResult{
		Result: Result{
			memoryTok:      r.memoryTok,
			Position:       r.Position,
			subRequestIdx:  r.subRequestIdx,
			subRequestDone: r.subRequestDone,
		},
		addEpoch:     r.addEpoch,
		onDisk:       true,
		diskResultID: diskResultID,
	}
	r.scanComplete = isScanComplete
}

// get returns the Result, deserializing it from disk if necessary. toConsume
// indicates how much memory needs to be consumed from the budget to hold this
// Result in-memory.
func (r *inOrderBufferedResult) get(
	ctx context.Context, b *inOrderResultsBuffer,
) (_ Result, toConsume int64, _ error) {
	if !r.onDisk {
		return r.Result, 0, nil
	}
	if err := b.diskBuffer.Deserialize(ctx, &r.Result, r.diskResultID); err != nil {
		return Result{}, 0, err
	}
	return r.Result, r.memoryTok.toRelease, nil
}
