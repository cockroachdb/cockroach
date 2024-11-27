// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstreamer

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/diskmap"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	//
	// Calling get() invalidates the results returned on the previous call.
	// TODO(yuzefovich): consider changing the interface to return a single
	// Result object in order to avoid some allocations.
	get(context.Context) (_ []Result, allComplete bool, _ error)

	// wait blocks until there is at least one Result available to be returned
	// to the client or the passed-in context is canceled.
	wait(context.Context) error

	// releaseOne decrements the number of unreleased Results by one.
	releaseOne()

	// clearOverhead releases some of the internal state of the resultsBuffer
	// reducing its usage of the budget. The method should **only** be used when
	// the resultsBuffer is empty.
	clearOverhead(context.Context)

	// close releases all of the resources associated with the buffer.
	// NB: close can only be called when all other goroutines (the worker
	// coordinator as well as any async request evaluators) have exited.
	close(context.Context)

	///////////////////////////////////////////////////////////////////////////
	//                                                                       //
	//            Methods that should be called by the goroutines            //
	//            evaluating the requests asynchronously.                    //
	//                                                                       //
	///////////////////////////////////////////////////////////////////////////

	// Lock and Unlock expose methods on the mutex of the resultsBuffer. If the
	// Streamer's mutex needs to be locked, then the Streamer's mutex must be
	// acquired first.
	Lock()
	Unlock()

	// addLocked adds the provided Result into the buffer. Note that if the
	// Result is available to be returned to the client and there is a goroutine
	// blocked in wait(), the goroutine is **not** woken up - doneAddingLocked()
	// has to be called.
	//
	// The combination of multiple addLocked() calls followed by a single
	// doneAddingLocked() call allows us to simulate adding many Results at
	// once, without having to allocate a slice for that.
	addLocked(Result)
	// doneAddingLocked notifies the resultsBuffer that the worker goroutine
	// added all Results it could, and the resultsBuffer checks whether any
	// Results are available to be returned to the client. If there is a
	// goroutine blocked in wait(), the goroutine is woken up.
	//
	// It is assumed that the budget's mutex is already being held.
	//
	// doneAddingLocked returns the number of results that have been added but
	// not yet returned to the client, and whether the client goroutine was woken.
	doneAddingLocked(context.Context) (int, bool)

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

	// numSpilledResults returns the number of Results that have been spilled to
	// disk by the resultsBuffer so far.
	numSpilledResults() int

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
	// overheadAccountedFor tracks how much overhead space for the Results in
	// this results buffer has been consumed from the budget. Note that this
	// does not include the memory usage of Get and Scan responses (i.e. neither
	// the footprint nor the overhead of a response is tracked by
	// overheadAccountedFor).
	overheadAccountedFor int64
	err                  error
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
		b.setErrorLocked(errors.AssertionFailedf(
			"Enqueue is called before the previous requests have been completed (expected %d, complete %d)",
			b.numExpectedResponses, b.numCompleteResponses,
		))
		return b.err
	}
	if !isEmpty {
		b.setErrorLocked(errors.AssertionFailedf("Enqueue is called before the results of the previous requests have been retrieved"))
		return b.err
	}
	if b.numUnreleasedResults > 0 {
		b.setErrorLocked(errors.AssertionFailedf("unexpectedly there are %d unreleased Results", b.numUnreleasedResults))
		return b.err
	}
	b.numExpectedResponses = numExpectedResponses
	b.numCompleteResponses = 0
	return nil
}

func (b *resultsBufferBase) checkIfCompleteLocked(r Result) {
	b.Mutex.AssertHeld()
	if r.GetResp != nil || r.scanComplete {
		b.numCompleteResponses++
	}
}

func (b *resultsBufferBase) accountForOverheadLocked(ctx context.Context, overheadMemUsage int64) {
	b.budget.mu.AssertHeld()
	b.Mutex.AssertHeld()
	if toConsume := overheadMemUsage - b.overheadAccountedFor; toConsume > 0 {
		// We're allowing the budget to go into debt here since the results
		// buffer doesn't have a way to push back on the Results. It would also
		// be unfortunate to discard these Results - instead, we rely on the
		// worker coordinator to make sure the budget gets out of debt.
		if err := b.budget.consumeLocked(ctx, toConsume, true /* allowDebt */); err != nil {
			b.setErrorLocked(err)
			return
		}
	} else if toConsume < 0 {
		b.budget.releaseLocked(ctx, -toConsume)
	}
	b.overheadAccountedFor = overheadMemUsage
}

// signal non-blockingly sends on hasResults channel and returns whether sent.
func (b *resultsBufferBase) signal() bool {
	select {
	case b.hasResults <- struct{}{}:
		return true
	default:
		return false
	}
}

func (b *resultsBufferBase) wait(ctx context.Context) error {
	select {
	case <-b.hasResults:
		return b.error()
	case <-ctx.Done():
		return ctx.Err()
	}
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

func (b *resultsBufferBase) clearOverhead(ctx context.Context) {
	b.budget.release(ctx, b.overheadAccountedFor)
	b.overheadAccountedFor = 0
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

func (b *outOfOrderResultsBuffer) addLocked(r Result) {
	b.Mutex.AssertHeld()
	b.results = append(b.results, r)
	b.checkIfCompleteLocked(r)
	b.numUnreleasedResults++
}

const resultSize = int64(unsafe.Sizeof(Result{}))

func (b *outOfOrderResultsBuffer) doneAddingLocked(ctx context.Context) (int, bool) {
	b.accountForOverheadLocked(ctx, int64(cap(b.results))*resultSize)
	return len(b.results), b.signal()
}

func (b *outOfOrderResultsBuffer) clearOverhead(ctx context.Context) {
	if buildutil.CrdbTestBuild {
		if len(b.results) > 0 {
			panic(errors.AssertionFailedf("non-empty resultsBuffer when clearOverhead is called"))
		}
	}
	b.results = nil
	b.resultsBufferBase.clearOverhead(ctx)
}

func (b *outOfOrderResultsBuffer) get(context.Context) ([]Result, bool, error) {
	b.Lock()
	defer b.Unlock()
	results := b.results
	// Note that although we're losing the reference to the Results slice, we
	// still keep the overhead of the slice accounted for with the budget. This
	// is done as a way of "amortizing" the reservation.
	b.results = nil
	allComplete := b.numCompleteResponses == b.numExpectedResponses
	if b.err == nil && b.numCompleteResponses > b.numExpectedResponses {
		b.err = errors.AssertionFailedf(
			"received %d complete responses when only %d were expected",
			b.numCompleteResponses, b.numExpectedResponses,
		)
	}
	return results, allComplete, b.err
}

func (b *outOfOrderResultsBuffer) spill(context.Context, int64, int) (bool, error) {
	// There is nothing to spill in the OutOfOrder mode, but we'll assert that
	// the budget's mutex is held.
	b.budget.mu.AssertHeld()
	return false, nil
}

func (b *outOfOrderResultsBuffer) numSpilledResults() int {
	return 0
}

func (b *outOfOrderResultsBuffer) close(context.Context) {}

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
	// Close closes the buffer and releases all of its resources. Cannot be
	// called concurrently with Serialize / Deserialize.
	Close(context.Context)
}

// TestResultDiskBufferConstructor constructs a ResultDiskBuffer for tests. It
// is injected to be rowcontainer.NewKVStreamerResultDiskBuffer in order to
// avoid an import cycle.
var TestResultDiskBufferConstructor func(_ diskmap.Factory, memAcc mon.BoundAccount, diskMonitor *mon.BytesMonitor) ResultDiskBuffer

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
//
// Note that the heap methods were copied (with minor adjustments) from the
// standard library as we chose not to make the struct implement the
// heap.Interface interface in order to avoid allocations.
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

	// numSpilled tracks how many Results have been spilled to disk so far.
	numSpilled int

	// resultScratch is a scratch space reused by get() calls.
	resultScratch []Result

	// addCounter tracks the number of times add() has been called. See
	// inOrderBufferedResult.addEpoch for why this is needed.
	addCounter int32
}

var _ resultsBuffer = &inOrderResultsBuffer{}

func newInOrderResultsBuffer(budget *budget, diskBuffer ResultDiskBuffer) resultsBuffer {
	if diskBuffer == nil {
		panic(errors.AssertionFailedf("diskBuffer is nil"))
	}
	return &inOrderResultsBuffer{
		resultsBufferBase: newResultsBufferBase(budget),
		diskBuffer:        diskBuffer,
	}
}

func (b *inOrderResultsBuffer) less(i, j int) bool {
	posI, posJ := b.buffered[i].Position, b.buffered[j].Position
	subI, subJ := b.buffered[i].subRequestIdx, b.buffered[j].subRequestIdx
	epochI, epochJ := b.buffered[i].addEpoch, b.buffered[j].addEpoch
	return posI < posJ || // results for different requests
		(posI == posJ && subI < subJ) || // results for the same Scan request, but for different ranges
		(posI == posJ && subI == subJ && epochI < epochJ) // results for the same Scan request, for the same range
}

func (b *inOrderResultsBuffer) swap(i, j int) {
	b.buffered[i], b.buffered[j] = b.buffered[j], b.buffered[i]
}

// heapPush pushes r onto the heap of the results.
func (b *inOrderResultsBuffer) heapPush(r inOrderBufferedResult) {
	b.buffered = append(b.buffered, r)
	b.heapUp(len(b.buffered) - 1)
}

// heapRemoveFirst removes the 0th result from the heap. It assumes that the
// heap is not empty.
func (b *inOrderResultsBuffer) heapRemoveFirst() {
	n := len(b.buffered) - 1
	b.swap(0, n)
	b.heapDown(0, n)
	b.buffered = b.buffered[:n]
}

func (b *inOrderResultsBuffer) heapUp(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !b.less(j, i) {
			break
		}
		b.swap(i, j)
		j = i
	}
}

func (b *inOrderResultsBuffer) heapDown(i, n int) {
	for {
		j1 := 2*i + 1
		if j1 >= n {
			return
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && b.less(j2, j1) {
			j = j2 // = 2*i + 2  // right child
		}
		if !b.less(j, i) {
			return
		}
		b.swap(i, j)
		i = j
	}
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

func (b *inOrderResultsBuffer) addLocked(r Result) {
	b.Mutex.AssertHeld()
	// Note that we don't increase b.numUnreleasedResults because all these
	// results are "buffered".
	b.checkIfCompleteLocked(r)
	// All the Results have already been registered with the budget, so we're
	// keeping them in-memory.
	b.heapPush(inOrderBufferedResult{Result: r, onDisk: false, addEpoch: b.addCounter})
	b.addCounter++
}

const inOrderBufferedResultSize = int64(unsafe.Sizeof(inOrderBufferedResult{}))

func (b *inOrderResultsBuffer) doneAddingLocked(ctx context.Context) (int, bool) {
	overhead := int64(cap(b.buffered))*inOrderBufferedResultSize + // b.buffered
		int64(cap(b.resultScratch))*resultSize // b.resultsScratch
	b.accountForOverheadLocked(ctx, overhead)
	if len(b.buffered) > 0 && b.buffered[0].Position == b.headOfLinePosition && b.buffered[0].subRequestIdx == b.headOfLineSubRequestIdx {
		return len(b.buffered), b.signal()
	}
	return len(b.buffered), false
}

func (b *inOrderResultsBuffer) clearOverhead(ctx context.Context) {
	if buildutil.CrdbTestBuild {
		if len(b.buffered) > 0 {
			panic(errors.AssertionFailedf("non-empty resultsBuffer when clearOverhead is called"))
		}
	}
	b.buffered = nil
	b.resultScratch = nil
	b.resultsBufferBase.clearOverhead(ctx)
}

func (b *inOrderResultsBuffer) get(ctx context.Context) ([]Result, bool, error) {
	// Whenever a result is picked up from disk, we need to make the memory
	// reservation for it, so we acquire the budget's mutex.
	b.budget.mu.Lock()
	defer b.budget.mu.Unlock()
	b.Lock()
	defer b.Unlock()
	res := b.resultScratch[:0]
	for len(b.buffered) > 0 && b.buffered[0].Position == b.headOfLinePosition && b.buffered[0].subRequestIdx == b.headOfLineSubRequestIdx {
		if r := &b.buffered[0]; r.onDisk {
			// The buffered result is currently on disk. Ensure that we have
			// enough budget to keep it in memory before deserializing it.
			if err := b.budget.consumeLocked(ctx, r.memoryTok.toRelease, len(res) == 0 /* allowDebt */); err != nil {
				if len(res) > 0 {
					// Most likely this error means that we'd put the budget in
					// debt if we kept this result in-memory. However, there are
					// already some results to return to the client, so we'll
					// return them and attempt to proceed with the current
					// result the next time the client asks.
					break
				}
				b.setErrorLocked(err)
				return nil, false, err
			}
			if err := b.diskBuffer.Deserialize(ctx, &r.Result, r.diskResultID); err != nil {
				b.setErrorLocked(err)
				return nil, false, err
			}
		}
		result := b.buffered[0].Result
		res = append(res, result)
		b.heapRemoveFirst()
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
	// All requests are complete IFF we have received the complete responses for
	// all requests and there no buffered Results.
	allComplete := b.numCompleteResponses == b.numExpectedResponses && len(b.buffered) == 0
	if b.err == nil && b.numCompleteResponses > b.numExpectedResponses {
		b.err = errors.AssertionFailedf(
			"received %d complete responses when only %d were expected",
			b.numCompleteResponses, b.numExpectedResponses,
		)
	}
	b.resultScratch = res
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
		result += fmt.Sprintf(
			"[%d]%s: size %d", b.buffered[i].Position, onDiskInfo, b.buffered[i].memoryTok.toRelease,
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
	//
	// Iterate in reverse order so that the results with higher priority values
	// are spilled first (this could matter if the query has a LIMIT).
	defer func(origAtLeastBytes int64, origSpilled int) {
		if b.numSpilled != origSpilled {
			log.VEventf(ctx, 2,
				"spilled %d results to release %d bytes (asked for %d bytes)",
				b.numSpilled-origSpilled, origAtLeastBytes-atLeastBytes, origAtLeastBytes,
			)
		}
	}(atLeastBytes, b.numSpilled)
	for idx := len(b.buffered) - 1; idx >= 0; idx-- {
		if r := &b.buffered[idx]; !r.onDisk && r.Position > spillingPriority {
			diskResultID, err := b.diskBuffer.Serialize(ctx, &b.buffered[idx].Result)
			if err != nil {
				b.setErrorLocked(err)
				return false, err
			}
			r.spill(diskResultID)
			b.numSpilled++
			b.budget.releaseLocked(ctx, r.memoryTok.toRelease)
			atLeastBytes -= r.memoryTok.toRelease
			if atLeastBytes <= 0 {
				return true, nil
			}
		}
	}
	return false, nil
}

func (b *inOrderResultsBuffer) numSpilledResults() int {
	b.Lock()
	defer b.Unlock()
	return b.numSpilled
}

func (b *inOrderResultsBuffer) close(ctx context.Context) {
	b.diskBuffer.Close(ctx)
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
	addEpoch int32
	// If onDisk is true, then the serialized Result is stored on disk in the
	// ResultDiskBuffer, identified by diskResultID.
	onDisk       bool
	diskResultID int
}

// spill updates r to represent a result that has been spilled to disk and is
// identified by the provided ordinal in the disk buffer.
func (r *inOrderBufferedResult) spill(diskResultID int) {
	*r = inOrderBufferedResult{
		Result: Result{
			memoryTok:      r.memoryTok,
			Position:       r.Position,
			subRequestIdx:  r.subRequestIdx,
			subRequestDone: r.subRequestDone,
			scanComplete:   r.scanComplete,
		},
		addEpoch:     r.addEpoch,
		onDisk:       true,
		diskResultID: diskResultID,
	}
}
