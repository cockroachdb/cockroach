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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
		if results[i].GetResp != nil || results[i].ScanResp.Complete {
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

func resultsToString(results []Result) string {
	result := "results for positions "
	for i, r := range results {
		if i > 0 {
			result += ", "
		}
		result += fmt.Sprintf("%d", r.position)
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

// inOrderResultsBuffer is a resultsBuffer that returns the Results in the same
// order as the original requests were Enqueued into the Streamer (in other
// words, in non-decreasing fashion of Result.position). Internally, it
// maintains a min heap over Result.position values, keeps track of the current
// head-of-the-line position, and might spill some of the Results to disk when
// asked.
//
// The Results that are buffered but cannot be returned to the client are not
// considered "unreleased" - we think of them as "buffered". This matters so
// that the Streamer could issue the head-of-the-line request with
// headOfLine=true even if there are some buffered results.
type inOrderResultsBuffer struct {
	*resultsBufferBase
	// headOfLinePosition is the position value of the next Result to be
	// returned.
	headOfLinePosition int
	// buffered contains all buffered Results, regardless of whether they are
	// stored in-memory or on disk.
	buffered []inOrderBufferedResult
	disk     struct {
		// initialized is set to true when the first Result is spilled to disk.
		initialized bool
		// container stores all Results that have been spilled to disk since the
		// last call to init().
		// TODO(yuzefovich): at the moment, all spilled results that have been
		// returned to the client still exist in the row container, so they have
		// to be skipped over many times leading to a quadratic behavior.
		// Improve this. One idea is to track the "garbage ratio" and once that
		// exceeds say 50%, a new container is created and non-garbage rows are
		// inserted into it.
		container rowcontainer.DiskRowContainer
		// iter, if non-nil, is the iterator currently positioned at iterRowID
		// row. If a new row is added into the container, the iterator becomes
		// invalid, so it'll be closed and nil-ed out.
		iter      rowcontainer.RowIterator
		iterRowID int

		engine     diskmap.Factory
		monitor    *mon.BytesMonitor
		rowScratch rowenc.EncDatumRow
		alloc      tree.DatumAlloc
	}
}

var _ resultsBuffer = &inOrderResultsBuffer{}
var _ heap.Interface = &inOrderResultsBuffer{}

func newInOrderResultsBuffer(
	budget *budget, engine diskmap.Factory, diskMonitor *mon.BytesMonitor,
) resultsBuffer {
	if engine == nil || diskMonitor == nil {
		panic(errors.AssertionFailedf("either engine or diskMonitor is nil"))
	}
	b := &inOrderResultsBuffer{resultsBufferBase: newResultsBufferBase(budget)}
	b.disk.engine = engine
	b.disk.monitor = diskMonitor
	return b
}

func (b *inOrderResultsBuffer) Len() int {
	return len(b.buffered)
}

func (b *inOrderResultsBuffer) Less(i, j int) bool {
	return b.buffered[i].position < b.buffered[j].position
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
	if b.disk.initialized {
		// If the row container was initialized, make sure to reset it.
		if b.disk.iter != nil {
			b.disk.iter.Close()
			b.disk.iter = nil
			b.disk.iterRowID = 0
		}
		if err := b.disk.container.UnsafeReset(ctx); err != nil {
			b.setErrorLocked(err)
			return err
		}
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
			fmt.Printf("adding a result for position %d of size %d\n", r.position, r.memoryTok.toRelease)
		}
		// All the Results have already been registered with the budget, so
		// we're keeping them in-memory.
		heap.Push(b, inOrderBufferedResult{Result: r, onDisk: false})
		if r.position == b.headOfLinePosition {
			foundHeadOfLine = true
		}
	}
	if foundHeadOfLine {
		if debug {
			fmt.Println("found head-of-the-line")
		}
		b.signal()
	}
}

// TODO(yuzefovich): this doesn't work for Scan responses spanning multiple
// Results, fix it once lookup joins are supported. In particular, it can
// reorder responses for a single ScanRequest since all of them have the same
// position value.
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
	for len(b.buffered) > 0 && b.buffered[0].position == b.headOfLinePosition {
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
					// don't need to update the row container since the
					// serialized Result is still stored at the same position,
					// we only need to make sure to lose the references to
					// Get/Scan responses.
					b.buffered[0].spill(b.buffered[0].diskRowID)
					break
				}
				b.setErrorLocked(err)
				return nil, false, err
			}
		}
		res = append(res, result)
		heap.Remove(b, 0)
		if result.GetResp != nil || result.ScanResp.Complete {
			// If the current Result is complete, then we need to advance the
			// head-of-the-line position.
			b.headOfLinePosition++
		}
	}
	// Now all the Results in res are no longer "buffered" and become
	// "unreleased", so we increment the corresponding tracker.
	b.numUnreleasedResults += len(res)
	if debug {
		if len(res) > 0 {
			fmt.Printf("returning %s to the client, headOfLinePosition is now %d\n", resultsToString(res), b.headOfLinePosition)
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
		result += fmt.Sprintf("[%d]%s: size %d", b.buffered[i].position, onDiskInfo, b.buffered[i].memoryTok.toRelease)
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
					if b.buffered[i].position > spillingPriority && !b.buffered[i].onDisk {
						panic(errors.AssertionFailedf(
							"unexpectedly result for position %d wasn't spilled, spilling priority %d\n%s\n",
							b.buffered[i].position, spillingPriority, b.stringLocked()),
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
	if !b.disk.initialized {
		b.disk.container = rowcontainer.MakeDiskRowContainer(
			b.disk.monitor,
			inOrderResultsBufferSpillTypeSchema,
			colinfo.ColumnOrdering{},
			b.disk.engine,
		)
		b.disk.initialized = true
		b.disk.rowScratch = make(rowenc.EncDatumRow, len(inOrderResultsBufferSpillTypeSchema))
	}
	// Iterate in reverse order so that the results with higher priority values
	// are spilled first (this could matter if the query has a LIMIT).
	for idx := len(b.buffered) - 1; idx >= 0; idx-- {
		if r := &b.buffered[idx]; !r.onDisk && r.position > spillingPriority {
			if debug {
				fmt.Printf(
					"spilling a result for position %d which will free up %d bytes\n",
					r.position, r.memoryTok.toRelease,
				)
			}
			if err := b.buffered[idx].serialize(b.disk.rowScratch, &b.disk.alloc); err != nil {
				return false, err
			}
			if err := b.disk.container.AddRow(ctx, b.disk.rowScratch); err != nil {
				b.setErrorLocked(err)
				return false, err
			}
			// The iterator became invalid, so we need to close it.
			if b.disk.iter != nil {
				b.disk.iter.Close()
				b.disk.iter = nil
				b.disk.iterRowID = 0
			}
			// The result is spilled as the current last row in the container.
			r.spill(b.disk.container.Len() - 1)
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
	if b.disk.initialized {
		if b.disk.iter != nil {
			b.disk.iter.Close()
			b.disk.iter = nil
		}
		b.disk.container.Close(ctx)
	}
	// Note that only the client's goroutine can be blocked waiting for the
	// results, and close() is called only by the same goroutine, so signaling
	// isn't necessary. However, we choose to be safe and do it anyway.
	b.signal()
}

// inOrderBufferedResult describes a single Result for InOrder mode, regardless
// of where it is stored (in-memory or on disk).
type inOrderBufferedResult struct {
	// If onDisk is true, then only Result.ScanResp.Complete, Result.memoryTok,
	// and Result.position are set.
	Result
	// If onDisk is true, then the serialized Result is stored on disk in the
	// row container at position diskRowID.
	onDisk    bool
	diskRowID int
}

// spill updates r to represent a result that has been spilled to disk and is
// stored at the provided ordinal in the disk row container.
func (r *inOrderBufferedResult) spill(diskRowID int) {
	isScanComplete := r.ScanResp.Complete
	*r = inOrderBufferedResult{
		Result:    Result{memoryTok: r.memoryTok, position: r.position},
		onDisk:    true,
		diskRowID: diskRowID,
	}
	r.ScanResp.Complete = isScanComplete
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
	// The Result is currently on disk, so we have to position the iterator at
	// the corresponding rowID first.
	if b.disk.iter == nil {
		b.disk.iter = b.disk.container.NewIterator(ctx)
		b.disk.iter.Rewind()
	}
	if r.diskRowID < b.disk.iterRowID {
		b.disk.iter.Rewind()
		b.disk.iterRowID = 0
	}
	for b.disk.iterRowID < r.diskRowID {
		b.disk.iter.Next()
		b.disk.iterRowID++
	}
	// Now we take the row representing the Result and deserialize it into r.
	serialized, err := b.disk.iter.Row()
	if err != nil {
		return Result{}, 0, err
	}
	if err = deserialize(&r.Result, serialized, &b.disk.alloc); err != nil {
		return Result{}, 0, err
	}
	return r.Result, r.memoryTok.toRelease, err
}

// inOrderResultsBufferSpillTypeSchema is the type schema of a single Result
// that is spilled to disk.
//
// It contains all the information except for 'ScanResp.Complete', 'memoryTok',
// and 'position' fields which are kept in-memory (because they are allocated in
// inOrderBufferedResult.Result anyway).
var inOrderResultsBufferSpillTypeSchema = []*types.T{
	types.Bool, // isGet
	// GetResp.Value:
	//	RawBytes []byte
	//	Timestamp hlc.Timestamp
	//	  WallTime int64
	//	  Logical int32
	//	  Synthetic bool
	types.Bytes, types.Int, types.Int, types.Bool,
	// ScanResp:
	//  BatchResponses [][]byte
	types.BytesArray,
	types.IntArray, // EnqueueKeysSatisfied []int
}

type resultSerializationIndex int

const (
	isGetIdx resultSerializationIndex = iota
	getRawBytesIdx
	getTSWallTimeIdx
	getTSLogicalIdx
	getTSSyntheticIdx
	scanBatchResponsesIdx
	enqueueKeysSatisfiedIdx
)

// serialize writes the serialized representation of the Result into row
// according to inOrderResultsBufferSpillTypeSchema.
func (r *Result) serialize(row rowenc.EncDatumRow, alloc *tree.DatumAlloc) error {
	row[isGetIdx] = rowenc.EncDatum{Datum: tree.MakeDBool(r.GetResp != nil)}
	if r.GetResp != nil && r.GetResp.Value != nil {
		// We have a non-empty Get response.
		v := r.GetResp.Value
		row[getRawBytesIdx] = rowenc.EncDatum{Datum: alloc.NewDBytes(tree.DBytes(v.RawBytes))}
		row[getTSWallTimeIdx] = rowenc.EncDatum{Datum: alloc.NewDInt(tree.DInt(v.Timestamp.WallTime))}
		row[getTSLogicalIdx] = rowenc.EncDatum{Datum: alloc.NewDInt(tree.DInt(v.Timestamp.Logical))}
		row[getTSSyntheticIdx] = rowenc.EncDatum{Datum: tree.MakeDBool(tree.DBool(v.Timestamp.Synthetic))}
		row[scanBatchResponsesIdx] = rowenc.EncDatum{Datum: tree.DNull}
	} else {
		row[getRawBytesIdx] = rowenc.EncDatum{Datum: tree.DNull}
		row[getTSWallTimeIdx] = rowenc.EncDatum{Datum: tree.DNull}
		row[getTSLogicalIdx] = rowenc.EncDatum{Datum: tree.DNull}
		row[getTSSyntheticIdx] = rowenc.EncDatum{Datum: tree.DNull}
		if r.GetResp != nil {
			// We have an empty Get response.
			row[scanBatchResponsesIdx] = rowenc.EncDatum{Datum: tree.DNull}
		} else {
			// We have a Scan response.
			batchResponses := tree.NewDArray(types.Bytes)
			batchResponses.Array = make(tree.Datums, 0, len(r.ScanResp.BatchResponses))
			for _, b := range r.ScanResp.BatchResponses {
				if err := batchResponses.Append(alloc.NewDBytes(tree.DBytes(b))); err != nil {
					return err
				}
			}
			row[scanBatchResponsesIdx] = rowenc.EncDatum{Datum: batchResponses}
		}
	}
	enqueueKeysSatisfied := tree.NewDArray(types.Int)
	enqueueKeysSatisfied.Array = make(tree.Datums, 0, len(r.EnqueueKeysSatisfied))
	for _, k := range r.EnqueueKeysSatisfied {
		if err := enqueueKeysSatisfied.Append(alloc.NewDInt(tree.DInt(k))); err != nil {
			return err
		}
	}
	row[enqueueKeysSatisfiedIdx] = rowenc.EncDatum{Datum: enqueueKeysSatisfied}
	return nil
}

// deserialize updates r in-place based on row which contains the serialized
// state of the Result according to inOrderResultsBufferSpillTypeSchema.
//
// 'ScanResp.Complete', 'memoryTok', and 'position' fields are left unchanged
// since those aren't serialized.
func deserialize(r *Result, row rowenc.EncDatumRow, alloc *tree.DatumAlloc) error {
	for i := range row {
		if err := row[i].EnsureDecoded(inOrderResultsBufferSpillTypeSchema[i], alloc); err != nil {
			return err
		}
	}
	if isGet := tree.MustBeDBool(row[isGetIdx].Datum); isGet {
		r.GetResp = &roachpb.GetResponse{}
		if row[getRawBytesIdx].Datum != tree.DNull {
			r.GetResp.Value = &roachpb.Value{
				RawBytes: []byte(tree.MustBeDBytes(row[getRawBytesIdx].Datum)),
				Timestamp: hlc.Timestamp{
					WallTime:  int64(tree.MustBeDInt(row[getTSWallTimeIdx].Datum)),
					Logical:   int32(tree.MustBeDInt(row[getTSLogicalIdx].Datum)),
					Synthetic: bool(tree.MustBeDBool(row[getTSSyntheticIdx].Datum)),
				},
			}
		}
	} else {
		r.ScanResp.ScanResponse = &roachpb.ScanResponse{}
		batchResponses := tree.MustBeDArray(row[scanBatchResponsesIdx].Datum)
		r.ScanResp.ScanResponse.BatchResponses = make([][]byte, batchResponses.Len())
		for i := range batchResponses.Array {
			r.ScanResp.ScanResponse.BatchResponses[i] = []byte(tree.MustBeDBytes(batchResponses.Array[i]))
		}
	}
	enqueueKeysSatisfied := tree.MustBeDArray(row[enqueueKeysSatisfiedIdx].Datum)
	r.EnqueueKeysSatisfied = make([]int, enqueueKeysSatisfied.Len())
	for i := range enqueueKeysSatisfied.Array {
		r.EnqueueKeysSatisfied[i] = int(tree.MustBeDInt(enqueueKeysSatisfied.Array[i]))
	}
	return nil
}
