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
	"fmt"
	"sort"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// singleRangeBatch contains parts of the originally enqueued requests that have
// been truncated to be within a single range. All requests within the
// singleRangeBatch will be issued as a single BatchRequest.
// TODO(yuzefovich): perform memory accounting for slices other than reqs in
// singleRangeBatch.
type singleRangeBatch struct {
	reqs []roachpb.RequestUnion
	// reqsKeys stores the start key of the corresponding request in reqs. It is
	// only set prior to sorting reqs within this object (currently, this is
	// done only in the OutOfOrder mode for the original requests - resume
	// requests don't set this), and the reference is niled out right after
	// sorting is complete. Thus, this slice doesn't have to be accounted for.
	reqsKeys []roachpb.Key
	// positions is a 1-to-1 mapping with reqs to indicate which ordinal among
	// the originally enqueued requests a particular reqs[i] corresponds to. In
	// other words, if reqs[i] is (or a part of) enqueuedReqs[j], then
	// positions[i] = j.
	//
	// In the InOrder mode, positions[0] is treated as the priority of this
	// singleRangeBatch where the smaller the value is, the sooner the Result
	// will be needed, so batches with the smallest priority value have the
	// highest "urgency". We look specifically at the 0th position because, by
	// construction, values in positions slice are increasing.
	// TODO(yuzefovich): this might need to be [][]int when non-unique requests
	// are supported.
	positions []int
	// subRequestIdx, if non-nil, is a 1-to-1 mapping with positions which
	// indicates the ordinal of the corresponding reqs[i] among all sub-requests
	// that comprise a single originally enqueued Scan request. This ordinal
	// allows us to maintain the order of these sub-requests, each going to a
	// different range. If reqs[i] is a Get request, then subRequestIdx[i] is 0.
	//
	// Consider the following example: original Scan request is Scan(b, f), and
	// we have three ranges: [a, c), [c, e), [e, g). In Streamer.Enqueue, the
	// original Scan is broken down into three single-range Scan requests:
	//   singleRangeReq[0]:
	//     reqs          = [Scan(b, c)]
	//     positions     = [0]
	//     subRequestIdx = [0]
	//   singleRangeReq[1]:
	//     reqs          = [Scan(c, e)]
	//     positions     = [0]
	//     subRequestIdx = [1]
	//   singleRangeReq[2]:
	//     reqs          = [Scan(e, f)]
	//     positions     = [0]
	//     subRequestIdx = [2]
	// Note that positions values are the same (indicating that each
	// single-range request is a part of the same original multi-range request),
	// but values of subRequestIdx are different - they will allow us to order
	// the responses to these single-range requests (which might come back in
	// any order) correctly.
	//
	// subRequestIdx is only allocated in InOrder mode when
	// Hints.SingleRowLookup is false and some Scan requests were enqueued.
	subRequestIdx []int32
	// reqsReservedBytes tracks the memory reservation against the budget for
	// the memory usage of reqs.
	reqsReservedBytes int64
	// minTargetBytes, if positive, indicates the minimum TargetBytes limit that
	// this singleRangeBatch should be sent with in order for the response to
	// not be empty. Note that TargetBytes of at least minTargetBytes is
	// necessary but might not be sufficient for the response to be non-empty.
	minTargetBytes int64
}

var _ sort.Interface = &singleRangeBatch{}

func (r *singleRangeBatch) Len() int {
	return len(r.reqs)
}

func (r *singleRangeBatch) Swap(i, j int) {
	r.reqs[i], r.reqs[j] = r.reqs[j], r.reqs[i]
	if r.reqsKeys != nil {
		r.reqsKeys[i], r.reqsKeys[j] = r.reqsKeys[j], r.reqsKeys[i]
	}
	r.positions[i], r.positions[j] = r.positions[j], r.positions[i]
	if r.subRequestIdx != nil {
		r.subRequestIdx[i], r.subRequestIdx[j] = r.subRequestIdx[j], r.subRequestIdx[i]
	}
}

// Less returns true if r.reqs[i]'s key comes before r.reqs[j]'s key.
func (r *singleRangeBatch) Less(i, j int) bool {
	return r.reqsKeys[i].Compare(r.reqsKeys[j]) < 0
}

// priority returns the priority value of this batch.
//
// It is invalid to call this method on a batch with no requests.
func (r singleRangeBatch) priority() int {
	return r.positions[0]
}

// subPriority returns the "sub-priority" value of this batch that should be
// compared when two batches have the same priority value.
//
// It is invalid to call this method on a batch with no requests.
func (r singleRangeBatch) subPriority() int32 {
	if r.subRequestIdx == nil {
		return 0
	}
	return r.subRequestIdx[0]
}

func reqsToString(reqs []singleRangeBatch) string {
	result := "requests for positions "
	for i, r := range reqs {
		if i > 0 {
			result += ", "
		}
		result += fmt.Sprintf("%v", r.positions)
	}
	return result
}

// requestsProvider encapsulates the logic of supplying the requests to serve in
// the Streamer. The implementations are concurrency safe and have its own
// mutex, separate from the Streamer's and the budget's ones, so the ordering of
// locking is totally independent.
type requestsProvider interface {
	///////////////////////////////////////////////////////////////////////////
	//                                                                       //
	//    Methods that should be called by the Streamer's user goroutine.    //
	//                                                                       //
	///////////////////////////////////////////////////////////////////////////

	// enqueue adds many requests into the provider. The lock of the provider
	// must not be already held. If there is a goroutine blocked in
	// waitLocked(), it is woken up. enqueue panics if the provider already
	// contains some requests.
	enqueue([]singleRangeBatch)
	// close closes the requests provider. If there is a goroutine blocked in
	// waitLocked(), it is woken up.
	close()

	///////////////////////////////////////////////////////////////////////////
	//                                                                       //
	//            Methods that should be called by the goroutines            //
	//            evaluating the requests asynchronously.                    //
	//                                                                       //
	///////////////////////////////////////////////////////////////////////////

	// add adds a single request into the provider. The lock of the provider
	// must not be already held. If there is a goroutine blocked in
	// waitLocked(), it is woken up.
	add(singleRangeBatch)

	///////////////////////////////////////////////////////////////////////////
	//                                                                       //
	//   Methods that should be called by the worker coordinator goroutine.  //
	//                                                                       //
	///////////////////////////////////////////////////////////////////////////

	Lock()
	Unlock()
	// waitLocked blocks until there is at least one request to serve or the
	// provider is closed. The lock of the provider must be already held, will
	// be unlocked atomically before blocking and will be re-locked once a
	// request is added (i.e. the behavior similar to sync.Cond.Wait).
	waitLocked()
	// emptyLocked returns true if there are no requests to serve at the moment.
	// The lock of the provider must be already held.
	emptyLocked() bool
	// firstLocked returns the next request to serve. In OutOfOrder mode, the
	// request is arbitrary, in InOrder mode, the request is the current
	// head-of-the-line. The lock of the provider must be already held. Panics
	// if there are no requests.
	firstLocked() singleRangeBatch
	// removeFirstLocked removes the next request to serve (returned by
	// firstLocked) from the provider. The lock of the provider must be already
	// held. Panics if there are no requests.
	removeFirstLocked()
}

type requestsProviderBase struct {
	syncutil.Mutex
	// hasWork is used by the requestsProvider to block until some requests are
	// added to be served.
	hasWork *sync.Cond
	// requests contains all single-range sub-requests that have yet to be
	// served.
	requests []singleRangeBatch
	// done is set to true once the Streamer is Close()'d.
	done bool
}

func (b *requestsProviderBase) init() {
	b.hasWork = sync.NewCond(&b.Mutex)
}

func (b *requestsProviderBase) waitLocked() {
	b.Mutex.AssertHeld()
	if b.done {
		// Don't wait if we're done.
		return
	}
	b.hasWork.Wait()
}

func (b *requestsProviderBase) emptyLocked() bool {
	b.Mutex.AssertHeld()
	return len(b.requests) == 0
}

func (b *requestsProviderBase) close() {
	b.Lock()
	defer b.Unlock()
	b.done = true
	b.hasWork.Signal()
}

// outOfOrderRequestsProvider is a requestProvider that returns requests in an
// arbitrary order (namely in the same order as the requests are enqueued and
// added).
type outOfOrderRequestsProvider struct {
	*requestsProviderBase
}

var _ requestsProvider = &outOfOrderRequestsProvider{}

func newOutOfOrderRequestsProvider() requestsProvider {
	p := outOfOrderRequestsProvider{requestsProviderBase: &requestsProviderBase{}}
	p.init()
	return &p
}

func (p *outOfOrderRequestsProvider) enqueue(requests []singleRangeBatch) {
	p.Lock()
	defer p.Unlock()
	if len(p.requests) > 0 {
		panic(errors.AssertionFailedf("outOfOrderRequestsProvider has old requests in enqueue"))
	}
	p.requests = requests
	p.hasWork.Signal()
}

func (p *outOfOrderRequestsProvider) add(request singleRangeBatch) {
	p.Lock()
	defer p.Unlock()
	p.requests = append(p.requests, request)
	p.hasWork.Signal()
}

func (p *outOfOrderRequestsProvider) firstLocked() singleRangeBatch {
	p.Mutex.AssertHeld()
	if len(p.requests) == 0 {
		panic(errors.AssertionFailedf("firstLocked called when requestsProvider is empty"))
	}
	return p.requests[0]
}

func (p *outOfOrderRequestsProvider) removeFirstLocked() {
	p.Mutex.AssertHeld()
	if len(p.requests) == 0 {
		panic(errors.AssertionFailedf("removeFirstLocked called when requestsProvider is empty"))
	}
	p.requests = p.requests[1:]
}

// inOrderRequestsProvider is a requestProvider that maintains a min heap of all
// requests according to the priority values (the smaller the priority value is,
// the higher actual priority of fulfilling the corresponding request).
type inOrderRequestsProvider struct {
	*requestsProviderBase
}

var _ requestsProvider = &inOrderRequestsProvider{}
var _ heap.Interface = &inOrderRequestsProvider{}

func newInOrderRequestsProvider() requestsProvider {
	p := inOrderRequestsProvider{requestsProviderBase: &requestsProviderBase{}}
	p.init()
	return &p
}

func (p *inOrderRequestsProvider) Len() int {
	return len(p.requests)
}

func (p *inOrderRequestsProvider) Less(i, j int) bool {
	rI, rJ := p.requests[i], p.requests[j]
	if buildutil.CrdbTestBuild {
		if rI.priority() == rJ.priority() {
			subI, subJ := rI.subRequestIdx, rJ.subRequestIdx
			if (subI != nil && subJ == nil) || (subI == nil && subJ != nil) {
				panic(errors.AssertionFailedf(
					"unexpectedly only one subRequestIdx is non-nil when priorities are the same",
				))
			}
		}
	}
	return rI.priority() < rJ.priority() ||
		(rI.priority() == rJ.priority() && rI.subPriority() < rJ.subPriority())
}

func (p *inOrderRequestsProvider) Swap(i, j int) {
	p.requests[i], p.requests[j] = p.requests[j], p.requests[i]
}

func (p *inOrderRequestsProvider) Push(x interface{}) {
	p.requests = append(p.requests, x.(singleRangeBatch))
}

func (p *inOrderRequestsProvider) Pop() interface{} {
	x := p.requests[len(p.requests)-1]
	p.requests = p.requests[:len(p.requests)-1]
	return x
}

func (p *inOrderRequestsProvider) enqueue(requests []singleRangeBatch) {
	p.Lock()
	defer p.Unlock()
	if len(p.requests) > 0 {
		panic(errors.AssertionFailedf("inOrderRequestsProvider has old requests in enqueue"))
	}
	p.requests = requests
	heap.Init(p)
	p.hasWork.Signal()
}

func (p *inOrderRequestsProvider) add(request singleRangeBatch) {
	p.Lock()
	defer p.Unlock()
	if debug {
		fmt.Printf("adding a request for positions %v to be served, minTargetBytes=%d\n", request.positions, request.minTargetBytes)
	}
	heap.Push(p, request)
	p.hasWork.Signal()
}

func (p *inOrderRequestsProvider) firstLocked() singleRangeBatch {
	p.Mutex.AssertHeld()
	if len(p.requests) == 0 {
		panic(errors.AssertionFailedf("firstLocked called when requestsProvider is empty"))
	}
	return p.requests[0]
}

func (p *inOrderRequestsProvider) removeFirstLocked() {
	p.Mutex.AssertHeld()
	if len(p.requests) == 0 {
		panic(errors.AssertionFailedf("removeFirstLocked called when requestsProvider is empty"))
	}
	heap.Remove(p, 0)
}
