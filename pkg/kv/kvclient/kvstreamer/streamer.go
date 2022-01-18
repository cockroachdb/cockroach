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
	"context"
	"runtime"
	"sort"
	"sync"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// OperationMode describes the mode of operation of the Streamer.
type OperationMode int

const (
	_ OperationMode = iota
	// InOrder is the mode of operation in which the results are delivered in
	// the order in which the requests were handed off to the Streamer. This
	// mode forces the Streamer to buffer the results it produces through its
	// internal parallel execution of the requests. Since the results of the
	// concurrent requests can come in an arbitrary order, they are buffered and
	// might end up being dropped (resulting in wasted/duplicate work) to make
	// space for the results at the front of the line. This would occur when the
	// budget limitBytes is reached and the size estimates that lead to too much
	// concurrency in the execution were wrong.
	InOrder
	// OutOfOrder is the mode of operation in which the results are delivered in
	// the order in which they're produced. The caller will use the keys field
	// of each Result to associate it with the corresponding requests. This mode
	// of operation lets the Streamer reuse the memory budget as quickly as
	// possible.
	OutOfOrder
)

// Remove an unused warning for now.
// TODO(yuzefovich): remove this when supported.
var _ = InOrder

// Result describes the result of performing a single KV request.
//
// The recipient of the Result is required to call Release() when the Result is
// not in use any more so that its memory is returned to the Streamer's budget.
type Result struct {
	// GetResp and ScanResp represent the response to a request. Only one of the
	// two will be populated.
	//
	// The responses are to be considered immutable; the Streamer might hold on
	// to the respective memory. Calling Result.Release() tells the Streamer
	// that the response is no longer needed.
	GetResp *roachpb.GetResponse
	// ScanResp can contain a partial response to a ScanRequest (when Complete
	// is false). In that case, there will be a further result with the
	// continuation; that result will use the same Key. Notably, SQL rows will
	// never be split across multiple results.
	ScanResp struct {
		*roachpb.ScanResponse
		// If the Result represents a scan result, Complete indicates whether
		// this is the last response for the respective scan, or if there are
		// more responses to come. In any case, ScanResp never contains partial
		// rows (i.e. a single row is never split into different Results).
		//
		// When running in InOrder mode, Results for a single scan will be
		// delivered in key order (in addition to results for different scans
		// being delivered in request order). When running in OutOfOrder mode,
		// Results for a single scan can be delivered out of key order (in
		// addition to results for different scans being delivered out of
		// request order).
		Complete bool
	}
	// EnqueueKeysSatisfied identifies the requests that this Result satisfies.
	// In OutOfOrder mode, a single Result can satisfy multiple identical
	// requests. In InOrder mode a Result can only satisfy multiple consecutive
	// requests.
	EnqueueKeysSatisfied []int
	// memoryTok describes the memory reservation of this Result that needs to
	// be released back to the budget when the Result is Release()'d.
	memoryTok struct {
		budget    *budget
		toRelease int64
	}
	// position tracks the ordinal among all originally enqueued requests that
	// this result satisfies. See singleRangeBatch.positions for more details.
	//
	// If Streamer.Enqueue() was called with nil enqueueKeys argument, then
	// EnqueueKeysSatisfied will exactly contain position; if non-nil
	// enqueueKeys argument was passed, then position is used as an ordinal to
	// lookup into enqueueKeys to populate EnqueueKeysSatisfied.
	// TODO(yuzefovich): this might need to be []int when non-unique requests
	// are supported.
	position int
}

// Hints provides different hints to the Streamer for optimization purposes.
type Hints struct {
	// UniqueRequests tells the Streamer that the requests will be unique. As
	// such, there's no point in de-duping them or caching results.
	UniqueRequests bool
}

// Release needs to be called by the recipient of the Result exactly once when
// this Result is not needed any more. If this was the last (or only) reference
// to this Result, the memory used by this Result is made available in the
// Streamer's budget.
//
// Internally, Results are refcounted. Multiple Results referencing the same
// GetResp/ScanResp can be returned from separate `GetResults()` calls, and the
// Streamer internally does buffering and caching of Results - which also
// contributes to the refcounts.
func (r Result) Release(ctx context.Context) {
	if r.memoryTok.budget != nil {
		r.memoryTok.budget.release(ctx, r.memoryTok.toRelease)
	}
}

// Streamer provides a streaming oriented API for reading from the KV layer. At
// the moment the Streamer only works when SQL rows are comprised of a single KV
// (i.e. a single column family).
// TODO(yuzefovich): lift the restriction on a single column family once KV is
// updated so that rows are never split across different BatchResponses when
// TargetBytes limitBytes is exceeded.
//
// The example usage is roughly as follows:
//
//  s := NewStreamer(...)
//  s.Init(OperationMode, Hints)
//  ...
//  for needMoreKVs {
//    // Check whether there are results to the previously enqueued requests.
//    // This will block if no results are available, but there are some
//    // enqueued requests.
//    results, err := s.GetResults(ctx)
//    // err check
//    ...
//    if len(results) > 0 {
//      processResults(results)
//      // return to the client
//      ...
//      // when results are no longer needed, Release() them
//    }
//    // All previously enqueued requests have already been responded to.
//    if moreRequestsToEnqueue {
//      err := s.Enqueue(ctx, requests, enqueueKeys)
//      // err check
//      ...
//    } else {
//      // done
//      ...
//    }
//  }
//  ...
//  s.Close()
//
// The Streamer builds on top of the BatchRequest API provided by the DistSender
// and aims to allow for executing the requests in parallel (to improve the
// performance) while setting the memory limits on those requests (for stability
// purposes).
//
// The parallelism is achieved by splitting the incoming requests into
// single-range batches where each such batch will hit a fast-path in the
// DistSender (unless there have been changes to range boundaries). Since these
// batches are executed concurrently, the LeafTxns are used.
//
// The memory limit handling is achieved by the Streamer guessing the size of
// the response for each request and setting TargetBytes accordingly. The
// concurrency of the Streamer is limited by its memory limit.
//
// The Streamer additionally utilizes different optimizations to improve the
// performance:
// - when possible, sorting requests in key order to take advantage of low-level
// Pebble locality optimizations
// - when necessary, buffering the responses received out of order
// - when necessary, caching the responses to short-circuit repeated lookups.
// TODO(yuzefovich): add an optimization of transparent refreshes when there is
// a single Streamer in the local flow.
// TODO(yuzefovich): support pipelining of Enqueue and GetResults calls.
type Streamer struct {
	distSender *kvcoord.DistSender
	stopper    *stop.Stopper

	mode   OperationMode
	hints  Hints
	budget *budget

	coordinator          workerCoordinator
	coordinatorStarted   bool
	coordinatorCtxCancel context.CancelFunc

	waitGroup sync.WaitGroup

	enqueueKeys []int

	// waitForResults is used to block GetResults() call until some results are
	// available.
	waitForResults chan struct{}

	mu struct {
		// If the budget's mutex also needs to be locked, the budget's mutex
		// must be acquired first.
		syncutil.Mutex

		avgResponseEstimator avgResponseEstimator

		// requestsToServe contains all single-range sub-requests that have yet
		// to be served.
		// TODO(yuzefovich): consider using ring.Buffer instead of a slice.
		requestsToServe []singleRangeBatch

		// numRangesLeftPerScanRequest tracks how many ranges a particular
		// originally enqueued ScanRequest touches, but scanning of those ranges
		// isn't complete. It is allocated lazily when the first ScanRequest is
		// encountered in Enqueue.
		numRangesLeftPerScanRequest []int

		// numEnqueuedRequests tracks the number of the originally enqueued
		// requests.
		numEnqueuedRequests int

		// numCompleteRequests tracks the number of the originally enqueued
		// requests that have already been completed.
		numCompleteRequests int

		// numRequestsInFlight tracks the number of single-range batches that
		// are currently being served asynchronously (i.e. those that have
		// already left requestsToServe queue, but for which we haven't received
		// the results yet).
		// TODO(yuzefovich): check whether the contention on mu when accessing
		// this field is sufficient to justify pulling it out into an atomic.
		numRequestsInFlight int

		// results are the results of already completed requests that haven't
		// been returned by GetResults() yet.
		results []Result
		err     error
	}
}

// streamerConcurrencyLimit is an upper bound on the number of asynchronous
// requests that a single Streamer can have in flight. The default value for
// this setting is chosen arbitrarily as 1/8th of the default value for the
// senderConcurrencyLimit.
var streamerConcurrencyLimit = settings.RegisterIntSetting(
	settings.TenantWritable,
	"kv.streamer.concurrency_limit",
	"maximum number of asynchronous requests by a single streamer",
	max(128, int64(8*runtime.GOMAXPROCS(0))),
	settings.NonNegativeInt,
)

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// NewStreamer creates a new Streamer.
//
// limitBytes determines the maximum amount of memory this Streamer is allowed
// to use (i.e. it'll be used lazily, as needed). The more memory it has, the
// higher its internal concurrency and throughput.
//
// acc should be bound to an unlimited memory monitor, and the Streamer itself
// is responsible for staying under the limitBytes.
//
// The Streamer takes ownership of the memory account, and the caller is allowed
// to interact with the account only after canceling the Streamer (because
// memory accounts are not thread-safe).
func NewStreamer(
	distSender *kvcoord.DistSender,
	stopper *stop.Stopper,
	txn *kv.Txn,
	st *cluster.Settings,
	lockWaitPolicy lock.WaitPolicy,
	limitBytes int64,
	acc *mon.BoundAccount,
) *Streamer {
	s := &Streamer{
		distSender: distSender,
		stopper:    stopper,
		budget:     newBudget(acc, limitBytes),
	}
	s.coordinator = workerCoordinator{
		s:                      s,
		txn:                    txn,
		lockWaitPolicy:         lockWaitPolicy,
		requestAdmissionHeader: txn.AdmissionHeader(),
		responseAdmissionQ:     txn.DB().SQLKVResponseAdmissionQ,
	}
	// TODO(yuzefovich): consider lazily allocating this IntPool only when
	// enqueued requests span multiple batches.
	s.coordinator.asyncSem = quotapool.NewIntPool(
		"single Streamer async concurrency",
		uint64(streamerConcurrencyLimit.Get(&st.SV)),
	)
	s.coordinator.mu.hasWork = sync.NewCond(&s.coordinator.mu)
	streamerConcurrencyLimit.SetOnChange(&st.SV, func(ctx context.Context) {
		s.coordinator.asyncSem.UpdateCapacity(uint64(streamerConcurrencyLimit.Get(&st.SV)))
	})
	stopper.AddCloser(s.coordinator.asyncSem.Closer("stopper"))
	return s
}

// Init initializes the Streamer.
//
// OperationMode controls the order in which results are delivered to the
// client. When possible, prefer OutOfOrder mode.
//
// Hints can be used to hint the aggressiveness of the caching policy. In
// particular, it can be used to disable caching when the client knows that all
// looked-up keys are unique (e.g. in the case of an index-join).
func (s *Streamer) Init(mode OperationMode, hints Hints) {
	if mode != OutOfOrder {
		panic(errors.AssertionFailedf("only OutOfOrder mode is supported"))
	}
	s.mode = mode
	if !hints.UniqueRequests {
		panic(errors.AssertionFailedf("only unique requests are currently supported"))
	}
	s.hints = hints
	s.waitForResults = make(chan struct{}, 1)
}

// Enqueue dispatches multiple requests for execution. Results are delivered
// through the GetResults call. If enqueueKeys is not nil, it needs to contain
// one ID for each request; responses will reference that ID so that the client
// can associate them to the requests. If enqueueKeys is nil, then the responses
// will reference the ordinals of the corresponding requests among reqs.
//
// Multiple requests can specify the same key. In this case, their respective
// responses will also reference the same key. This is useful, for example, for
// "range-based lookup joins" where multiple spans are read in the context of
// the same input-side row (see multiSpanGenerator implementation of
// rowexec.joinReaderSpanGenerator interface for more details).
//
// The Streamer takes over the given requests, will perform the memory
// accounting against its budget and might modify the requests in place.
//
// In InOrder operation mode, responses will be delivered in reqs order.
//
// It is the caller's responsibility to ensure that the memory footprint of reqs
// (i.e. roachpb.Spans inside of the requests) is reasonable. Enqueue will
// return an error if that footprint exceeds the Streamer's limitBytes. The
// exception is made only when a single request is enqueued in order to allow
// the caller to proceed when the key to lookup is arbitrarily large. As a rule
// of thumb though, the footprint of reqs should be on the order of MBs, and not
// tens of MBs.
//
// Currently, enqueuing new requests while there are still requests in progress
// from the previous invocation is prohibited.
// TODO(yuzefovich): lift this restriction and introduce the pipelining.
func (s *Streamer) Enqueue(
	ctx context.Context, reqs []roachpb.RequestUnion, enqueueKeys []int,
) (retErr error) {
	if !s.coordinatorStarted {
		var coordinatorCtx context.Context
		coordinatorCtx, s.coordinatorCtxCancel = context.WithCancel(ctx)
		s.waitGroup.Add(1)
		if err := s.stopper.RunAsyncTask(coordinatorCtx, "streamer-coordinator", s.coordinator.mainLoop); err != nil {
			// The new goroutine wasn't spun up, so mainLoop won't get executed
			// and we have to decrement the wait group ourselves.
			s.waitGroup.Done()
			return err
		}
		s.coordinatorStarted = true
	}

	// TODO(yuzefovich): we might want to have more fine-grained lock
	// acquisitions once pipelining is implemented.
	s.mu.Lock()
	defer func() {
		// We assume that the Streamer's mutex is held when Enqueue returns.
		s.mu.AssertHeld()
		if buildutil.CrdbTestBuild {
			if s.mu.err != nil {
				panic(errors.NewAssertionErrorWithWrappedErrf(s.mu.err, "s.mu.err is non-nil"))
			}
		}
		// Set the error (if present) so that mainLoop of the worker coordinator
		// exits as soon as possible, without issuing any requests. Note that
		// s.mu.err couldn't have already been set by the worker coordinator or
		// the asynchronous requests because the worker coordinator starts
		// issuing the requests only after Enqueue() returns.
		s.mu.err = retErr
		s.mu.Unlock()
	}()

	if enqueueKeys != nil && len(enqueueKeys) != len(reqs) {
		return errors.AssertionFailedf("invalid enqueueKeys: len(reqs) = %d, len(enqueueKeys) = %d", len(reqs), len(enqueueKeys))
	}
	s.enqueueKeys = enqueueKeys

	if s.mu.numEnqueuedRequests != s.mu.numCompleteRequests {
		return errors.AssertionFailedf("Enqueue is called before the previous requests have been completed")
	}
	if len(s.mu.results) > 0 {
		return errors.AssertionFailedf("Enqueue is called before the results of the previous requests have been retrieved")
	}

	s.mu.numEnqueuedRequests = len(reqs)
	s.mu.numCompleteRequests = 0

	// The minimal key range encompassing all requests contained within.
	// Local addressing has already been resolved.
	rs, err := keys.Range(reqs)
	if err != nil {
		return err
	}

	// Divide the given requests into single-range batches that are added to
	// requestsToServe, and the worker coordinator will then pick those batches
	// up to execute asynchronously.
	var totalReqsMemUsage int64
	// TODO(yuzefovich): in InOrder mode we need to treat the head-of-the-line
	// request differently.
	seekKey := rs.Key
	const scanDir = kvcoord.Ascending
	ri := kvcoord.MakeRangeIterator(s.distSender)
	ri.Seek(ctx, seekKey, scanDir)
	if !ri.Valid() {
		return ri.Error()
	}
	firstScanRequest := true
	for ; ri.Valid(); ri.Seek(ctx, seekKey, scanDir) {
		// Truncate the request span to the current range.
		singleRangeSpan, err := rs.Intersect(ri.Token().Desc())
		if err != nil {
			return err
		}
		// Find all requests that touch the current range.
		singleRangeReqs, positions, err := kvcoord.Truncate(reqs, singleRangeSpan)
		if err != nil {
			return err
		}
		for _, pos := range positions {
			if _, isScan := reqs[pos].GetInner().(*roachpb.ScanRequest); isScan {
				if firstScanRequest {
					// We have some ScanRequests, so we have to set up
					// numRangesLeftPerScanRequest.
					if cap(s.mu.numRangesLeftPerScanRequest) < len(reqs) {
						s.mu.numRangesLeftPerScanRequest = make([]int, len(reqs))
					} else {
						// We can reuse numRangesLeftPerScanRequest allocated on
						// the previous call to Enqueue after we zero it out.
						s.mu.numRangesLeftPerScanRequest = s.mu.numRangesLeftPerScanRequest[:len(reqs)]
						for n := 0; n < len(s.mu.numRangesLeftPerScanRequest); {
							n += copy(s.mu.numRangesLeftPerScanRequest[n:], zeroIntSlice)
						}
					}
				}
				s.mu.numRangesLeftPerScanRequest[pos]++
				firstScanRequest = false
			}
		}

		// TODO(yuzefovich): perform the de-duplication here.
		//if !s.hints.UniqueRequests {
		//}

		r := singleRangeBatch{
			reqs:              singleRangeReqs,
			positions:         positions,
			reqsReservedBytes: requestsMemUsage(singleRangeReqs),
		}
		totalReqsMemUsage += r.reqsReservedBytes

		if s.mode == OutOfOrder {
			// Sort all single-range requests to be in the key order.
			sort.Sort(&r)
		}

		s.mu.requestsToServe = append(s.mu.requestsToServe, r)

		// Determine next seek key, taking potentially sparse requests into
		// consideration.
		//
		// In next iteration, query next range.
		// It's important that we use the EndKey of the current descriptor
		// as opposed to the StartKey of the next one: if the former is stale,
		// it's possible that the next range has since merged the subsequent
		// one, and unless both descriptors are stale, the next descriptor's
		// StartKey would move us to the beginning of the current range,
		// resulting in a duplicate scan.
		seekKey, err = kvcoord.Next(reqs, ri.Desc().EndKey)
		rs.Key = seekKey
		if err != nil {
			return err
		}
	}

	// Release the Streamer's mutex so that there is no overlap with the
	// budget's mutex - the budget's mutex needs to be acquired first in order
	// to eliminate a potential deadlock.
	s.mu.Unlock()

	// Account for the memory used by all the requests. We allow the budget to
	// go into debt iff a single request was enqueued. This is needed to support
	// the case of arbitrarily large keys - the caller is expected to produce
	// requests with such cases one at a time.
	allowDebt := len(reqs) == 1
	err = s.budget.consume(ctx, totalReqsMemUsage, allowDebt)

	// Now acquire the Streamer's mutex since the defer above assumes it is
	// being held when this function returns.
	s.mu.Lock()

	if err != nil {
		return err
	}

	// TODO(yuzefovich): it might be better to notify the coordinator once
	// one singleRangeBatch object has been appended to s.mu.requestsToServe.
	s.coordinator.mu.hasWork.Signal()
	return nil
}

// GetResults blocks until at least one result is available. If the operation
// mode is OutOfOrder, any result will do, and the caller is expected to examine
// Result.EnqueueKeysSatisfied to understand which request the result
// corresponds to. For InOrder, only head-of-line results will do. Zero-length
// result slice is returned once all enqueued requests have been responded to.
func (s *Streamer) GetResults(ctx context.Context) ([]Result, error) {
	s.mu.Lock()
	results := s.mu.results
	err := s.mu.err
	s.mu.results = nil
	allComplete := s.mu.numCompleteRequests == s.mu.numEnqueuedRequests
	// Non-blockingly clear the waitForResults channel in case we've just picked
	// up some results. We do so while holding the mutex so that new results
	// aren't appended.
	select {
	case <-s.waitForResults:
	default:
	}
	s.mu.Unlock()

	if len(results) > 0 || allComplete || err != nil {
		return results, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.waitForResults:
		s.mu.Lock()
		results = s.mu.results
		err = s.mu.err
		s.mu.results = nil
		s.mu.Unlock()
		return results, err
	}
}

// notifyGetResultsLocked non-blockingly sends a message on waitForResults
// channel. This method should be called only while holding the lock of s.mu so
// that other results couldn't be appended which would cause us to miss the
// notification about that.
func (s *Streamer) notifyGetResultsLocked() {
	s.mu.AssertHeld()
	select {
	case s.waitForResults <- struct{}{}:
	default:
	}
}

// setError sets the error on the Streamer if no error has been set previously
// and unblocks GetResults() if needed.
//
// The mutex of s must not be already held.
func (s *Streamer) setError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.err == nil {
		s.mu.err = err
	}
	s.notifyGetResultsLocked()
}

// Close cancels all in-flight operations and releases all of the resources of
// the Streamer. It blocks until all goroutines created by the Streamer exit. No
// other calls on s are allowed after this.
func (s *Streamer) Close() {
	if s.coordinatorStarted {
		s.coordinatorCtxCancel()
		s.coordinator.mu.Lock()
		s.coordinator.mu.done = true
		// Unblock the coordinator in case it is waiting for more work.
		s.coordinator.mu.hasWork.Signal()
		s.coordinator.mu.Unlock()
	}
	s.waitGroup.Wait()
	*s = Streamer{}
}

// getNumRequestsInFlight returns the number of requests that are currently in
// flight. This method should be called without holding the lock of s.
func (s *Streamer) getNumRequestsInFlight() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.numRequestsInFlight
}

// adjustNumRequestsInFlight updates the number of requests that are currently
// in flight. This method should be called without holding the lock of s.
func (s *Streamer) adjustNumRequestsInFlight(delta int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.numRequestsInFlight += delta
}

// singleRangeBatch contains parts of the originally enqueued requests that have
// been truncated to be within a single range. All requests within the
// singleRangeBatch will be issued as a single BatchRequest.
type singleRangeBatch struct {
	reqs []roachpb.RequestUnion
	// positions is a 1-to-1 mapping with reqs to indicate which ordinal among
	// the originally enqueued requests a particular reqs[i] corresponds to. In
	// other words, if reqs[i] is (or a part of) enqueuedReqs[j], then
	// positions[i] = j.
	// TODO(yuzefovich): this might need to be [][]int when non-unique requests
	// are supported.
	positions []int
	// reqsReservedBytes tracks the memory reservation against the budget for
	// the memory usage of reqs.
	reqsReservedBytes int64
}

var _ sort.Interface = &singleRangeBatch{}

func (r *singleRangeBatch) Len() int {
	return len(r.reqs)
}

func (r *singleRangeBatch) Swap(i, j int) {
	r.reqs[i], r.reqs[j] = r.reqs[j], r.reqs[i]
	r.positions[i], r.positions[j] = r.positions[j], r.positions[i]
}

// Less returns true if r.reqs[i]'s key comes before r.reqs[j]'s key.
func (r *singleRangeBatch) Less(i, j int) bool {
	// TODO(yuzefovich): figure out whether it's worth extracting the keys when
	// constructing singleRangeBatch object.
	return r.reqs[i].GetInner().Header().Key.Compare(r.reqs[j].GetInner().Header().Key) < 0
}

type workerCoordinator struct {
	s              *Streamer
	txn            *kv.Txn
	lockWaitPolicy lock.WaitPolicy

	asyncSem *quotapool.IntPool

	// For request and response admission control.
	requestAdmissionHeader roachpb.AdmissionHeader
	responseAdmissionQ     *admission.WorkQueue

	mu struct {
		syncutil.Mutex
		hasWork *sync.Cond
		// done is set to true once the Streamer is closed meaning the worker
		// coordinator must exit.
		done bool
	}
}

// mainLoop runs throughout the lifetime of the Streamer (from the first Enqueue
// call until Cancel) and routes the single-range batches for asynchronous
// execution. This function is dividing up the Streamer's budget for each of
// those batches and won't start executing the batches if the available budget
// is insufficient. The function exits when an error is encountered by one of
// the asynchronous requests.
func (w *workerCoordinator) mainLoop(ctx context.Context) {
	defer w.s.waitGroup.Done()
	for {
		// Get next requests to serve.
		requestsToServe, avgResponseSize, shouldExit := w.getRequests()
		if shouldExit {
			return
		}
		if len(requestsToServe) == 0 {
			// If the Streamer isn't closed yet, block until there are enqueued
			// requests.
			w.mu.Lock()
			if !w.mu.done {
				w.mu.hasWork.Wait()
			}
			w.mu.Unlock()
			if ctx.Err() != nil {
				w.s.setError(ctx.Err())
				return
			}
			continue
		}

		// Now wait until there is enough budget to at least receive one full
		// response (but only if there are requests in flight - if there are
		// none, then we might have a degenerate case when a single row is
		// expected to exceed the budget).
		// TODO(yuzefovich): consider using a multiple of avgResponseSize here.
		for w.s.getNumRequestsInFlight() > 0 && w.s.budget.available() < avgResponseSize {
			select {
			case <-w.s.budget.waitCh:
			case <-ctx.Done():
				w.s.setError(ctx.Err())
				return
			}
		}

		err := w.issueRequestsForAsyncProcessing(ctx, requestsToServe, avgResponseSize)
		if err != nil {
			w.s.setError(err)
			return
		}
	}
}

// getRequests returns all currently enqueued requests to be served.
//
// A boolean that indicates whether the coordinator should exit is returned.
func (w *workerCoordinator) getRequests() (
	requestsToServe []singleRangeBatch,
	avgResponseSize int64,
	shouldExit bool,
) {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	requestsToServe = w.s.mu.requestsToServe
	avgResponseSize = w.s.mu.avgResponseEstimator.getAvgResponseSize()
	shouldExit = w.s.mu.err != nil
	return requestsToServe, avgResponseSize, shouldExit
}

// issueRequestsForAsyncProcessing iterates over the given requests and issues
// them to be served asynchronously while there is enough budget available to
// receive the responses. Once the budget is exhausted, no new requests are
// issued, the only exception is made for the case when there are no other
// requests in flight, and in that scenario, a single request will be issued.
//
// It is assumed that requestsToServe is a prefix of w.s.mu.requestsToServe
// (i.e. it is possible that some other requests have been appended to
// w.s.mu.requestsToServe after requestsToServe have been grabbed). All issued
// requests are removed from w.s.mu.requestToServe.
func (w *workerCoordinator) issueRequestsForAsyncProcessing(
	ctx context.Context, requestsToServe []singleRangeBatch, avgResponseSize int64,
) error {
	var numRequestsIssued int
	defer func() {
		w.s.mu.Lock()
		// We can just slice here since we only append to requestToServe at
		// the moment.
		w.s.mu.requestsToServe = w.s.mu.requestsToServe[numRequestsIssued:]
		w.s.mu.Unlock()
	}()
	w.s.budget.mu.Lock()
	defer w.s.budget.mu.Unlock()

	headOfLine := w.s.getNumRequestsInFlight() == 0
	var budgetIsExhausted bool
	for numRequestsIssued < len(requestsToServe) && !budgetIsExhausted {
		availableBudget := w.s.budget.limitBytes - w.s.budget.mu.acc.Used()
		if availableBudget < avgResponseSize {
			if !headOfLine {
				// We don't have enough budget available to serve this request,
				// and there are other requests in flight, so we'll wait for
				// some of them to finish.
				break
			}
			budgetIsExhausted = true
			if availableBudget < 1 {
				// The budget is already in debt, and we have no requests in
				// flight. This occurs when we have very large roachpb.Span in
				// the request. In such a case, we still want to make progress
				// by giving the smallest TargetBytes possible while asking the
				// KV layer to not return an empty response.
				availableBudget = 1
			}
		}
		singleRangeReqs := requestsToServe[numRequestsIssued]
		// Calculate what TargetBytes limit to use for the BatchRequest that
		// will be issued based on singleRangeReqs. We use the estimate to guess
		// how much memory the response will need, and we reserve this
		// estimation up front.
		//
		// Note that TargetBytes will be a strict limit on the response size
		// (except in a degenerate case for head-of-the-line request that will
		// get a very large single row in response which will exceed this
		// limit).
		targetBytes := int64(len(singleRangeReqs.reqs)) * avgResponseSize
		if targetBytes > availableBudget {
			// The estimate tells us that we don't have enough budget to receive
			// the full response; however, in order to utilize the available
			// budget fully, we can still issue this request with the truncated
			// TargetBytes value hoping to receive a partial response.
			targetBytes = availableBudget
		}
		if err := w.s.budget.consumeLocked(ctx, targetBytes, headOfLine /* allowDebt */); err != nil {
			// This error cannot be because of the budget going into debt. If
			// headOfLine is true, then we're allowing debt; otherwise, we have
			// truncated targetBytes above to not exceed availableBudget, and
			// we're holding the budget's mutex. Thus, the error indicates that
			// the root memory pool has been exhausted.
			if !headOfLine {
				// There are some requests in flight, so we'll let them finish.
				//
				// This is opportunistic behavior where we're hoping that once
				// other requests are fully processed (i.e. the corresponding
				// results are Release()'d), we'll be able to make progress on
				// this request too, without exceeding the root memory pool.
				//
				// We're not really concerned about pushing the node towards the
				// OOM situation because we're still staying within the root
				// memory pool limit (which should have some safety gap with the
				// available RAM). Furthermore, if other queries are consuming
				// all of the root memory pool limit, then the head-of-the-line
				// request will notice it and will exit accordingly.
				break
			}
			// We don't have any requests in flight, so we'll exit to be safe
			// (in order not to OOM the node). Most likely this occurs when
			// there are concurrent memory-intensive queries which this Streamer
			// has no control over.
			//
			// We could have issued this head-of-the-line request with lower
			// targetBytes value (unless it is already 1), but the fact that the
			// root memory pool is exhausted indicates that the node might be
			// overloaded already, so it seems better to not ask it to receive
			// any more responses at the moment.
			return err
		}
		w.performRequestAsync(ctx, singleRangeReqs, targetBytes, headOfLine)
		numRequestsIssued++
		headOfLine = false
	}
	return nil
}

// addRequest adds a single-range batch to be processed later.
func (w *workerCoordinator) addRequest(req singleRangeBatch) {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	w.s.mu.requestsToServe = append(w.s.mu.requestsToServe, req)
	w.mu.hasWork.Signal()
}

func (w *workerCoordinator) asyncRequestCleanup() {
	w.s.adjustNumRequestsInFlight(-1 /* delta */)
	w.s.waitGroup.Done()
}

// performRequestAsync dispatches the given single-range batch for evaluation
// asynchronously. If the batch cannot be evaluated fully (due to exhausting its
// memory limitBytes), the "resume" single-range batch will be added into
// requestsToServe, and mainLoop will pick that up to process later.
//
// targetBytes specifies the memory budget that this single-range batch should
// be issued with. targetBytes bytes have already been consumed from the budget,
// and this amount of memory is owned by the goroutine that is spun up to
// perform the request. Once the response is received, performRequestAsync
// reconciles the budget so that the actual footprint of the response is
// consumed. Each Result produced based on that response will track a part of
// the memory reservation (according to the Result's footprint) that will be
// returned back to the budget once Result.Release() is called.
//
// headOfLine indicates whether this request is the current head of the line.
// Head-of-the-line requests are treated specially in a sense that they are
// allowed to put the budget into debt. The caller is responsible for ensuring
// that there is at most one asynchronous request with headOfLine=true at all
// times.
func (w *workerCoordinator) performRequestAsync(
	ctx context.Context, req singleRangeBatch, targetBytes int64, headOfLine bool,
) {
	w.s.waitGroup.Add(1)
	w.s.adjustNumRequestsInFlight(1 /* delta */)
	if err := w.s.stopper.RunAsyncTaskEx(
		ctx,
		stop.TaskOpts{
			TaskName:   "streamer-lookup-async",
			Sem:        w.asyncSem,
			WaitForSem: true,
		},
		func(ctx context.Context) {
			defer w.asyncRequestCleanup()
			var ba roachpb.BatchRequest
			ba.Header.WaitPolicy = w.lockWaitPolicy
			ba.Header.TargetBytes = targetBytes
			ba.Header.AllowEmpty = !headOfLine
			// TODO(yuzefovich): consider setting MaxSpanRequestKeys whenever
			// applicable (#67885).
			ba.AdmissionHeader = w.requestAdmissionHeader
			// We always have some memory reserved against the memory account,
			// regardless of the value of headOfLine.
			ba.AdmissionHeader.NoMemoryReservedAtSource = false
			ba.Requests = req.reqs

			// TODO(yuzefovich): in Enqueue we split all requests into
			// single-range batches, so ideally ba touches a single range in
			// which case we hit the fast path in the DistSender. However, if
			// the range boundaries have changed after we performed the split
			// (or we had stale range cache at the time of the split), the
			// DistSender will transparently re-split ba into several
			// sub-batches that will be executed sequentially because of the
			// presence of limits. We could, instead, ask the DistSender to not
			// perform that re-splitting and return an error, then we'll rely on
			// the updated range cache to perform re-splitting ourselves. This
			// should offer some performance improvements since we'd eliminate
			// unnecessary blocking (due to sequential evaluation of sub-batches
			// by the DistSender). For the initial implementation it doesn't
			// seem important though.
			br, err := w.txn.Send(ctx, ba)
			if err != nil {
				// TODO(yuzefovich): if err is
				// ReadWithinUncertaintyIntervalError and there is only a single
				// Streamer in a single local flow, attempt to transparently
				// refresh.
				w.s.setError(err.GoError())
				return
			}

			var resumeReq singleRangeBatch
			// We will reuse the slices for the resume spans, if any.
			resumeReq.reqs = req.reqs[:0]
			resumeReq.positions = req.positions[:0]
			var results []Result
			var numCompleteGetResponses int
			// memoryFootprintBytes tracks the total memory footprint of
			// non-empty responses. This will be equal to the sum of memory
			// tokens created for all Results.
			var memoryFootprintBytes int64
			var hasNonEmptyScanResponse bool
			for i, resp := range br.Responses {
				enqueueKey := req.positions[i]
				if w.s.enqueueKeys != nil {
					enqueueKey = w.s.enqueueKeys[req.positions[i]]
				}
				reply := resp.GetInner()
				origReq := req.reqs[i]
				// Unset the original request so that we lose the reference to
				// the span.
				req.reqs[i] = roachpb.RequestUnion{}
				switch origRequest := origReq.GetInner().(type) {
				case *roachpb.GetRequest:
					get := reply.(*roachpb.GetResponse)
					if get.ResumeSpan != nil {
						// This Get wasn't completed - update the original
						// request according to the ResumeSpan and include it
						// into the batch again.
						origRequest.SetSpan(*get.ResumeSpan)
						resumeReq.reqs = append(resumeReq.reqs, origReq)
						resumeReq.positions = append(resumeReq.positions, req.positions[i])
					} else {
						// This Get was completed.
						toRelease := int64(get.Size())
						result := Result{
							GetResp: get,
							// This currently only works because all requests
							// are unique.
							EnqueueKeysSatisfied: []int{enqueueKey},
							position:             req.positions[i],
						}
						result.memoryTok.budget = w.s.budget
						result.memoryTok.toRelease = toRelease
						memoryFootprintBytes += toRelease
						results = append(results, result)
						numCompleteGetResponses++
					}

				case *roachpb.ScanRequest:
					scan := reply.(*roachpb.ScanResponse)
					resumeSpan := scan.ResumeSpan
					if len(scan.Rows) > 0 || len(scan.BatchResponses) > 0 {
						toRelease := int64(scan.Size())
						result := Result{
							// This currently only works because all requests
							// are unique.
							EnqueueKeysSatisfied: []int{enqueueKey},
							position:             req.positions[i],
						}
						result.memoryTok.budget = w.s.budget
						result.memoryTok.toRelease = toRelease
						result.ScanResp.ScanResponse = scan
						// Complete field will be set below.
						memoryFootprintBytes += toRelease
						results = append(results, result)
						hasNonEmptyScanResponse = true
					}
					if resumeSpan != nil {
						// This Scan wasn't completed - update the original
						// request according to the resumeSpan and include it
						// into the batch again.
						origRequest.SetSpan(*resumeSpan)
						resumeReq.reqs = append(resumeReq.reqs, origReq)
						resumeReq.positions = append(resumeReq.positions, req.positions[i])
					}
				}
			}

			// Now adjust the budget based on the actual memory footprint of
			// non-empty responses as well as resume spans, if any.
			respOverestimate := targetBytes - memoryFootprintBytes
			var reqsMemUsage int64
			if len(resumeReq.reqs) > 0 {
				reqsMemUsage = requestsMemUsage(resumeReq.reqs)
			}
			reqOveraccounted := req.reqsReservedBytes - reqsMemUsage
			overaccountedTotal := respOverestimate + reqOveraccounted
			if overaccountedTotal >= 0 {
				w.s.budget.release(ctx, overaccountedTotal)
			} else {
				// There is an under-accounting at the moment, so we have to
				// increase the memory reservation.
				//
				// This under-accounting can occur in a couple of edge cases:
				// 1) the estimate of the response sizes is pretty good (i.e.
				// respOverestimate is around 0), but we received many partial
				// responses with ResumeSpans that take up much more space than
				// the original requests;
				// 2) we have a single large row in the response. In this case
				// headOfLine must be true (targetBytes might be 1 or higher,
				// but not enough for that large row).
				toConsume := -overaccountedTotal
				if err := w.s.budget.consume(ctx, toConsume, headOfLine /* allowDebt */); err != nil {
					w.s.budget.release(ctx, targetBytes)
					if !headOfLine {
						// Since this is not the head of the line, we'll just
						// discard the result and add the request back to be
						// served.
						//
						// This is opportunistic behavior where we're hoping
						// that once other requests are fully processed (i.e.
						// the corresponding results are Release()'d), we'll be
						// able to make progress on this request too.
						// TODO(yuzefovich): consider updating the
						// avgResponseSize and/or storing the information about
						// the returned bytes size in req.
						w.addRequest(req)
						return
					}
					// The error indicates that the root memory pool has been
					// exhausted, so we'll exit to be safe (in order not to OOM
					// the node).
					// TODO(yuzefovich): if the response contains multiple rows,
					// consider adding the request back to be served with a note
					// to issue it with smaller targetBytes.
					w.s.setError(err)
					return
				}
			}
			// Update the resume request accordingly.
			resumeReq.reqsReservedBytes = reqsMemUsage

			// Do admission control after we've finalized the memory accounting.
			if br != nil && w.responseAdmissionQ != nil {
				responseAdmission := admission.WorkInfo{
					TenantID:   roachpb.SystemTenantID,
					Priority:   admission.WorkPriority(w.requestAdmissionHeader.Priority),
					CreateTime: w.requestAdmissionHeader.CreateTime,
				}
				if _, err := w.responseAdmissionQ.Admit(ctx, responseAdmission); err != nil {
					w.s.setError(err)
					return
				}
			}

			// If we have any results, finalize them.
			if len(results) > 0 {
				w.finalizeSingleRangeResults(
					results, memoryFootprintBytes, hasNonEmptyScanResponse,
					numCompleteGetResponses,
				)
			}

			// If we have any incomplete requests, add them back into the work
			// pool.
			if len(resumeReq.reqs) > 0 {
				w.addRequest(resumeReq)
			}
		}); err != nil {
		// The new goroutine for the request wasn't spun up, so we have to
		// perform the cleanup of this request ourselves.
		w.asyncRequestCleanup()
		w.s.setError(err)
	}
}

// finalizeSingleRangeResults "finalizes" the results of evaluation of a
// singleRangeBatch. By "finalization" we mean setting Complete field of
// ScanResp to correct value for all scan responses, updating the estimate of an
// average response size, and telling the Streamer about these results.
//
// This method assumes that results has length greater than zero.
func (w *workerCoordinator) finalizeSingleRangeResults(
	results []Result,
	actualMemoryReservation int64,
	hasNonEmptyScanResponse bool,
	numCompleteGetResponses int,
) {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()

	numCompleteResponses := numCompleteGetResponses
	// If we have non-empty scan response, it might be complete. This will be
	// the case when a scan response doesn't have a resume span and there are no
	// other scan requests in flight (involving other ranges) that are part of
	// the same original ScanRequest.
	//
	// We need to do this check as well as adding the results to be returned to
	// the client as an atomic operation so that Complete is set to true only on
	// the last partial scan response.
	if hasNonEmptyScanResponse {
		for _, r := range results {
			if r.ScanResp.ScanResponse != nil {
				if r.ScanResp.ResumeSpan == nil {
					// The scan within the range is complete.
					w.s.mu.numRangesLeftPerScanRequest[r.position]--
					if w.s.mu.numRangesLeftPerScanRequest[r.position] == 0 {
						// The scan across all ranges is now complete too.
						r.ScanResp.Complete = true
						numCompleteResponses++
					}
				} else {
					// Unset the ResumeSpan on the result in order to not
					// confuse the user of the Streamer. Non-nil resume span was
					// already included into resumeReq populated in
					// performRequestAsync.
					r.ScanResp.ResumeSpan = nil
				}
			}
		}
	}

	// Update the average response size based on this batch.
	// TODO(yuzefovich): some of the responses might be partial, yet the
	// estimator doesn't distinguish the footprint of the full response vs the
	// partial one. Think more about this.
	w.s.mu.avgResponseEstimator.update(actualMemoryReservation, int64(len(results)))
	w.s.mu.numCompleteRequests += numCompleteResponses
	// Store the results and non-blockingly notify the Streamer about them.
	w.s.mu.results = append(w.s.mu.results, results...)
	w.s.notifyGetResultsLocked()
}

var zeroIntSlice []int

func init() {
	zeroIntSlice = make([]int, 1<<10)
}

const requestUnionSliceOverhead = int64(unsafe.Sizeof([]roachpb.RequestUnion{}))

func requestsMemUsage(reqs []roachpb.RequestUnion) int64 {
	memUsage := requestUnionSliceOverhead
	// Slice up to the capacity to account for everything.
	for _, r := range reqs[:cap(reqs)] {
		memUsage += int64(r.Size())
	}
	return memUsage
}
