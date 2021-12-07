// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"context"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"

	pkgKeys "github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
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
	"github.com/dustin/go-humanize"
)

// Streamer provides a streaming oriented API for reading from the KV layer. At
// the moment the Streamer only works when rows are comprised of a single KV
// (i.e. a single column family).
// TODO(yuzefovich): lift that restriction once KV is updated so that rows are
// never split across different BatchResponses when TargetBytes limit is
// exceeded.
type Streamer interface {
	// Init initializes the Streamer.
	//
	// OperationMode controls the order in which results are delivered to the
	// client. When possible, prefer OutOfOrder mode.
	//
	// StreamerHints can be used to hint the aggressiveness of the caching
	// policy. In particular, it can be used to disable caching when the client
	// knows that all looked-up keys are unique (e.g. in the case of an
	// index-join).
	//
	// Budget controls how much memory the Streamer is allowed to use. The more
	// memory it has, the higher its internal concurrency and throughput.
	Init(OperationMode, StreamerHints, Budget)

	// Enqueue dispatches multiple requests for execution. Results are delivered
	// through the GetResults call. If keys is not nil, it needs to contain one
	// ID for each request; responses will reference that ID so that the client
	// can associate them to the requests. In OutOfOrder mode it's mandatory to
	// specify keys.
	//
	// Multiple requests can specify the same key. In this case, their
	// respective responses will also reference the same key. This is useful,
	// for example, for "range-based lookup joins" where multiple spans are read
	// in the context of the same input-side row (see multiSpanGenerator
	// implementation of rowexec.joinReaderSpanGenerator interface for more
	// details).
	//
	// In InOrder mode, responses will be delivered in reqs order.
	//
	// Currently, enqueuing new requests while there are still requests in
	// progress from the previous invocation is prohibited.
	// TODO(yuzefovich): lift this restriction and introduce the pipelining.
	Enqueue(ctx context.Context, reqs []roachpb.RequestUnion, keys []int) error

	// GetResults blocks until at least one result is available. If the
	// operation mode is OutOfOrder, any result will do. For InOrder, only
	// head-of-line results will do.
	GetResults(context.Context) ([]Result, error)

	// Cancel cancels all in-flight operations and discards all buffered results
	// if operating in InOrder mode. It blocks until all goroutines created by
	// the Streamer exit.
	Cancel()
}

// OperationMode describes the mode of operation of the Streamer.
type OperationMode int

const (
	_ OperationMode = iota
	// InOrder is the mode of operation in which the results are delivered in
	// the order in which the requests were handed off to the Streamer. This
	// mode forces the Streamer to buffer the results it produces through its
	// internal out-of-order execution. Out-of-order results might have to be
	// dropped (resulting in wasted/duplicate work) when the budget limit is
	// reached and the size estimates that lead to too much OoO execution were
	// wrong.
	InOrder
	// OutOfOrder is the mode of operation in which the results are delivered in
	// the order in which they're produced. The caller will use the keys field
	// of each Result to associate it with the corresponding requests. This mode
	// of operation lets the Streamer reuse the memory budget as quickly as
	// possible.
	OutOfOrder
)

// Remove an unused warning for now.
var _ = InOrder

// Result describes the result of performing a single KV request.
type Result struct {
	// GetResp and ScanResp represent the response to a request. Only one of the
	// two will be populated.
	//
	// The responses are to be considered immutable; the Streamer might hold on
	// to the respective memory.
	GetResp *roachpb.GetResponse
	// ScanResp can have a ResumeSpan in it. In that case, there will be a
	// further result with the continuation; that result will use the same Key.
	ScanResp *roachpb.ScanResponse
	// If the Result represents a scan result, ScanComplete indicates whether
	// this is the last response for the respective scan, or if there are more
	// responses to come. In any case, ScanResp never contains partial rows
	// (i.e. a single row is never split into different Results).
	//
	// When running in InOrder mode, Results for a single scan will be delivered
	// in key order (in addition to results for different scans being delivered
	// in request order). When running in OutOfOrder mode, Results for a single
	// scan can be delivered out of key order (in addition to results for
	// different scans being delivered out of request order).
	ScanComplete bool
	// Keys identifies the requests that this Result satisfies. In OutOfOrder
	// mode, a single Result can satisfy multiple identical requests. In InOrder
	// mode a Result can only satisfy multiple consecutive requests.
	Keys []int
	// MemoryTok.Release() needs to be called by the recipient once it's not
	// referencing this Result any more. If this was the last (or only)
	// reference to this Result, the memory used by this Result is made
	// available in the Streamer's budget.
	//
	// Internally, Results are refcounted. Multiple Results referencing the same
	// GetResp/ScanResp can be returned from separate `GetResults()` calls, and
	// the Streamer internally does buffering and caching of Results - which
	// also contributes to the refcounts.
	MemoryTok ResultMemoryToken
}

// ResultMemoryToken represents a handle to a Result's memory tracking. The
// recipient of a Result is required to call Release() when the Result is not in
// use any more so that its memory is returned to the Streamer's Budget.
//
// ResultMemoryToken is thread-safe.
type ResultMemoryToken interface {
	// Release decrements the refcount.
	Release()
}

// Budget abstracts the memory budget that is provided to the Streamer by its
// client.
type Budget interface {
	// Available returns how many bytes are currently available in the budget.
	// The answer can be negative, in case the Streamer has used un-budgeted
	// memory (e.g. one result was very large).
	//
	// Note that it's possible that actually available budget is less than the
	// number returned - this might occur if --max-sql-memory root pool is
	// almost fully used up.
	Available() int64
	// Consume draws bytes from the available budget. An error is returned iff
	// the root pool budget is used up such that the budget's limit cannot be
	// fully reserved.
	Consume(ctx context.Context, bytes int64) error
	// Release returns bytes to the available budget.
	Release(bytes int64)
	// WaitForBudget blocks until Available() becomes positive (until some
	// Release calls).
	WaitForBudget(context.Context)
}

// StreamerHints provides different hints to the Streamer for optimization
// purposes.
type StreamerHints struct {
	// UniqueRequests tells the Streamer that the requests will be unique. As
	// such, there's no point in de-duping them or caching results.
	UniqueRequests bool
}

type budget struct {
	mu struct {
		syncutil.Mutex
		// acc represents the current reservation of this budget against the
		// memory pool. acc.Used() will never grow past the limit.
		acc *mon.BoundAccount
		// used is the amount of currently used up bytes of this budget. This
		// number can exceed limit in degenerate cases (e.g. when a single row
		// exceeds the limit), but usually it should not exceed acc.Used().
		used int64
	}
	// limit is the maximum amount of bytes that this budget can reserve against
	// the account.
	limit  int64
	waitCh chan struct{}
}

var _ Budget = &budget{}

// NewBudget creates a new Budget with the specified limit. The limit determines
// the maximum amount of memory this budget is allowed to use (i.e. it'll be
// used lazily, as needed), but mon.DefaultPoolAllocationSize is reserved right
// away. An error is returned if the initial reservation is denied.
//
// acc should be bound to an unlimited memory monitor, and the Budget itself is
// responsible for staying under the limit.
//
// The budget takes ownership of the memory account, and the caller is allowed
// to interact with the account only after canceling the Streamer (because
// memory accounts are not thread-safe).
func NewBudget(ctx context.Context, acc *mon.BoundAccount, limit int64) (Budget, error) {
	var b budget
	b.mu.acc = acc
	initialBudgetReservation := mon.DefaultPoolAllocationSize
	if err := acc.Grow(ctx, initialBudgetReservation); err != nil {
		return nil, err
	}
	b.limit = limit
	b.waitCh = make(chan struct{}, 1)
	return &b, nil
}

func (b *budget) Available() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.availableLocked()
}

func (b *budget) availableLocked() int64 {
	return b.limit - b.mu.used
}

func (b *budget) Consume(ctx context.Context, bytes int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	// We need to make sure that we have accounted for either the new b.mu.used
	// value or up to the limit. Note that we don't update b.mu.used right away
	// since this resizing can be denied, yet the Streamer keeps on going
	// without emitting the memory reservation denial error to the client.
	accUsed := b.mu.used + bytes
	if accUsed > b.limit {
		accUsed = b.limit
	}
	if b.mu.acc.Used() < accUsed {
		if err := b.mu.acc.ResizeTo(ctx, accUsed); err != nil {
			return err
		}
	}
	b.mu.used += bytes
	return nil
}

func (b *budget) Release(bytes int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if buildutil.CrdbTestBuild {
		if b.mu.used < bytes {
			panic(errors.AssertionFailedf(
				"want to release %s when only %s is used",
				humanize.Bytes(uint64(bytes)), humanize.Bytes(uint64(b.mu.used)),
			))
		}
	}
	b.mu.used -= bytes
	if b.availableLocked() > 0 {
		select {
		case b.waitCh <- struct{}{}:
		default:
		}
	}
}

func (b *budget) WaitForBudget(ctx context.Context) {
	select {
	case <-b.waitCh:
	case <-ctx.Done():
	}
}

type resultMemoryToken struct {
	budget    Budget
	toRelease int64
}

var _ ResultMemoryToken = &resultMemoryToken{}

func (t *resultMemoryToken) Release() {
	t.budget.Release(t.toRelease)
}

type streamer struct {
	distSender *DistSender
	stopper    *stop.Stopper

	mode   OperationMode
	hints  StreamerHints
	budget Budget

	coordinator          workerCoordinator
	coordinatorStarted   bool
	coordinatorCtxCancel context.CancelFunc

	waitGroup sync.WaitGroup

	enqueueKeys []int

	// waitForResults is used to block GetResults() call until some results are
	// available.
	waitForResults chan struct{}

	mu struct {
		syncutil.Mutex

		// avgResponseSize tracks the estimated response size for a single
		// request. It is zero when no responses have been received yet, in
		// which case initialAvgResponseSize is used as the estimate.
		avgResponseSize int64

		// requestsToServe contains all single-range sub-requests that have yet
		// to be served.
		// TODO(yuzefovich): consider using ring.Buffer instead of a slice.
		requestsToServe []singleRangeBatch

		// numRangesLeftPerScanRequest tracks how many ranges a particular
		// originally enqueued ScanRequest touched, but scanning of those ranges
		// isn't complete.
		numRangesLeftPerScanRequest []int

		// numCompleteRequests tracks the number of the originally enqueued
		// requests that have already been completed.
		numCompleteRequests int

		// results are the results of already completed requests that haven't
		// been returned by GetResults() yet.
		results []Result
		err     error
	}

	atomics struct {
		// numRequestsInFlight tracks the number of single-range batches that
		// are currently being served asynchronously (i.e. those that have
		// already left requestsToServe queue, but for which we haven't received
		// the results yet).
		numRequestsInFlight int32
	}
}

var _ Streamer = &streamer{}

// streamerConcurrencyLimit is an upper bound on the number of asynchronous
// requests that a single Streamer can have in flight. The default value for
// this setting is chosen arbitrarily as 1/8th of the default value for the
// senderConcurrencyLimit.
// TODO(RFC): would it be preferable to have a global semaphore, similar to the
// DistSender?
var streamerConcurrencyLimit = settings.RegisterIntSetting(
	"kv.streamer.concurrency_limit",
	"maximum number of asynchronous requests by a single streamer",
	max(defaultSenderConcurrency/8, int64(8*runtime.GOMAXPROCS(0))),
	settings.NonNegativeInt,
)

// NewStreamer creates a new Streamer.
func NewStreamer(
	distSender *DistSender,
	stopper *stop.Stopper,
	txn *kv.Txn,
	st *cluster.Settings,
	lockWaitPolicy lock.WaitPolicy,
) Streamer {
	s := &streamer{
		distSender: distSender,
		stopper:    stopper,
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
		uint64(senderConcurrencyLimit.Get(&st.SV)),
	)
	streamerConcurrencyLimit.SetOnChange(&st.SV, func(ctx context.Context) {
		s.coordinator.asyncSem.UpdateCapacity(uint64(streamerConcurrencyLimit.Get(&st.SV)))
	})
	stopper.AddCloser(s.coordinator.asyncSem.Closer("stopper"))
	return s
}

// TODO(yuzefovich): use the optimizer-driven estimates.
const initialAvgResponseSize = 1 << 10 // 1KiB

// Init implements the Streamer interface.
func (s *streamer) Init(mode OperationMode, hints StreamerHints, budget Budget) {
	if mode != OutOfOrder {
		panic(errors.AssertionFailedf("only OutOfOrder mode is supported"))
	}
	s.mode = mode
	if !hints.UniqueRequests {
		panic(errors.AssertionFailedf("only unique requests are currently supported"))
	}
	s.hints = hints
	s.budget = budget
	s.coordinator.hasWork = make(chan struct{}, 1)
	s.waitForResults = make(chan struct{}, 1)
}

// Enqueue implements the Streamer interface.
//
// The Streamer takes over the given requests, will perform the memory
// accounting against its budget and might modify the requests in place.
//
// Enqueue divides the given requests into single-range batches that are added
// to requestsToServe, and the worker coordinator will then pick those batches
// up to execute asynchronously.
//
// The worker coordinator goroutine is started on the first call to Enqueue.
func (s *streamer) Enqueue(ctx context.Context, reqs []roachpb.RequestUnion, keys []int) error {
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

	s.enqueueKeys = keys

	var totalReqsMemUsage int64

	s.mu.Lock()
	// TODO(yuzefovich): we might want to have more fine-grained lock
	// acquisitions once pipelining is implemented.
	defer s.mu.Unlock()
	s.mu.numCompleteRequests = 0

	// The minimal key range encompassing all requests contained within.
	// Local addressing has already been resolved.
	rs, err := pkgKeys.Range(reqs)
	if err != nil {
		return err
	}

	// Split all requests into single-range batches.
	// TODO(yuzefovich): in InOrder mode we need to treat the head-of-the-line
	// request differently.
	seekKey := rs.Key
	const scanDir = Ascending
	ri := NewRangeIterator(s.distSender)
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
		singleRangeReqs, positions, err := truncate(reqs, singleRangeSpan)
		if err != nil {
			return err
		}
		for _, pos := range positions {
			if _, isScan := reqs[pos].GetInner().(*roachpb.ScanRequest); isScan {
				if firstScanRequest {
					if cap(s.mu.numRangesLeftPerScanRequest) < len(reqs) {
						s.mu.numRangesLeftPerScanRequest = make([]int, len(reqs))
					} else {
						s.mu.numRangesLeftPerScanRequest = s.mu.numRangesLeftPerScanRequest[:len(reqs)]
						for n := 0; n < len(s.mu.numRangesLeftPerScanRequest); n += copy(s.mu.numRangesLeftPerScanRequest[n:], zeroIntSlice) {
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
			reqs:             singleRangeReqs,
			positions:        positions,
			reqsAccountedFor: requestsMemUsage(singleRangeReqs),
		}
		totalReqsMemUsage += r.reqsAccountedFor

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
		seekKey, err = next(reqs, ri.Desc().EndKey)
		rs.Key = seekKey
		if err != nil {
			return err
		}
	}

	// Account for the memory used by all the requests.
	if err = s.budget.Consume(ctx, totalReqsMemUsage); err != nil {
		return err
	}

	// TODO(yuzefovich): it might be better to notify the coordinator once
	// one singleRangeBatch object has been appended to s.mu.requestsToServe.
	s.coordinator.hasWork <- struct{}{}
	return nil
}

// GetResults implements the Streamer interface.
func (s *streamer) GetResults(ctx context.Context) ([]Result, error) {
	s.mu.Lock()
	results := s.mu.results
	err := s.mu.err
	s.mu.results = nil
	allComplete := s.mu.numCompleteRequests == len(s.enqueueKeys)
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
func (s *streamer) notifyGetResultsLocked() {
	select {
	case s.waitForResults <- struct{}{}:
	default:
	}
}

// setError sets the error on the streamer if no error has been set previously
// and unblocks GetResults() if needed.
func (s *streamer) setError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.err == nil {
		s.mu.err = err
	}
	s.notifyGetResultsLocked()
}

// Cancel implements the Streamer interface.
func (s *streamer) Cancel() {
	if s.coordinatorCtxCancel != nil {
		s.coordinatorCtxCancel()
	}
	s.waitGroup.Wait()
}

func (s *streamer) getNumRequestsInFlight() int32 {
	return atomic.LoadInt32(&s.atomics.numRequestsInFlight)
}

type singleRangeBatch struct {
	reqs []roachpb.RequestUnion
	// positions is 1-to-1 mapping with reqs to indicate which ordinal among
	// originally enqueued requests a particular reqs[i] corresponds to. In
	// other words, if reqs[i] is (or a part of) enqueuedReqs[j], then
	// positions[i] = j.
	positions []int
	// reqsAccountedFor tracks the memory reservation against the Budget for the
	// memory usage of reqs.
	reqsAccountedFor int64
}

var _ sort.Interface = &singleRangeBatch{}

func (r *singleRangeBatch) Len() int {
	return len(r.reqs)
}

func (r *singleRangeBatch) Swap(i, j int) {
	r.reqs[i], r.reqs[j] = r.reqs[j], r.reqs[i]
	r.positions[i], r.positions[j] = r.positions[j], r.positions[i]
}

func (r *singleRangeBatch) Less(i, j int) bool {
	// TODO(yuzefovich): figure out whether it's worth extracting the keys when
	// constructing singleRangeBatch object.
	return r.reqs[i].GetInner().Header().Key.Compare(r.reqs[j].GetInner().Header().Key) < 0
}

type workerCoordinator struct {
	s              *streamer
	txn            *kv.Txn
	lockWaitPolicy lock.WaitPolicy

	asyncSem *quotapool.IntPool

	// For request and response admission control.
	requestAdmissionHeader roachpb.AdmissionHeader
	responseAdmissionQ     *admission.WorkQueue

	hasWork chan struct{}
}

// mainLoop runs throughout the lifetime of the Streamer (from the first Enqueue
// call until Cancel) and routes the single-range batches for asynchronous
// execution. This function is dividing up the Streamer's Budget for each of
// those batches and won't start executing the batches if the available budget
// is insufficient. The function exits when an error is encountered by one of
// the asynchronous requests.
func (w *workerCoordinator) mainLoop(ctx context.Context) {
	defer w.s.waitGroup.Done()
	for {
		// Wait until there are some requests to fulfill.
		w.s.mu.Lock()
		requestsToServe := w.s.mu.requestsToServe
		avgResponseSize := w.s.mu.avgResponseSize
		err := w.s.mu.err
		// Clear the buffer of the channel in case we've just picked up some
		// work. We're doing so while holding the lock so that no new requests
		// could be added (we want to preserve an object in hasWork channel if
		// new requests are added).
		if len(requestsToServe) > 0 {
			select {
			case <-w.hasWork:
			default:
			}
		}
		w.s.mu.Unlock()
		if len(requestsToServe) == 0 {
			// Block until there are enqueued requests.
			select {
			case <-w.hasWork:
			case <-ctx.Done():
				return
			}
			w.s.mu.Lock()
			requestsToServe = w.s.mu.requestsToServe
			avgResponseSize = w.s.mu.avgResponseSize
			err = w.s.mu.err
			w.s.mu.Unlock()
		}
		if err != nil {
			return
		}

		if avgResponseSize == 0 {
			avgResponseSize = initialAvgResponseSize
		}

		// Now wait until there is enough budget to at least receive one full
		// response (but only if there are requests in flight - if there are
		// none, then we might have a degenerate case when a single row is
		// expected to exceed the budget).
		// TODO(yuzefovich): consider using a multiple of avgResponseSize here.
		for w.s.getNumRequestsInFlight() > 0 && w.s.budget.Available() < avgResponseSize {
			w.s.budget.WaitForBudget(ctx)
			if ctx.Err() != nil {
				return
			}
		}

		reqsToServeIdx := 0
		for reqsToServeIdx < len(requestsToServe) {
			var targetBytesReservationDenied bool
			availableBudget := w.s.budget.Available()
			if availableBudget < avgResponseSize {
				if w.s.getNumRequestsInFlight() > 0 {
					// We don't have enough budget available to serve this
					// request, and there are other requests in flight, so we'll
					// wait for some of them to finish.
					break
				}
				availableBudget = 1
				targetBytesReservationDenied = true
			}
			singleRangeReqs := requestsToServe[reqsToServeIdx]
			targetBytes := int64(len(singleRangeReqs.reqs)) * avgResponseSize
			if targetBytes > availableBudget {
				targetBytes = availableBudget
			}
			if !targetBytesReservationDenied {
				if err = w.s.budget.Consume(ctx, targetBytes); err != nil {
					// We don't have enough budget to evaluate this single-range
					// batch.
					if w.s.getNumRequestsInFlight() > 0 {
						// There are some requests in flight, so we'll let them
						// finish.
						break
					}
					// We don't have any requests in flight, and the error
					// indicates that the root memory pool has been exhausted,
					// so we'll exit to be safe (in order not to OOM the node).
					w.s.setError(err)
					return
				}
			}
			w.performRequestAsync(
				ctx, singleRangeReqs, targetBytes, w.s.getNumRequestsInFlight() == 0,
				targetBytesReservationDenied,
			)
			reqsToServeIdx++
			if targetBytesReservationDenied {
				break
			}
		}
		if reqsToServeIdx > 0 {
			w.s.mu.Lock()
			// We can just slice here since we only append to requestToServe at
			// the moment.
			w.s.mu.requestsToServe = w.s.mu.requestsToServe[reqsToServeIdx:]
			w.s.mu.Unlock()
		}
	}
}

// addRequest adds a single-range batch to be processed later.
func (w *workerCoordinator) addRequest(req singleRangeBatch) {
	// TODO(yuzefovich): in InOrder mode we cannot just append.
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	w.s.mu.requestsToServe = append(w.s.mu.requestsToServe, req)
	// Non-blockingly notify the coordinator about new work.
	select {
	case w.hasWork <- struct{}{}:
	default:
	}
}

func (w *workerCoordinator) asyncRequestCleanup() {
	atomic.AddInt32(&w.s.atomics.numRequestsInFlight, -1)
	w.s.waitGroup.Done()
}

// performRequestAsync dispatches the given single-range batch for evaluation
// asynchronously. If the batch cannot be evaluated fully (due to exhausting its
// memory limit), the "resume" single-range batch will be added into
// requestsToServe, and mainLoop will pick that up to process later.
// - targetBytes specifies the memory budget that this single-range batch should
// be issued with.
// - headOfLine indicates whether this request is the current head of the line.
// In OutOfOrder mode any request can be treated as such.
// - targetBytesReservationDenied indicates whether the reservation of
// targetBytes from the budget has been denied. This can be true iff headOfLine
// is true.
func (w *workerCoordinator) performRequestAsync(
	ctx context.Context,
	req singleRangeBatch,
	targetBytes int64,
	headOfLine bool,
	targetBytesReservationDenied bool,
) {
	w.s.waitGroup.Add(1)
	atomic.AddInt32(&w.s.atomics.numRequestsInFlight, 1)
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
			ba.Header.TargetBytesAllowEmpty = !headOfLine
			// TODO(yuzefovich): consider setting MaxSpanRequestKeys whenever
			// applicable (#67885).
			ba.AdmissionHeader = w.requestAdmissionHeader
			// We override NoMemoryReservedAtSource to false in order to
			// simulate the logic of row.txnKVFetcher.fetch where we set this to
			// true iff the fetcher hasn't reserved at least 1KiB; however, we
			// have already reserved at least 10KiB when creating the Budget. If
			// targetBytesReservationDenied is true, then this request is the
			// only one that will be in flight.
			ba.AdmissionHeader.NoMemoryReservedAtSource = false
			ba.Requests = req.reqs

			// TODO(yuzefovich): probably disable batch splitting done by the
			// DistSender and handle the errors on BatchRequests that require
			// splitting here.
			br, err := w.txn.Send(ctx, ba)
			if err != nil {
				// TODO(yuzefovich): if err is
				// ReadWithinUncertaintyIntervalError and there is only a single
				// streamer in a single local flow, attempt to transparently
				// refresh.
				w.s.setError(err.GoError())
				return
			}
			// Quickly adjust the budget based on the total footprint of the
			// batch. This is an overestimate because it includes the overhead
			// of the batch; we will, however, update the reservation before
			// exiting in order to have precise match with all
			// ResultMemoryTokens.
			returnedBytes := int64(br.Size())
			targetBytesReservation := targetBytes
			if targetBytesReservationDenied {
				targetBytesReservation = 0
			}
			if returnedBytes < targetBytesReservation {
				w.s.budget.Release(targetBytesReservation - returnedBytes)
			} else if returnedBytes > targetBytesReservation {
				if err := w.s.budget.Consume(ctx, returnedBytes-targetBytesReservation); err != nil {
					// We don't have the budget to account for the returned
					// result.
					w.s.budget.Release(targetBytesReservation)
					if !headOfLine {
						// Since this is not the head of the line, we'll just
						// discard the result and add the request back to be
						// served.
						// TODO(yuzefovich): consider updating the
						// avgResponseSize and/or storing the information about
						// the returned bytes size in req.
						w.addRequest(req)
						return
					}
					// TODO(RFC): --max-sql-memory pool has been used up such
					// that the budget's limit cannot be reserved, so here we
					// have a choice to make:
					// 1. be safe and return an error to the caller, or
					// 2. proceed nonetheless given that we already have the
					//    response. We could also check whether there are other
					//    requests in flight and choose a different strategy
					//    based on that (e.g. return an error only if it is the
					//    only request in flight, and if it's not the only, then
					//    somehow notify the workerCoordinator that there is
					//    some memory flying around that is not accounted for).
					w.s.setError(err)
					return
				}
			}

			// Do admission control after we've accounted for the response
			// bytes.
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

			var resumeReq singleRangeBatch
			// We will reuse the slices for the resume spans, if any.
			resumeReq.reqs = req.reqs[:0]
			resumeReq.positions = req.positions[:0]
			var results []Result
			var totalResponseSize, nonEmptyResponseCount, numCompleteResponses int
			var actualMemoryReservation int64
			for i, resp := range br.Responses {
				reply := resp.GetInner()
				resumeSpan := reply.Header().ResumeSpan
				origReq := req.reqs[i]
				// Unset the original request so that we lose the reference to
				// the span.
				req.reqs[i] = roachpb.RequestUnion{}
				switch origRequest := origReq.GetInner().(type) {
				case *roachpb.GetRequest:
					get := reply.(*roachpb.GetResponse)
					if resumeSpan != nil {
						// This Get wasn't completed - update the original
						// request according to the resumeSpan and include it
						// into the batch again.
						origRequest.SetSpan(*resumeSpan)
						resumeReq.reqs = append(resumeReq.reqs, origReq)
						resumeReq.positions = append(resumeReq.positions, req.positions[i])
					} else {
						// This Get was completed.
						var result Result
						result.GetResp = get
						// This currently only works because all requests are
						// unique.
						result.Keys = []int{w.s.enqueueKeys[req.positions[i]]}
						toRelease := int64(get.Size())
						actualMemoryReservation += toRelease
						result.MemoryTok = &resultMemoryToken{
							toRelease: toRelease,
							budget:    w.s.budget,
						}
						results = append(results, result)
						totalResponseSize += get.Size()
						nonEmptyResponseCount++
						numCompleteResponses++
					}

				case *roachpb.ScanRequest:
					scan := reply.(*roachpb.ScanResponse)
					if len(scan.Rows) > 0 || len(scan.BatchResponses) > 0 {
						var result Result
						result.ScanResp = scan
						if resumeSpan == nil {
							// The Scan within the range is complete.
							w.s.mu.Lock()
							w.s.mu.numRangesLeftPerScanRequest[req.positions[i]]--
							scanComplete := w.s.mu.numRangesLeftPerScanRequest[req.positions[i]] == 0
							w.s.mu.Unlock()
							result.ScanComplete = scanComplete
							numCompleteResponses++
						}
						// This currently only works because all requests are
						// unique.
						result.Keys = []int{w.s.enqueueKeys[req.positions[i]]}
						toRelease := int64(scan.Size())
						actualMemoryReservation += toRelease
						result.MemoryTok = &resultMemoryToken{
							toRelease: toRelease,
							budget:    w.s.budget,
						}
						results = append(results, result)
						totalResponseSize += scan.Size()
						nonEmptyResponseCount++
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

			// Adjust the memory reservation according to the sum of all memory
			// tokens. This releases the memory used by the overhead of the
			// BatchResponse.
			w.s.budget.Release(returnedBytes - actualMemoryReservation)

			// Update the budget based on the usage of the new resume requests.
			// Note that the resume spans might be larger in size than the spans
			// in the original requests.
			var newMemUsage int64
			if len(resumeReq.reqs) > 0 {
				newMemUsage = requestsMemUsage(resumeReq.reqs)
			}
			if newMemUsage <= req.reqsAccountedFor {
				w.s.budget.Release(req.reqsAccountedFor - newMemUsage)
				resumeReq.reqsAccountedFor = newMemUsage
			} else {
				// We actually need to consume a part of the budget. If that
				// consumption is not denied, then we'll update resumeReq;
				// otherwise, we keep the original accounting in order to be
				// precise.
				//
				// We choose not to set an error on the streamer if the
				// consumption is denied assuming that the unaccounted memory
				// should be small (we've already accounted for the original
				// spans in Enqueue()).
				// TODO(RFC): thoughts about this assumption?
				if w.s.budget.Consume(ctx, newMemUsage-req.reqsAccountedFor) == nil {
					resumeReq.reqsAccountedFor = newMemUsage
				}
			}

			if nonEmptyResponseCount > 0 {
				// Update the average response size based on this batch.
				//
				// We do not want to use a regular average here because it would
				// be unfair to "large" batches that come in late (i.e. it would
				// not be reactive enough). Currently, we're using an
				// exponential moving average where the ratio of "decay" is in
				// [0.01, 0.1] range with batches with more responses having
				// larger ratio.
				//
				// The current choice of ratios is such that:
				// - for "small" BatchResponses (in the number of non-empty
				// responses within it), avgResponseSize behaves like an average
				// over the last 100 BatchResponses (1 / 0.01 = 100)
				// - for "large" BatchResponses, avgResponseSize behaves like an
				// average over the last 10 BatchResponses (1 / 0.1 = 100).
				// TODO(yuzefovich): think through how we should be updating the
				// average estimate.
				const minRatio, maxRatio = 0.01, 0.1
				ratio := float64(nonEmptyResponseCount) / 1000
				if ratio < minRatio {
					ratio = minRatio
				} else if ratio > maxRatio {
					ratio = maxRatio
				}
				w.s.mu.Lock()
				if w.s.mu.avgResponseSize == 0 {
					// These responses are the first actual responses we've
					// received, so use their average size as the initial
					// estimate.
					ratio = 1
				}
				w.s.mu.avgResponseSize -= int64(float64(w.s.mu.avgResponseSize) * ratio)
				w.s.mu.avgResponseSize += int64(float64(totalResponseSize) / float64(nonEmptyResponseCount) * ratio)
				w.s.mu.numCompleteRequests += numCompleteResponses
				// Store the results and non-blockingly notify the streamer about
				// them.
				w.s.mu.results = append(w.s.mu.results, results...)
				w.s.notifyGetResultsLocked()
				w.s.mu.Unlock()
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
