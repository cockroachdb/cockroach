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
	"math"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	//
	// When there are multiple results associated with a given request, they are
	// sorted in lookup order for that request (though not globally).
	InOrder
	// OutOfOrder is the mode of operation in which the results are delivered in
	// the order in which they're produced. The caller will use the keys field
	// of each Result to associate it with the corresponding requests. This mode
	// of operation lets the Streamer reuse the memory budget as quickly as
	// possible.
	OutOfOrder
)

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
	//
	// GetResp is guaranteed to have nil IntentValue.
	GetResp *roachpb.GetResponse
	// ScanResp can contain a partial response to a ScanRequest (when
	// scanComplete is false). In that case, there will be a further result with
	// the continuation; that result will use the same Key. Notably, SQL rows
	// will never be split across multiple results.
	//
	// The response is always using BATCH_RESPONSE format (meaning that Rows
	// field is always nil). IntentRows field is also nil.
	ScanResp *roachpb.ScanResponse
	// Position tracks the ordinal among all originally enqueued requests that
	// this result satisfies. See singleRangeBatch.positions for more details.
	// TODO(yuzefovich): this might need to be []int when non-unique requests
	// are supported.
	Position int
	// memoryTok describes the memory reservation of this Result that needs to
	// be released back to the Streamer's budget when the Result is Release()'d.
	memoryTok struct {
		streamer  *Streamer
		toRelease int64
	}
	// subRequestIdx allows us to order two Results that come for the same
	// original Scan request but from different ranges. It is non-zero only in
	// InOrder mode when Hints.SingleRowLookup is false, in all other cases it
	// will remain zero. See singleRangeBatch.subRequestIdx for more details.
	subRequestIdx int32
	// subRequestDone is true if the current Result is the last one for the
	// corresponding sub-request. For all Get requests and for Scan requests
	// contained within a single range, it is always true since those can only
	// have a single sub-request.
	//
	// Note that for correctness, it is only necessary that this value is set
	// properly if this Result is a Scan response and Hints.SingleRowLookup is
	// false.
	subRequestDone bool
	// If the Result represents a scan result, scanComplete indicates whether
	// this is the last response for the respective scan, or if there are more
	// responses to come. In any case, ScanResp never contains partial rows
	// (i.e. a single row is never split into different Results).
	//
	// When running in InOrder mode, Results for a single scan will be delivered
	// in key order (in addition to results for different scans being delivered
	// in request order). When running in OutOfOrder mode, Results for a single
	// scan can be delivered out of key order (in addition to results for
	// different scans being delivered out of request order).
	scanComplete bool
}

// Hints provides different hints to the Streamer for optimization purposes.
type Hints struct {
	// UniqueRequests tells the Streamer that the requests will be unique. As
	// such, there's no point in de-duping them or caching results.
	UniqueRequests bool
	// SingleRowLookup tells the Streamer that each enqueued request will result
	// in a single row lookup (in other words, the request contains a "key"). If
	// true, then the Streamer knows that no request will be split across
	// multiple ranges, so some internal state can be optimized away.
	SingleRowLookup bool
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
	if s := r.memoryTok.streamer; s != nil {
		s.results.releaseOne()
		s.budget.mu.Lock()
		defer s.budget.mu.Unlock()
		s.budget.releaseLocked(ctx, r.memoryTok.toRelease)
		s.mu.Lock()
		defer s.mu.Unlock()
		s.signalBudgetIfNoRequestsInProgressLocked()
	}
}

// Streamer provides a streaming oriented API for reading from the KV layer.
//
// The example usage is roughly as follows:
//
//	s := NewStreamer(...)
//	s.Init(OperationMode, Hints)
//	...
//	for needMoreKVs {
//	  // Check whether there are results to the previously enqueued requests.
//	  // This will block if no results are available, but there are some
//	  // enqueued requests.
//	  results, err := s.GetResults(ctx)
//	  // err check
//	  ...
//	  if len(results) > 0 {
//	    processResults(results)
//	    // return to the client
//	    ...
//	    // when results are no longer needed, Release() them
//	  }
//	  // All previously enqueued requests have already been responded to.
//	  if moreRequestsToEnqueue {
//	    err := s.Enqueue(ctx, requests)
//	    // err check
//	    ...
//	  } else {
//	    // done
//	    ...
//	  }
//	}
//	...
//	s.Close()
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

	mode          OperationMode
	hints         Hints
	maxKeysPerRow int32
	budget        *budget
	keyLocking    lock.Strength

	streamerStatistics

	coordinator          workerCoordinator
	coordinatorStarted   bool
	coordinatorCtxCancel context.CancelFunc

	waitGroup sync.WaitGroup

	truncationHelper *kvcoord.BatchTruncationHelper
	// truncationHelperAccountedFor tracks how much space has been consumed from
	// the budget in order to account for the memory usage of the truncation
	// helper.
	truncationHelperAccountedFor int64

	// requestsToServe contains all single-range sub-requests that have yet
	// to be served.
	requestsToServe requestsProvider

	// results are the results of already completed requests that haven't
	// been returned by GetResults() yet.
	results resultsBuffer

	// numRangesPerScanRequestAccountedFor tracks how much space has been
	// consumed from the budget in order to account for the
	// numRangesPerScanRequest slice.
	//
	// It is only accessed from the Streamer's user goroutine, so it doesn't
	// need the mutex protection.
	numRangesPerScanRequestAccountedFor int64

	mu struct {
		// If the budget's mutex also needs to be locked, the budget's mutex
		// must be acquired first. If the results' mutex needs to be locked,
		// then this mutex must be acquired first.
		syncutil.Mutex

		avgResponseEstimator avgResponseEstimator

		// In OutOfOrder mode, numRangesPerScanRequest tracks how many
		// ranges a particular originally enqueued ScanRequest touches, but
		// scanning of those ranges isn't complete.
		//
		// In InOrder mode, it tracks how many ranges a particular originally
		// enqueued ScanRequest touches. In other words, it contains how many
		// "sub-requests" the original Scan request was broken down into.
		//
		// It is allocated lazily if Hints.SingleRowLookup is false when the
		// first ScanRequest is encountered in Enqueue.
		numRangesPerScanRequest []int32

		// numRequestsInFlight tracks the number of single-range batches that
		// are currently being served asynchronously (i.e. those that have
		// already left requestsToServe queue, but for which we haven't received
		// the results yet).
		numRequestsInFlight int

		// done is set to true once the Streamer is closed meaning the worker
		// coordinator must exit.
		done bool
	}
}

type streamerStatistics struct {
	atomics struct {
		batchRequestsIssued *int64
		// resumeBatchRequests tracks the number of BatchRequests created for
		// the ResumeSpans throughout the lifetime of the Streamer.
		resumeBatchRequests int64
		// resumeSingleRangeRequests tracks the number of single-range requests
		// that were created for the ResumeSpans throughout the lifetime of the
		// Streamer.
		resumeSingleRangeRequests int64
		// emptyBatchResponses tracks the number of BatchRequests that resulted
		// in empty BatchResponses because they were issued with too low value
		// of TargetBytes parameter.
		emptyBatchResponses int64
		// droppedBatchResponses tracks the number of the received
		// BatchResponses that were dropped because the memory reservation
		// during the budget reconciliation was denied (i.e. the original
		// estimate was too low, and the budget has been used up by the time
		// response came).
		droppedBatchResponses int64
	}
	// enqueueCalls tracks the number of times Enqueue() has been called.
	enqueueCalls int
	// enqueuedRequests tracks the number of possibly-multi-range requests that
	// have been Enqueue()'d into the Streamer.
	enqueuedRequests int
	// enqueuedSingleRangeRequests tracks the number of single-range
	// sub-requests that were created during the truncation process in Enqueue()
	enqueuedSingleRangeRequests int
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
	settings.PositiveInt,
)

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// NewStreamer creates a new Streamer.
//
// txn must be a LeafTxn that is not used by anything other than this Streamer.
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
//
// batchRequestsIssued should be incremented every time a new BatchRequest is
// sent.
func NewStreamer(
	distSender *kvcoord.DistSender,
	stopper *stop.Stopper,
	txn *kv.Txn,
	st *cluster.Settings,
	lockWaitPolicy lock.WaitPolicy,
	limitBytes int64,
	acc *mon.BoundAccount,
	batchRequestsIssued *int64,
	keyLocking lock.Strength,
) *Streamer {
	if txn.Type() != kv.LeafTxn {
		panic(errors.AssertionFailedf("RootTxn is given to the Streamer"))
	}
	s := &Streamer{
		distSender: distSender,
		stopper:    stopper,
		budget:     newBudget(acc, limitBytes),
		keyLocking: keyLocking,
	}
	if batchRequestsIssued == nil {
		batchRequestsIssued = new(int64)
	}
	s.atomics.batchRequestsIssued = batchRequestsIssued
	s.coordinator = workerCoordinator{
		s:                      s,
		txn:                    txn,
		lockWaitPolicy:         lockWaitPolicy,
		requestAdmissionHeader: txn.AdmissionHeader(),
		responseAdmissionQ:     txn.DB().SQLKVResponseAdmissionQ,
	}
	s.coordinator.asyncSem = quotapool.NewIntPool(
		"single Streamer async concurrency",
		uint64(streamerConcurrencyLimit.Get(&st.SV)),
	)
	s.mu.avgResponseEstimator.init(&st.SV)
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
//
// maxKeysPerRow indicates the maximum number of KV pairs that comprise a single
// SQL row (i.e. the number of column families in the index being scanned).
//
// In InOrder mode, diskBuffer argument must be non-nil.
func (s *Streamer) Init(
	mode OperationMode, hints Hints, maxKeysPerRow int, diskBuffer ResultDiskBuffer,
) {
	s.mode = mode
	if mode == OutOfOrder {
		s.requestsToServe = newOutOfOrderRequestsProvider()
		s.results = newOutOfOrderResultsBuffer(s.budget)
	} else {
		s.requestsToServe = newInOrderRequestsProvider()
		s.results = newInOrderResultsBuffer(s.budget, diskBuffer)
	}
	if !hints.UniqueRequests {
		panic(errors.AssertionFailedf("only unique requests are currently supported"))
	}
	s.hints = hints
	s.maxKeysPerRow = int32(maxKeysPerRow)
}

// Enqueue dispatches multiple requests for execution. Results are delivered
// through the GetResults call.
//
// The Streamer takes over the given requests, will perform the memory
// accounting against its budget and might modify the requests in place.
//
// In InOrder operation mode, responses will be delivered in reqs order. When
// more than one row is returned for a given request, the rows for that request
// will be sorted in the order of the lookup index.
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
func (s *Streamer) Enqueue(ctx context.Context, reqs []roachpb.RequestUnion) (retErr error) {
	if !s.coordinatorStarted {
		var coordinatorCtx context.Context
		coordinatorCtx, s.coordinatorCtxCancel = s.stopper.WithCancelOnQuiesce(ctx)
		s.waitGroup.Add(1)
		if err := s.stopper.RunAsyncTaskEx(
			coordinatorCtx,
			stop.TaskOpts{
				TaskName: "streamer-coordinator",
				SpanOpt:  stop.ChildSpan,
			},
			s.coordinator.mainLoop,
		); err != nil {
			// The new goroutine wasn't spun up, so mainLoop won't get executed
			// and we have to decrement the wait group ourselves.
			s.waitGroup.Done()
			return err
		}
		s.coordinatorStarted = true
	}

	defer func() {
		// Set the error (if present) so that mainLoop of the worker coordinator
		// exits as soon as possible, without issuing any requests.
		if retErr != nil {
			s.results.setError(retErr)
		}
	}()

	if err := s.results.init(ctx, len(reqs)); err != nil {
		return err
	}

	s.enqueueCalls++
	s.enqueuedRequests += len(reqs)

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
	// Use a local variable for requestsToServe rather than adding them to the
	// requestsProvider right away. This is needed in order for the worker
	// coordinator to not pick up any work until we account for
	// totalReqsMemUsage.
	// TODO(yuzefovich): this memory is not accounted for. However, the number
	// of singleRangeBatch objects in flight is limited by the number of ranges
	// of a single table, so it doesn't seem urgent to fix the accounting here.
	var requestsToServe []singleRangeBatch
	const scanDir = kvcoord.Ascending
	ri := kvcoord.MakeRangeIterator(s.distSender)
	ri.Seek(ctx, rs.Key, scanDir)
	if !ri.Valid() {
		return ri.Error()
	}
	firstScanRequest := true
	streamerLocked := false
	defer func() {
		if streamerLocked {
			s.mu.Unlock()
		}
	}()
	allRequestsAreWithinSingleRange := !ri.NeedAnother(rs)
	if !allRequestsAreWithinSingleRange {
		// We only need the truncation helper if the requests span multiple
		// ranges.
		if s.truncationHelper == nil {
			// The streamer can process the responses in an arbitrary order, so
			// we don't require the helper to preserve the order of requests and
			// allow it to reorder the reqs slice too.
			const mustPreserveOrder = false
			const canReorderRequestsSlice = true
			s.truncationHelper, err = kvcoord.NewBatchTruncationHelper(
				scanDir, reqs, mustPreserveOrder, canReorderRequestsSlice,
			)
		} else {
			err = s.truncationHelper.Init(reqs)
		}
		if err != nil {
			return err
		}
	}
	var reqsKeysScratch []roachpb.Key
	var newNumRangesPerScanRequestMemoryUsage int64
	for ; ; ri.Seek(ctx, rs.Key, scanDir) {
		if !ri.Valid() {
			return ri.Error()
		}
		// Find all requests that touch the current range.
		var singleRangeReqs []roachpb.RequestUnion
		var positions []int
		if allRequestsAreWithinSingleRange {
			// All requests are within this range, so we can just use the
			// enqueued requests directly.
			singleRangeReqs = reqs
			positions = make([]int, len(reqs))
			for i := range positions {
				positions[i] = i
			}
			rs.Key = roachpb.RKeyMax
		} else {
			// Truncate the request span to the current range.
			singleRangeSpan, err := rs.Intersect(ri.Token().Desc().RSpan())
			if err != nil {
				return err
			}
			singleRangeReqs, positions, rs.Key, err = s.truncationHelper.Truncate(singleRangeSpan)
			if err != nil {
				return err
			}
		}
		var subRequestIdx []int32
		var subRequestIdxOverhead int64
		if !s.hints.SingleRowLookup {
			for i, pos := range positions {
				if _, isScan := reqs[pos].GetInner().(*roachpb.ScanRequest); isScan {
					if firstScanRequest {
						// We have some ScanRequests, and each might touch
						// multiple ranges, so we have to set up
						// numRangesPerScanRequest.
						streamerLocked = true
						s.mu.Lock()
						if cap(s.mu.numRangesPerScanRequest) < len(reqs) {
							s.mu.numRangesPerScanRequest = make([]int32, len(reqs))
							newNumRangesPerScanRequestMemoryUsage = int64(cap(s.mu.numRangesPerScanRequest)) * int32Size
						} else {
							// We can reuse numRangesPerScanRequest allocated on
							// the previous call to Enqueue after we zero it
							// out.
							s.mu.numRangesPerScanRequest = s.mu.numRangesPerScanRequest[:len(reqs)]
							for n := 0; n < len(s.mu.numRangesPerScanRequest); {
								n += copy(s.mu.numRangesPerScanRequest[n:], zeroInt32Slice)
							}
						}
					}
					if s.mode == InOrder {
						if subRequestIdx == nil {
							subRequestIdx = make([]int32, len(singleRangeReqs))
							subRequestIdxOverhead = int32SliceOverhead + int32Size*int64(cap(subRequestIdx))
						}
						subRequestIdx[i] = s.mu.numRangesPerScanRequest[pos]
					}
					s.mu.numRangesPerScanRequest[pos]++
					firstScanRequest = false
				}
			}
		}

		overheadAccountedFor := requestUnionSliceOverhead + requestUnionOverhead*int64(cap(singleRangeReqs)) + // reqs
			intSliceOverhead + intSize*int64(cap(positions)) + // positions
			subRequestIdxOverhead // subRequestIdx
		r := singleRangeBatch{
			reqs:                 singleRangeReqs,
			positions:            positions,
			subRequestIdx:        subRequestIdx,
			reqsReservedBytes:    requestsMemUsage(singleRangeReqs),
			overheadAccountedFor: overheadAccountedFor,
		}
		totalReqsMemUsage += r.reqsReservedBytes + r.overheadAccountedFor

		if s.mode == OutOfOrder {
			// Sort all single-range requests to be in the key order.
			// TODO(yuzefovich): we should be able to sort not head-of-the-line
			// request in the InOrder mode too; however, there would be
			// complications whenever a request (either original or with
			// ResumeSpans) is put back because in such a scenario any request
			// can become head-of-the-line in the future. We probably will need
			// to introduce a way to "restore" the original order within
			// singleRangeBatch if it is sorted and issued with headOfLine=true.
			r.reqsKeys = reqsKeysScratch[:0]
			for i := range r.reqs {
				r.reqsKeys = append(r.reqsKeys, r.reqs[i].GetInner().Header().Key)
			}
			sort.Sort(&r)
			reqsKeysScratch = r.reqsKeys
			r.reqsKeys = nil
		}

		requestsToServe = append(requestsToServe, r)
		s.enqueuedSingleRangeRequests += len(singleRangeReqs)

		if allRequestsAreWithinSingleRange || !ri.NeedAnother(rs) {
			// This was the last range. Breaking here rather than Seek'ing the
			// iterator to RKeyMax (and, thus, invalidating it) allows us to
			// avoid adding a confusing message into the trace.
			break
		}
	}

	if streamerLocked {
		// Per the contract of the budget's mutex (which must be acquired first,
		// before the Streamer's mutex), we cannot hold the mutex of s when
		// consuming below, so we have to unlock it.
		s.mu.Unlock()
		streamerLocked = false
	}

	toConsume := totalReqsMemUsage
	// Track separately the memory usage of the truncation helper so that we
	// could correctly release it in case we're low on memory budget.
	var truncationHelperMemUsage, truncationHelperToConsume int64
	if !allRequestsAreWithinSingleRange {
		truncationHelperMemUsage = s.truncationHelper.MemUsage()
		truncationHelperToConsume = truncationHelperMemUsage - s.truncationHelperAccountedFor
		toConsume += truncationHelperToConsume
	}
	if newNumRangesPerScanRequestMemoryUsage != 0 && newNumRangesPerScanRequestMemoryUsage != s.numRangesPerScanRequestAccountedFor {
		toConsume += newNumRangesPerScanRequestMemoryUsage - s.numRangesPerScanRequestAccountedFor
		s.numRangesPerScanRequestAccountedFor = newNumRangesPerScanRequestMemoryUsage
	}
	// We allow the budget to go into debt iff a single request was enqueued.
	// This is needed to support the case of arbitrarily large keys - the caller
	// is expected to produce requests with such cases one at a time.
	allowDebt := len(reqs) == 1
	if err = s.budget.consume(ctx, toConsume, allowDebt); err != nil {
		if allowDebt {
			// This error indicates that we're using the whole --max-sql-memory
			// budget, so we'll just give up in order to protect the node.
			return err
		}
		// We have two things (used only to reduce allocations) we could dispose of
		// without sacrificing the correctness:
		// - don't reuse the truncation helper (if present)
		// - clear the overhead of the results buffer.
		// Once disposed of those, we attempt to consume the budget again.
		if s.truncationHelper != nil {
			s.truncationHelper = nil
			s.budget.release(ctx, s.truncationHelperAccountedFor)
			s.truncationHelperAccountedFor = 0
			toConsume -= truncationHelperToConsume
		}
		s.results.clearOverhead(ctx)
		if err = s.budget.consume(ctx, toConsume, allowDebt); err != nil {
			return err
		}
	} else if !allRequestsAreWithinSingleRange {
		// The consumption was approved, so we're keeping the reference to the
		// truncation helper.
		s.truncationHelperAccountedFor = truncationHelperMemUsage
	}

	// Memory reservation was approved, so the requests are good to go.
	s.requestsToServe.enqueue(requestsToServe)
	return nil
}

// GetResults blocks until at least one result is available. If the operation
// mode is OutOfOrder, any result will do, and the caller is expected to examine
// Result.Position to understand which request the result corresponds to. For
// InOrder, only head-of-line results will do. Zero-length result slice is
// returned once all enqueued requests have been responded to.
//
// Calling GetResults() invalidates the results returned on the previous call.
func (s *Streamer) GetResults(ctx context.Context) ([]Result, error) {
	for {
		results, allComplete, err := s.results.get(ctx)
		if len(results) > 0 || allComplete || err != nil {
			return results, err
		}
		s.results.wait()
		// Check whether the Streamer has been canceled or closed while we were
		// waiting for the results.
		if err = ctx.Err(); err != nil {
			s.results.setError(err)
			return nil, err
		}
	}
}

// Close cancels all in-flight operations and releases all of the resources of
// the Streamer. It blocks until all goroutines created by the Streamer exit. No
// other calls on s are allowed after this.
func (s *Streamer) Close(ctx context.Context) {
	if s.coordinatorStarted {
		s.coordinator.logStatistics(ctx)
		s.coordinatorCtxCancel()
		s.mu.Lock()
		s.mu.done = true
		s.mu.Unlock()
		s.requestsToServe.close()
		s.results.close(ctx)
		// Unblock the coordinator in case it is waiting for the budget.
		s.budget.mu.waitForBudget.Signal()
	}
	s.waitGroup.Wait()
	*s = Streamer{}
}

// getNumRequestsInProgress returns the number of requests that are currently
// "in progress" - already issued requests that are in flight combined with the
// number of unreleased results. This method should be called without holding
// the lock of s.
func (s *Streamer) getNumRequestsInProgress() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.numRequestsInFlight + s.results.numUnreleased()
}

// signalBudgetIfNoRequestsInProgressLocked checks whether there are no requests
// in progress and signals the budget's condition variable if so.
//
// We have to explicitly signal the condition variable to make sure that if the
// budget doesn't get out of debt, the worker coordinator doesn't wait for any
// other request to be completed / other result be released.
//
// The mutex of s must be held.
func (s *Streamer) signalBudgetIfNoRequestsInProgressLocked() {
	s.mu.AssertHeld()
	if s.mu.numRequestsInFlight == 0 && s.results.numUnreleased() == 0 {
		s.budget.mu.waitForBudget.Signal()
	}
}

// adjustNumRequestsInFlight updates the number of requests that are currently
// in flight. This method should be called without holding the lock of s.
func (s *Streamer) adjustNumRequestsInFlight(delta int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.numRequestsInFlight += delta
	s.signalBudgetIfNoRequestsInProgressLocked()
}

type workerCoordinator struct {
	s              *Streamer
	txn            *kv.Txn
	lockWaitPolicy lock.WaitPolicy

	asyncSem *quotapool.IntPool

	// For request and response admission control.
	requestAdmissionHeader roachpb.AdmissionHeader
	responseAdmissionQ     *admission.WorkQueue
}

// mainLoop runs throughout the lifetime of the Streamer (from the first Enqueue
// call until Close) and routes the single-range batches for asynchronous
// execution. This function is dividing up the Streamer's budget for each of
// those batches and won't start executing the batches if the available budget
// is insufficient. The function exits when an error is encountered by one of
// the asynchronous requests.
func (w *workerCoordinator) mainLoop(ctx context.Context) {
	defer w.s.waitGroup.Done()
	for {
		if err := w.waitForRequests(ctx); err != nil {
			w.s.results.setError(err)
			return
		}

		var atLeastBytes int64
		// The higher the value of priority is, the lower the actual priority of
		// spilling. Use the maximum value by default.
		spillingPriority := math.MaxInt64
		w.s.requestsToServe.Lock()
		if !w.s.requestsToServe.emptyLocked() {
			// If we already have minTargetBytes set on the first request to be
			// issued, then use that.
			atLeastBytes = w.s.requestsToServe.nextLocked().minTargetBytes
			// The first request has the highest urgency among all current
			// requests to serve, so we use its priority to spill everything
			// with less urgency when necessary to free up the budget.
			spillingPriority = w.s.requestsToServe.nextLocked().priority()
		}
		w.s.requestsToServe.Unlock()

		avgResponseSize, shouldExit := w.getAvgResponseSize()
		if shouldExit {
			return
		}

		if atLeastBytes == 0 {
			atLeastBytes = avgResponseSize
		}

		shouldExit = w.waitUntilEnoughBudget(ctx, atLeastBytes, spillingPriority)
		if shouldExit {
			return
		}

		// Now check how many requests we can issue.
		maxNumRequestsToIssue, shouldExit := w.getMaxNumRequestsToIssue(ctx)
		if shouldExit {
			return
		}

		err := w.issueRequestsForAsyncProcessing(ctx, maxNumRequestsToIssue, avgResponseSize)
		if err != nil {
			w.s.results.setError(err)
			return
		}
	}
}

// logStatistics logs some of the statistics about the Streamer. It should be
// called at the end of the Streamer's lifecycle.
// TODO(yuzefovich): at the moment, these statistics will be attached to the
// tracing span of the Streamer's user. Some time has been spent to figure it
// out but led to no success. This should be cleaned up.
func (w *workerCoordinator) logStatistics(ctx context.Context) {
	avgResponseSize, _ := w.getAvgResponseSize()
	log.VEventf(
		ctx, 1,
		"enqueueCalls=%d enqueuedRequests=%d enqueuedSingleRangeRequests=%d "+
			"batchRequestsIssued=%d resumeBatchRequests=%d resumeSingleRangeRequests=%d "+
			"numSpilledResults=%d emptyBatchResponses=%d droppedBatchResponses=%d avgResponseSize=%s",
		w.s.enqueueCalls,
		w.s.enqueuedRequests,
		w.s.enqueuedSingleRangeRequests,
		atomic.LoadInt64(w.s.atomics.batchRequestsIssued),
		atomic.LoadInt64(&w.s.atomics.resumeBatchRequests),
		atomic.LoadInt64(&w.s.atomics.resumeSingleRangeRequests),
		w.s.results.numSpilledResults(),
		atomic.LoadInt64(&w.s.atomics.emptyBatchResponses),
		atomic.LoadInt64(&w.s.atomics.droppedBatchResponses),
		humanizeutil.IBytes(avgResponseSize),
	)
}

// waitForRequests blocks until there is at least one request to be served.
func (w *workerCoordinator) waitForRequests(ctx context.Context) error {
	w.s.requestsToServe.Lock()
	defer w.s.requestsToServe.Unlock()
	if w.s.requestsToServe.emptyLocked() {
		w.s.requestsToServe.waitLocked()
		// Check if the Streamer has been canceled or closed while we were
		// waiting.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		w.s.mu.Lock()
		shouldExit := w.s.results.error() != nil || w.s.mu.done
		w.s.mu.Unlock()
		if shouldExit {
			return nil
		}
		if buildutil.CrdbTestBuild {
			if w.s.requestsToServe.emptyLocked() {
				panic(errors.AssertionFailedf("unexpectedly zero requests to serve after waiting "))
			}
		}
	}
	return nil
}

func (w *workerCoordinator) getAvgResponseSize() (avgResponseSize int64, shouldExit bool) {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	avgResponseSize = w.s.mu.avgResponseEstimator.getAvgResponseSize()
	shouldExit = w.s.results.error() != nil || w.s.mu.done
	return avgResponseSize, shouldExit
}

// waitUntilEnoughBudget waits until atLeastBytes bytes is available in the
// budget.
//
// A boolean that indicates whether the coordinator should exit is returned.
func (w *workerCoordinator) waitUntilEnoughBudget(
	ctx context.Context, atLeastBytes int64, spillingPriority int,
) (shouldExit bool) {
	w.s.budget.mu.Lock()
	defer w.s.budget.mu.Unlock()
	for w.s.budget.limitBytes-w.s.budget.mu.acc.Used() < atLeastBytes {
		// There isn't enough budget at the moment.
		//
		// First, ask the results buffer to spill some results to disk in order
		// to free up budget.
		if ok, err := w.s.results.spill(
			ctx, atLeastBytes-(w.s.budget.limitBytes-w.s.budget.mu.acc.Used()), spillingPriority,
		); err != nil {
			w.s.results.setError(err)
			return true
		} else if ok {
			// The spilling was successful.
			return false
		}

		// The spilling didn't succeed, so we need to wait for budget to open
		// up.

		// Check whether there are any requests in progress.
		if w.s.getNumRequestsInProgress() == 0 {
			// We have a degenerate case when a single row is expected to exceed
			// the budget.
			return false
		}
		// We have to wait for some budget.release() calls.
		w.s.budget.mu.waitForBudget.Wait()
		// Check if the Streamer has been canceled or closed while we were
		// waiting.
		if ctx.Err() != nil {
			w.s.results.setError(ctx.Err())
			return true
		}
	}
	return false
}

// getMaxNumRequestsToIssue returns the maximum number of new async requests the
// worker coordinator can issue without exceeding streamerConcurrencyLimit
// limit. It blocks until at least one request can be issued.
//
// This behavior is needed to ensure that the creation of a new async task in
// performRequestAsync doesn't block on w.asyncSem. If it did block, then we
// could get into a deadlock because the main goroutine of the worker
// coordinator is holding the budget's mutex waiting for quota to open up while
// all asynchronous requests that could free up that quota would block on
// attempting to acquire the budget's mutex.
//
// A boolean that indicates whether the coordinator should exit is also
// returned.
func (w *workerCoordinator) getMaxNumRequestsToIssue(ctx context.Context) (_ int, shouldExit bool) {
	// Since the worker coordinator goroutine is the only one acquiring quota
	// from the semaphore, ApproximateQuota returns the precise quota at the
	// moment.
	q := w.asyncSem.ApproximateQuota()
	if q > 0 {
		return int(q), false
	}
	// The whole quota is currently used up, so we blockingly acquire a quota of
	// 1.
	alloc, err := w.asyncSem.Acquire(ctx, 1)
	if err != nil {
		w.s.results.setError(err)
		return 0, true
	}
	alloc.Release()
	return 1, false
}

// issueRequestsForAsyncProcessing iterates over the single-range requests
// (supplied by the requestsProvider) and issues them to be served
// asynchronously while there is enough budget available to receive the
// responses. Once the budget is exhausted, no new requests are issued, the only
// exception is made for the case when there are no requests in progress (both
// requests in flight as well as unreleased results), and in that scenario, a
// single request will be issued.
//
// maxNumRequestsToIssue specifies the maximum number of requests that can be
// issued as part of this call. The caller guarantees that w.asyncSem has at
// least that much quota available.
func (w *workerCoordinator) issueRequestsForAsyncProcessing(
	ctx context.Context, maxNumRequestsToIssue int, avgResponseSize int64,
) error {
	w.s.requestsToServe.Lock()
	defer w.s.requestsToServe.Unlock()
	w.s.budget.mu.Lock()
	defer w.s.budget.mu.Unlock()

	headOfLine := w.s.getNumRequestsInProgress() == 0
	var budgetIsExhausted bool
	for !w.s.requestsToServe.emptyLocked() && maxNumRequestsToIssue > 0 && !budgetIsExhausted {
		singleRangeReqs := w.s.requestsToServe.nextLocked()
		availableBudget := w.s.budget.limitBytes - w.s.budget.mu.acc.Used()
		// minAcceptableBudget is the minimum TargetBytes limit with which it
		// makes sense to issue this request (if we issue the request with
		// smaller limit, then it's very likely to come back with an empty
		// response).
		minAcceptableBudget := singleRangeReqs.minTargetBytes
		if minAcceptableBudget == 0 {
			minAcceptableBudget = avgResponseSize
		}
		if availableBudget < minAcceptableBudget {
			if !headOfLine {
				// We don't have enough budget available to serve this request,
				// and there are other requests in progress, so we'll wait for
				// some of them to finish.
				return nil
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
		// Make sure that targetBytes is sufficient to receive non-empty
		// response. Our estimate might be an under-estimate when responses vary
		// significantly in size.
		if targetBytes < singleRangeReqs.minTargetBytes {
			targetBytes = singleRangeReqs.minTargetBytes
		}
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
				// There are some requests in progress, so we'll let them
				// finish / be released.
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
				return nil
			}
			// We don't have any requests in progress, so we'll exit to be safe
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
		w.s.requestsToServe.removeNextLocked()
		maxNumRequestsToIssue--
		headOfLine = false
	}
	return nil
}

// budgetMuAlreadyLocked must be true if the caller is currently holding the
// budget's mutex.
func (w *workerCoordinator) asyncRequestCleanup(budgetMuAlreadyLocked bool) {
	if !budgetMuAlreadyLocked {
		// Since we're decrementing the number of requests in flight, we want to
		// make sure that the budget's mutex is locked, and it currently isn't.
		// This is needed so that if we signal the budget in
		// adjustNumRequestsInFlight, the worker coordinator doesn't miss the
		// signal.
		//
		// If we don't do this, then it is possible for the worker coordinator
		// to be blocked forever in waitUntilEnoughBudget. Namely, the following
		// sequence of events is possible:
		// 1. the worker coordinator checks that there are some requests in
		//    progress, then it goes to sleep before waiting on waitForBudget
		//    condition variable;
		// 2. the last request in flight exits without creating any Results (so
		//    that no Release() calls will happen in the future), it decrements
		//    the number of requests in flight, signals waitForBudget condition
		//    variable, but nobody is waiting on that, so no goroutine is woken
		//    up;
		// 3. the worker coordinator wakes up and starts waiting on the
		//    condition variable, forever.
		// Acquiring the budget's mutex makes sure that such sequence doesn't
		// occur.
		w.s.budget.mu.Lock()
		defer w.s.budget.mu.Unlock()
	} else {
		w.s.budget.mu.AssertHeld()
	}
	w.s.adjustNumRequestsInFlight(-1 /* delta */)
	w.s.waitGroup.Done()
}

// AsyncRequestOp is the operation name (in tracing) of all requests issued by
// the Streamer asynchronously.
const AsyncRequestOp = "streamer-lookup-async"

// performRequestAsync dispatches the given single-range batch for evaluation
// asynchronously. If the batch cannot be evaluated fully (due to exhausting its
// memory limitBytes), the "resume" single-range batch will be added into
// requestsToServe, and mainLoop will pick that up to process later.
//
// The caller is responsible for ensuring that there is enough quota in
// w.asyncSem to spin up a new goroutine for this request.
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
// headOfLine indicates whether this request is the current head of the line and
// there are no unreleased Results. Head-of-the-line requests are treated
// specially in a sense that they are allowed to put the budget into debt. The
// caller is responsible for ensuring that there is at most one asynchronous
// request with headOfLine=true at all times.
func (w *workerCoordinator) performRequestAsync(
	ctx context.Context, req singleRangeBatch, targetBytes int64, headOfLine bool,
) {
	w.s.waitGroup.Add(1)
	w.s.adjustNumRequestsInFlight(1 /* delta */)
	if err := w.s.stopper.RunAsyncTaskEx(
		ctx,
		stop.TaskOpts{
			TaskName: AsyncRequestOp,
			SpanOpt:  stop.ChildSpan,
			// Note that we don't wait for the semaphore since it's the caller's
			// responsibility to ensure that a new goroutine can be spun up.
			Sem: w.asyncSem,
		},
		func(ctx context.Context) {
			defer w.asyncRequestCleanup(false /* budgetMuAlreadyLocked */)
			ba := &roachpb.BatchRequest{}
			ba.Header.WaitPolicy = w.lockWaitPolicy
			ba.Header.TargetBytes = targetBytes
			ba.Header.AllowEmpty = !headOfLine
			ba.Header.WholeRowsOfSize = w.s.maxKeysPerRow
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
			br, pErr := w.txn.Send(ctx, ba)
			if pErr != nil {
				// TODO(yuzefovich): if err is
				// ReadWithinUncertaintyIntervalError and there is only a single
				// Streamer in a single local flow, attempt to transparently
				// refresh.
				w.s.results.setError(pErr.GoError())
				return
			}
			atomic.AddInt64(w.s.atomics.batchRequestsIssued, 1)

			// First, we have to reconcile the memory budget. We do it
			// separately from processing the results because we want to know
			// how many Gets and Scans need to be allocated for the ResumeSpans,
			// if any are present. At the moment, due to limitations of the KV
			// layer (#75452) we cannot reuse original requests because the KV
			// doesn't allow mutability.
			fp, err := calculateFootprint(req, br)
			if err != nil {
				w.s.results.setError(err)
				return
			}

			// Now adjust the budget based on the actual memory footprint of
			// non-empty responses as well as resume spans, if any.
			respOverestimate := targetBytes - fp.memoryFootprintBytes
			reqOveraccounted := req.reqsReservedBytes - fp.resumeReqsMemUsage
			if fp.resumeReqsMemUsage == 0 {
				// There will be no resume request, so we will lose the
				// reference to the slices in req and can release its memory
				// reservation.
				reqOveraccounted += req.overheadAccountedFor
			}
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
				if err = w.s.budget.consume(ctx, toConsume, headOfLine /* allowDebt */); err != nil {
					atomic.AddInt64(&w.s.atomics.droppedBatchResponses, 1)
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
						w.s.requestsToServe.add(req)
						return
					}
					// The error indicates that the root memory pool has been
					// exhausted, so we'll exit to be safe (in order not to OOM
					// the node).
					// TODO(yuzefovich): if the response contains multiple rows,
					// consider adding the request back to be served with a note
					// to issue it with smaller targetBytes.
					w.s.results.setError(err)
					return
				}
			}

			// Do admission control after we've finalized the memory accounting.
			if br != nil && w.responseAdmissionQ != nil {
				responseAdmission := admission.WorkInfo{
					TenantID:   roachpb.SystemTenantID,
					Priority:   admissionpb.WorkPriority(w.requestAdmissionHeader.Priority),
					CreateTime: w.requestAdmissionHeader.CreateTime,
				}
				if _, err = w.responseAdmissionQ.Admit(ctx, responseAdmission); err != nil {
					w.s.results.setError(err)
					return
				}
			}

			// Finally, process the results and add the ResumeSpans to be
			// processed as well.
			processSingleRangeResponse(ctx, w.s, req, br, fp)
		}); err != nil {
		// The new goroutine for the request wasn't spun up, so we have to
		// perform the cleanup of this request ourselves.
		w.asyncRequestCleanup(true /* budgetMuAlreadyLocked */)
		w.s.results.setError(err)
	}
}

// singleRangeBatchResponseFootprint is the footprint of the shape of the
// response to a singleRangeBatch.
type singleRangeBatchResponseFootprint struct {
	// memoryFootprintBytes tracks the total memory footprint of non-empty
	// responses. This will be equal to the sum of memory tokens created for all
	// Results.
	memoryFootprintBytes int64
	// resumeReqsMemUsage tracks the memory usage of the requests for the
	// ResumeSpans.
	resumeReqsMemUsage int64
	// numGetResults and numScanResults indicate how many Result objects will
	// need to be created for Get and Scan responses, respectively.
	numGetResults, numScanResults         int
	numIncompleteGets, numIncompleteScans int
}

func (fp singleRangeBatchResponseFootprint) hasResults() bool {
	return fp.numGetResults > 0 || fp.numScanResults > 0
}

func (fp singleRangeBatchResponseFootprint) hasIncomplete() bool {
	return fp.numIncompleteGets > 0 || fp.numIncompleteScans > 0
}

// calculateFootprint calculates the memory footprint of the batch response as
// well as of the requests that will have to be created for the ResumeSpans.
func calculateFootprint(
	req singleRangeBatch, br *roachpb.BatchResponse,
) (fp singleRangeBatchResponseFootprint, _ error) {
	for i, resp := range br.Responses {
		reply := resp.GetInner()
		switch req.reqs[i].GetInner().(type) {
		case *roachpb.GetRequest:
			get := reply.(*roachpb.GetResponse)
			if get.IntentValue != nil {
				return fp, errors.AssertionFailedf(
					"unexpectedly got an IntentValue back from a SQL GetRequest %v", *get.IntentValue,
				)
			}
			if get.ResumeSpan != nil {
				// This Get wasn't completed.
				fp.resumeReqsMemUsage += requestSize(get.ResumeSpan.Key, get.ResumeSpan.EndKey)
				fp.numIncompleteGets++
			} else {
				// This Get was completed.
				fp.memoryFootprintBytes += getResponseSize(get)
				fp.numGetResults++
			}
		case *roachpb.ScanRequest:
			scan := reply.(*roachpb.ScanResponse)
			if len(scan.Rows) > 0 {
				return fp, errors.AssertionFailedf(
					"unexpectedly got a ScanResponse using KEY_VALUES response format",
				)
			}
			if len(scan.IntentRows) > 0 {
				return fp, errors.AssertionFailedf(
					"unexpectedly got a ScanResponse with non-nil IntentRows",
				)
			}
			if len(scan.BatchResponses) > 0 {
				fp.memoryFootprintBytes += scanResponseSize(scan)
			}
			if len(scan.BatchResponses) > 0 || scan.ResumeSpan == nil {
				fp.numScanResults++
			}
			if scan.ResumeSpan != nil {
				// This Scan wasn't completed.
				fp.resumeReqsMemUsage += requestSize(scan.ResumeSpan.Key, scan.ResumeSpan.EndKey)
				fp.numIncompleteScans++
			}
		}
	}
	return fp, nil
}

// processSingleRangeResponse creates a Result for each non-empty response found
// in the BatchResponse. The ResumeSpans, if found, are added into a new
// singleRangeBatch request that is added to be picked up by the mainLoop of the
// worker coordinator. This method assumes that req is no longer needed by the
// caller, so the slices from req are reused for the ResumeSpans.
//
// It also assumes that the budget has already been reconciled with the
// reservations for Results that will be created.
func processSingleRangeResponse(
	ctx context.Context,
	s *Streamer,
	req singleRangeBatch,
	br *roachpb.BatchResponse,
	fp singleRangeBatchResponseFootprint,
) {
	processSingleRangeResults(ctx, s, req, br, fp)
	if fp.hasIncomplete() {
		resumeReq := buildResumeSingleRangeBatch(s, req, br, fp)
		s.requestsToServe.add(resumeReq)
	}
}

// processSingleRangeResults examines the body of a BatchResponse and its
// associated singleRangeBatch to add any results. If there are no results, this
// function is a no-op. This function handles the associated bookkeeping on the
// streamer as it processes the results.
func processSingleRangeResults(
	ctx context.Context,
	s *Streamer,
	req singleRangeBatch,
	br *roachpb.BatchResponse,
	fp singleRangeBatchResponseFootprint,
) {
	// If there are no results, this function has nothing to do.
	if !fp.hasResults() {
		return
	}

	// We will add some Results into the results buffer, and doneAddingLocked()
	// call below requires that the budget's mutex is held. It also must be
	// acquired before the streamer's mutex is locked, so we have to do this
	// right away.
	// TODO(yuzefovich): check whether the lock contention on this mutex is
	// noticeable and possibly refactor the code so that the budget's mutex is
	// only acquired for the duration of doneAddingLocked().
	s.budget.mu.Lock()
	defer s.budget.mu.Unlock()
	s.mu.Lock()

	// TODO(yuzefovich): some of the responses might be partial, yet the
	// estimator doesn't distinguish the footprint of the full response vs
	// the partial one. Think more about this.
	s.mu.avgResponseEstimator.update(
		fp.memoryFootprintBytes, int64(fp.numGetResults+fp.numScanResults),
	)

	// If we have any Scan results to create and the Scan requests can return
	// multiple rows, we'll need to consult s.mu.numRangesPerScanRequest, so
	// we'll defer unlocking the streamer's mutex. However, if only Get results
	// or Scan results of single rows will be created, we can unlock the
	// streamer's mutex right away.
	if fp.numScanResults > 0 && !s.hints.SingleRowLookup {
		defer s.mu.Unlock()
	} else {
		s.mu.Unlock()
	}

	// Now we can get the resultsBuffer's mutex - it must be acquired after
	// the Streamer's one.
	s.results.Lock()
	defer s.results.Unlock()
	defer s.results.doneAddingLocked(ctx)

	// memoryTokensBytes accumulates all reservations that are made for all
	// Results created below. The accounting for these reservations has already
	// been performed, and memoryTokensBytes should be exactly equal to
	// memoryFootprintBytes, so we use it only as an additional check.
	var memoryTokensBytes int64
	for i, resp := range br.Responses {
		position := req.positions[i]
		var subRequestIdx int32
		if req.subRequestIdx != nil {
			subRequestIdx = req.subRequestIdx[i]
		}
		reply := resp.GetInner()
		switch response := reply.(type) {
		case *roachpb.GetResponse:
			get := response
			if get.ResumeSpan != nil {
				// This Get wasn't completed.
				continue
			}
			// This Get was completed.
			result := Result{
				GetResp:        get,
				Position:       position,
				subRequestIdx:  subRequestIdx,
				subRequestDone: true,
			}
			result.memoryTok.streamer = s
			result.memoryTok.toRelease = getResponseSize(get)
			memoryTokensBytes += result.memoryTok.toRelease
			if buildutil.CrdbTestBuild {
				if fp.numGetResults == 0 {
					panic(errors.AssertionFailedf(
						"unexpectedly found a non-empty GetResponse when numGetResults is zero",
					))
				}
			}
			s.results.addLocked(result)

		case *roachpb.ScanResponse:
			scan := response
			if len(scan.BatchResponses) == 0 && scan.ResumeSpan != nil {
				// Only the first part of the conditional is true whenever we
				// received an empty response for the Scan request (i.e. there
				// was no data in the span to scan). In such a scenario we still
				// create a Result with no data that the client will skip over
				// (this approach makes it easier to support Scans that span
				// multiple ranges and the last range has no data in it - we
				// want to be able to set scanComplete field on such an empty
				// Result).
				continue
			}
			result := Result{
				Position:       position,
				subRequestIdx:  subRequestIdx,
				subRequestDone: scan.ResumeSpan == nil,
			}
			result.memoryTok.streamer = s
			result.memoryTok.toRelease = scanResponseSize(scan)
			memoryTokensBytes += result.memoryTok.toRelease
			result.ScanResp = scan
			if s.hints.SingleRowLookup {
				result.scanComplete = true
			} else if scan.ResumeSpan == nil {
				// The scan within the range is complete.
				if s.mode == OutOfOrder {
					s.mu.numRangesPerScanRequest[position]--
					result.scanComplete = s.mu.numRangesPerScanRequest[position] == 0
				} else {
					// In InOrder mode, the scan is marked as complete when the
					// last sub-request is satisfied. Note that it is ok if the
					// previous sub-requests haven't been satisfied yet - the
					// inOrderResultsBuffer will not emit this Result until the
					// previous sub-requests are responded to.
					numSubRequests := s.mu.numRangesPerScanRequest[position]
					result.scanComplete = result.subRequestIdx+1 == numSubRequests
				}
			}
			if buildutil.CrdbTestBuild {
				if fp.numScanResults == 0 {
					panic(errors.AssertionFailedf(
						"unexpectedly found a ScanResponse when numScanResults is zero",
					))
				}
			}
			s.results.addLocked(result)
		}
	}

	if buildutil.CrdbTestBuild {
		if fp.memoryFootprintBytes != memoryTokensBytes {
			panic(errors.AssertionFailedf(
				"different calculation of memory footprint\ncalculateFootprint: %d bytes\n"+
					"processSingleRangeResults: %d bytes", fp.memoryFootprintBytes, memoryTokensBytes,
			))
		}
	}
}

// buildResumeSingleRangeBatch consumes a BatchResponse for a singleRangeBatch
// which contains incomplete requests and returns the next singleRangeBatch to
// be submitted. Note that for maximal memory reuse, the original request and
// response may no longer be utilized.
//
// Note that it should only be called if the response has any incomplete
// requests.
func buildResumeSingleRangeBatch(
	s *Streamer,
	req singleRangeBatch,
	br *roachpb.BatchResponse,
	fp singleRangeBatchResponseFootprint,
) (resumeReq singleRangeBatch) {
	numIncompleteRequests := fp.numIncompleteGets + fp.numIncompleteScans
	// We have to allocate the new Get and Scan requests, but we can reuse the
	// reqs and the positions slices.
	resumeReq.reqs = req.reqs[:numIncompleteRequests]
	resumeReq.positions = req.positions[:0]
	resumeReq.subRequestIdx = req.subRequestIdx[:0]
	// We've already reconciled the budget with the actual reservation for the
	// requests with the ResumeSpans.
	resumeReq.reqsReservedBytes = fp.resumeReqsMemUsage
	resumeReq.overheadAccountedFor = req.overheadAccountedFor
	// Note that due to limitations of the KV layer (#75452) we cannot reuse
	// original requests because the KV doesn't allow mutability (and all
	// requests are modified by txnSeqNumAllocator, even if they are not
	// evaluated due to TargetBytes limit).
	gets := make([]struct {
		req   roachpb.GetRequest
		union roachpb.RequestUnion_Get
	}, fp.numIncompleteGets)
	scans := make([]struct {
		req   roachpb.ScanRequest
		union roachpb.RequestUnion_Scan
	}, fp.numIncompleteScans)
	var resumeReqIdx int
	for i, resp := range br.Responses {
		position := req.positions[i]
		reply := resp.GetInner()
		switch response := reply.(type) {
		case *roachpb.GetResponse:
			get := response
			if get.ResumeSpan == nil {
				continue
			}
			// This Get wasn't completed - create a new request according to the
			// ResumeSpan and include it into the batch.
			newGet := gets[0]
			gets = gets[1:]
			newGet.req.SetSpan(*get.ResumeSpan)
			newGet.req.KeyLocking = s.keyLocking
			newGet.union.Get = &newGet.req
			resumeReq.reqs[resumeReqIdx].Value = &newGet.union
			resumeReq.positions = append(resumeReq.positions, position)
			if req.subRequestIdx != nil {
				resumeReq.subRequestIdx = append(resumeReq.subRequestIdx, req.subRequestIdx[i])
			}
			if resumeReq.minTargetBytes == 0 {
				resumeReq.minTargetBytes = get.ResumeNextBytes
			}
			resumeReqIdx++

		case *roachpb.ScanResponse:
			scan := response
			if scan.ResumeSpan == nil {
				continue
			}
			// This Scan wasn't completed - create a new request according to
			// the ResumeSpan and include it into the batch.
			newScan := scans[0]
			scans = scans[1:]
			newScan.req.SetSpan(*scan.ResumeSpan)
			newScan.req.ScanFormat = roachpb.BATCH_RESPONSE
			newScan.req.KeyLocking = s.keyLocking
			newScan.union.Scan = &newScan.req
			resumeReq.reqs[resumeReqIdx].Value = &newScan.union
			resumeReq.positions = append(resumeReq.positions, position)
			if req.subRequestIdx != nil {
				resumeReq.subRequestIdx = append(resumeReq.subRequestIdx, req.subRequestIdx[i])
			}
			if resumeReq.minTargetBytes == 0 {
				resumeReq.minTargetBytes = scan.ResumeNextBytes
			}
			resumeReqIdx++

			if s.hints.SingleRowLookup {
				// Unset the ResumeSpan on the result in order to not
				// confuse the user of the Streamer. Non-nil resume span was
				// already included into resumeReq above.
				//
				// When SingleRowLookup is false, this will be done in
				// finalizeSingleRangeResults().
				scan.ResumeSpan = nil
			}
		}
	}

	if !fp.hasResults() {
		// We received an empty response.
		atomic.AddInt64(&s.atomics.emptyBatchResponses, 1)
		if req.minTargetBytes != 0 {
			// We previously have already received an empty response for this
			// request, and minTargetBytes wasn't sufficient. Make sure that
			// minTargetBytes on the resume request has increased.
			if resumeReq.minTargetBytes <= req.minTargetBytes {
				// Since ResumeNextBytes is populated on a best-effort basis, we
				// cannot rely on it to make progress, so we make sure that if
				// minTargetBytes hasn't increased for the resume request, we
				// use the double of the original target.
				resumeReq.minTargetBytes = 2 * req.minTargetBytes
				if resumeReq.minTargetBytes < 0 {
					// Prevent the overflow.
					resumeReq.minTargetBytes = math.MaxInt64
				}
			}
		}
	}

	// Make sure to nil out old requests that we didn't include into the resume
	// request. We don't have to do this if there aren't any incomplete requests
	// since req and resumeReq will be garbage collected on their own.
	for i := numIncompleteRequests; i < len(req.reqs); i++ {
		req.reqs[i] = roachpb.RequestUnion{}
	}
	atomic.AddInt64(&s.atomics.resumeBatchRequests, 1)
	atomic.AddInt64(&s.atomics.resumeSingleRangeRequests, int64(numIncompleteRequests))

	return resumeReq
}
