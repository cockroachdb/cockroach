// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvadmission"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/uncertainty"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
)

// executeReadOnlyBatch is the execution logic for client requests which do not
// mutate the range's replicated state. The method uses a single RocksDB
// iterator to evaluate the batch and then updates the timestamp cache to
// reflect the key spans that it read.
func (r *Replica) executeReadOnlyBatch(
	ctx context.Context, ba *kvpb.BatchRequest, g *concurrency.Guard,
) (
	br *kvpb.BatchResponse,
	_ *concurrency.Guard,
	_ *kvadmission.StoreWriteBytes,
	pErr *kvpb.Error,
) {
	r.readOnlyCmdMu.RLock()
	defer r.readOnlyCmdMu.RUnlock()

	// Verify that the batch can be executed.
	st, err := r.checkExecutionCanProceedBeforeStorageSnapshot(ctx, ba, g)
	if err != nil {
		return nil, g, nil, kvpb.NewError(err)
	}

	if fn := r.store.TestingKnobs().PreStorageSnapshotButChecksCompleteInterceptor; fn != nil {
		fn(r)
	}

	// Compute the transaction's local uncertainty limit using observed
	// timestamps, which can help avoid uncertainty restarts.
	ui := uncertainty.ComputeInterval(&ba.Header, st, r.Clock().MaxOffset())

	// Evaluate read-only batch command.
	rec := NewReplicaEvalContext(
		ctx, r, g.LatchSpans(), ba.RequiresClosedTSOlderThanStorageSnapshot(), ba.AdmissionHeader)
	defer rec.Release()

	// TODO(irfansharif): It's unfortunate that in this read-only code path,
	// we're stuck with a ReadWriter because of the way evaluateBatch is
	// designed.
	rw := r.store.TODOEngine().NewReadOnly(storage.StandardDurability)
	if !rw.ConsistentIterators() {
		// This is not currently needed for correctness, but future optimizations
		// may start relying on this, so we assert here.
		panic("expected consistent iterators")
	}
	// Pin engine state eagerly so that all iterators created over this Reader are
	// based off the state of the engine as of this point and are mutually
	// consistent.
	readCategory := fs.BatchEvalReadCategory
	for _, union := range ba.Requests {
		inner := union.GetInner()
		switch inner.(type) {
		case *kvpb.ScanRequest, *kvpb.ReverseScanRequest:
			readCategory = batcheval.ScanReadCategory(ba.AdmissionHeader)
		}
		break
	}
	if err := rw.PinEngineStateForIterators(readCategory); err != nil {
		return nil, g, nil, kvpb.NewError(err)
	}
	if util.RaceEnabled {
		rw = spanset.NewReadWriterAt(rw, g.LatchSpans(), ba.Timestamp)
	}
	defer rw.Close()

	if err := r.checkExecutionCanProceedAfterStorageSnapshot(ctx, ba, st); err != nil {
		return nil, g, nil, kvpb.NewError(err)
	}
	ok, stillNeedsInterleavedIntents, pErr := r.canDropLatchesBeforeEval(ctx, rw, ba, g, st)
	if pErr != nil {
		return nil, g, nil, pErr
	}
	evalPath := readOnlyDefault
	if ok {
		// Since the concurrency manager has sequenced this request all the intents
		// that are in the concurrency manager's lock table, and we've scanned the
		// replicated lock-table keyspace above in `canDropLatchesBeforeEval`, we
		// can be sure that if we reached this point, we will not conflict with any
		// of them during evaluation. This in turn means that we can bump the
		// timestamp cache *before* evaluation without risk of starving writes.
		// Consequently, we're free to release latches here since we've acquired a
		// pebble iterator as long as we're performing a non-locking read (also
		// checked in `canDropLatchesBeforeEval`). Note that this also requires that
		// the request is not being optimistically evaluated (optimistic evaluation
		// does not wait for latches or check locks).
		log.VEventf(ctx, 3, "lock table scan complete without conflicts; dropping latches early")
		r.store.metrics.ReplicaReadBatchDroppedLatchesBeforeEval.Inc(1)
		if !stillNeedsInterleavedIntents {
			r.store.metrics.ReplicaReadBatchWithoutInterleavingIter.Inc(1)
			evalPath = readOnlyWithoutInterleavedIntents
		}
		r.updateTimestampCacheAndDropLatches(ctx, g, ba, nil /* br */, nil /* pErr */, st)
		g = nil
	}

	var result result.Result
	ba, br, result, pErr = r.executeReadOnlyBatchWithServersideRefreshes(ctx, rw, rec, ba, g, &st, ui, evalPath)

	// If the request hit a server-side concurrency retry error, immediately
	// propagate the error. Don't assume ownership of the concurrency guard.
	if isConcurrencyRetryError(pErr) {
		if g != nil && g.EvalKind == concurrency.OptimisticEval {
			// Since this request was not holding latches, it could have raced with
			// intent resolution. So we can't trust it to add discovered locks, if
			// there is a latch conflict. This means that a discovered lock plus a
			// latch conflict will likely cause the request to evaluate at least 3
			// times: optimistically; pessimistically and add the discovered lock;
			// wait until resolution and evaluate pessimistically again.
			//
			// TODO(sumeer): scans and gets are correctly setting the resume span
			// when returning a LockConflictError. I am not sure about other
			// concurrency errors. We could narrow the spans we check the latch
			// conflicts for by using collectSpansRead as done below in the
			// non-error path.
			if !g.CheckOptimisticNoLatchConflicts() {
				return nil, g, nil, kvpb.NewError(kvpb.NewOptimisticEvalConflictsError())
			}
		}
		pErr = maybeAttachLease(pErr, &st.Lease)
		return nil, g, nil, pErr
	}

	if g != nil && g.EvalKind == concurrency.OptimisticEval {
		if pErr == nil {
			// Gather the spans that were read -- we distinguish the spans in the
			// request from the spans that were actually read, using resume spans in
			// the response.
			latchSpansRead, lockSpansRead, err := r.collectSpansRead(ba, br)
			if err != nil {
				return nil, g, nil, kvpb.NewError(err)
			}
			defer latchSpansRead.Release()
			defer lockSpansRead.Release()
			if ok := g.CheckOptimisticNoConflicts(latchSpansRead, lockSpansRead); !ok {
				return nil, g, nil, kvpb.NewError(kvpb.NewOptimisticEvalConflictsError())
			}
		} else {
			// There was an error, that was not classified as a concurrency retry
			// error, and this request was not holding latches. This should be rare,
			// and in the interest of not having subtle correctness bugs, we retry
			// pessimistically.
			return nil, g, nil, kvpb.NewError(kvpb.NewOptimisticEvalConflictsError())
		}
	}

	// Handle any local (leaseholder-only) side-effects of the request.
	//
	// Processing these is fine for optimistic evaluation since these were non
	// conflicting intents so intent resolution could have been racing with this
	// request even if latches were held.
	intents := result.Local.DetachEncounteredIntents()
	if pErr == nil {
		pErr = r.handleReadOnlyLocalEvalResult(ctx, ba, result.Local)
	}
	if g != nil {
		// If we didn't already drop latches earlier, do so now.
		r.updateTimestampCacheAndDropLatches(ctx, g, ba, br, pErr, st)
		g = nil
	}

	// Semi-synchronously process any intents that need resolving here in order
	// to apply back pressure on the client which generated them. The resolution
	// is semi-synchronous in that there is a limited number of outstanding
	// asynchronous resolution tasks allowed after which further calls will
	// block. The limited number of asynchronous resolution tasks ensures that
	// the number of goroutines doing intent resolution does not diverge from
	// the number of workload goroutines (see
	// https://github.com/cockroachdb/cockroach/issues/4925#issuecomment-193015586
	// for an old problem predating such a limit).
	if len(intents) > 0 {
		log.Eventf(ctx, "submitting %d intents to asynchronous processing", len(intents))
		// We only allow synchronous intent resolution for consistent requests.
		// Intent resolution is async/best-effort for inconsistent requests and
		// for requests using the SkipLocked wait policy.
		//
		// An important case where this logic is necessary is for RangeLookup
		// requests. In their case, synchronous intent resolution can deadlock
		// if the request originated from the local node which means the local
		// range descriptor cache has an in-flight RangeLookup request which
		// prohibits any concurrent requests for the same range. See #17760.
		allowSyncProcessing := ba.ReadConsistency == kvpb.CONSISTENT &&
			ba.WaitPolicy != lock.WaitPolicy_SkipLocked
		if err := r.store.intentResolver.CleanupIntentsAsync(
			ctx,
			ba.AdmissionHeader,
			intents,
			allowSyncProcessing,
		); err != nil {
			log.Warningf(ctx, "%v", err)
		}
	}

	if pErr != nil {
		log.VErrEventf(ctx, 3, "%v", pErr.String())
	} else {
		keysRead, bytesRead := getBatchResponseReadStats(br)
		r.loadStats.RecordReadKeys(keysRead)
		r.loadStats.RecordReadBytes(bytesRead)
		log.Event(ctx, "read completed")
	}
	return br, nil, nil, pErr
}

// updateTimestampCacheAndDropLatches updates the timestamp cache and releases
// the concurrency guard.
// Note:
// - If `br` is nil, then this method assumes that latches are being released
// before evaluation of the request, and the timestamp cache is updated based
// only on the spans declared in the request.
// - The update to the timestamp cache is not gated on pErr == nil, since
// certain semantic errors (e.g. ConditionFailedError on CPut) require updating
// the timestamp cache (see updatesTSCacheOnErr).
// - For optimistic evaluation, used for limited scans, the update to the
// timestamp cache limits itself to the spans that were read, by using the
// ResumeSpans.
func (r *Replica) updateTimestampCacheAndDropLatches(
	ctx context.Context,
	g *concurrency.Guard,
	ba *kvpb.BatchRequest,
	br *kvpb.BatchResponse,
	pErr *kvpb.Error,
	st kvserverpb.LeaseStatus,
) {
	ec := makeUnreplicatedEndCmds(r, g, st)
	ec.done(ctx, ba, br, pErr)
}

var allowDroppingLatchesBeforeEval = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.transaction.dropping_latches_before_eval.enabled",
	"if enabled, allows certain read-only KV requests to drop latches before they evaluate",
	true,
)

// canDropLatchesBeforeEval determines whether a given batch request can proceed
// with evaluation without continuing to hold onto its latches[1] and if so,
// whether the evaluation of the requests in the batch needs an intent
// interleaving iterator[2].
//
// [1] whether the request can safely release latches at this point in the
// execution.
// For certain qualifying types of requests (certain types of read-only
// requests: see `canReadOnlyRequestDropLatchesBeforeEval`), this method
// performs a scan of the lock table keyspace corresponding to the latch spans
// declared by the BatchRequest.
// If no conflicting intents are found, then it is deemed safe for this request
// to release its latches at this point. This is because read-only requests
// evaluate over a stable pebble snapshot (see the call to
// `PinEngineStateForIterators` in `executeReadOnlyBatch`), so if there are no
// lock conflicts, the rest of the execution is guaranteed to be isolated from
// the effects of other requests.
// If any conflicting intents are found, then it returns a LockConflictError
// which needs to be handled by the caller before proceeding.
//
// [2] if the request can drop its latches early, whether it needs an intent
// interleaving iterator to perform its evaluation.
// If the aforementioned lock table scan determines that any of the requests in
// the batch may need access to the intent history of a key, then an intent
// interleaving iterator is needed to perform the evaluation.
func (r *Replica) canDropLatchesBeforeEval(
	ctx context.Context,
	rw storage.ReadWriter,
	ba *kvpb.BatchRequest,
	g *concurrency.Guard,
	st kvserverpb.LeaseStatus,
) (ok, stillNeedsIntentInterleaving bool, pErr *kvpb.Error) {
	if !allowDroppingLatchesBeforeEval.Get(&r.store.cfg.Settings.SV) ||
		!canReadOnlyRequestDropLatchesBeforeEval(ba, g) {
		// If the request does not qualify, we neither drop latches nor use a
		// non-interleaving iterator.
		return false /* ok */, true /* stillNeedsIntentInterleaving */, nil
	}

	log.VEventf(
		ctx, 3, "can drop latches early for batch (%v); scanning lock table first to detect conflicts", ba,
	)

	maxLockConflicts := storage.MaxConflictsPerLockConflictError.Get(&r.store.cfg.Settings.SV)
	targetLockConflictBytes := storage.TargetBytesPerLockConflictError.Get(&r.store.cfg.Settings.SV)
	var intents []roachpb.Intent
	// Check if any of the requests within the batch need to resolve any intents
	// or if any of them need to use an intent interleaving iterator.
	for _, req := range ba.Requests {
		reqHeader := req.GetInner().Header()
		start, end := reqHeader.Key, reqHeader.EndKey
		var txnID uuid.UUID
		if ba.Txn != nil {
			txnID = ba.Txn.ID
		}
		needsIntentInterleavingForThisRequest, err := storage.ScanConflictingIntentsForDroppingLatchesEarly(
			ctx, rw, txnID, ba.Header.Timestamp, start, end, &intents, maxLockConflicts, targetLockConflictBytes,
		)
		if err != nil {
			return false /* ok */, true /* stillNeedsIntentInterleaving */, kvpb.NewError(
				errors.Wrap(err, "scanning intents"),
			)
		}
		stillNeedsIntentInterleaving = stillNeedsIntentInterleaving || needsIntentInterleavingForThisRequest
		if maxLockConflicts != 0 && int64(len(intents)) >= maxLockConflicts {
			break
		}
	}
	if len(intents) > 0 {
		return false /* ok */, false /* stillNeedsIntentInterleaving */, maybeAttachLease(
			kvpb.NewError(&kvpb.LockConflictError{Locks: roachpb.AsLocks(intents)}), &st.Lease,
		)
	}
	// If there were no conflicts, then the request can drop its latches and
	// proceed with evaluation.
	return true /* ok */, stillNeedsIntentInterleaving, nil
}

// evalContextWithAccount wraps an EvalContext to provide a non-nil
// mon.BoundAccount. This wrapping is conditional on various factors, and
// specific to a request (see executeReadOnlyBatchWithServersideRefreshes),
// which is why the implementation of EvalContext by Replica does not by
// default provide a non-nil mon.BoundAccount.
//
// If we start using evalContextWithAccount on more code paths we should
// consider using it everywhere and lift it to an earlier point in the code.
// Then code that decides that we need a non-nil BoundAccount can set a field
// instead of wrapping.
type evalContextWithAccount struct {
	batcheval.EvalContext
	memAccount *mon.BoundAccount
}

var evalContextWithAccountPool = sync.Pool{
	New: func() interface{} {
		return &evalContextWithAccount{}
	},
}

// newEvalContextWithAccount creates an evalContextWithAccount with an account
// connected to the given monitor. It uses a sync.Pool.
func newEvalContextWithAccount(
	ctx context.Context, evalCtx batcheval.EvalContext, mon *mon.BytesMonitor,
) *evalContextWithAccount {
	ec := evalContextWithAccountPool.Get().(*evalContextWithAccount)
	ec.EvalContext = evalCtx
	if ec.memAccount != nil {
		ec.memAccount.Init(ctx, mon)
	} else {
		acc := mon.MakeBoundAccount()
		ec.memAccount = &acc
	}
	return ec
}

// close returns the accounted memory and returns objects to the sync.Pool.
func (e *evalContextWithAccount) close(ctx context.Context) {
	e.memAccount.Close(ctx)
	// Clear the BoundAccount struct, so it can be later reused.
	*e.memAccount = mon.BoundAccount{}
	evalContextWithAccountPool.Put(e)
}
func (e evalContextWithAccount) GetResponseMemoryAccount() *mon.BoundAccount {
	return e.memAccount
}

// batchEvalPath enumerates the different evaluation paths that can be taken by
// a batch.
type batchEvalPath int

const (
	// readOnlyDefault is the default evaluation path taken by read only requests.
	readOnlyDefault batchEvalPath = iota
	// readOnlyWithoutInterleavedIntents indicates that the request does not need
	// an intent interleaving iterator during its evaluation.
	readOnlyWithoutInterleavedIntents
	readWrite
)

// executeReadOnlyBatchWithServersideRefreshes invokes evaluateBatch and retries
// at a higher timestamp in the event of some retriable errors if allowed by the
// batch/txn.
func (r *Replica) executeReadOnlyBatchWithServersideRefreshes(
	ctx context.Context,
	rw storage.ReadWriter,
	rec batcheval.EvalContext,
	ba *kvpb.BatchRequest,
	g *concurrency.Guard,
	st *kvserverpb.LeaseStatus,
	ui uncertainty.Interval,
	evalPath batchEvalPath,
) (_ *kvpb.BatchRequest, br *kvpb.BatchResponse, res result.Result, pErr *kvpb.Error) {
	log.Event(ctx, "executing read-only batch")

	var rootMonitor *mon.BytesMonitor
	// Only do memory allocation accounting if the request did not originate
	// locally, or for a local request that has reserved no memory. Local
	// requests (originating in DistSQL) do memory accounting before issuing the
	// request. Even though the accounting for the first request in the caller
	// is small (the NoMemoryReservedAtSource=true case), subsequent ones use
	// the size of the response for subsequent requests (see row.txnKVFetcher).
	// Note that we could additionally add an OR-clause with
	// ba.AdmissionHeader.Source != FROM_SQL for the if-block that does memory
	// accounting. We don't do that currently since there are some SQL requests
	// that are not marked as FROM_SQL.
	//
	// This whole scheme could be tightened, both in terms of marking, and
	// compensating for the amount of memory reserved at the source.
	//
	// TODO(sumeer): for multi-tenant KV we should be accounting on a per-tenant
	// basis and not letting a single tenant consume all the memory (we could
	// place a limit equal to total/2).
	if ba.AdmissionHeader.SourceLocation != kvpb.AdmissionHeader_LOCAL ||
		ba.AdmissionHeader.NoMemoryReservedAtSource {
		// rootMonitor will never be nil in production settings, but it can be nil
		// for tests that do not have a monitor.
		rootMonitor = r.store.getRootMemoryMonitorForKV()
	}
	var boundAccount *mon.BoundAccount
	if rootMonitor != nil {
		evalCtx := newEvalContextWithAccount(ctx, rec, rootMonitor)
		boundAccount = evalCtx.memAccount
		// Closing evalCtx also closes boundAccount. Memory is not actually
		// released when this function returns, but at least the batch is fully
		// evaluated. Ideally we would like to release after grpc has sent the
		// response, but there are no interceptors at that stage. The interceptors
		// execute before the response is marshaled in Server.processUnaryRPC by
		// calling sendResponse. We are intentionally not using finalizers because
		// they delay GC and because they have had bugs in the past (and can
		// prevent GC of objects with cyclic references).
		defer evalCtx.close(ctx)
		rec = evalCtx
	}

	for retries := 0; ; retries++ {
		if retries > 0 {
			if boundAccount != nil {
				boundAccount.Clear(ctx)
			}
			log.VEventf(ctx, 2, "server-side retry of batch")
		}
		now := timeutil.Now()
		br, res, pErr = evaluateBatch(
			ctx, kvserverbase.CmdIDKey(""), rw, rec, nil /* ms */, ba, g,
			st, ui, evalPath, false, /* omitInRangefeeds */
		)
		r.store.metrics.ReplicaReadBatchEvaluationLatency.RecordValue(timeutil.Since(now).Nanoseconds())
		// Allow only one retry.
		if pErr == nil || retries > 0 {
			break
		}
		// If we can retry, set a higher batch timestamp and continue.
		//
		// Note that if the batch request has already released its latches (as
		// indicated by the latch guard being nil) before this point, then it cannot
		// retry at a higher timestamp because it is not isolated at higher
		// timestamps.
		latchesHeld := g != nil
		var ok bool
		if latchesHeld {
			ba, ok = canDoServersideRetry(ctx, pErr, ba, g, hlc.Timestamp{})
		}
		if !ok {
			// TODO(aayush,arul): These metrics are incorrect at the moment since
			// hitting this branch does not mean that we won't serverside retry, it
			// just means that we will have to reacquire latches.
			r.store.Metrics().ReadEvaluationServerSideRetryFailure.Inc(1)
			break
		} else {
			r.store.Metrics().ReadEvaluationServerSideRetrySuccess.Inc(1)
		}
	}

	if pErr != nil {
		// Failed read-only batches can't have any Result except for what's
		// allowlisted here.
		res.Local = result.LocalResult{
			EncounteredIntents: res.Local.DetachEncounteredIntents(),
			Metrics:            res.Local.Metrics,
		}
		return ba, nil, res, pErr
	}
	return ba, br, res, nil
}

func (r *Replica) handleReadOnlyLocalEvalResult(
	ctx context.Context, ba *kvpb.BatchRequest, lResult result.LocalResult,
) *kvpb.Error {
	// Fields for which no action is taken in this method are zeroed so that
	// they don't trigger an assertion at the end of the method (which checks
	// that all fields were handled).
	{
		lResult.Reply = nil
	}

	if lResult.AcquiredLocks != nil {
		// These will all be unreplicated locks.
		log.Eventf(ctx, "acquiring %d unreplicated locks", len(lResult.AcquiredLocks))
		for i := range lResult.AcquiredLocks {
			r.concMgr.OnLockAcquired(ctx, &lResult.AcquiredLocks[i])
		}
		lResult.AcquiredLocks = nil
	}

	if !lResult.IsZero() {
		log.Fatalf(ctx, "unhandled field in LocalEvalResult: %s", pretty.Diff(lResult, result.LocalResult{}))
	}
	return nil
}

// collectSpansRead uses the spans declared in the requests, and the resume
// spans in the responses, to construct the effective spans that were read,
// and uses that to compute the latch and lock spans.
func (r *Replica) collectSpansRead(
	ba *kvpb.BatchRequest, br *kvpb.BatchResponse,
) (latchSpans *spanset.SpanSet, lockSpans *lockspanset.LockSpanSet, _ error) {
	baCopy := *ba
	baCopy.Requests = make([]kvpb.RequestUnion, 0, len(ba.Requests))
	for i := 0; i < len(ba.Requests); i++ {
		baReq := ba.Requests[i]
		req := baReq.GetInner()
		header := req.Header()
		resp := br.Responses[i].GetInner()

		if ba.WaitPolicy == lock.WaitPolicy_SkipLocked && kvpb.CanSkipLocked(req) {
			if ba.IndexFetchSpec != nil {
				return nil, nil, errors.AssertionFailedf("unexpectedly IndexFetchSpec is set with SKIP LOCKED wait policy")
			}
			// If the request is using a SkipLocked wait policy, it behaves as if run
			// at a lower isolation level for any keys that it skips over. If the read
			// request did not return a key, it does not need to check for conflicts
			// with latches held on that key. Instead, the request only needs to check
			// for conflicting latches on the keys that were returned.
			//
			// To achieve this, we add a Get request for each of the keys in the
			// response's result set, even if the original request was a ranged scan.
			// This will lead to the returned span set (which is used for optimistic
			// eval validation) containing a set of point latch spans which correspond
			// to the response keys. Note that the Get requests are constructed with
			// the same key locking mode as the original read.
			//
			// This is similar to how the timestamp cache and refresh spans handle the
			// SkipLocked wait policy.
			if err := kvpb.ResponseKeyIterate(req, resp, func(key roachpb.Key) {
				// TODO(nvanbenschoten): we currently perform a per-response key memory
				// allocation. If this becomes an issue, we could pre-allocate chunks of
				// these structs to amortize the cost.
				getAlloc := new(struct {
					get   kvpb.GetRequest
					union kvpb.RequestUnion_Get
				})
				getAlloc.get.Key = key
				getAlloc.get.KeyLockingStrength,
					getAlloc.get.KeyLockingDurability = req.(kvpb.LockingReadRequest).KeyLocking()
				getAlloc.union.Get = &getAlloc.get
				ru := kvpb.RequestUnion{Value: &getAlloc.union}
				baCopy.Requests = append(baCopy.Requests, ru)
			}); err != nil {
				return nil, nil, err
			}
			continue
		}

		if resp.Header().ResumeSpan == nil {
			// Fully evaluated.
			baCopy.Requests = append(baCopy.Requests, baReq)
			continue
		}

		switch t := resp.(type) {
		case *kvpb.GetResponse:
			// The request did not evaluate. Ignore it.
			continue
		case *kvpb.ScanResponse:
			if header.Key.Equal(t.ResumeSpan.Key) {
				// The request did not evaluate. Ignore it.
				continue
			}
			// The scan will resume at the inclusive ResumeSpan.Key. So
			// ResumeSpan.Key has not been read and becomes the exclusive end key of
			// what was read.
			header.EndKey = t.ResumeSpan.Key
		case *kvpb.ReverseScanResponse:
			if header.EndKey.Equal(t.ResumeSpan.EndKey) {
				// The request did not evaluate. Ignore it.
				continue
			}
			// The scan will resume at the exclusive ResumeSpan.EndKey and proceed
			// towards the current header.Key. So ResumeSpan.EndKey has been read
			// and becomes the inclusive start key of what was read.
			header.Key = t.ResumeSpan.EndKey
		default:
			// Consider it fully evaluated, which is safe.
			baCopy.Requests = append(baCopy.Requests, baReq)
			continue
		}
		// The ResumeSpan has changed the header.
		var ru kvpb.RequestUnion
		req = req.ShallowCopy()
		req.SetHeader(header)
		ru.MustSetInner(req)
		baCopy.Requests = append(baCopy.Requests, ru)
	}

	// Collect the batch's declared spans again, this time with the
	// span bounds constrained to what was read.
	var err error
	latchSpans, lockSpans, _, err = r.collectSpans(&baCopy)
	return latchSpans, lockSpans, err
}

func getBatchResponseReadStats(br *kvpb.BatchResponse) (float64, float64) {
	var keys, bytes float64
	for _, reply := range br.Responses {
		h := reply.GetInner().Header()
		if keysRead := h.NumKeys; keysRead > 0 {
			keys += float64(keysRead)
			bytes += float64(h.NumBytes)
		}
	}
	return keys, bytes
}
