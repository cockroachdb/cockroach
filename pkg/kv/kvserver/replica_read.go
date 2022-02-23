// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/uncertainty"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/kr/pretty"
)

// executeReadOnlyBatch is the execution logic for client requests which do not
// mutate the range's replicated state. The method uses a single RocksDB
// iterator to evaluate the batch and then updates the timestamp cache to
// reflect the key spans that it read.
func (r *Replica) executeReadOnlyBatch(
	ctx context.Context, ba *roachpb.BatchRequest, g *concurrency.Guard,
) (br *roachpb.BatchResponse, _ *concurrency.Guard, pErr *roachpb.Error) {
	r.readOnlyCmdMu.RLock()
	defer r.readOnlyCmdMu.RUnlock()

	// Verify that the batch can be executed.
	st, err := r.checkExecutionCanProceed(ctx, ba, g)
	if err != nil {
		return nil, g, roachpb.NewError(err)
	}

	// Compute the transaction's local uncertainty limit using observed
	// timestamps, which can help avoid uncertainty restarts.
	ui := uncertainty.ComputeInterval(&ba.Header, st, r.Clock().MaxOffset())

	// Evaluate read-only batch command.
	rec := NewReplicaEvalContext(r, g.LatchSpans())

	// TODO(irfansharif): It's unfortunate that in this read-only code path,
	// we're stuck with a ReadWriter because of the way evaluateBatch is
	// designed.
	rw := r.store.Engine().NewReadOnly(storage.StandardDurability)
	if !rw.ConsistentIterators() {
		// This is not currently needed for correctness, but future optimizations
		// may start relying on this, so we assert here.
		panic("expected consistent iterators")
	}
	if util.RaceEnabled {
		rw = spanset.NewReadWriterAt(rw, g.LatchSpans(), ba.Timestamp)
	}
	defer rw.Close()

	// TODO(nvanbenschoten): once all replicated intents are pulled into the
	// concurrency manager's lock-table, we can be sure that if we reached this
	// point, we will not conflict with any of them during evaluation. This in
	// turn means that we can bump the timestamp cache *before* evaluation
	// without risk of starving writes. Once we start doing that, we're free to
	// release latches immediately after we acquire an engine iterator as long
	// as we're performing a non-locking read. Note that this also requires that
	// the request is not being optimistically evaluated (optimistic evaluation
	// does not wait for latches or check locks). It would also be nice, but not
	// required for correctness, that the read-only engine eagerly create an
	// iterator (that is later cloned) while the latches are held, so that this
	// request does not "see" the effect of any later requests that happen after
	// the latches are released.

	var result result.Result
	br, result, pErr = r.executeReadOnlyBatchWithServersideRefreshes(ctx, rw, rec, ba, ui, g)

	// If the request hit a server-side concurrency retry error, immediately
	// propagate the error. Don't assume ownership of the concurrency guard.
	if isConcurrencyRetryError(pErr) {
		if g.EvalKind == concurrency.OptimisticEval {
			// Since this request was not holding latches, it could have raced with
			// intent resolution. So we can't trust it to add discovered locks, if
			// there is a latch conflict. This means that a discovered lock plus a
			// latch conflict will likely cause the request to evaluate at least 3
			// times: optimistically; pessimistically and add the discovered lock;
			// wait until resolution and evaluate pessimistically again.
			//
			// TODO(sumeer): scans and gets are correctly setting the resume span
			// when returning a WriteIntentError. I am not sure about other
			// concurrency errors. We could narrow the spans we check the latch
			// conflicts for by using collectSpansRead as done below in the
			// non-error path.
			if !g.CheckOptimisticNoLatchConflicts() {
				return nil, g, roachpb.NewError(roachpb.NewOptimisticEvalConflictsError())
			}
		}
		pErr = maybeAttachLease(pErr, &st.Lease)
		return nil, g, pErr
	}

	if g.EvalKind == concurrency.OptimisticEval {
		if pErr == nil {
			// Gather the spans that were read -- we distinguish the spans in the
			// request from the spans that were actually read, using resume spans in
			// the response.
			latchSpansRead, lockSpansRead, err := r.collectSpansRead(ba, br)
			if err != nil {
				return nil, g, roachpb.NewError(err)
			}
			if ok := g.CheckOptimisticNoConflicts(latchSpansRead, lockSpansRead); !ok {
				return nil, g, roachpb.NewError(roachpb.NewOptimisticEvalConflictsError())
			}
		} else {
			// There was an error, that was not classified as a concurrency retry
			// error, and this request was not holding latches. This should be rare,
			// and in the interest of not having subtle correctness bugs, we retry
			// pessimistically.
			return nil, g, roachpb.NewError(roachpb.NewOptimisticEvalConflictsError())
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

	// Otherwise, update the timestamp cache and release the concurrency guard.
	// Note:
	// - The update to the timestamp cache is not gated on pErr == nil,
	//   since certain semantic errors (e.g. ConditionFailedError on CPut)
	//   require updating the timestamp cache (see updatesTSCacheOnErr).
	// - For optimistic evaluation, used for limited scans, the update to the
	//   timestamp cache limits itself to the spans that were read, by using
	//   the ResumeSpans.
	ec, g := endCmds{repl: r, g: g, st: st}, nil
	ec.done(ctx, ba, br, pErr)

	// Semi-synchronously process any intents that need resolving here in
	// order to apply back pressure on the client which generated them. The
	// resolution is semi-synchronous in that there is a limited number of
	// outstanding asynchronous resolution tasks allowed after which
	// further calls will block.
	if len(intents) > 0 {
		log.Eventf(ctx, "submitting %d intents to asynchronous processing", len(intents))
		// We only allow synchronous intent resolution for consistent requests.
		// Intent resolution is async/best-effort for inconsistent requests.
		//
		// An important case where this logic is necessary is for RangeLookup
		// requests. In their case, synchronous intent resolution can deadlock
		// if the request originated from the local node which means the local
		// range descriptor cache has an in-flight RangeLookup request which
		// prohibits any concurrent requests for the same range. See #17760.
		allowSyncProcessing := ba.ReadConsistency == roachpb.CONSISTENT
		if err := r.store.intentResolver.CleanupIntentsAsync(ctx, intents, allowSyncProcessing); err != nil {
			log.Warningf(ctx, "%v", err)
		}
	}

	if pErr != nil {
		log.VErrEventf(ctx, 3, "%v", pErr.String())
	} else {
		log.Event(ctx, "read completed")
	}
	return br, nil, pErr
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

// executeReadOnlyBatchWithServersideRefreshes invokes evaluateBatch and retries
// at a higher timestamp in the event of some retriable errors if allowed by the
// batch/txn.
func (r *Replica) executeReadOnlyBatchWithServersideRefreshes(
	ctx context.Context,
	rw storage.ReadWriter,
	rec batcheval.EvalContext,
	ba *roachpb.BatchRequest,
	ui uncertainty.Interval,
	g *concurrency.Guard,
) (br *roachpb.BatchResponse, res result.Result, pErr *roachpb.Error) {
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
	if ba.AdmissionHeader.SourceLocation != roachpb.AdmissionHeader_LOCAL ||
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
			// It is safe to call Clear on an uninitialized BoundAccount.
			boundAccount.Clear(ctx)
			log.VEventf(ctx, 2, "server-side retry of batch")
		}
		br, res, pErr = evaluateBatch(ctx, kvserverbase.CmdIDKey(""), rw, rec, nil, ba, ui, true /* readOnly */)
		// If we can retry, set a higher batch timestamp and continue.
		// Allow one retry only.
		if pErr == nil || retries > 0 || !canDoServersideRetry(ctx, pErr, ba, br, g, nil /* deadline */) {
			break
		}
	}

	if pErr != nil {
		// Failed read-only batches can't have any Result except for what's
		// allowlisted here.
		res.Local = result.LocalResult{
			EncounteredIntents: res.Local.DetachEncounteredIntents(),
			Metrics:            res.Local.Metrics,
		}
		return nil, res, pErr
	}
	return br, res, nil
}

func (r *Replica) handleReadOnlyLocalEvalResult(
	ctx context.Context, ba *roachpb.BatchRequest, lResult result.LocalResult,
) *roachpb.Error {
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
	ba *roachpb.BatchRequest, br *roachpb.BatchResponse,
) (latchSpans, lockSpans *spanset.SpanSet, _ error) {
	baCopy := *ba
	baCopy.Requests = make([]roachpb.RequestUnion, len(baCopy.Requests))
	j := 0
	for i := 0; i < len(baCopy.Requests); i++ {
		baReq := ba.Requests[i]
		req := baReq.GetInner()
		header := req.Header()

		resp := br.Responses[i].GetInner()
		if resp.Header().ResumeSpan == nil {
			// Fully evaluated.
			baCopy.Requests[j] = baReq
			j++
			continue
		}

		switch t := resp.(type) {
		case *roachpb.ScanResponse:
			if header.Key.Equal(t.ResumeSpan.Key) {
				// The request did not evaluate. Ignore it.
				continue
			}
			// The scan will resume at the inclusive ResumeSpan.Key. So
			// ResumeSpan.Key has not been read and becomes the exclusive end key of
			// what was read.
			header.EndKey = t.ResumeSpan.Key
		case *roachpb.ReverseScanResponse:
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
			baCopy.Requests[j] = baReq
			j++
			continue
		}
		// The ResumeSpan has changed the header.
		req = req.ShallowCopy()
		req.SetHeader(header)
		baCopy.Requests[j].MustSetInner(req)
		j++
	}
	baCopy.Requests = baCopy.Requests[:j]

	// Collect the batch's declared spans again, this time with the
	// span bounds constrained to what was read.
	var err error
	latchSpans, lockSpans, _, err = r.collectSpans(&baCopy)
	return latchSpans, lockSpans, err
}
