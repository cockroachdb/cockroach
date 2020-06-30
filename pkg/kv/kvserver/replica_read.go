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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/kr/pretty"
)

// executeReadOnlyBatch is the execution logic for client requests which do not
// mutate the range's replicated state. The method uses a single RocksDB
// iterator to evaluate the batch and then updates the timestamp cache to
// reflect the key spans that it read.
func (r *Replica) executeReadOnlyBatch(
	ctx context.Context, ba *roachpb.BatchRequest, st kvserverpb.LeaseStatus, g *concurrency.Guard,
) (br *roachpb.BatchResponse, _ *concurrency.Guard, pErr *roachpb.Error) {
	r.readOnlyCmdMu.RLock()
	defer r.readOnlyCmdMu.RUnlock()

	// Verify that the batch can be executed.
	if err := r.checkExecutionCanProceed(ctx, ba, g, &st); err != nil {
		return nil, g, roachpb.NewError(err)
	}

	// Evaluate read-only batch command.
	spans := g.LatchSpans()
	rec := NewReplicaEvalContext(r, spans)

	// TODO(irfansharif): It's unfortunate that in this read-only code path,
	// we're stuck with a ReadWriter because of the way evaluateBatch is
	// designed.
	rw := r.store.Engine().NewReadOnly()
	if util.RaceEnabled {
		rw = spanset.NewReadWriterAt(rw, spans, ba.Timestamp)
	}
	defer rw.Close()

	// TODO(nvanbenschoten): once all replicated intents are pulled into the
	// concurrency manager's lock-table, we can be sure that if we reached this
	// point, we will not conflict with any of them during evaluation. This in
	// turn means that we can bump the timestamp cache *before* evaluation
	// without risk of starving writes. Once we start doing that, we're free to
	// release latches immediately after we acquire an engine iterator as long
	// as we're performing a non-locking read.

	var result result.Result
	br, result, pErr = r.executeReadOnlyBatchWithServersideRefreshes(ctx, rw, rec, ba, spans)

	// If the request hit a server-side concurrency retry error, immediately
	// proagate the error. Don't assume ownership of the concurrency guard.
	if isConcurrencyRetryError(pErr) {
		return nil, g, pErr
	}

	// Handle any local (leaseholder-only) side-effects of the request.
	intents := result.Local.DetachEncounteredIntents()
	if pErr == nil {
		pErr = r.handleReadOnlyLocalEvalResult(ctx, ba, result.Local)
	}

	// Otherwise, update the timestamp cache and release the concurrency guard.
	ec, g := endCmds{repl: r, g: g}, nil
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

// executeReadOnlyBatchWithServersideRefreshes invokes evaluateBatch and retries
// at a higher timestamp in the event of some retriable errors if allowed by the
// batch/txn.
func (r *Replica) executeReadOnlyBatchWithServersideRefreshes(
	ctx context.Context,
	rw storage.ReadWriter,
	rec batcheval.EvalContext,
	ba *roachpb.BatchRequest,
	latchSpans *spanset.SpanSet,
) (br *roachpb.BatchResponse, res result.Result, pErr *roachpb.Error) {
	log.Event(ctx, "executing read-only batch")

	for retries := 0; ; retries++ {
		if retries > 0 {
			log.VEventf(ctx, 2, "server-side retry of batch")
		}
		br, res, pErr = evaluateBatch(ctx, kvserverbase.CmdIDKey(""), rw, rec, nil, ba, true /* readOnly */)

		// If we can retry, set a higher batch timestamp and continue.
		// Allow one retry only.
		if pErr == nil || retries > 0 || !canDoServersideRetry(ctx, pErr, ba, br, latchSpans, nil /* deadline */) {
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

	if lResult.MaybeWatchForMerge {
		// A merge is (likely) about to be carried out, and this replica needs
		// to block all traffic until the merge either commits or aborts. See
		// docs/tech-notes/range-merges.md.
		if err := r.maybeWatchForMerge(ctx); err != nil {
			return roachpb.NewError(err)
		}
		lResult.MaybeWatchForMerge = false
	}

	if !lResult.IsZero() {
		log.Fatalf(ctx, "unhandled field in LocalEvalResult: %s", pretty.Diff(lResult, result.LocalResult{}))
	}
	return nil
}
