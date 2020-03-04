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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storagebase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storagepb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/kr/pretty"
)

// executeReadOnlyBatch is the execution logic for client requests which do not
// mutate the range's replicated state. The method uses a single RocksDB
// iterator to evaluate the batch and then updates the timestamp cache to
// reflect the key spans that it read.
func (r *Replica) executeReadOnlyBatch(
	ctx context.Context, ba *roachpb.BatchRequest, g *concurrency.Guard,
) (br *roachpb.BatchResponse, _ *concurrency.Guard, pErr *roachpb.Error) {
	// If the read is not inconsistent, the read requires the range lease or
	// permission to serve via follower reads.
	var status storagepb.LeaseStatus
	if ba.ReadConsistency.RequiresReadLease() {
		if status, pErr = r.redirectOnOrAcquireLease(ctx); pErr != nil {
			if nErr := r.canServeFollowerRead(ctx, ba, pErr); nErr != nil {
				return nil, g, nErr
			}
			r.store.metrics.FollowerReadsCount.Inc(1)
		}
	}
	r.limitTxnMaxTimestamp(ctx, ba, status)

	log.Event(ctx, "waiting for read lock")
	r.readOnlyCmdMu.RLock()
	defer r.readOnlyCmdMu.RUnlock()

	// Verify that the batch can be executed.
	if err := r.checkExecutionCanProceed(ba, g, &status); err != nil {
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
	br, result, pErr = evaluateBatch(ctx, storagebase.CmdIDKey(""), rw, rec, nil, ba, true /* readOnly */)
	if err := r.handleReadOnlyLocalEvalResult(ctx, ba, result.Local); err != nil {
		pErr = roachpb.NewError(err)
	}
	r.updateTimestampCache(ctx, ba, br, pErr)

	if pErr != nil {
		log.VErrEvent(ctx, 3, pErr.String())
	} else {
		log.Event(ctx, "read completed")
	}
	return br, g, pErr
}

func (r *Replica) handleReadOnlyLocalEvalResult(
	ctx context.Context, ba *roachpb.BatchRequest, lResult result.LocalResult,
) error {
	// Fields for which no action is taken in this method are zeroed so that
	// they don't trigger an assertion at the end of the method (which checks
	// that all fields were handled).
	{
		lResult.Reply = nil
	}

	if lResult.MaybeWatchForMerge {
		// A merge is (likely) about to be carried out, and this replica needs
		// to block all traffic until the merge either commits or aborts. See
		// docs/tech-notes/range-merges.md.
		if err := r.maybeWatchForMerge(ctx); err != nil {
			return err
		}
		lResult.MaybeWatchForMerge = false
	}

	if lResult.WrittenIntents != nil {
		// These will all be unreplicated locks.
		for i := range lResult.WrittenIntents {
			r.concMgr.OnLockAcquired(ctx, &lResult.WrittenIntents[i])
		}
		lResult.WrittenIntents = nil
	}

	if intents := lResult.DetachEncounteredIntents(); len(intents) > 0 {
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
			log.Warning(ctx, err)
		}
	}

	if !lResult.IsZero() {
		log.Fatalf(ctx, "unhandled field in LocalEvalResult: %s", pretty.Diff(lResult, result.LocalResult{}))
	}
	return nil
}
