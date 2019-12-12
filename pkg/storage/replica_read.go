// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/spanlatch"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// executeReadOnlyBatch is the execution logic for client requests which do not
// mutate the range's replicated state. The method uses a single RocksDB
// iterator to evaluate the batch and then updates the read timestamp cache to
// reflect the key spans that it read.
func (r *Replica) executeReadOnlyBatch(
	ctx context.Context, ba *roachpb.BatchRequest, spans *spanset.SpanSet, lg *spanlatch.Guard,
) (br *roachpb.BatchResponse, pErr *roachpb.Error) {
	// Guarantee we release the provided latches. This is wrapped to delay pErr
	// evaluation to its value when returning.
	ec := endCmds{repl: r, lg: lg}
	defer func() {
		ec.done(ba, br, pErr)
	}()

	// If the read is not inconsistent, the read requires the range lease or
	// permission to serve via follower reads.
	var status storagepb.LeaseStatus
	if ba.ReadConsistency.RequiresReadLease() {
		if status, pErr = r.redirectOnOrAcquireLease(ctx); pErr != nil {
			if nErr := r.canServeFollowerRead(ctx, ba, pErr); nErr != nil {
				return nil, nErr
			}
			r.store.metrics.FollowerReadsCount.Inc(1)
		}
	}
	r.limitTxnMaxTimestamp(ctx, ba, status)

	log.Event(ctx, "waiting for read lock")
	r.readOnlyCmdMu.RLock()
	defer r.readOnlyCmdMu.RUnlock()

	// Verify that the batch can be executed.
	if err := r.checkExecutionCanProceed(ba, ec.lg, &status); err != nil {
		return nil, roachpb.NewError(err)
	}

	// Evaluate read-only batch command.
	var result result.Result
	rec := NewReplicaEvalContext(r, spans)
	readOnly := r.store.Engine().NewReadOnly()
	if util.RaceEnabled {
		readOnly = spanset.NewReadWriterAt(readOnly, spans, ba.Timestamp)
	}
	defer readOnly.Close()
	br, result, pErr = evaluateBatch(ctx, storagebase.CmdIDKey(""), readOnly, rec, nil, ba, true /* readOnly */)

	// A merge is (likely) about to be carried out, and this replica
	// needs to block all traffic until the merge either commits or
	// aborts. See docs/tech-notes/range-merges.md.
	if result.Local.DetachMaybeWatchForMerge() {
		if err := r.maybeWatchForMerge(ctx); err != nil {
			return nil, roachpb.NewError(err)
		}
	}

	if intents := result.Local.DetachIntents(); len(intents) > 0 {
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
	if pErr != nil {
		log.VErrEvent(ctx, 3, pErr.String())
	} else {
		log.Event(ctx, "read completed")
	}
	return br, pErr
}
