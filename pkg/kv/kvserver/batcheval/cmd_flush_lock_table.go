// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

func init() {
	RegisterReadWriteCommand(kvpb.FlushLockTable, declareKeysFlushLockTable, FlushLockTable)
}

func declareKeysFlushLockTable(
	_ ImmutableRangeState,
	_ *kvpb.Header,
	req kvpb.Request,
	latchSpans *spanset.SpanSet,
	_ *lockspanset.LockSpanSet,
	_ time.Duration,
) error {
	// We declare non-MVCC read-write latches over the entire span we are
	// exporting. This is similar to the latches LeaseTransfer and Merge will
	// take. This is perhaps more aggressive than needed.
	//
	// TODO(ssd): Consider moving this back to normal MVCC write latches.
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, req.Header().Span())
	return nil
}

// FlushLockTable scans the in-memory lock tbale for unreplicated locks and
// writes them as replicated locks.
func FlushLockTable(
	ctx context.Context, rw storage.ReadWriter, cArgs CommandArgs, response kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.FlushLockTableRequest)
	resp := response.(*kvpb.FlushLockTableResponse)

	// TODO(ssd): Allow the caller to limit how many locks we write out.
	locksToFlush := make([]roachpb.LockAcquisition, 0)
	cArgs.EvalCtx.GetConcurrencyManager().ExportUnreplicatedLocks(args.Span(), func(l *roachpb.LockAcquisition) {
		locksToFlush = append(locksToFlush, *l)
	})

	for i, l := range locksToFlush {
		locksToFlush[i].Durability = lock.Replicated
		if err := storage.MVCCAcquireLock(ctx, rw,
			&l.Txn, l.IgnoredSeqNums, l.Strength, l.Key,
			cArgs.Stats, 0, 0, true /* allowSequenceNumberRegression */); err != nil {
			return result.Result{}, err
		}
	}
	resp.LocksWritten = uint64(len(locksToFlush))

	// NOTE: The locks still exist in the in-memory lock table. They are not
	// cleared until OnLockAcquired is called by (*replica).handleReadWriteLocalEvalResult.
	return result.WithAcquiredLocks(locksToFlush...), nil
}
