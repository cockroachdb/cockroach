// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
)

// ClearRangeBytesThreshold is the threshold over which the ClearRange
// command will use engine.ClearRange to efficiently perform a range
// deletion. Otherwise, will revert to iterating through the values
// and clearing them individually with engine.Clear.
const ClearRangeBytesThreshold = 512 << 10 // 512KiB

func init() {
	RegisterReadWriteCommand(kvpb.ClearRange, declareKeysClearRange, ClearRange)
}

func declareKeysClearRange(
	rs ImmutableRangeState,
	header *kvpb.Header,
	req kvpb.Request,
	latchSpans *spanset.SpanSet,
	lockSpans *lockspanset.LockSpanSet,
	maxOffset time.Duration,
) error {
	err := DefaultDeclareIsolatedKeys(rs, header, req, latchSpans, lockSpans, maxOffset)
	if err != nil {
		return err
	}
	// We look up the range descriptor key to check whether the span
	// is equal to the entire range for fast stats updating.
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(rs.GetStartKey())})

	// We must peek beyond the span for MVCC range tombstones that straddle the
	// span bounds, to update MVCC stats with their new bounds. But we make sure
	// to stay within the range.
	//
	// NB: The range end key is not available, so this will pessimistically latch
	// up to args.EndKey.Next(). If EndKey falls on the range end key, the span
	// will be tightened during evaluation.
	// Even if we obtain latches beyond the end range here, it won't cause
	// contention with the subsequent range because latches are enforced per
	// range.
	args := req.(*kvpb.ClearRangeRequest)
	l, r := rangeTombstonePeekBounds(args.Key, args.EndKey, rs.GetStartKey().AsRawKey(), nil)
	latchSpans.AddMVCC(spanset.SpanReadOnly, roachpb.Span{Key: l, EndKey: r}, header.Timestamp)

	// Obtain a read only lock on range key GC key to serialize with
	// range tombstone GC requests.
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{
		Key: keys.MVCCRangeKeyGCKey(rs.GetRangeID()),
	})
	return nil
}

// ClearRange wipes all MVCC versions of keys covered by the specified
// span, adjusting the MVCC stats accordingly.
//
// Note that "correct" use of this command is only possible for key
// spans consisting of user data that we know is not being written to
// or queried any more, such as after a DROP or TRUNCATE table, or
// DROP index.
func ClearRange(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	if cArgs.Header.Txn != nil {
		return result.Result{}, errors.New("cannot execute ClearRange within a transaction")
	}
	log.VEventf(ctx, 2, "ClearRange %+v", cArgs.Args)

	// Encode MVCCKey values for start and end of clear span.
	args := cArgs.Args.(*kvpb.ClearRangeRequest)
	from := args.Key
	to := args.EndKey

	if !args.Deadline.IsEmpty() {
		if now := cArgs.EvalCtx.Clock().Now(); args.Deadline.LessEq(now) {
			return result.Result{}, errors.Errorf("ClearRange has deadline %s <= %s", args.Deadline, now)
		}
	}

	pd := result.Result{
		Replicated: kvserverpb.ReplicatedEvalResult{
			MVCCHistoryMutation: &kvserverpb.ReplicatedEvalResult_MVCCHistoryMutation{
				Spans: []roachpb.Span{{Key: from, EndKey: to}},
			},
		},
	}

	// Check for any locks, and return them for the caller to resolve. This
	// prevents removal of intents belonging to implicitly committed STAGING
	// txns. Otherwise, txn recovery would fail to find these intents and
	// consider the txn incomplete, uncommitting it and its writes (even those
	// outside of the cleared range).
	maxLockConflicts := storage.MaxConflictsPerLockConflictError.Get(&cArgs.EvalCtx.ClusterSettings().SV)
	targetLockConflictBytes := storage.TargetBytesPerLockConflictError.Get(&cArgs.EvalCtx.ClusterSettings().SV)
	locks, err := storage.ScanLocks(ctx, readWriter, from, to, maxLockConflicts, targetLockConflictBytes)
	if err != nil {
		return result.Result{}, err
	} else if len(locks) > 0 {
		return result.Result{}, &kvpb.LockConflictError{Locks: locks}
	}

	// Before clearing, compute the delta in MVCCStats.
	statsDelta, err := computeStatsDelta(ctx, readWriter, cArgs, from, to)
	if err != nil {
		return result.Result{}, err
	}
	cArgs.Stats.Subtract(statsDelta)

	// If the total size of data to be cleared is less than
	// clearRangeBytesThreshold, clear the individual values with an iterator,
	// instead of using a range tombstone (inefficient for small ranges).
	//
	// However, don't do this if the stats contain estimates -- this can only
	// happen when we're clearing an entire range and we're using the existing
	// range stats. We've seen cases where these estimates are wildly inaccurate
	// (even negative), and it's better to drop an unnecessary range tombstone
	// than to submit a huge write batch that'll get rejected by Raft.
	if statsDelta.ContainsEstimates == 0 && statsDelta.Total() < ClearRangeBytesThreshold {
		log.VEventf(ctx, 2, "delta=%d < threshold=%d; using non-range clear",
			statsDelta.Total(), ClearRangeBytesThreshold)
		err = readWriter.ClearMVCCIteratorRange(from, to, true /* pointKeys */, true /* rangeKeys */)
		if err != nil {
			return result.Result{}, err
		}
		return pd, nil
	}

	// If we're writing Pebble range tombstones, use ClearRangeWithHeuristic to
	// avoid writing tombstones across empty spans -- in particular, across the
	// range key span, since we expect range keys to be rare.
	const pointKeyThreshold, rangeKeyThreshold = 2, 2
	if err := storage.ClearRangeWithHeuristic(
		ctx, readWriter, readWriter, from, to, pointKeyThreshold, rangeKeyThreshold,
	); err != nil {
		return result.Result{}, err
	}
	return pd, nil
}

// computeStatsDelta determines the change in stats caused by the
// ClearRange command. If the cleared span is the entire range,
// computing MVCCStats is easy. We just negate all fields except sys
// bytes and count. Note that if a race build is enabled, we use the
// expectation of running in a CI environment to compute stats by
// iterating over the span to provide extra verification that the fast
// path of simply subtracting the non-system values is accurate.
// Returns the delta stats.
func computeStatsDelta(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, from, to roachpb.Key,
) (enginepb.MVCCStats, error) {
	desc := cArgs.EvalCtx.Desc()
	var delta enginepb.MVCCStats

	// We can avoid manually computing the stats delta if we're clearing
	// the entire range.
	entireRange := desc.StartKey.Equal(from) && desc.EndKey.Equal(to)
	if entireRange {
		// Note this it is safe to use the full range MVCC stats, as
		// opposed to the usual method of computing only a localizied
		// stats delta, because a full-range clear prevents any concurrent
		// access to the stats. Concurrent changes to range-local keys are
		// explicitly ignored (i.e. SysCount, SysBytes).
		delta = cArgs.EvalCtx.GetMVCCStats()
		delta.SysCount, delta.SysBytes, delta.AbortSpanBytes = 0, 0, 0 // no change to system stats
	}

	// If we can't use the fast stats path, or race test is enabled, compute stats
	// across the key span to be cleared.
	if !entireRange || util.RaceEnabled {
		computed, err := storage.ComputeStats(ctx, readWriter, from, to, delta.LastUpdateNanos)
		if err != nil {
			return enginepb.MVCCStats{}, err
		}
		// If we took the fast path but race is enabled, assert stats were correctly
		// computed.
		if entireRange {
			// Retain the value of ContainsEstimates for tests under race.
			computed.ContainsEstimates = delta.ContainsEstimates
			// We only want to assert the correctness of stats that do not contain
			// estimates.
			if delta.ContainsEstimates == 0 && !delta.Equal(computed) {
				log.Fatalf(ctx, "fast-path MVCCStats computation gave wrong result: diff(fast, computed) = %s",
					pretty.Diff(delta, computed))
			}
		}
		delta = computed

		// If we're not clearing the entire range, we need to adjust for the
		// fragmentation of any MVCC range tombstones that straddle the span bounds.
		// The clearing of the inner fragments has already been accounted for above.
		// We take care not to peek outside the Raft range bounds.
		if !entireRange {
			leftPeekBound, rightPeekBound := rangeTombstonePeekBounds(
				from, to, desc.StartKey.AsRawKey(), desc.EndKey.AsRawKey())
			rkIter, err := readWriter.NewMVCCIterator(ctx, storage.MVCCKeyIterKind, storage.IterOptions{
				KeyTypes:     storage.IterKeyTypeRangesOnly,
				LowerBound:   leftPeekBound,
				UpperBound:   rightPeekBound,
				ReadCategory: fs.BatchEvalReadCategory,
			})
			if err != nil {
				return enginepb.MVCCStats{}, err
			}
			defer rkIter.Close()

			if cmp, lhs, err := storage.PeekRangeKeysLeft(rkIter, from); err != nil {
				return enginepb.MVCCStats{}, err
			} else if cmp > 0 {
				delta.Subtract(storage.UpdateStatsOnRangeKeySplit(from, lhs.Versions))
			}

			if cmp, rhs, err := storage.PeekRangeKeysRight(rkIter, to); err != nil {
				return enginepb.MVCCStats{}, err
			} else if cmp < 0 {
				delta.Subtract(storage.UpdateStatsOnRangeKeySplit(to, rhs.Versions))
			}
		}
	}

	return delta, nil
}
