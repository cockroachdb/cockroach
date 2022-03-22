// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
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
	RegisterReadWriteCommand(roachpb.ClearRange, declareKeysClearRange, ClearRange)
}

func declareKeysClearRange(
	rs ImmutableRangeState,
	header *roachpb.Header,
	req roachpb.Request,
	latchSpans, lockSpans *spanset.SpanSet,
	maxOffset time.Duration,
) {
	DefaultDeclareIsolatedKeys(rs, header, req, latchSpans, lockSpans, maxOffset)
	// We look up the range descriptor key to check whether the span
	// is equal to the entire range for fast stats updating.
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(rs.GetStartKey())})
}

// ClearRange wipes all MVCC versions of keys covered by the specified
// span, adjusting the MVCC stats accordingly.
//
// Note that "correct" use of this command is only possible for key
// spans consisting of user data that we know is not being written to
// or queried any more, such as after a DROP or TRUNCATE table, or
// DROP index.
func ClearRange(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	if cArgs.Header.Txn != nil {
		return result.Result{}, errors.New("cannot execute ClearRange within a transaction")
	}
	log.VEventf(ctx, 2, "ClearRange %+v", cArgs.Args)

	// Encode MVCCKey values for start and end of clear span.
	args := cArgs.Args.(*roachpb.ClearRangeRequest)
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

	// Check for any intents, and return them for the caller to resolve. This
	// prevents removal of intents belonging to implicitly committed STAGING
	// txns. Otherwise, txn recovery would fail to find these intents and
	// consider the txn incomplete, uncommitting it and its writes (even those
	// outside of the cleared range).
	maxIntents := storage.MaxIntentsPerWriteIntentError.Get(&cArgs.EvalCtx.ClusterSettings().SV)
	intents, err := storage.ScanIntents(ctx, readWriter, from, to, maxIntents, 0)
	if err != nil {
		return result.Result{}, err
	} else if len(intents) > 0 {
		return result.Result{}, &roachpb.WriteIntentError{Intents: intents}
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
		iter := readWriter.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
			LowerBound: from,
			UpperBound: to,
		})
		defer iter.Close()
		if err = readWriter.ClearIterRange(iter, from, to); err != nil {
			return result.Result{}, err
		}
		return pd, nil
	}

	if err := readWriter.ClearMVCCRangeAndIntents(from, to); err != nil {
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
	fast := desc.StartKey.Equal(from) && desc.EndKey.Equal(to)
	if fast {
		// Note this it is safe to use the full range MVCC stats, as
		// opposed to the usual method of computing only a localizied
		// stats delta, because a full-range clear prevents any concurrent
		// access to the stats. Concurrent changes to range-local keys are
		// explicitly ignored (i.e. SysCount, SysBytes).
		delta = cArgs.EvalCtx.GetMVCCStats()
		delta.SysCount, delta.SysBytes, delta.AbortSpanBytes = 0, 0, 0 // no change to system stats
	}

	// If we can't use the fast stats path, or race test is enabled,
	// compute stats across the key span to be cleared.
	if !fast || util.RaceEnabled {
		iter := readWriter.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{UpperBound: to})
		computed, err := iter.ComputeStats(from, to, delta.LastUpdateNanos)
		iter.Close()
		if err != nil {
			return enginepb.MVCCStats{}, err
		}
		// If we took the fast path but race is enabled, assert stats were correctly computed.
		if fast {
			computed.ContainsEstimates = delta.ContainsEstimates // retained for tests under race
			if !delta.Equal(computed) {
				log.Fatalf(ctx, "fast-path MVCCStats computation gave wrong result: diff(fast, computed) = %s",
					pretty.Diff(delta, computed))
			}
		}
		delta = computed
	}

	return delta, nil
}
