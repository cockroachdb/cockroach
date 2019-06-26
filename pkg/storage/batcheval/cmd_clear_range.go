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
	"errors"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/rditer"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/kr/pretty"
)

// ClearRangeBytesThreshold is the threshold over which the ClearRange
// command will use engine.ClearRange to efficiently perform a range
// deletion. Otherwise, will revert to iterating through the values
// and clearing them individually with engine.Clear.
const ClearRangeBytesThreshold = 512 << 10 // 512KiB

func init() {
	RegisterCommand(roachpb.ClearRange, declareKeysClearRange, ClearRange)
}

func declareKeysClearRange(
	desc *roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	DefaultDeclareKeys(desc, header, req, spans)
	// We look up the range descriptor key to check whether the span
	// is equal to the entire range for fast stats updating.
	spans.Add(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(desc.StartKey)})
}

// ClearRange wipes all MVCC versions of keys covered by the specified
// span, adjusting the MVCC stats accordingly.
//
// Note that "correct" use of this command is only possible for key
// spans consisting of user data that we know is not being written to
// or queried any more, such as after a DROP or TRUNCATE table, or
// DROP index.
func ClearRange(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	if cArgs.Header.Txn != nil {
		return result.Result{}, errors.New("cannot execute ClearRange within a transaction")
	}
	log.VEventf(ctx, 2, "ClearRange %+v", cArgs.Args)

	// Encode MVCCKey values for start and end of clear span.
	args := cArgs.Args.(*roachpb.ClearRangeRequest)
	from := engine.MVCCKey{Key: args.Key}
	to := engine.MVCCKey{Key: args.EndKey}
	var pd result.Result

	if !args.StartTime.IsEmpty() {
		madeChanges := false
		log.VEventf(ctx, 2, "clearing keys with timestamp >= %v", args.StartTime)
		// TODO(dt): time-bound iter could potentially be a big win here -- the
		// expected use-case for this is to run over an entire table's span with a
		// very recent timestamp, rolling back just the writes of some failed IMPORT
		// and that could very likely only have hit some small subset of the table's
		// keyspace. However to get the stats right we need a non-time-bound iter
		// e.g. we need to know if there is an older key under the one we are
		// clearing to know if we're changing the number of live keys. An approach
		// that seems promising might be to use time-bound iter to find candidates
		// for deletion, allowing us to quickly skip over swaths of uninteresting
		// keys, but then use a normal iteration to actually do the delete including
		// updating the live key stats correctly.
		if err := batch.Iterate(
			from, to,
			func(kv engine.MVCCKeyValue) (bool, error) {
				if !kv.Key.Timestamp.Less(args.StartTime) {
					madeChanges = true
					return false, batch.Clear(kv.Key)
				}
				return false, nil
			},
		); err != nil {
			return result.Result{}, err
		}
		if madeChanges {
			// TODO(dt): can we update stats deltas during the interation above instead
			// of doing this extra iteration?
			stats, err := rditer.ComputeStatsForRange(cArgs.EvalCtx.Desc(), batch, cArgs.Header.Timestamp.WallTime)
			if err != nil {
				return result.Result{}, err
			}
			*cArgs.Stats = stats
		}
		return pd, nil
	}

	// Before clearing, compute the delta in MVCCStats.
	statsDelta, err := computeStatsDelta(ctx, batch, cArgs, from, to)
	if err != nil {
		return result.Result{}, err
	}
	cArgs.Stats.Subtract(statsDelta)

	// If the total size of data to be cleared is less than
	// clearRangeBytesThreshold, clear the individual values manually,
	// instead of using a range tombstone (inefficient for small ranges).
	if total := statsDelta.Total(); total < ClearRangeBytesThreshold {
		log.VEventf(ctx, 2, "delta=%d < threshold=%d; using non-range clear", total, ClearRangeBytesThreshold)
		if err := batch.Iterate(
			from, to,
			func(kv engine.MVCCKeyValue) (bool, error) {
				return false, batch.Clear(kv.Key)
			},
		); err != nil {
			return result.Result{}, err
		}
		return pd, nil
	}

	// Otherwise, suggest a compaction for the cleared range and clear
	// the key span using engine.ClearRange.
	pd.Replicated.SuggestedCompactions = []storagepb.SuggestedCompaction{
		{
			StartKey: from.Key,
			EndKey:   to.Key,
			Compaction: storagepb.Compaction{
				Bytes:            statsDelta.Total(),
				SuggestedAtNanos: cArgs.Header.Timestamp.WallTime,
			},
		},
	}
	if err := batch.ClearRange(from, to); err != nil {
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
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, from, to engine.MVCCKey,
) (enginepb.MVCCStats, error) {
	desc := cArgs.EvalCtx.Desc()
	var delta enginepb.MVCCStats

	// We can avoid manually computing the stats delta if we're clearing
	// the entire range.
	fast := desc.StartKey.Equal(from.Key) && desc.EndKey.Equal(to.Key)
	if fast {
		// Note this it is safe to use the full range MVCC stats, as
		// opposed to the usual method of computing only a localizied
		// stats delta, because a full-range clear prevents any concurrent
		// access to the stats. Concurrent changes to range-local keys are
		// explicitly ignored (i.e. SysCount, SysBytes).
		delta = cArgs.EvalCtx.GetMVCCStats()
		delta.SysCount, delta.SysBytes = 0, 0 // no change to system stats
	}

	// If we can't use the fast stats path, or race test is enabled,
	// compute stats across the key span to be cleared.
	if !fast || util.RaceEnabled {
		iter := batch.NewIterator(engine.IterOptions{UpperBound: to.Key})
		computed, err := iter.ComputeStats(from, to, delta.LastUpdateNanos)
		iter.Close()
		if err != nil {
			return enginepb.MVCCStats{}, err
		}
		// If we took the fast path but race is enabled, assert stats were correctly computed.
		if fast {
			if !delta.Equal(computed) {
				log.Fatalf(ctx, "fast-path MVCCStats computation gave wrong result: diff(fast, computed) = %s",
					pretty.Diff(delta, computed))
			}
		}
		delta = computed
	}

	return delta, nil
}
