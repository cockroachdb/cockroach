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
	desc *roachpb.RangeDescriptor,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, lockSpans *spanset.SpanSet,
) {
	DefaultDeclareKeys(desc, header, req, latchSpans, lockSpans)
	// We look up the range descriptor key to check whether the span
	// is equal to the entire range for fast stats updating.
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(desc.StartKey)})
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
	var pd result.Result

	// Before clearing, compute the delta in MVCCStats.
	statsDelta, err := computeStatsDelta(ctx, readWriter, cArgs, from, to)
	if err != nil {
		return result.Result{}, err
	}
	cArgs.Stats.Subtract(statsDelta)

	// If the total size of data to be cleared is less than
	// clearRangeBytesThreshold, clear the individual values manually,
	// instead of using a range tombstone (inefficient for small ranges).
	if total := statsDelta.Total(); total < ClearRangeBytesThreshold {
		log.VEventf(ctx, 2, "delta=%d < threshold=%d; using non-range clear", total, ClearRangeBytesThreshold)
		if err := readWriter.Iterate(from, to,
			func(kv storage.MVCCKeyValue) (bool, error) {
				return false, readWriter.Clear(kv.Key)
			},
		); err != nil {
			return result.Result{}, err
		}
		return pd, nil
	}

	// Otherwise, suggest a compaction for the cleared range and clear
	// the key span using engine.ClearRange.
	pd.Replicated.SuggestedCompactions = []kvserverpb.SuggestedCompaction{
		{
			StartKey: from,
			EndKey:   to,
			Compaction: kvserverpb.Compaction{
				Bytes:            statsDelta.Total(),
				SuggestedAtNanos: cArgs.Header.Timestamp.WallTime,
			},
		},
	}
	if err := readWriter.ClearRange(storage.MakeMVCCMetadataKey(from),
		storage.MakeMVCCMetadataKey(to)); err != nil {
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
		delta.SysCount, delta.SysBytes = 0, 0 // no change to system stats
	}

	// If we can't use the fast stats path, or race test is enabled,
	// compute stats across the key span to be cleared.
	if !fast || util.RaceEnabled {
		iter := readWriter.NewIterator(storage.IterOptions{UpperBound: to})
		computed, err := iter.ComputeStats(from, to, delta.LastUpdateNanos)
		iter.Close()
		if err != nil {
			return enginepb.MVCCStats{}, err
		}
		// If we took the fast path but race is enabled, assert stats were correctly computed.
		if fast {
			delta.ContainsEstimates = computed.ContainsEstimates
			if !delta.Equal(computed) {
				log.Fatalf(ctx, "fast-path MVCCStats computation gave wrong result: diff(fast, computed) = %s",
					pretty.Diff(delta, computed))
			}
		}
		delta = computed
	}

	return delta, nil
}
