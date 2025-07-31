// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/gc"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// QueryResolvedTimestampIntentCleanupAge configures the minimum intent age that
// QueryResolvedTimestamp requests will consider for async intent cleanup.
var QueryResolvedTimestampIntentCleanupAge = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kv.query_resolved_timestamp.intent_cleanup_age",
	"minimum intent age that QueryResolvedTimestamp requests will consider for async intent cleanup",
	10*time.Second,
	settings.NonNegativeDuration,
)

func init() {
	RegisterReadOnlyCommand(kvpb.QueryResolvedTimestamp, DefaultDeclareKeys, QueryResolvedTimestamp)
}

// QueryResolvedTimestamp requests a resolved timestamp for the key span it is
// issued over. A resolved timestamp for a key span is a timestamp at or below
// which all future reads within the span are guaranteed to produce the same
// results, i.e. at which MVCC history has become immutable.
func QueryResolvedTimestamp(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.QueryResolvedTimestampRequest)
	reply := resp.(*kvpb.QueryResolvedTimestampResponse)

	// Grab the closed timestamp from the local replica. We do this before
	// iterating over intents to ensure that we observe any and all intents
	// written before the closed timestamp went into effect. This is important
	// because QueryResolvedTimestamp requests are often run without acquiring
	// latches (see kvpb.INCONSISTENT) and often also on follower replicas,
	// so latches won't help them to synchronize with writes.
	closedTS := cArgs.EvalCtx.GetClosedTimestampOlderThanStorageSnapshot()

	// Compute the minimum timestamp of any intent in the request's key span,
	// which may span the entire range, but does not need to.
	//
	// While doing so, collect a set of intents that are encountered and that are
	// sufficiently old, such that it seems valuable to try to clean them up. We
	// do so to ensure that an abandoned intent that is only being observed by
	// bounded staleness reads cannot hold up the resolved timestamp indefinitely
	// (or until the GC runs). We cap the maximum size of this set to limit its
	// cost, since this is all best-effort anyway.
	st := cArgs.EvalCtx.ClusterSettings()
	maxEncounteredIntents := gc.MaxLocksPerCleanupBatch.Get(&st.SV)
	maxEncounteredIntentKeyBytes := gc.MaxLockKeyBytesPerCleanupBatch.Get(&st.SV)
	intentCleanupAge := QueryResolvedTimestampIntentCleanupAge.Get(&st.SV)
	intentCleanupThresh := cArgs.EvalCtx.Clock().Now().Add(-intentCleanupAge.Nanoseconds(), 0)
	minIntentTS, encounteredIntents, err := computeMinIntentTimestamp(
		ctx, reader, args.Span(), maxEncounteredIntents, maxEncounteredIntentKeyBytes, intentCleanupThresh,
	)
	if err != nil {
		return result.Result{}, errors.Wrapf(err, "computing minimum intent timestamp")
	}

	// Compute the span's resolved timestamp. Start with the range's closed
	// timestamp and then backdate this to a timestamp before any active intents.
	reply.ResolvedTS = closedTS
	if !minIntentTS.IsEmpty() {
		reply.ResolvedTS.Backward(minIntentTS.Prev())
	}

	var res result.Result
	res.Local.EncounteredIntents = encounteredIntents
	return res, nil
}

// computeMinIntentTimestamp scans the specified key span and determines the
// minimum timestamp of any intent. While doing so, it also collects and returns
// up to maxEncounteredIntents intents that are older than intentCleanupThresh.
func computeMinIntentTimestamp(
	ctx context.Context,
	reader storage.Reader,
	span roachpb.Span,
	maxEncounteredIntents int64,
	maxEncounteredIntentKeyBytes int64,
	intentCleanupThresh hlc.Timestamp,
) (hlc.Timestamp, []roachpb.Intent, error) {
	ltStart, _ := keys.LockTableSingleKey(span.Key, nil)
	ltEnd, _ := keys.LockTableSingleKey(span.EndKey, nil)
	opts := storage.LockTableIteratorOptions{
		LowerBound: ltStart,
		UpperBound: ltEnd,
		// Ignore Exclusive and Shared locks. We only care about intents.
		MatchMinStr:  lock.Intent,
		ReadCategory: fs.BatchEvalReadCategory,
	}
	iter, err := storage.NewLockTableIterator(ctx, reader, opts)
	if err != nil {
		return hlc.Timestamp{}, nil, err
	}
	defer iter.Close()

	var meta enginepb.MVCCMetadata
	var minTS hlc.Timestamp
	var encountered []roachpb.Intent
	var encounteredKeyBytes int64
	for valid, err := iter.SeekEngineKeyGE(storage.EngineKey{Key: ltStart}); ; valid, err = iter.NextEngineKey() {
		if err != nil {
			return hlc.Timestamp{}, nil, err
		} else if !valid {
			break
		}
		engineKey, err := iter.EngineKey()
		if err != nil {
			return hlc.Timestamp{}, nil, err
		}
		ltKey, err := engineKey.ToLockTableKey()
		if err != nil {
			return hlc.Timestamp{}, nil, errors.Wrapf(err, "decoding LockTable key: %v", ltKey)
		}
		if ltKey.Strength != lock.Intent {
			return hlc.Timestamp{}, nil, errors.AssertionFailedf(
				"unexpected strength for LockTableKey %s: %v", ltKey.Strength, ltKey)
		}
		// Unmarshal.
		if err := iter.ValueProto(&meta); err != nil {
			return hlc.Timestamp{}, nil, errors.Wrapf(err, "unmarshaling mvcc meta: %v", ltKey)
		}
		if meta.Txn == nil {
			return hlc.Timestamp{}, nil, errors.AssertionFailedf("nil transaction in LockTable: %v", ltKey)
		}

		if minTS.IsEmpty() {
			minTS = meta.Txn.WriteTimestamp
		} else {
			minTS.Backward(meta.Txn.WriteTimestamp)
		}

		// Also, add the intent to the encountered intents set if it is old enough
		// and we have room, both in terms of the number of intents and the size
		// of the intent keys.
		oldEnough := meta.Txn.WriteTimestamp.Less(intentCleanupThresh)
		intentFitsByCount := int64(len(encountered)) < maxEncounteredIntents
		intentFitsByBytes := encounteredKeyBytes < maxEncounteredIntentKeyBytes
		if oldEnough && intentFitsByCount && intentFitsByBytes {
			encountered = append(encountered, roachpb.MakeIntent(meta.Txn, ltKey.Key))
			encounteredKeyBytes += int64(len(ltKey.Key))
		}
	}
	return minTS, encountered, nil
}
