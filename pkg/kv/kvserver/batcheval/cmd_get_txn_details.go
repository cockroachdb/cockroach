// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"bytes"
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnfeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func init() {
	RegisterReadOnlyCommand(kvpb.GetTxnDetails, declareKeysGetTxnDetails, GetTxnDetails)
}

// declareKeysGetTxnDetails declares latches and lock spans for the
// write and read spans that GetTxnDetails will scan. Lock spans
// (lock.None) cause the concurrency manager to wait on any intents
// discovered in the lock table before evaluation begins, avoiding
// unnecessary evaluate-error-retry cycles.
func declareKeysGetTxnDetails(
	_ ImmutableRangeState,
	_ *kvpb.Header,
	req kvpb.Request,
	latchSpans *spanset.SpanSet,
	lockSpans *lockspanset.LockSpanSet,
	_ time.Duration,
) error {
	args := req.(*kvpb.GetTxnDetailsRequest)
	for _, ws := range args.WriteSpans {
		latchSpans.AddMVCC(spanset.SpanReadOnly, ws, args.CommitTimestamp)
		lockSpans.Add(lock.None, ws)
	}
	for _, rs := range args.ReadSpans {
		latchSpans.AddMVCC(spanset.SpanReadOnly, rs, args.CommitTimestamp)
		lockSpans.Add(lock.None, rs)
	}
	return nil
}

// GetTxnDetails retrieves a transaction's raw writes and dependencies on a
// single range. See GetTxnDetailsRequest for the full API contract.
func GetTxnDetails(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.GetTxnDetailsRequest)
	reply := resp.(*kvpb.GetTxnDetailsResponse)

	desc := cArgs.EvalCtx.Desc()
	rangeBounds := desc.KeySpan().AsRawSpanWithNoLocals()

	for _, ws := range args.WriteSpans {
		clipped := ws.Intersect(rangeBounds)
		if !clipped.Valid() {
			continue
		}
		if err := collectWrites(ctx, reader, &clipped, args.CommitTimestamp, reply); err != nil {
			return result.Result{}, err
		}
	}

	// Collect read dependencies using the CommitIndex.
	commitIndex := cArgs.EvalCtx.GetCommitIndex()
	if commitIndex == nil || args.DependencyCutoff.IsEmpty() {
		reply.EventHorizon = args.CommitTimestamp
	} else {
		deps := make(map[uuid.UUID]struct{})
		reply.EventHorizon = args.DependencyCutoff
		for _, rs := range args.ReadSpans {
			clipped := rs.Intersect(rangeBounds)
			if !clipped.Valid() {
				continue
			}
			if err := collectDependencies(
				ctx, reader, &clipped,
				args.CommitTimestamp, args.DependencyCutoff,
				args.TxnID, commitIndex, deps, &reply.EventHorizon,
			); err != nil {
				return result.Result{}, err
			}
		}
		for id := range deps {
			reply.Dependencies = append(reply.Dependencies, id)
		}
	}

	return result.Result{}, nil
}

// collectWrites scans a single span for MVCC values written at exactly
// commitTS and appends TxnDetailKV entries to the response. For each write
// found, it also retrieves the previous value by stepping to the next older
// MVCC version using NextIgnoringTime.
func collectWrites(
	ctx context.Context,
	reader storage.Reader,
	span *roachpb.Span,
	commitTS hlc.Timestamp,
	reply *kvpb.GetTxnDetailsResponse,
) error {
	startKey := span.Key
	endKey := span.EndKey
	if len(endKey) == 0 {
		endKey = startKey.Next()
	}

	iter, err := storage.NewMVCCIncrementalIterator(ctx, reader, storage.MVCCIncrementalIterOptions{
		KeyTypes:     storage.IterKeyTypePointsOnly,
		StartKey:     startKey,
		EndKey:       endKey,
		StartTime:    commitTS.Prev(),
		EndTime:      commitTS,
		IntentPolicy: storage.MVCCIncrementalIterIntentPolicyIgnore,
		ReadCategory: fs.BatchEvalReadCategory,
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	iter.SeekGE(storage.MVCCKey{Key: startKey})
	for {
		ok, err := iter.Valid()
		if err != nil {
			return err
		}
		if !ok {
			break
		}

		key := iter.UnsafeKey()
		if !key.IsValue() {
			iter.Next()
			continue
		}

		valRaw, err := iter.UnsafeValue()
		if err != nil {
			return err
		}
		mvccVal, err := storage.DecodeMVCCValue(valRaw)
		if err != nil {
			return err
		}

		// Clone the key and value before advancing the iterator, since
		// UnsafeKey/UnsafeValue share the iterator's internal buffer.
		clonedKey := key.Key.Clone()
		var kv roachpb.KeyValue
		kv.Key = clonedKey
		if !mvccVal.IsTombstone() {
			kv.Value.RawBytes = append([]byte(nil), mvccVal.Value.RawBytes...)
			kv.Value.Timestamp = key.Timestamp
		}

		detail := kvpb.TxnDetailKV{KeyValue: kv}

		// Step to the next MVCC version to get the previous value. We use
		// NextIgnoringTime since the previous version is outside our time
		// bounds.
		iter.NextIgnoringTime()
		if ok, err = iter.Valid(); err != nil {
			return err
		}
		onSameKey := false
		if ok {
			prevKey := iter.UnsafeKey()
			if prevKey.IsValue() && bytes.Equal(prevKey.Key, clonedKey) {
				onSameKey = true
				prevRaw, err := iter.UnsafeValue()
				if err != nil {
					return err
				}
				prevMVCC, err := storage.DecodeMVCCValue(prevRaw)
				if err != nil {
					return err
				}
				if !prevMVCC.IsTombstone() {
					detail.PrevValue.RawBytes = append([]byte(nil), prevMVCC.Value.RawBytes...)
					detail.PrevValue.Timestamp = prevKey.Timestamp
				}
			}
		}

		reply.Writes = append(reply.Writes, detail)

		// After NextIgnoringTime, the iterator is in "ignoring time" mode.
		// We must re-enter time-bounded iteration without skipping keys.
		if onSameKey {
			// Still on the same user key; skip remaining versions.
			iter.NextKey()
		} else if ok {
			// On a different key. Re-seek at the user key to re-apply
			// time bounds from the current position without advancing.
			iter.SeekGE(storage.MVCCKey{Key: iter.UnsafeKey().Key})
		}
	}
	return nil
}

// collectDependencies scans a single read span for the latest MVCC version of
// each key within (dependencyCutoff, commitTS]. For each such version, it looks
// up the writing transaction in the CommitIndex and adds it to deps. Tombstones
// are skipped since they represent deletes, not values the transaction read.
//
// When the latest version has timestamp == commitTS, it may be our own write.
// In that case the value we actually read is the prior version. Rather than
// inspecting the CommitIndex to determine authorship, we simply process both
// versions — the self-exclusion filter (id != selfTxnID) prevents us from
// adding ourselves, and overstating deps is acceptable.
//
// If a timestamp is not found in the CommitIndex, eventHorizon is forwarded to
// that timestamp. The caller uses eventHorizon to communicate which portion of
// the time window has complete dependency coverage.
func collectDependencies(
	ctx context.Context,
	reader storage.Reader,
	span *roachpb.Span,
	commitTS, dependencyCutoff hlc.Timestamp,
	selfTxnID uuid.UUID,
	commitIndex *txnfeed.CommitIndex,
	deps map[uuid.UUID]struct{},
	eventHorizon *hlc.Timestamp,
) error {
	startKey := span.Key
	endKey := span.EndKey
	if len(endKey) == 0 {
		endKey = startKey.Next()
	}

	// Use IntentPolicyError so that unresolved intents surface as
	// WriteIntentError. The concurrency manager will push the intent's
	// transaction, resolve the intent (populating the CommitIndex via
	// MVCCCommitIntentOp), and retry. This guarantees the CommitIndex
	// has an entry for every committed writer by the time we look it up.
	iter, err := storage.NewMVCCIncrementalIterator(ctx, reader, storage.MVCCIncrementalIterOptions{
		KeyTypes:     storage.IterKeyTypePointsOnly,
		StartKey:     startKey,
		EndKey:       endKey,
		StartTime:    dependencyCutoff,
		EndTime:      commitTS,
		IntentPolicy: storage.MVCCIncrementalIterIntentPolicyError,
		ReadCategory: fs.BatchEvalReadCategory,
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	iter.SeekGE(storage.MVCCKey{Key: startKey})
	for {
		ok, err := iter.Valid()
		if err != nil {
			return err
		}
		if !ok {
			break
		}

		key := iter.UnsafeKey()
		if !key.IsValue() {
			iter.Next()
			continue
		}

		valRaw, err := iter.UnsafeValue()
		if err != nil {
			return err
		}
		mvccVal, err := storage.DecodeMVCCValue(valRaw)
		if err != nil {
			return err
		}

		if !mvccVal.IsTombstone() {
			addDep(key.Timestamp, selfTxnID, commitIndex, deps, eventHorizon)
		}

		if key.Timestamp == commitTS {
			// This version may be our own write. The value we actually
			// read is the prior version, so also check it. If it turns
			// out this wasn't our write, we harmlessly overstate deps.
			savedKey := key.Key.Clone()
			iter.Next()
			ok, err = iter.Valid()
			if err != nil {
				return err
			}
			if ok && iter.UnsafeKey().IsValue() &&
				bytes.Equal(iter.UnsafeKey().Key, savedKey) {
				prevRaw, err := iter.UnsafeValue()
				if err != nil {
					return err
				}
				prevVal, err := storage.DecodeMVCCValue(prevRaw)
				if err != nil {
					return err
				}
				if !prevVal.IsTombstone() {
					addDep(iter.UnsafeKey().Timestamp, selfTxnID, commitIndex, deps, eventHorizon)
				}
				iter.NextKey()
			}
			// If no prior version was found on this key, the iterator
			// is already positioned on the next key (or exhausted).
		} else {
			iter.NextKey()
		}
	}
	return nil
}

// addDep looks up ts in the CommitIndex and adds all txnIDs except
// selfTxnID to deps. If ts is not found, eventHorizon is forwarded.
func addDep(
	ts hlc.Timestamp,
	selfTxnID uuid.UUID,
	commitIndex *txnfeed.CommitIndex,
	deps map[uuid.UUID]struct{},
	eventHorizon *hlc.Timestamp,
) {
	txnIDs, found := commitIndex.Lookup(ts)
	if found {
		for _, id := range txnIDs {
			if id != selfTxnID {
				deps[id] = struct{}{}
			}
		}
	} else {
		eventHorizon.Forward(ts)
	}
}
