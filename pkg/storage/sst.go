// Copyright 2021 The Cockroach Authors.
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
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/sstable"
)

// CheckSSTConflicts iterates over an SST and a Reader in lockstep and errors
// out if it finds any conflicts. This includes intents and existing keys with a
// timestamp at or above the SST key timestamp.
//
// If disallowShadowingBelow is non-empty, it also errors for any existing live
// key at the SST key timestamp, but allows shadowing an existing key if its
// timestamp is above the given timestamp and the values are equal. See comment
// on AddSSTableRequest.DisallowShadowingBelow for details.
//
// If disallowShadowing is true, it also errors for any existing live key at the
// SST key timestamp, and ignores entries that exactly match an existing entry
// (key/value/timestamp), for backwards compatibility. If disallowShadowingBelow
// is non-empty, disallowShadowing is ignored.
//
// The given SST and reader cannot contain intents or inline values (i.e. zero
// timestamps), but this is only checked for keys that exist in both sides, for
// performance.
//
// The returned MVCC statistics is a delta between the SST-only statistics and
// their effect when applied, which when added to the SST statistics will adjust
// them for existing keys and values.
func CheckSSTConflicts(
	ctx context.Context,
	sst []byte,
	reader Reader,
	start, end MVCCKey,
	disallowShadowing bool,
	disallowShadowingBelow hlc.Timestamp,
	maxIntents int64,
	usePrefixSeek bool,
) (enginepb.MVCCStats, error) {

	allowIdempotentHelper := func(extTimestamp hlc.Timestamp) bool {
		return (!disallowShadowingBelow.IsEmpty() && disallowShadowingBelow.LessEq(extTimestamp)) ||
			(disallowShadowingBelow.IsEmpty() && disallowShadowing)
	}

	// Check for any range keys.
	rkIter, err := NewPebbleMemSSTIterator(sst, false /* verify */, IterOptions{
		KeyTypes:   IterKeyTypeRangesOnly,
		LowerBound: keys.MinKey,
		UpperBound: keys.MaxKey,
	})
	if err != nil {
		return enginepb.MVCCStats{}, err
	}
	rkIter.SeekGE(NilKey)

	if ok, err := rkIter.Valid(); err != nil {
		return enginepb.MVCCStats{}, err
	} else if ok {
		// If the incoming SST contains range tombstones, we cannot use prefix
		// iteration.
		usePrefixSeek = false
	}
	rkIter.Close()

	rkIter = reader.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{
		UpperBound: end.Key,
		KeyTypes:   IterKeyTypeRangesOnly,
	})
	rkIter.SeekLT(start)

	// Check both the current key and the next key for range keys. We need to
	// check the next one in case SeekLT took us well behind the span
	// we're interested in.
	if ok, err := rkIter.Valid(); err != nil {
		return enginepb.MVCCStats{}, err
	} else if ok {
		if rkIter.RangeBounds().ContainsKey(start.Key) {
			// If the engine contains range tombstones in this span, we cannot use prefix
			// iteration.
			usePrefixSeek = false
		}
		rkIter.Next()
	} else {
		rkIter.SeekGE(start)
	}
	if ok, err := rkIter.Valid(); err != nil {
		return enginepb.MVCCStats{}, err
	} else if ok {
		// If the engine contains range tombstones in this span, we cannot use prefix
		// iteration.
		usePrefixSeek = false
	}
	rkIter.Close()

	var statsDiff enginepb.MVCCStats
	var intents []roachpb.Intent

	if usePrefixSeek {
		// If we're going to be using a prefix iterator, check for the fast path
		// first, where there are no keys in the reader between the sstable's start
		// and end keys. We use a non-prefix iterator for this search, and reopen a
		// prefix one if there are engine keys in the span.
		nonPrefixIter := reader.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: end.Key})
		nonPrefixIter.SeekGE(start)
		valid, err := nonPrefixIter.Valid()
		nonPrefixIter.Close()
		if !valid {
			return statsDiff, err
		}
	}

	extIter := reader.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{
		KeyTypes:     IterKeyTypePointsAndRanges,
		UpperBound:   end.Key,
		Prefix:       usePrefixSeek,
		useL6Filters: true,
	})
	defer extIter.Close()

	sstIter, err := NewPebbleMemSSTIterator(sst, false, IterOptions{
		KeyTypes:   IterKeyTypePointsAndRanges,
		UpperBound: end.Key,
	})
	if err != nil {
		return enginepb.MVCCStats{}, err
	}
	defer sstIter.Close()

	compareForCollision := func(sstKey, extKey MVCCKey, sstValueRaw, extValueRaw []byte) error {
		// Make sure both keys are proper committed MVCC keys. Note that this is
		// only checked when the key exists both in the SST and existing data, it is
		// not an exhaustive check of the SST.
		if !sstKey.IsValue() {
			return errors.New("SST keys must have timestamps")
		}
		sstValue, ok, err := tryDecodeSimpleMVCCValue(sstValueRaw)
		if !ok && err == nil {
			sstValue, err = decodeExtendedMVCCValue(sstValueRaw)
		}
		if err != nil {
			return err
		}
		if !extKey.IsValue() {
			var mvccMeta enginepb.MVCCMetadata
			if err = extIter.ValueProto(&mvccMeta); err != nil {
				return err
			}
			if len(mvccMeta.RawBytes) > 0 {
				return errors.New("inline values are unsupported")
			} else if mvccMeta.Txn == nil {
				return errors.New("found intent without transaction")
			} else {
				// If we encounter a write intent, keep looking for additional intents
				// in order to return a large batch for intent resolution. The caller
				// will likely resolve the returned intents and retry the call, which
				// would be quadratic, so this significantly reduces the overall number
				// of scans.
				intents = append(intents, roachpb.MakeIntent(mvccMeta.Txn, extIter.Key().Key))
				if int64(len(intents)) >= maxIntents {
					return &roachpb.WriteIntentError{Intents: intents}
				}
				return nil
			}
		}
		extValue, ok, err := tryDecodeSimpleMVCCValue(extValueRaw)
		if !ok && err == nil {
			extValue, err = decodeExtendedMVCCValue(extValueRaw)
		}
		if err != nil {
			return err
		}

		// Allow certain idempotent writes where key/timestamp/value all match:
		//
		// * disallowShadowing: any matching key.
		// * disallowShadowingBelow: any matching key at or above the given timestamp.
		allowIdempotent := (!disallowShadowingBelow.IsEmpty() && disallowShadowingBelow.LessEq(extKey.Timestamp)) ||
			(disallowShadowingBelow.IsEmpty() && disallowShadowing)
		if allowIdempotent && sstKey.Timestamp.Equal(extKey.Timestamp) &&
			bytes.Equal(extValueRaw, sstValueRaw) {
			// This SST entry will effectively be a noop, but its stats have already
			// been accounted for resulting in double-counting. To address this we
			// send back a stats diff for these existing KVs so that we can subtract
			// them later. This enables us to construct accurate MVCCStats and
			// prevents expensive recomputation in the future.
			metaKeySize := int64(len(sstKey.Key) + 1)
			metaValSize := int64(0)
			totalBytes := metaKeySize + metaValSize

			// Update the skipped stats to account for the skipped meta key.
			if !sstValue.IsTombstone() {
				statsDiff.LiveBytes -= totalBytes
				statsDiff.LiveCount--
			}
			statsDiff.KeyBytes -= metaKeySize
			statsDiff.ValBytes -= metaValSize
			statsDiff.KeyCount--

			// Update the stats to account for the skipped versioned key/value.
			totalBytes = int64(len(sstValueRaw)) + MVCCVersionTimestampSize
			if !sstValue.IsTombstone() {
				statsDiff.LiveBytes -= totalBytes
			}
			statsDiff.KeyBytes -= MVCCVersionTimestampSize
			statsDiff.ValBytes -= int64(len(sstValueRaw))
			statsDiff.ValCount--

			return nil
		}

		// If requested, check that we're not shadowing a live key. Note that
		// we check this before we check the timestamp, and avoid returning
		// a WriteTooOldError -- that error implies that the client should
		// retry at a higher timestamp, but we already know that such a retry
		// would fail (because it will shadow an existing key).
		if !extValue.IsTombstone() && (!disallowShadowingBelow.IsEmpty() || disallowShadowing) {
			allowShadow := !disallowShadowingBelow.IsEmpty() &&
				disallowShadowingBelow.LessEq(extKey.Timestamp) && bytes.Equal(extValueRaw, sstValueRaw)
			if !allowShadow {
				return errors.Errorf(
					"ingested key collides with an existing one: %s", sstKey.Key)
			}
		}

		// If the existing key has a timestamp at or above the SST key, return a
		// WriteTooOldError. Normally this could cause a transactional request to be
		// automatically retried after a read refresh, which we would only want to
		// do if AddSSTable had SSTTimestampToRequestTimestamp set, but AddSSTable
		// cannot be used in transactions so we don't need to check.
		if sstKey.Timestamp.LessEq(extKey.Timestamp) {
			return roachpb.NewWriteTooOldError(
				sstKey.Timestamp, extKey.Timestamp.Next(), sstKey.Key)
		}

		// If we are shadowing an existing key, we must update the stats accordingly
		// to take into account the existing KV pair.
		statsDiff.KeyCount--
		statsDiff.KeyBytes -= int64(len(extKey.Key) + 1)
		if !extValue.IsTombstone() {
			statsDiff.LiveCount--
			statsDiff.LiveBytes -= int64(len(extKey.Key) + 1)
			statsDiff.LiveBytes -= int64(len(extValueRaw)) + MVCCVersionTimestampSize
		}
		return nil
	}

	sstIter.SeekGE(start)
	sstOK, sstErr := sstIter.Valid()
	var extOK bool
	var extErr error
	var extPrevRangeKey MVCCRangeKeyValue

	if usePrefixSeek {
		// In the case of prefix seeks, do not look at engine iter exhaustion. This
		// is because the engine prefix iterator could be exhausted when it has
		// iterated past its prefix, even if there are other keys after the prefix
		// that should be checked.
		for sstErr == nil && sstOK {
			if err := ctx.Err(); err != nil {
				return enginepb.MVCCStats{}, err
			}
			// extIter is a prefix iterator; it is expected to skip keys that belong
			// to different prefixes. Only iterate along the sst iterator, and re-seek
			// extIter each time.
			extIter.SeekGE(MVCCKey{Key: sstIter.UnsafeKey().Key})
			extOK, extErr = extIter.Valid()
			if extErr != nil {
				break
			}
			if !extOK {
				// There is no key in extIter matching this prefix. Check the next key in
				// sstIter. Note that we can't just use an exhausted extIter as a sign that
				// we are done with the loop; extIter is a prefix iterator and could
				// have keys after the current prefix that it will not return unless
				// re-seeked.
				sstIter.NextKey()
				sstOK, sstErr = sstIter.Valid()
				continue
			}

			extKey, extValueRaw := extIter.UnsafeKey(), extIter.UnsafeValue()
			sstKey, sstValueRaw := sstIter.UnsafeKey(), sstIter.UnsafeValue()

			// We just seeked the engine iter. If it has a mismatching prefix, the
			// iterator is not obeying its contract.
			if !bytes.Equal(extKey.Key, sstKey.Key) {
				return enginepb.MVCCStats{}, errors.Errorf("prefix iterator returned mismatching prefix: %s != %s", extKey.Key, sstKey.Key)
			}

			if err := compareForCollision(sstKey, extKey, sstValueRaw, extValueRaw); err != nil {
				return enginepb.MVCCStats{}, err
			}

			sstIter.NextKey()
			sstOK, sstErr = sstIter.Valid()
		}
	} else {
		// We do a SeekLT to get the previous range key, as we might be interested
		// in that later.
		extIter.SeekLT(start)
		extOK, extErr = extIter.Valid()
		if extOK {
			if _, extHasRange := extIter.HasPointAndRange(); extHasRange {
				extPrevRangeKey = extIter.RangeKeys()[0].Clone()
			}
			extIter.Next()
		} else {
			extIter.SeekGE(start)
		}
		extOK, extErr = extIter.Valid()
	}

	for !usePrefixSeek && sstErr == nil && sstOK && extOK && extErr == nil {
		if err := ctx.Err(); err != nil {
			return enginepb.MVCCStats{}, err
		}
		// We maintain the invariant that the extIter is always positioned at or
		// ahead of the sst iterator. This follows from the fact that we seek
		// extIter after each sstIter positioning operation. This simplifies some
		// of the range key logic below.
		extHasPoint, extHasRange := extIter.HasPointAndRange()
		sstHasPoint, sstHasRange := sstIter.HasPointAndRange()
		// There's a special case where either iterator might be at the start of
		// a range key but followed immediately by a point key that we're actually
		// interested in. This happens when the range key start is interleaved
		// before the MVCC point keys. Detect this case and position at the MVCC
		// key, but only if it has the same MVCC key.
		//
		// We don't need to do this check here; we could just proceed with
		// sstHasPoint == false or extHasPoint == false, but we'd then have to change
		// the NextKey() call at the bottom with a Next() in some cases, and also
		// modify cases where we seek extIter.
		if !sstHasPoint {
			oldKey := sstIter.Key()
			sstIter.Next()
			valid, _ := sstIter.Valid()
			if !valid || !sstIter.UnsafeKey().Key.Equal(oldKey.Key) {
				sstIter.Prev()
			}
			sstHasPoint, sstHasRange = sstIter.HasPointAndRange()
		}
		if !extHasPoint {
			oldKey := extIter.Key()
			extIter.Next()
			valid, _ := extIter.Valid()
			if !valid || !extIter.UnsafeKey().Key.Equal(oldKey.Key) {
				extIter.Prev()
			}
			extHasPoint, extHasRange = extIter.HasPointAndRange()
		}
		// Case where SST and engine both have range keys at the current iterator
		// points. The SST range keys must be newer than engine range keys.
		if extHasRange && sstHasRange {
			extRangeKeys := extIter.RangeKeys()
			sstRangeKeys := sstIter.RangeKeys()
			// Check if the oldest SST range key conflicts with the newest ext
			// range key.
			if sstRangeKeys[len(sstRangeKeys)-1].RangeKey.Overlaps(extRangeKeys[0].RangeKey) {
				sstTombstone := sstRangeKeys[len(sstRangeKeys)-1].RangeKey
				if sstTombstone.Timestamp.Less(extRangeKeys[0].RangeKey.Timestamp) {
					// Conflict. We can't slide an MVCC range tombstone below an
					// existing MVCC range tombstone in the engine.
					return enginepb.MVCCStats{}, roachpb.NewWriteTooOldError(
						sstTombstone.Timestamp, extRangeKeys[0].RangeKey.Timestamp.Next(), sstTombstone.StartKey)
				}
				if ok := allowIdempotentHelper(extRangeKeys[0].RangeKey.Timestamp); !ok &&
					(extRangeKeys[0].RangeKey.Timestamp == sstTombstone.Timestamp) {
					// Idempotence is not allowed.
					return enginepb.MVCCStats{}, errors.Errorf(
						"ingested range key collides with an existing one: %s", sstTombstone)
				}
			}
		}
		// Case where the engine has a range key that might delete the current SST
		// point.
		if sstHasPoint && (extHasRange || extPrevRangeKey.RangeKey.Validate() == nil) {
			// Check if the sst point is deleted by either the current ext range key
			// or the previous one. As the extIter is slightly ahead of the sstIter,
			// the range key overlapping with the current sst point key could be
			// extPrevRangeKey, not extIter.RangeKeys()[0].
			extTombstone := extPrevRangeKey
			sstKey := sstIter.UnsafeKey()
			if extHasRange && (extTombstone.RangeKey.Validate() != nil || extIter.RangeKeys()[0].RangeKey.Includes(sstKey.Key)) {
				extTombstone = extIter.RangeKeys()[0]
			}
			if extTombstone.RangeKey.Deletes(sstKey) {
				// A range tombstone in the engine deletes this SST key. Return
				// a WriteTooOldError.
				return enginepb.MVCCStats{}, roachpb.NewWriteTooOldError(
					sstKey.Timestamp, extTombstone.RangeKey.Timestamp.Next(), sstKey.Key)
			}
		}
		// Mark stats as estimated if there are any SST range keys.
		//
		// TODO(bilal): Remove this when we are able to accurately calculate
		// stats from ingesting MVCC range tombstones.
		if sstHasRange {
			statsDiff.ContainsEstimates = 1
		}
		// Case where the SST has a range key that might overlap with AND be older
		// than the previous engine range key.
		if sstHasRange && extPrevRangeKey.RangeKey.Validate() == nil {
			// Check SST's oldest range tombstone for conflict with the previous
			// engine range key.
			sstTombstone := sstIter.RangeKeys()[len(sstIter.RangeKeys())-1].RangeKey
			if extPrevRangeKey.RangeKey.Overlaps(sstTombstone) && sstTombstone.Timestamp.Less(extPrevRangeKey.RangeKey.Timestamp) {
				return enginepb.MVCCStats{}, roachpb.NewWriteTooOldError(
					sstTombstone.Timestamp, extPrevRangeKey.RangeKey.Timestamp.Next(), sstTombstone.StartKey)
			}
			if ok := allowIdempotentHelper(extPrevRangeKey.RangeKey.Timestamp); !ok &&
				(extPrevRangeKey.RangeKey.Timestamp == sstTombstone.Timestamp) {
				// Idempotence is not allowed.
				return enginepb.MVCCStats{}, errors.Errorf(
					"ingested range key collides with an existing one: %s", sstTombstone)
			}
		}
		// Check that the oldest SST range key is not underneath the current ext
		// point key. If requested (with disallowShadowing or
		// disallowShadowingBelow), check that the newest SST range tombstone does
		// not shadow a live key.
		if sstHasRange && extHasPoint {
			sstBottomTombstone := sstIter.RangeKeys()[len(sstIter.RangeKeys())-1]
			sstTopTombstone := sstIter.RangeKeys()[0]
			extKey := extIter.UnsafeKey()
			extValue, ok, err := tryDecodeSimpleMVCCValue(extIter.UnsafeValue())
			if !ok && err == nil {
				extValue, err = decodeExtendedMVCCValue(extIter.UnsafeValue())
			}
			if err != nil {
				return enginepb.MVCCStats{}, err
			}

			if sstBottomTombstone.RangeKey.Timestamp.LessEq(extKey.Timestamp) {
				// Conflict.
				return enginepb.MVCCStats{}, roachpb.NewWriteTooOldError(
					sstBottomTombstone.RangeKey.Timestamp, extKey.Timestamp.Next(), sstBottomTombstone.RangeKey.StartKey)
			}
			if sstTopTombstone.RangeKey.Deletes(extKey) {
				// Check if shadowing a live key is allowed. Deleting a live key counts
				// as a shadow.
				extValueDeleted := extHasRange && extIter.RangeKeys()[0].RangeKey.Deletes(extKey)
				if !extValue.IsTombstone() && !extValueDeleted && (!disallowShadowingBelow.IsEmpty() || disallowShadowing) {
					// Note that we don't check for value equality here, unlike in the
					// point key shadow case. This is because a range key and a point key
					// by definition have different values.
					return enginepb.MVCCStats{}, errors.Errorf(
						"ingested range key collides with an existing one: %s", sstTopTombstone)
				}
			}
		}
		if extHasRange {
			extPrevRangeKey = extIter.RangeKeys()[0].Clone()
		}

		extKey, extValueRaw := extIter.UnsafeKey(), extIter.UnsafeValue()
		sstKey, sstValueRaw := sstIter.UnsafeKey(), sstIter.UnsafeValue()

		// Keep seeking the iterators until both keys are equal.
		if cmp := bytes.Compare(extKey.Key, sstKey.Key); cmp < 0 {
			// sstIter is further ahead. This should never happen; we always seek
			// extIter after seeking/nexting sstIter.
			panic("unreachable")
		} else if cmp > 0 {
			sstIter.SeekGE(MVCCKey{Key: extKey.Key})
			sstOK, sstErr = sstIter.Valid()
			if sstOK {
				extIter.SeekGE(MVCCKey{Key: sstIter.UnsafeKey().Key})
			}
			extOK, extErr = extIter.Valid()
			continue
		}

		extValueDeleted := extHasRange && extIter.RangeKeys()[0].RangeKey.Deletes(extKey)
		if sstHasPoint && (extHasPoint && !extValueDeleted) {
			if err := compareForCollision(sstKey, extKey, sstValueRaw, extValueRaw); err != nil {
				return enginepb.MVCCStats{}, err
			}
		} else if extValueDeleted {
			statsDiff.KeyCount--
			statsDiff.KeyBytes -= int64(len(extKey.Key) + 1)
		}

		sstIter.NextKey()
		sstOK, sstErr = sstIter.Valid()
		if sstOK {
			extIter.SeekGE(MVCCKey{Key: sstIter.UnsafeKey().Key})
		}
		extOK, extErr = extIter.Valid()
	}

	if extErr != nil {
		return enginepb.MVCCStats{}, extErr
	}
	if sstErr != nil {
		return enginepb.MVCCStats{}, sstErr
	}
	if len(intents) > 0 {
		return enginepb.MVCCStats{}, &roachpb.WriteIntentError{Intents: intents}
	}

	return statsDiff, nil
}

// UpdateSSTTimestamps replaces all MVCC timestamp in the provided SST to the
// given timestamp. All keys must already have the given "from" timestamp.
func UpdateSSTTimestamps(
	ctx context.Context, st *cluster.Settings, sst []byte, from, to hlc.Timestamp, concurrency int,
) ([]byte, error) {
	if from.IsEmpty() {
		return nil, errors.Errorf("from timestamp not given")
	}
	if to.IsEmpty() {
		return nil, errors.Errorf("to timestamp not given")
	}

	sstOut := &MemFile{}
	sstOut.Buffer.Grow(len(sst))

	// Fancy optimized Pebble SST rewriter.
	if concurrency > 0 {
		defaults := DefaultPebbleOptions()
		opts := defaults.MakeReaderOptions()
		if fp := defaults.Levels[0].FilterPolicy; fp != nil && len(opts.Filters) == 0 {
			opts.Filters = map[string]sstable.FilterPolicy{fp.Name(): fp}
		}
		if _, err := sstable.RewriteKeySuffixes(sst,
			opts,
			sstOut,
			MakeIngestionWriterOptions(ctx, st),
			EncodeMVCCTimestampSuffix(from),
			EncodeMVCCTimestampSuffix(to),
			concurrency,
		); err != nil {
			return nil, err
		}
		return sstOut.Bytes(), nil
	}

	// Naïve read/write loop.
	writer := MakeIngestionSSTWriter(ctx, st, sstOut)
	defer writer.Close()

	// Rewrite point keys.
	iter, err := NewPebbleMemSSTIterator(sst, false /* verify */, IterOptions{
		KeyTypes:   IterKeyTypePointsOnly,
		LowerBound: keys.MinKey,
		UpperBound: keys.MaxKey,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for iter.SeekGE(MVCCKey{Key: keys.MinKey}); ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			return nil, err
		} else if !ok {
			break
		}
		key := iter.UnsafeKey()
		if key.Timestamp != from {
			return nil, errors.Errorf("unexpected timestamp %s (expected %s) for key %s",
				key.Timestamp, from, key.Key)
		}
		err = writer.PutRawMVCC(MVCCKey{Key: key.Key, Timestamp: to}, iter.UnsafeValue())
		if err != nil {
			return nil, err
		}
	}

	// Rewrite range keys.
	iter, err = NewPebbleMemSSTIterator(sst, false /* verify */, IterOptions{
		KeyTypes:   IterKeyTypeRangesOnly,
		LowerBound: keys.MinKey,
		UpperBound: keys.MaxKey,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for iter.SeekGE(MVCCKey{Key: keys.MinKey}); ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			return nil, err
		} else if !ok {
			break
		}
		for _, rkv := range iter.RangeKeys() {
			if rkv.RangeKey.Timestamp != from {
				return nil, errors.Errorf("unexpected timestamp %s (expected %s) for range key %s",
					rkv.RangeKey.Timestamp, from, rkv.RangeKey)
			}
			rkv.RangeKey.Timestamp = to
			mvccValue, ok, err := tryDecodeSimpleMVCCValue(rkv.Value)
			if !ok && err == nil {
				mvccValue, err = decodeExtendedMVCCValue(rkv.Value)
			}
			if err != nil {
				return nil, err
			}
			if err = writer.PutMVCCRangeKey(rkv.RangeKey, mvccValue); err != nil {
				return nil, err
			}
		}
	}

	if err = writer.Finish(); err != nil {
		return nil, err
	}

	return sstOut.Bytes(), nil
}
