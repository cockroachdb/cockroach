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
	leftPeekBound, rightPeekBound roachpb.Key,
	disallowShadowing bool,
	disallowShadowingBelow hlc.Timestamp,
	maxIntents int64,
	usePrefixSeek bool,
) (enginepb.MVCCStats, error) {

	allowIdempotentHelper := func(_ hlc.Timestamp) bool { return false }
	if disallowShadowingBelow.IsEmpty() && disallowShadowing {
		allowIdempotentHelper = func(_ hlc.Timestamp) bool { return true }
	} else if !disallowShadowingBelow.IsEmpty() {
		allowIdempotentHelper = func(extTimestamp hlc.Timestamp) bool {
			return disallowShadowingBelow.LessEq(extTimestamp)
		}
	}
	if leftPeekBound == nil {
		leftPeekBound = keys.MinKey
	}
	if rightPeekBound == nil {
		rightPeekBound = keys.MaxKey
	}

	// Check for any range keys.
	//
	// TODO(bilal): Expose reader.Properties.NumRangeKeys() here, so we don't
	// need to read the SST to figure out if it has range keys.
	rkIter, err := NewMemSSTIterator(sst, false /* verify */, IterOptions{
		KeyTypes:   IterKeyTypeRangesOnly,
		LowerBound: keys.MinKey,
		UpperBound: keys.MaxKey,
	})
	if err != nil {
		rkIter.Close()
		return enginepb.MVCCStats{}, err
	}
	rkIter.SeekGE(NilKey)

	if ok, err := rkIter.Valid(); err != nil {
		rkIter.Close()
		return enginepb.MVCCStats{}, err
	} else if ok {
		// If the incoming SST contains range tombstones, we cannot use prefix
		// iteration.
		usePrefixSeek = false
	}
	rkIter.Close()

	rkIter = reader.NewMVCCIterator(MVCCKeyIterKind, IterOptions{
		UpperBound: rightPeekBound,
		KeyTypes:   IterKeyTypeRangesOnly,
	})
	rkIter.SeekGE(start)

	if ok, err := rkIter.Valid(); err != nil {
		rkIter.Close()
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
		LowerBound:   leftPeekBound,
		UpperBound:   rightPeekBound,
		Prefix:       usePrefixSeek,
		useL6Filters: true,
	})
	defer extIter.Close()

	sstIter, err := NewMemSSTIterator(sst, false, IterOptions{
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

			// Cancel the GCBytesAge contribution of the point tombstone (if any)
			// that exists in the SST stats.
			statsDiff.AgeTo(extKey.Timestamp.WallTime)
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
		if extValue.IsTombstone() {
			statsDiff.AgeTo(extKey.Timestamp.WallTime)
		} else {
			statsDiff.AgeTo(sstKey.Timestamp.WallTime)
		}
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
	var sstPrevRangeKeys, extPrevRangeKeys MVCCRangeKeyStack
	var sstFirstRangeKey MVCCRangeKeyStack
	var extPrevKey MVCCKey

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
	} else if sstOK {
		extIter.SeekGE(MVCCKey{Key: sstIter.UnsafeKey().Key})
		extOK, extErr = extIter.Valid()
	}

	for !usePrefixSeek && sstErr == nil && sstOK && extOK && extErr == nil {
		if err := ctx.Err(); err != nil {
			return enginepb.MVCCStats{}, err
		}
		extHasPoint, extHasRange := extIter.HasPointAndRange()
		sstHasPoint, sstHasRange := sstIter.HasPointAndRange()
		var extRangeKeys, sstRangeKeys MVCCRangeKeyStack
		if sstHasRange {
			sstRangeKeys = sstIter.RangeKeys()
			if sstFirstRangeKey.IsEmpty() {
				sstFirstRangeKey = sstRangeKeys.Clone()
			}
		}
		if extHasRange {
			extRangeKeys = extIter.RangeKeys()
		}
		sstRangeKeysChanged := sstHasRange && !sstPrevRangeKeys.Bounds.Equal(sstRangeKeys.Bounds)
		extRangeKeysChanged := extHasRange && !extPrevRangeKeys.Bounds.Equal(extRangeKeys.Bounds)
		extKeyChanged := !extPrevKey.Equal(extIter.UnsafeKey())
		if extKeyChanged {
			extPrevKey = extIter.Key()
		}
		// Case where SST and engine both have range keys at the current iterator
		// points. The SST range keys must be newer than engine range keys.
		if extHasRange && sstHasRange {
			// Check if the oldest SST range key conflicts with the newest ext
			// range key.
			if (sstRangeKeysChanged || extRangeKeysChanged) && sstRangeKeys.Bounds.Overlaps(extRangeKeys.Bounds) {
				sstTombstone := sstRangeKeys.Versions[len(sstRangeKeys.Versions)-1]
				if sstTombstone.Timestamp.Less(extRangeKeys.Versions[0].Timestamp) {
					// Conflict. We can't slide an MVCC range tombstone below an
					// existing MVCC range tombstone in the engine.
					return enginepb.MVCCStats{}, roachpb.NewWriteTooOldError(
						sstTombstone.Timestamp, extRangeKeys.Versions[0].Timestamp.Next(), sstRangeKeys.Bounds.Key)
				}
				if !extRangeKeys.Versions[0].Timestamp.Less(sstTombstone.Timestamp) {
					// Check for idempotent range key additions. The top
					// len(sstRangeKeys.Versions) timestamps must match between the two range
					// key stacks.
					extTombstones := extRangeKeys.Versions.Clone()
					extTombstones.Trim(sstTombstone.Timestamp, hlc.MaxTimestamp)
					isIdempotent := extTombstones.Equal(sstRangeKeys.Versions)
					if ok := allowIdempotentHelper(extRangeKeys.Versions[0].Timestamp); !ok || !isIdempotent {
						// Idempotence is either not allowed or there's a conflict.
						return enginepb.MVCCStats{}, errors.Errorf(
							"ingested range key collides with an existing one: %s", sstTombstone)
					}
				}
			}
		}
		// Case where the engine has a range key that might delete the current SST
		// point.
		if sstHasPoint && extHasRange {
			sstKey := sstIter.UnsafeKey()
			if extRangeKeys.Covers(sstKey) {
				// A range tombstone in the engine deletes this SST key. Return
				// a WriteTooOldError.
				return enginepb.MVCCStats{}, roachpb.NewWriteTooOldError(
					sstKey.Timestamp, extRangeKeys.Versions[0].Timestamp.Next(), sstKey.Key)
			}
		}
		// Check that the oldest SST range key is not underneath the current ext
		// point key. If requested (with disallowShadowing or
		// disallowShadowingBelow), check that the newest SST range tombstone does
		// not shadow a live key.
		if sstHasRange && extHasPoint {
			sstBottomTombstone := sstRangeKeys.Versions[len(sstRangeKeys.Versions)-1]
			sstTopTombstone := sstRangeKeys.Versions[0]
			extKey := extIter.UnsafeKey()
			extValue, ok, err := tryDecodeSimpleMVCCValue(extIter.UnsafeValue())
			if !ok && err == nil {
				extValue, err = decodeExtendedMVCCValue(extIter.UnsafeValue())
			}
			if err != nil {
				return enginepb.MVCCStats{}, err
			}

			if sstBottomTombstone.Timestamp.LessEq(extKey.Timestamp) {
				// Conflict.
				return enginepb.MVCCStats{}, roachpb.NewWriteTooOldError(
					sstBottomTombstone.Timestamp, extKey.Timestamp.Next(), sstRangeKeys.Bounds.Key)
			}
			if sstRangeKeys.Covers(extKey) {
				// Check if shadowing a live key is allowed. Deleting a live key counts
				// as a shadow.
				extValueDeleted := extHasRange && extRangeKeys.Covers(extKey)
				if !extValue.IsTombstone() && !extValueDeleted && (!disallowShadowingBelow.IsEmpty() || disallowShadowing) {
					// Note that we don't check for value equality here, unlike in the
					// point key shadow case. This is because a range key and a point key
					// by definition have different values.
					return enginepb.MVCCStats{}, errors.Errorf(
						"ingested range key collides with an existing one: %s", sstTopTombstone)
				}
				if !extValueDeleted {
					sstRangeKeyVersion, ok := sstRangeKeys.FirstAtOrAbove(extKey.Timestamp)
					if !ok {
						return enginepb.MVCCStats{}, errors.AssertionFailedf("expected range tombstone above timestamp %v", extKey.Timestamp)
					}
					sstPointShadowsExtPoint := sstHasPoint && sstIter.UnsafeKey().Key.Equal(extKey.Key)
					if (extKeyChanged || sstRangeKeysChanged) && !sstPointShadowsExtPoint {
						statsDiff.Add(updateStatsOnRangeKeyCover(sstRangeKeyVersion.Timestamp, extKey, extIter.UnsafeValue()))
					} else if !extKeyChanged && sstPointShadowsExtPoint {
						// This is either a conflict, shadow, or idempotent operation.
						// Subtract the RangeKeyCover stats diff from the last iteration, as
						// compareForCollision will account for the shadow.
						statsDiff.Subtract(updateStatsOnRangeKeyCover(sstRangeKeyVersion.Timestamp, extKey, extIter.UnsafeValue()))
					}
				}
			}
		}

		if sstRangeKeysChanged {
			if extHasRange && extRangeKeys.Bounds.Overlaps(sstRangeKeys.Bounds) {
				mergedIntoExisting := false
				switch sstRangeKeys.Bounds.Key.Compare(extRangeKeys.Bounds.Key) {
				case -1:
					// sstRangeKey starts earlier than extRangeKey. Add a fragment
					statsDiff.RangeKeyBytes += int64(EncodedMVCCKeyPrefixLength(extRangeKeys.Bounds.Key))
					addedFragment := MVCCRangeKeyStack{
						Bounds:   roachpb.Span{Key: sstRangeKeys.Bounds.Key, EndKey: extRangeKeys.Bounds.Key},
						Versions: sstRangeKeys.Versions,
					}
					if addedFragment.CanMergeRight(extRangeKeys) {
						statsDiff.Add(updateStatsOnRangeKeyMerge(extRangeKeys.Bounds.Key, sstRangeKeys.Versions))
						// Remove the contribution for the end key.
						statsDiff.RangeKeyBytes -= int64(EncodedMVCCKeyPrefixLength(sstRangeKeys.Bounds.EndKey))
						mergedIntoExisting = true
					} else {
						// Add the sst range key versions again, to account for the overlap
						// with extRangeKeys.
						for _, v := range sstRangeKeys.Versions {
							statsDiff.Add(updateStatsOnRangeKeyPutVersion(extRangeKeys, v))
						}
					}
				case 0:
					// Same start key. No need to encode the start key again.
					statsDiff.RangeKeyCount--
					statsDiff.RangeKeyBytes -= int64(EncodedMVCCKeyPrefixLength(sstRangeKeys.Bounds.Key))
				case 1:
					// This SST start key fragments the ext range key. Unless the ext
					// range key has already been fragmented at this point by sstPrevRangeKey.
					if sstPrevRangeKeys.IsEmpty() || !sstPrevRangeKeys.Bounds.EndKey.Equal(sstRangeKeys.Bounds.Key) {
						statsDiff.Add(UpdateStatsOnRangeKeySplit(sstRangeKeys.Bounds.Key, extRangeKeys.Versions))
					}
					// No need to re-encode the start key, as UpdateStatsOnRangeKeySplit has already
					// done that for us.
					statsDiff.RangeKeyCount--
					statsDiff.RangeKeyBytes -= int64(EncodedMVCCKeyPrefixLength(sstRangeKeys.Bounds.Key))
				}
				// Check if the overlapping part of sstRangeKeys and extRangeKeys has
				// idempotent versions. We already know this isn't a conflict, as that
				// check happened earlier.
				if !mergedIntoExisting {
					idempotentIdx := 0
					for _, v := range sstRangeKeys.Versions {
						if idempotentIdx >= len(extRangeKeys.Versions) || !v.Equal(extRangeKeys.Versions[idempotentIdx]) {
							break
						}
						// Subtract stats for this version, as it already exists in the
						// engine.
						statsDiff.Subtract(updateStatsOnRangeKeyPutVersion(sstRangeKeys, v))
						idempotentIdx++
					}
					switch extRangeKeys.Bounds.EndKey.Compare(sstRangeKeys.Bounds.EndKey) {
					case +1:
						statsDiff.Add(UpdateStatsOnRangeKeySplit(sstRangeKeys.Bounds.EndKey, extRangeKeys.Versions))
						// Remove the contribution for the end key.
						statsDiff.RangeKeyBytes -= int64(EncodedMVCCKeyPrefixLength(sstRangeKeys.Bounds.EndKey))
					case 0:
						// Remove the contribution for the end key.
						statsDiff.RangeKeyBytes -= int64(EncodedMVCCKeyPrefixLength(sstRangeKeys.Bounds.EndKey))
					case -1:
						statsDiff.Add(UpdateStatsOnRangeKeySplit(extRangeKeys.Bounds.EndKey, sstRangeKeys.Versions))
						// Remove the contribution for the end key.
						statsDiff.RangeKeyBytes -= int64(EncodedMVCCKeyPrefixLength(sstRangeKeys.Bounds.EndKey))
					}
				}
			}
			if extHasRange && sstRangeKeys.CanMergeRight(extRangeKeys) {
				statsDiff.Add(updateStatsOnRangeKeyMerge(sstRangeKeys.Bounds.EndKey, sstRangeKeys.Versions))
			}
			if !extPrevRangeKeys.IsEmpty() && extPrevRangeKeys.CanMergeRight(sstRangeKeys) {
				statsDiff.Add(updateStatsOnRangeKeyMerge(sstRangeKeys.Bounds.Key, sstRangeKeys.Versions))
			} else if !extHasRange || extRangeKeys.Bounds.Key.Compare(sstRangeKeys.Bounds.Key) >= 0 {
				// Complication: we need to check if there's a range key to the left of
				// this range key that we could merge with. The only foolproof way
				// to do that is to copy the current iterator position in its entirety,
				// call PeekRangeKeyLeft, and then SeekGE the engine iterator back
				// to its original position.
				savedExtKey := extIter.Key()
				pos, peekedExtRangeKeys, err := PeekRangeKeysLeft(extIter, sstRangeKeys.Bounds.Key)
				if err != nil {
					return enginepb.MVCCStats{}, err
				}
				if pos == 0 && peekedExtRangeKeys.CanMergeRight(sstRangeKeys) {
					statsDiff.Add(updateStatsOnRangeKeyMerge(sstRangeKeys.Bounds.Key, sstRangeKeys.Versions))
				}
				extIter.SeekGE(savedExtKey)
			}
			if extRangeKeysChanged && !sstPrevRangeKeys.IsEmpty() && sstPrevRangeKeys.Bounds.Overlaps(extRangeKeys.Bounds) {
				// Because we always re-seek the extIter after every sstIter step,
				// it is possible that we missed an overlap between extRangeKeys and
				// sstPrevRangeKeys. Account for that here by adding the version stats
				// for sstPrevRangeKeys.
				for _, v := range sstPrevRangeKeys.Versions {
					statsDiff.Add(updateStatsOnRangeKeyPutVersion(extRangeKeys, v))
				}
			}
			sstPrevRangeKeys = sstRangeKeys.Clone()
		}
		if extRangeKeysChanged {
			if !extPrevRangeKeys.IsEmpty() && extPrevRangeKeys.Bounds.EndKey.Compare(extRangeKeys.Bounds.Key) < 0 &&
				sstHasRange && sstRangeKeys.Bounds.Key.Compare(extRangeKeys.Bounds.Key) < 0 &&
				sstRangeKeys.Bounds.Overlaps(extRangeKeys.Bounds) {
				// We're adding a fragment between two non-abutting engine range keys.
				startKey := extPrevRangeKeys.Bounds.EndKey
				if sstRangeKeys.Bounds.Key.Compare(startKey) > 0 {
					startKey = sstRangeKeys.Bounds.Key
				}
				endKey := extRangeKeys.Bounds.Key
				newRangeKeyStack := MVCCRangeKeyStack{
					Versions: sstRangeKeys.Versions,
					Bounds:   roachpb.Span{Key: startKey, EndKey: endKey},
				}
				statsDiff.Add(updateStatsOnRangeKeyPut(newRangeKeyStack))
				if extPrevRangeKeys.CanMergeRight(newRangeKeyStack) {
					statsDiff.Add(updateStatsOnRangeKeyMerge(startKey, newRangeKeyStack.Versions))
				}
			}
			// Note that we exclude sstRangeKeysChanged below, as this case only
			// accounts for additional ext range keys that this SST range key stack
			// could be adding versions to. The very first ext range key stack that
			// this sst stack contributes stats to is already accounted by the
			// sstRangeKeysChanged conditional above.
			if sstHasRange && sstRangeKeys.Bounds.Overlaps(extRangeKeys.Bounds) && !sstRangeKeysChanged {
				idempotentIdx := 0
				for _, v := range sstRangeKeys.Versions {
					if len(extRangeKeys.Versions) > idempotentIdx && v.Timestamp.Equal(extRangeKeys.Versions[idempotentIdx].Timestamp) {
						// Skip this version, as it already exists in the engine.
						idempotentIdx++
						continue
					}
					statsDiff.Add(updateStatsOnRangeKeyPutVersion(extRangeKeys, v))
				}
				// Check if this ext range key is going to fragment the SST range key.
				if sstRangeKeys.Bounds.Key.Compare(extRangeKeys.Bounds.Key) < 0 && !extRangeKeys.Versions.Equal(sstRangeKeys.Versions) &&
					(extPrevRangeKeys.IsEmpty() || !extPrevRangeKeys.Bounds.EndKey.Equal(extRangeKeys.Bounds.Key)) {
					statsDiff.Add(UpdateStatsOnRangeKeySplit(extRangeKeys.Bounds.Key, sstRangeKeys.Versions))
					// Remove the contribution for versions, as that's already been added.
					for _, v := range sstRangeKeys.Versions {
						statsDiff.Subtract(updateStatsOnRangeKeyPutVersion(extRangeKeys, v))
					}
					statsDiff.RangeKeyBytes -= int64(EncodedMVCCKeyPrefixLength(extRangeKeys.Bounds.Key))
					statsDiff.RangeKeyCount--
				} else if !extPrevRangeKeys.IsEmpty() && extPrevRangeKeys.Bounds.EndKey.Equal(extRangeKeys.Bounds.Key) {
					// Remove the contribution for versions, as that's already been added.
					for _, v := range sstRangeKeys.Versions {
						statsDiff.Subtract(updateStatsOnRangeKeyPutVersion(extRangeKeys, v))
					}
					statsDiff.RangeKeyBytes -= int64(EncodedMVCCKeyPrefixLength(extRangeKeys.Bounds.Key))
					statsDiff.RangeKeyCount--
				}
				// Check if this ext range key is going to be fragmented by the sst
				// range key's end key.
				switch extRangeKeys.Bounds.EndKey.Compare(sstRangeKeys.Bounds.EndKey) {
				case +1:
					if !extRangeKeys.Versions.Equal(sstRangeKeys.Versions) {
						// This SST range key will fragment this ext range key.
						statsDiff.Add(UpdateStatsOnRangeKeySplit(sstRangeKeys.Bounds.EndKey, extRangeKeys.Versions))
					}
					// Remove the contribution for the end key.
					statsDiff.RangeKeyBytes -= int64(EncodedMVCCKeyPrefixLength(sstRangeKeys.Bounds.EndKey))
				case 0:
					// Remove the contribution for the end key.
					statsDiff.RangeKeyBytes -= int64(EncodedMVCCKeyPrefixLength(sstRangeKeys.Bounds.EndKey))
				case -1:
					if !extRangeKeys.Versions.Equal(sstRangeKeys.Versions) {
						// This ext range key's end will fragment this sst range key.
						statsDiff.Add(UpdateStatsOnRangeKeySplit(extRangeKeys.Bounds.EndKey, sstRangeKeys.Versions))
						statsDiff.RangeKeyBytes -= int64(EncodedMVCCKeyPrefixLength(extRangeKeys.Bounds.EndKey))
					}
				}
			}
			if !sstPrevRangeKeys.IsEmpty() && sstPrevRangeKeys.CanMergeRight(extRangeKeys) && !sstRangeKeysChanged {
				// We exclude !sstRangeKeysChanged to avoid double-counting this merge.
				statsDiff.Add(updateStatsOnRangeKeyMerge(sstPrevRangeKeys.Bounds.EndKey, extRangeKeys.Versions))
			}
			extPrevRangeKeys = extRangeKeys.Clone()
		}

		extKey, extValueRaw := extIter.UnsafeKey(), extIter.UnsafeValue()
		sstKey, sstValueRaw := sstIter.UnsafeKey(), sstIter.UnsafeValue()

		// Keep seeking the iterators until both keys are equal.
		if cmp := bytes.Compare(extKey.Key, sstKey.Key); cmp < 0 {
			// sstIter is further ahead. This should never happen; we always seek
			// extIter after seeking/nexting sstIter.
			return enginepb.MVCCStats{}, errors.AssertionFailedf("expected engine iter to be ahead of sst iter")
		} else if cmp > 0 && sstHasPoint {
			// We exclude !sstHasPoint above in case we were at a range key pause
			// point that matches extKey. In that case, the below SeekGE would make
			// no forward progress.
			sstIter.SeekGE(MVCCKey{Key: extKey.Key})
			sstOK, sstErr = sstIter.Valid()
			if sstOK {
				extIter.SeekGE(MVCCKey{Key: sstIter.UnsafeKey().Key})
			}
			extOK, extErr = extIter.Valid()
			continue
		}

		extValueDeletedByRange := extHasRange && extHasPoint && extRangeKeys.Covers(extKey)
		if sstHasPoint && extHasPoint && !extValueDeletedByRange {
			if err := compareForCollision(sstKey, extKey, sstValueRaw, extValueRaw); err != nil {
				return enginepb.MVCCStats{}, err
			}
		} else if extValueDeletedByRange {
			// Don't double-count the current key.
			statsDiff.KeyCount--
			statsDiff.KeyBytes -= int64(len(extKey.Key) + 1)
		}

		steppedExtIter := false
		// Before Next-ing the SST iter, if it contains any range keys, check if both:
		// 1) the next SST key takes us outside the current SST range key
		// 2) the next ext key overlaps with the current sst range key
		// In that case, we want to step the ext iter forward and seek the sst
		// iter back at it.
		//
		// This handles cases like this, where the b-d range key could get ignored:
		// sst:  a-----c     e
		// ext:  a  b-----d
		if sstHasRange && sstRangeKeys.Bounds.ContainsKey(extKey.Key) {
			// Check for condition 1.
			//
			// NB: sstPrevRangeKeys is already a clone of the current sstRangeKeys.
			sstPrevKey := sstIter.Key()
			sstRangeKeys = sstPrevRangeKeys
			if sstHasPoint {
				sstIter.NextKey()
			} else {
				sstIter.Next()
			}
			sstOK, _ = sstIter.Valid()
			if !sstOK || sstPrevRangeKeys.Bounds.ContainsKey(sstIter.UnsafeKey().Key) {
				// Restore the sst iter and continue on. The below Next()ing logic is
				// sufficient in this case.
				sstIter.SeekGE(sstPrevKey)
				sstOK, sstErr = sstIter.Valid()
			} else {
				extPrevKey := extIter.Key()
				if extHasPoint {
					extIter.NextKey()
				} else {
					extIter.Next()
				}
				extOK, extErr = extIter.Valid()
				if extOK && sstPrevRangeKeys.Bounds.ContainsKey(extIter.UnsafeKey().Key) {
					// Skip the Next()ing logic below so we can check for overlaps
					// between this ext key and the same sst key. Note that we need
					// to restore the sst iter back to the same range key pause point.
					steppedExtIter = true
					sstIter.SeekGE(MVCCKey{Key: extIter.UnsafeKey().Key})
					sstOK, sstErr = sstIter.Valid()
				} else {
					// Special case: if extIter is at a range key that sstPrevRangeKeys
					// merges into, *and* the next SST key is outside the bounds of this
					// SST range key, then account for that merge. If we hadn't excluded
					// the case where the current SST key is within its own range key
					// bounds, we'd have double-counted the merge when we did the collision
					// check.
					if extOK && sstOK && !sstPrevRangeKeys.Bounds.ContainsKey(sstIter.UnsafeKey().Key) {
						_, extHasRange = extIter.HasPointAndRange()
						if extHasRange && sstPrevRangeKeys.CanMergeRight(extIter.RangeKeys()) {
							statsDiff.Add(updateStatsOnRangeKeyMerge(sstPrevRangeKeys.Bounds.EndKey, sstPrevRangeKeys.Versions))
						}
					}
					// Fall back to the below Next()ing logic.
					sstIter.SeekGE(sstPrevKey)
					sstOK, sstErr = sstIter.Valid()
					extIter.SeekGE(extPrevKey)
					extOK, extErr = extIter.Valid()
				}
			}
		}
		// Calling NextKey is only safe if both iterators are at a point key. This is
		// because there could be a point key hiding behind the range key that we're
		// currently at, and NextKey() would skip over it.
		//
		// The below logic accounts for all combinations of point keys and range
		// keys being present and not present at the current iterator positions.
		// Note that SeekGE()s pause at the seek key if there's a covering range key
		// however we need to take care to not go into an infinite loop of seeks
		// if we step one iterator past a transient range key pausing point and
		// seek the other, and on the next iteration, step the second iterator
		// and seek the former iterator back to the same point.
		if sstHasPoint && extHasPoint && !steppedExtIter {
			sstIter.NextKey()
			sstOK, sstErr = sstIter.Valid()
			if sstOK {
				extIter.SeekGE(MVCCKey{Key: sstIter.UnsafeKey().Key})
			}
			extOK, extErr = extIter.Valid()
		} else if !steppedExtIter {
			oldKey := sstIter.Key()
			if sstHasPoint { // !extHasPoint
				// Check if ext has a point at this key. If not, NextKey() on sstIter
				// and seek extIter.
				extIter.Next()
				steppedExtIter = true
				extOK, extErr = extIter.Valid()
				if extErr != nil {
					return enginepb.MVCCStats{}, extErr
				}
				if !extOK || !extIter.UnsafeKey().Key.Equal(oldKey.Key) {
					// extIter either went out of bounds or stepped one key ahead. Re-seek
					// it at the next SST key.
					sstIter.NextKey()
					sstOK, sstErr = sstIter.Valid()
					if sstOK {
						extIter.SeekGE(MVCCKey{Key: sstIter.UnsafeKey().Key})
					}
					extOK, extErr = extIter.Valid()
				}
				// If extIter found a point key at the same MVCC Key, we still need
				// to check for conflicts against it.
			} else if extHasPoint { // !sstHasPoint
				// Similar logic as above, but with the iterators swapped. The one key
				// difference is what we do when the sstIter changes keys.
				sstIter.Next()
				sstOK, sstErr = sstIter.Valid()
				if sstErr != nil {
					return enginepb.MVCCStats{}, sstErr
				}
				if sstOK && !sstIter.UnsafeKey().Key.Equal(oldKey.Key) {
					// sstIter stepped one key ahead. Re-seek extIter at this key.
					extIter.SeekGE(MVCCKey{Key: sstIter.UnsafeKey().Key})
					extOK, extErr = extIter.Valid()
				}
				// If sstIter found a point key at the same MVCC Key, we still need
				// to check for conflicts against it.
			} else { // sstHasRange && extHasRange && !sstHasPoint && !extHasPoint
				// Step both iterators forward. If one iterator stays at the same key,
				// seek the other one back to the same key.
				//
				// Note that we can't do this if either sstHasPoint or extHasPoint, as
				// this logic does not guarantee forward progress in those cases.
				sstIter.Next()
				sstOK, sstErr = sstIter.Valid()
				extIter.Next()
				steppedExtIter = true
				extOK, extErr = extIter.Valid()
				sstChangedKeys := !sstOK || !sstIter.UnsafeKey().Key.Equal(oldKey.Key)
				extChangedKeys := !extOK || !extIter.UnsafeKey().Key.Equal(oldKey.Key)
				if sstChangedKeys && !extChangedKeys {
					sstIter.SeekGE(MVCCKey{Key: extIter.UnsafeKey().Key})
					sstOK, sstErr = sstIter.Valid()
				}
				if extChangedKeys && !sstChangedKeys {
					extIter.SeekGE(MVCCKey{Key: sstIter.UnsafeKey().Key})
					extOK, extErr = extIter.Valid()
				}
				if extChangedKeys && sstChangedKeys && !extOK && sstOK {
					extIter.SeekGE(MVCCKey{Key: sstIter.UnsafeKey().Key})
					extOK, extErr = extIter.Valid()
				}
				// If both iterators are invalid, we are now done. If one both iterators
				// are at point keys under the same MVCC key, then we can check for
				// conflicts between them.
			}
		}
		if !sstOK && extOK && !sstPrevRangeKeys.IsEmpty() {
			// If the SST iter previously had a range key, it's possible that the
			// ext iter has future range keys that we have yet to process. Check
			// if that's the case.
			if !steppedExtIter {
				extIter.NextKey()
			}
			extOK, extErr = extIter.Valid()
			if extOK {
				sstIter.SeekGE(MVCCKey{Key: extIter.UnsafeKey().Key})
				sstOK, sstErr = sstIter.Valid()
				if sstOK {
					// This SeekGE is purely to maintain the extIter > sstIter invariant
					// as in most cases it'll be a no-op.
					extIter.SeekGE(MVCCKey{Key: sstIter.UnsafeKey().Key})
				}
			}
		}
		// Handle case where the ext iter has a range key that we could possibly
		// merge into, but the sst iter has been exhausted.
		if !sstOK && extOK && !sstPrevRangeKeys.IsEmpty() {
			_, extHasRange = extIter.HasPointAndRange()
			if extHasRange && sstPrevRangeKeys.CanMergeRight(extIter.RangeKeys()) {
				statsDiff.Add(updateStatsOnRangeKeyMerge(sstPrevRangeKeys.Bounds.EndKey, sstPrevRangeKeys.Versions))
			}
		}
	}
	// Handle case where there's an ext range key behind the last sst range key,
	// that was also not processed in the loop itself (i.e. sstPrevRangeKeys !=
	// sstIter.RangeKeys()).
	if sstOK && !extOK {
		_, sstHasRange := sstIter.HasPointAndRange()
		if sstHasRange {
			sstRangeKeys := sstIter.RangeKeys()
			if !sstRangeKeys.Bounds.Equal(sstPrevRangeKeys.Bounds) {
				pos, peekedExtRangeKeys, err := PeekRangeKeysLeft(extIter, sstRangeKeys.Bounds.Key)
				if err != nil {
					return enginepb.MVCCStats{}, err
				}
				if pos == 0 && peekedExtRangeKeys.CanMergeRight(sstRangeKeys) {
					statsDiff.Add(updateStatsOnRangeKeyMerge(sstRangeKeys.Bounds.Key, sstRangeKeys.Versions))
				}
			}
		}
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
	ctx context.Context,
	st *cluster.Settings,
	sst []byte,
	from, to hlc.Timestamp,
	concurrency int,
	stats *enginepb.MVCCStats,
) ([]byte, enginepb.MVCCStats, error) {
	if from.IsEmpty() {
		return nil, enginepb.MVCCStats{}, errors.Errorf("from timestamp not given")
	}
	if to.IsEmpty() {
		return nil, enginepb.MVCCStats{}, errors.Errorf("to timestamp not given")
	}

	sstOut := &MemFile{}
	sstOut.Buffer.Grow(len(sst))

	var statsDelta enginepb.MVCCStats
	if stats != nil {
		// There could be a GCBytesAge delta between the old and new timestamps.
		// Calculate this delta by subtracting all the relevant stats at the
		// old timestamp, and then aging the stats to the new timestamp before
		// zeroing the stats again.
		statsDelta.AgeTo(from.WallTime)
		statsDelta.KeyBytes -= stats.KeyBytes
		statsDelta.ValBytes -= stats.ValBytes
		statsDelta.RangeKeyBytes -= stats.RangeKeyBytes
		statsDelta.RangeValBytes -= stats.RangeValBytes
		statsDelta.LiveBytes -= stats.LiveBytes
		statsDelta.IntentBytes -= stats.IntentBytes
		statsDelta.IntentCount -= stats.IntentCount
		statsDelta.AgeTo(to.WallTime)
		statsDelta.KeyBytes += stats.KeyBytes
		statsDelta.ValBytes += stats.ValBytes
		statsDelta.RangeKeyBytes += stats.RangeKeyBytes
		statsDelta.RangeValBytes += stats.RangeValBytes
		statsDelta.LiveBytes += stats.LiveBytes
		statsDelta.IntentBytes += stats.IntentBytes
		statsDelta.IntentCount += stats.IntentCount
	}

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
			return nil, enginepb.MVCCStats{}, err
		}
		return sstOut.Bytes(), statsDelta, nil
	}

	// Naïve read/write loop.
	writer := MakeIngestionSSTWriter(ctx, st, sstOut)
	defer writer.Close()

	// Rewrite point keys.
	iter, err := NewMemSSTIterator(sst, false /* verify */, IterOptions{
		KeyTypes:   IterKeyTypePointsOnly,
		LowerBound: keys.MinKey,
		UpperBound: keys.MaxKey,
	})
	if err != nil {
		return nil, enginepb.MVCCStats{}, err
	}
	defer iter.Close()

	for iter.SeekGE(MVCCKey{Key: keys.MinKey}); ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			return nil, enginepb.MVCCStats{}, err
		} else if !ok {
			break
		}
		key := iter.UnsafeKey()
		if key.Timestamp != from {
			return nil, enginepb.MVCCStats{}, errors.Errorf("unexpected timestamp %s (expected %s) for key %s",
				key.Timestamp, from, key.Key)
		}
		err = writer.PutRawMVCC(MVCCKey{Key: key.Key, Timestamp: to}, iter.UnsafeValue())
		if err != nil {
			return nil, enginepb.MVCCStats{}, err
		}
	}

	// Rewrite range keys.
	iter, err = NewMemSSTIterator(sst, false /* verify */, IterOptions{
		KeyTypes:   IterKeyTypeRangesOnly,
		LowerBound: keys.MinKey,
		UpperBound: keys.MaxKey,
	})
	if err != nil {
		return nil, enginepb.MVCCStats{}, err
	}
	defer iter.Close()

	for iter.SeekGE(MVCCKey{Key: keys.MinKey}); ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			return nil, enginepb.MVCCStats{}, err
		} else if !ok {
			break
		}
		rangeKeys := iter.RangeKeys()
		for _, v := range rangeKeys.Versions {
			if v.Timestamp != from {
				return nil, enginepb.MVCCStats{}, errors.Errorf("unexpected timestamp %s (expected %s) for range key %s",
					v.Timestamp, from, rangeKeys.Bounds)
			}
			v.Timestamp = to
			if err = writer.PutRawMVCCRangeKey(rangeKeys.AsRangeKey(v), v.Value); err != nil {
				return nil, enginepb.MVCCStats{}, err
			}
		}
	}

	if err = writer.Finish(); err != nil {
		return nil, enginepb.MVCCStats{}, err
	}

	return sstOut.Bytes(), statsDelta, nil
}
