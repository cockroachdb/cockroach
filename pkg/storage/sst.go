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
// timestamps), nor tombstones (i.e. empty values), but this is only checked for
// keys that exist in both sides, for performance.
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
) (enginepb.MVCCStats, error) {
	var statsDiff enginepb.MVCCStats
	var intents []roachpb.Intent

	extIter := reader.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: end.Key})
	defer extIter.Close()
	extIter.SeekGE(start)

	sstIter, err := NewMemSSTIterator(sst, false)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}
	defer sstIter.Close()
	sstIter.SeekGE(start)

	extOK, extErr := extIter.Valid()
	sstOK, sstErr := sstIter.Valid()
	for extErr == nil && sstErr == nil && extOK && sstOK {
		if err := ctx.Err(); err != nil {
			return enginepb.MVCCStats{}, err
		}

		extKey, extValue := extIter.UnsafeKey(), extIter.UnsafeValue()
		sstKey, sstValue := sstIter.UnsafeKey(), sstIter.UnsafeValue()

		// Keep seeking the iterators until both keys are equal.
		if cmp := bytes.Compare(extKey.Key, sstKey.Key); cmp < 0 {
			extIter.SeekGE(MVCCKey{Key: sstKey.Key})
			extOK, extErr = extIter.Valid()
			continue
		} else if cmp > 0 {
			sstIter.SeekGE(MVCCKey{Key: extKey.Key})
			sstOK, sstErr = sstIter.Valid()
			continue
		}

		// Make sure both keys are proper committed MVCC keys. Note that this is
		// only checked when the key exists both in the SST and existing data, it is
		// not an exhaustive check of the SST.
		if !sstKey.IsValue() {
			return enginepb.MVCCStats{}, errors.New("SST keys must have timestamps")
		}
		if len(sstValue) == 0 {
			return enginepb.MVCCStats{}, errors.New("SST values cannot be tombstones")
		}
		if !extKey.IsValue() {
			var mvccMeta enginepb.MVCCMetadata
			if err = extIter.ValueProto(&mvccMeta); err != nil {
				return enginepb.MVCCStats{}, err
			}
			if len(mvccMeta.RawBytes) > 0 {
				return enginepb.MVCCStats{}, errors.New("inline values are unsupported")
			} else if mvccMeta.Txn == nil {
				return enginepb.MVCCStats{}, errors.New("found intent without transaction")
			} else {
				// If we encounter a write intent, keep looking for additional intents
				// in order to return a large batch for intent resolution. The caller
				// will likely resolve the returned intents and retry the call, which
				// would be quadratic, so this significantly reduces the overall number
				// of scans.
				intents = append(intents, roachpb.MakeIntent(mvccMeta.Txn, extIter.Key().Key))
				if int64(len(intents)) >= maxIntents {
					return enginepb.MVCCStats{}, &roachpb.WriteIntentError{Intents: intents}
				}
				sstIter.NextKey()
				sstOK, sstErr = sstIter.Valid()
				if sstOK {
					extIter.SeekGE(MVCCKey{Key: sstIter.UnsafeKey().Key})
				}
				extOK, extErr = extIter.Valid()
				continue
			}
		}

		// Allow certain idempotent writes where key/timestamp/value all match:
		//
		// * disallowShadowing: any matching key.
		// * disallowShadowingBelow: any matching key at or above the given timestamp.
		allowIdempotent := (!disallowShadowingBelow.IsEmpty() && disallowShadowingBelow.LessEq(extKey.Timestamp)) ||
			(disallowShadowingBelow.IsEmpty() && disallowShadowing)
		if allowIdempotent && sstKey.Timestamp.Equal(extKey.Timestamp) &&
			bytes.Equal(extValue, sstValue) {
			// This SST entry will effectively be a noop, but its stats have already
			// been accounted for resulting in double-counting. To address this we
			// send back a stats diff for these existing KVs so that we can subtract
			// them later. This enables us to construct accurate MVCCStats and
			// prevents expensive recomputation in the future.
			metaKeySize := int64(len(sstKey.Key) + 1)
			metaValSize := int64(0)
			totalBytes := metaKeySize + metaValSize

			// Update the skipped stats to account for the skipped meta key.
			statsDiff.LiveBytes -= totalBytes
			statsDiff.LiveCount--
			statsDiff.KeyBytes -= metaKeySize
			statsDiff.ValBytes -= metaValSize
			statsDiff.KeyCount--

			// Update the stats to account for the skipped versioned key/value.
			totalBytes = int64(len(sstValue)) + MVCCVersionTimestampSize
			statsDiff.LiveBytes -= totalBytes
			statsDiff.KeyBytes -= MVCCVersionTimestampSize
			statsDiff.ValBytes -= int64(len(sstValue))
			statsDiff.ValCount--

			sstIter.NextKey()
			sstOK, sstErr = sstIter.Valid()
			if sstOK {
				extIter.SeekGE(MVCCKey{Key: sstIter.UnsafeKey().Key})
			}
			extOK, extErr = extIter.Valid()
			continue
		}

		// If requested, check that we're not shadowing a live key. Note that
		// we check this before we check the timestamp, and avoid returning
		// a WriteTooOldError -- that error implies that the client should
		// retry at a higher timestamp, but we already know that such a retry
		// would fail (because it will shadow an existing key).
		if len(extValue) > 0 && (!disallowShadowingBelow.IsEmpty() || disallowShadowing) {
			allowShadow := !disallowShadowingBelow.IsEmpty() &&
				disallowShadowingBelow.LessEq(extKey.Timestamp) && bytes.Equal(extValue, sstValue)
			if !allowShadow {
				return enginepb.MVCCStats{}, errors.Errorf(
					"ingested key collides with an existing one: %s", sstKey.Key)
			}
		}

		// If the existing key has a timestamp at or above the SST key, return a
		// WriteTooOldError. Normally this could cause a transactional request to be
		// automatically retried after a read refresh, which we would only want to
		// do if AddSSTable had SSTTimestampToRequestTimestamp set, but AddSSTable
		// cannot be used in transactions so we don't need to check.
		if sstKey.Timestamp.LessEq(extKey.Timestamp) {
			return enginepb.MVCCStats{}, roachpb.NewWriteTooOldError(
				sstKey.Timestamp, extKey.Timestamp.Next(), sstKey.Key)
		}

		// If we are shadowing an existing key, we must update the stats accordingly
		// to take into account the existing KV pair.
		statsDiff.KeyCount--
		statsDiff.KeyBytes -= int64(len(extKey.Key) + 1)
		if len(extValue) > 0 {
			statsDiff.LiveCount--
			statsDiff.LiveBytes -= int64(len(extKey.Key) + 1)
			statsDiff.LiveBytes -= int64(len(extValue)) + MVCCVersionTimestampSize
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

	iter, err := NewMemSSTIterator(sst, false)
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
		err = writer.PutMVCC(MVCCKey{Key: iter.UnsafeKey().Key, Timestamp: to}, iter.UnsafeValue())
		if err != nil {
			return nil, err
		}
	}

	if err = writer.Finish(); err != nil {
		return nil, err
	}

	return sstOut.Bytes(), nil
}
