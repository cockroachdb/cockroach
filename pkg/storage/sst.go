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
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
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

	// Fast path: there are no keys in the reader between the sstable's start and
	// end keys. We use a non-prefix iterator for this search, and reopen a prefix
	// one if there are engine keys in the span.
	nonPrefixIter := reader.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: end.Key})
	nonPrefixIter.SeekGE(start)
	valid, _ := nonPrefixIter.Valid()
	nonPrefixIter.Close()
	if !valid {
		return statsDiff, nil
	}

	extIter := reader.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: end.Key, Prefix: true})
	defer extIter.Close()

	sstIter, err := NewMemSSTIterator(sst, false)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}
	defer sstIter.Close()
	sstIter.SeekGE(start)

	// extIter is a prefix iterator; it is expected to skip keys that belong
	// to different prefixes. Only iterate along the sst iterator, and re-seek
	// extIter each time.
	sstOK, sstErr := sstIter.Valid()
	if sstOK {
		extIter.SeekGE(MVCCKey{Key: sstIter.UnsafeKey().Key})
	}
	extOK, extErr := extIter.Valid()
	for sstErr == nil && sstOK {
		if err := ctx.Err(); err != nil {
			return enginepb.MVCCStats{}, err
		}
		if !extOK {
			// There is no key in extIter matching this prefix. Check the next
			// key in sstIter. Note that we can't just use an exhausted extIter
			// as a sign that we are done; extIter could be skipping keys, so it
			// must be re-seeked.
			sstIter.NextKey()
			sstOK, sstErr = sstIter.Valid()
			if sstOK {
				extIter.SeekGE(MVCCKey{Key: sstIter.UnsafeKey().Key})
			}
			extOK, extErr = extIter.Valid()
			continue
		}

		extKey, extValue := extIter.UnsafeKey(), extIter.UnsafeValue()
		sstKey, sstValue := sstIter.UnsafeKey(), sstIter.UnsafeValue()

		// Keep seeking the iterators until both keys are equal.
		if cmp := bytes.Compare(extKey.Key, sstKey.Key); cmp < 0 {
			// sstIter is further ahead. Seek extIter.
			extIter.SeekGE(MVCCKey{Key: sstKey.Key})
			extOK, extErr = extIter.Valid()
			continue
		} else if cmp > 0 {
			// extIter is further ahead. But it could have skipped keys in between,
			// so re-seek it at the next sst key.
			sstIter.NextKey()
			sstOK, sstErr = sstIter.Valid()
			if sstOK {
				extIter.SeekGE(MVCCKey{Key: sstIter.UnsafeKey().Key})
			}
			extOK, extErr = extIter.Valid()
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
		// do if AddSSTable had WriteAtRequestTimestamp set, but AddSSTable cannot
		// be used in transactions so we don't need to check.
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

	if sstErr != nil {
		return enginepb.MVCCStats{}, sstErr
	}
	if extErr != nil {
		return enginepb.MVCCStats{}, extErr
	}
	if len(intents) > 0 {
		return enginepb.MVCCStats{}, &roachpb.WriteIntentError{Intents: intents}
	}

	return statsDiff, nil
}

// UpdateSSTTimestamps replaces all MVCC timestamp in the provided SST with the
// given timestamp. All keys must have a non-zero timestamp, otherwise an error
// is returned to protect against accidental inclusion of intents or inline
// values. Tombstones are also rejected opportunistically, since we're iterating
// over the entire SST anyway.
//
// TODO(erikgrinaker): This is a naïve implementation that will need significant
// optimization. For example, the SST blocks can be rewritten in parallel, and
// the Bloom filters and value checksums (which do not depend on the timestamp)
// can be copied across without recomputation.
func UpdateSSTTimestamps(sst []byte, ts hlc.Timestamp) ([]byte, error) {
	sstOut := &MemFile{}
	writer := MakeIngestionSSTWriter(sstOut)
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
		if iter.UnsafeKey().Timestamp.IsEmpty() {
			return nil, errors.New("inline values or intents are not supported")
		}
		if len(iter.UnsafeValue()) == 0 {
			return nil, errors.New("SST values cannot be tombstones")
		}
		err = writer.PutMVCC(MVCCKey{Key: iter.UnsafeKey().Key, Timestamp: ts}, iter.UnsafeValue())
		if err != nil {
			return nil, err
		}
	}

	if err = writer.Finish(); err != nil {
		return nil, err
	}

	return sstOut.Bytes(), nil
}
