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
// timestamp at or above the SST key timestamp. If disallowShadowing is true, it
// also errors for any existing live key at the SST key timestamp, and ignores
// entries that exactly match an existing entry (key/value/timestamp), for
// backwards compatibility.
//
// The given SST cannot contain intents or inline values (i.e. zero timestamps),
// nor tombstones (i.e. empty values). The returned MVCC statistics is a delta
// between the SST-only statistics and their effect when applied, which when
// added to the SST statistics will adjust them for existing keys and values.
func CheckSSTConflicts(
	ctx context.Context,
	sst []byte,
	reader Reader,
	start, end MVCCKey,
	disallowShadowing bool,
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
	for extErr == nil && sstErr == nil && extOK && sstOK && ctx.Err() == nil {
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

		// Make sure both keys are proper committed MVCC keys.
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
				extIter.NextKey()
				extOK, extErr = extIter.Valid()
				continue
			}
		}

		// If the ingested key/value pair is identical to an existing one then we
		// do not consider it to be a collision.
		//
		// TODO(erikgrinaker): We retain this behavior mostly for backwards
		// compatibility, and only use it when disallowShadowing is true. It can
		// result in e.g. emitting rangefeed events below the closed timestamp, so
		// it should be removed in 22.2 if possible.
		if disallowShadowing && sstKey.Timestamp.Equal(extKey.Timestamp) &&
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

			extIter.NextKey()
			extOK, extErr = extIter.Valid()
			continue
		}

		// If the existing key has a timestamp at or above the SST key, return a
		// write error.
		if sstKey.Timestamp.LessEq(extKey.Timestamp) {
			// FIXME: Make sure returning this is OK, in particular with 22.1 clients.
			return enginepb.MVCCStats{}, roachpb.NewWriteTooOldError(
				sstKey.Timestamp, extKey.Timestamp.Next())
		}

		// If requested, check that we're not shadowing a live key.
		if disallowShadowing && len(extValue) != 0 {
			return enginepb.MVCCStats{}, errors.Errorf(
				"SST key %q shadows existing key %q", sstKey, extKey)
		}

		// If we are shadowing an existing key, we must update the live stats
		// accordingly to take into account the existing KV pair.
		if len(extValue) != 0 {
			// FIXME: Verify and test this.
			statsDiff.LiveCount--
			statsDiff.LiveBytes -= int64(len(extKey.Key) + 1)
			statsDiff.LiveBytes -= int64(len(extValue)) + MVCCVersionTimestampSize
		}

		extIter.NextKey()
		extOK, extErr = extIter.Valid()
	}

	switch {
	case extErr != nil:
		return enginepb.MVCCStats{}, extErr
	case sstErr != nil:
		return enginepb.MVCCStats{}, sstErr
	case len(intents) > 0:
		return enginepb.MVCCStats{}, &roachpb.WriteIntentError{Intents: intents}
	case ctx.Err() != nil:
		return enginepb.MVCCStats{}, ctx.Err()
	}

	return statsDiff, nil
}

// UpdateSSTTimestamps replaces all MVCC timestamp in the provided SST with the
// given timestamp. All keys must have an existing timestamp, otherwise an error
// is returned, to protect against accidental inclusion of intents or inline
// values.
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
