// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package engineccl

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// MVCCIncrementalIterator iterates over the diff of the key range
// [startKey,endKey) and time range (startTime,endTime]. If a key was added or
// modified between startTime and endTime, the iterator will position at the
// most recent version (before or at endTime) of that key. If the key was most
// recently deleted, this is signalled with an empty value.
//
// Note: The endTime is inclusive to be consistent with the non-incremental
// iterator, where reads at a given timestamp return writes at that
// timestamp. The startTime is then made exclusive so that iterating time 1 to
// 2 and then 2 to 3 will only return values with time 2 once. An exclusive
// start time would normally make it difficult to scan timestamp 0, but
// CockroachDB uses that as a sentinel for key metadata anyway.
//
// Expected usage:
//    iter := NewMVCCIncrementalIterator(e, startTime, endTime)
//    defer iter.Close()
//    for iter.Seek(startKey); ; iter.Next() {
//        ok, err := iter.Valid()
//        if !ok { ... }
//        [code using iter.Key() and iter.Value()]
//    }
//    if err := iter.Error(); err != nil {
//      ...
//    }
type MVCCIncrementalIterator struct {
	// TODO(dan): Move all this logic into c++ and make this a thin wrapper.

	iter engine.Iterator

	startTime hlc.Timestamp
	endTime   hlc.Timestamp
	err       error
	valid     bool

	// For allocation avoidance.
	meta enginepb.MVCCMetadata
}

var _ engine.SimpleIterator = &MVCCIncrementalIterator{}

// NewMVCCIncrementalIterator creates an MVCCIncrementalIterator with the
// specified engine and time range.
func NewMVCCIncrementalIterator(
	e engine.Reader, startTime, endTime hlc.Timestamp,
) *MVCCIncrementalIterator {
	return &MVCCIncrementalIterator{
		iter:      e.NewTimeBoundIterator(startTime, endTime),
		startTime: startTime,
		endTime:   endTime,
	}
}

// Seek advances the iterator to the first key in the engine which is >= the
// provided key.
func (i *MVCCIncrementalIterator) Seek(startKey engine.MVCCKey) {
	i.iter.Seek(startKey)
	i.err = nil
	i.valid = true
	i.advance()
}

// Close frees up resources held by the iterator.
func (i *MVCCIncrementalIterator) Close() {
	i.iter.Close()
}

// Next advances the iterator to the next key/value in the iteration. After this
// call, Valid() will be true if the iterator was not positioned at the last
// key.
func (i *MVCCIncrementalIterator) Next() {
	i.iter.Next()
	i.advance()
}

// NextKey advances the iterator to the next MVCC key. This operation is
// distinct from Next which advances to the next version of the current key or
// the next key if the iterator is currently located at the last version for a
// key.
func (i *MVCCIncrementalIterator) NextKey() {
	i.iter.NextKey()
	i.advance()
}

func (i *MVCCIncrementalIterator) advance() {
	for {
		if !i.valid {
			return
		}
		if ok, err := i.iter.Valid(); !ok {
			i.err = err
			i.valid = false
			return
		}

		unsafeMetaKey := i.iter.UnsafeKey()
		if unsafeMetaKey.IsValue() {
			i.meta.Reset()
			i.meta.Timestamp = hlc.LegacyTimestamp(unsafeMetaKey.Timestamp)
		} else {
			if i.err = i.iter.ValueProto(&i.meta); i.err != nil {
				i.valid = false
				return
			}
		}
		if i.meta.IsInline() {
			// Inline values are only used in non-user data. They're not needed
			// for backup, so they're not handled by this method. If one shows
			// up, throw an error so it's obvious something is wrong.
			i.valid = false
			i.err = errors.Errorf("inline values are unsupported by MVCCIncrementalIterator: %s",
				unsafeMetaKey.Key)
			return
		}

		metaTimestamp := hlc.Timestamp(i.meta.Timestamp)
		if i.meta.Txn != nil {
			if !i.endTime.Less(metaTimestamp) {
				i.err = &roachpb.WriteIntentError{
					Intents: []roachpb.Intent{{
						Span:   roachpb.Span{Key: i.iter.Key().Key},
						Status: roachpb.PENDING,
						Txn:    *i.meta.Txn,
					}},
				}
				i.valid = false
				return
			}
			i.iter.Next()
			continue
		}

		if i.endTime.Less(metaTimestamp) {
			i.iter.Next()
			continue
		}
		if !i.startTime.Less(metaTimestamp) {
			i.iter.NextKey()
			continue
		}

		break
	}
}

// Valid must be called after any call to Reset(), Next(), or similar methods.
// It returns (true, nil) if the iterator points to a valid key (it is undefined
// to call Key(), Value(), or similar methods unless Valid() has returned (true,
// nil)). It returns (false, nil) if the iterator has moved past the end of the
// valid range, or (false, err) if an error has occurred. Valid() will never
// return true with a non-nil error.
func (i *MVCCIncrementalIterator) Valid() (bool, error) {
	return i.valid, i.err
}

// Key returns the current key.
func (i *MVCCIncrementalIterator) Key() engine.MVCCKey {
	return i.iter.Key()
}

// Value returns the current value as a byte slice.
func (i *MVCCIncrementalIterator) Value() []byte {
	return i.iter.Value()
}

// UnsafeKey returns the same key as Key, but the memory is invalidated on the
// next call to {Next,Reset,Close}.
func (i *MVCCIncrementalIterator) UnsafeKey() engine.MVCCKey {
	return i.iter.UnsafeKey()
}

// UnsafeValue returns the same value as Value, but the memory is invalidated on
// the next call to {Next,Reset,Close}.
func (i *MVCCIncrementalIterator) UnsafeValue() []byte {
	return i.iter.UnsafeValue()
}

// RowCounter counts rows and index entires.
type RowCounter struct {
	prev roachpb.Key
	roachpb.BulkOpSummary
}

// Count determines if the passed key is a new row and updates its counts if so.
func (r *RowCounter) Count(key roachpb.Key) error {
	// EnsureSafeSplitKey is usually used to avoid splitting a row across ranges,
	// by returning the row's key prefix.
	// We reuse it here to count "rows" by counting when it changes.
	// Non-SQL keys are returned unchanged or may error -- we ignore them, since
	// non-SQL keys are obviously thus not SQL rows.
	row, err := keys.EnsureSafeSplitKey(key)
	if err != nil || len(key) == len(row) {
		return nil
	}

	// no change key prefix => no new row.
	if bytes.Equal(row, r.prev) {
		return nil
	}
	r.prev = append(r.prev[:0], row...)

	rest, tbl, err := keys.DecodeTablePrefix(row)
	if err != nil {
		return err
	}

	if tbl < keys.MaxReservedDescID {
		r.SystemRecords++
	} else {
		if _, indexID, err := encoding.DecodeUvarintAscending(rest); err != nil {
			return err
		} else if indexID == 1 {
			r.Rows++
		} else {
			r.IndexEntries++
		}
	}

	return nil
}

// MVCCIncIterExport exports changes to the keyrange [startKey,endKey) over the
// interval (startTime, endTime]. Passing allRevisions exports every revision of
// a key for the interval, otherwise only the latest value within the interval
// is exported. Deletions are included if all revisions are requested or if the
// startTime is non-zero. Returns the bytes of an SSTable containing the
// exported keys, the number of keys exported and size of exported data, or an
// error.
func MVCCIncIterExport(
	ctx context.Context,
	e engine.Reader,
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	allRevisions bool,
) ([]byte, roachpb.BulkOpSummary, error) {
	var rows RowCounter

	sst, err := engine.MakeRocksDBSstFileWriter()
	if err != nil {
		return nil, rows.BulkOpSummary, err
	}
	defer sst.Close()

	// TODO(dan): Consider checking ctx periodically during the MVCCIterate call.
	iter := NewMVCCIncrementalIterator(e, startTime, endTime)
	defer iter.Close()
	for iter.Seek(engine.MakeMVCCMetadataKey(startKey)); ; {
		ok, err := iter.Valid()
		if err != nil {
			// The error may be a WriteIntentError. In which case, returning it will
			// cause this command to be retried.
			return nil, rows.BulkOpSummary, err
		}
		if !ok || iter.UnsafeKey().Key.Compare(endKey) >= 0 {
			break
		}

		// Skip tombstone (len=0) records when startTime is zero
		// (non-incremental) and we're not exporting all versions.
		if !allRevisions && (startTime == hlc.Timestamp{}) && len(iter.UnsafeValue()) == 0 {
			iter.NextKey()
			continue
		}

		if log.V(3) {
			v := roachpb.Value{RawBytes: iter.UnsafeValue()}
			log.Infof(ctx, "Export %s %s", iter.UnsafeKey(), v.PrettyPrint())
		}

		if err := rows.Count(iter.UnsafeKey().Key); err != nil {
			return nil, rows.BulkOpSummary, errors.Wrapf(err, "decoding %s", iter.UnsafeKey())
		}
		if err := sst.Add(engine.MVCCKeyValue{Key: iter.UnsafeKey(), Value: iter.UnsafeValue()}); err != nil {
			return nil, rows.BulkOpSummary, errors.Wrapf(err, "adding key %s", iter.UnsafeKey())
		}
		if allRevisions {
			iter.Next()
		} else {
			iter.NextKey()
		}
	}

	rows.BulkOpSummary.DataSize = sst.DataSize
	if sst.DataSize == 0 {
		return nil, rows.BulkOpSummary, nil
	}

	sstContents, err := sst.Finish()
	return sstContents, rows.BulkOpSummary, err
}
