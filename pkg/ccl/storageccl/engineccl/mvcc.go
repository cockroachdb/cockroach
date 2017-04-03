// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package engineccl

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/pkg/errors"
)

// MVCCIncrementalIterator iterates over the diff of the key range
// [startKey,endKey) and time range [startTime,endTime). If a key was added or
// modified between startTime and endTime, the iterator will position at the
// most recent version (before endTime) of that key. If the key was most
// recently deleted, this is signalled with an empty value.
//
// Expected usage:
//    iter := NewMVCCIncrementalIterator(e)
//    defer iter.Close()
//    for iter.Reset(startKey, endKey, ...); iter.Valid(); iter.Next() {
//        [code using iter.Key() and iter.Value()]
//    }
//    if err := iter.Error(); err != nil {
//      ...
//    }
type MVCCIncrementalIterator struct {
	// TODO(dan): Move all this logic into c++ and make this a thin wrapper.

	iter engine.Iterator

	endKey    engine.MVCCKey
	startTime hlc.Timestamp
	endTime   hlc.Timestamp
	err       error
	valid     bool
	nextkey   bool

	// For allocation avoidance.
	meta enginepb.MVCCMetadata
}

// NewMVCCIncrementalIterator creates an MVCCIncrementalIterator with the
// specified engine.
func NewMVCCIncrementalIterator(e engine.Reader) *MVCCIncrementalIterator {
	return &MVCCIncrementalIterator{iter: e.NewIterator(false)}
}

// Close frees up resources held by the iterator.
func (i *MVCCIncrementalIterator) Close() {
	i.iter.Close()
}

// Reset begins a new iteration with the specified key and time ranges.
func (i *MVCCIncrementalIterator) Reset(
	startKey, endKey roachpb.Key, startTime, endTime hlc.Timestamp,
) {
	i.iter.Seek(engine.MakeMVCCMetadataKey(startKey))
	i.endKey = engine.MakeMVCCMetadataKey(endKey)
	i.startTime, i.endTime = startTime, endTime
	i.err = nil
	i.valid = true
	i.nextkey = false
	i.Next()
}

// Next advances the iterator to the next key/value in the iteration.
func (i *MVCCIncrementalIterator) Next() {
	for {
		if !i.valid {
			return
		}
		if ok, err := i.iter.Valid(); !ok {
			i.err = err
			i.valid = false
			return
		}

		if i.nextkey {
			i.nextkey = false
			i.iter.NextKey()
			continue
		}

		unsafeMetaKey := i.iter.UnsafeKey()
		if !unsafeMetaKey.Less(i.endKey) {
			i.valid = false
			return
		}
		if unsafeMetaKey.IsValue() {
			i.meta.Reset()
			i.meta.Timestamp = unsafeMetaKey.Timestamp
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
		if unsafeMetaKey.Key == nil {
			// iter was pointed after i.endKey.
			break
		}

		if i.meta.Txn != nil {
			if !i.endTime.Less(i.meta.Timestamp) {
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

		if !i.meta.Timestamp.Less(i.endTime) {
			i.iter.Next()
			continue
		}
		if i.meta.Timestamp.Less(i.startTime) {
			i.iter.NextKey()
			continue
		}

		i.nextkey = true
		break
	}
}

// Valid returns true if the iterator is currently valid. An iterator that
// hasn't had Reset called on it or has gone past the end of the key range is
// invalid.
func (i *MVCCIncrementalIterator) Valid() bool {
	return i.valid
}

// Error returns the error, if any, which the iterator encountered.
func (i *MVCCIncrementalIterator) Error() error {
	return i.err
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
