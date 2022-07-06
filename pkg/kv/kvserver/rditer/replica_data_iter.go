// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rditer

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

// ReplicaMVCCDataIterator provides a complete iteration over MVCC or unversioned
// (which can be made to look like an MVCCKey) key / value
// rows in a range, including system-local metadata and user data.
// The ranges keyRange slice specifies the key ranges which comprise
// the range's data. This cannot be used to iterate over keys that are not
// representable as MVCCKeys, except when such non-MVCCKeys are limited to
// intents, which can be made to look like interleaved MVCCKeys. Most callers
// want the real keys, and should use ReplicaEngineDataIterator.
//
// A ReplicaMVCCDataIterator provides a subset of the engine.MVCCIterator interface.
type ReplicaMVCCDataIterator struct {
	reader   storage.Reader
	curIndex int
	spans    []roachpb.Span
	// When it is non-nil, it represents the iterator for curIndex.
	// A non-nil it is valid, else it is either done, or err != nil.
	it      storage.MVCCIterator
	err     error
	reverse bool
}

// ReplicaEngineDataIterator provides a complete iteration over all data in a
// range, including system-local metadata and user data. The ranges KeyRange
// slice specifies the key ranges which comprise the range's data.
//
// The iterator iterates over both point keys and range keys (i.e. MVCC range
// tombstones), but in a somewhat peculiar order: for each key range, it first
// iterates over all point keys in order, then over all range keys in order,
// signalled via HasPointAndRange(). This allows efficient non-interleaved
// iteration of point/range keys, and keeps them grouped by key range for
// efficient Raft snapshot ingestion into a single SST per key range.
//
// TODO(erikgrinaker): Reconsider the above ordering/scheme for point/range
// keys.
type ReplicaEngineDataIterator struct {
	reader     storage.Reader
	curIndex   int
	curKeyType storage.IterKeyType
	spans      []roachpb.Span
	it         storage.EngineIterator
}

// NewReplicaMVCCDataIterator creates a ReplicaMVCCDataIterator for the given
// replica. It iterates over the replicated key ranges excluding the lock
// table key range. Separated locks are made to appear as interleaved. The
// iterator can do one of reverse or forward iteration, based on whether
// seekEnd is true or false, respectively. With reverse iteration, it is
// initially positioned at the end of the last range, else it is initially
// positioned at the start of the first range.
//
// The iterator requires the reader.ConsistentIterators is true, since it
// creates a different iterator for each replicated key range. This is because
// MVCCIterator only allows changing the upper-bound of an existing iterator,
// and not both upper and lower bound.
//
// TODO(erikgrinaker): ReplicaMVCCDataIterator does not support MVCC range keys.
// This should be deprecated in favor of e.g. ReplicaEngineDataIterator.
func NewReplicaMVCCDataIterator(
	d *roachpb.RangeDescriptor, reader storage.Reader, seekEnd bool,
) *ReplicaMVCCDataIterator {
	if !reader.ConsistentIterators() {
		panic("ReplicaMVCCDataIterator needs a Reader that provides ConsistentIterators")
	}
	ri := &ReplicaMVCCDataIterator{
		reader:  reader,
		spans:   keys.MakeReplicatedRangeSpansExceptLockTable(d),
		reverse: seekEnd,
	}
	if ri.reverse {
		ri.curIndex = len(ri.spans) - 1
	} else {
		ri.curIndex = 0
	}
	ri.tryCloseAndCreateIter()
	return ri
}

func (ri *ReplicaMVCCDataIterator) tryCloseAndCreateIter() {
	for {
		if ri.it != nil {
			ri.it.Close()
			ri.it = nil
		}
		if ri.curIndex < 0 || ri.curIndex >= len(ri.spans) {
			return
		}
		ri.it = ri.reader.NewMVCCIterator(
			storage.MVCCKeyAndIntentsIterKind,
			storage.IterOptions{
				LowerBound: ri.spans[ri.curIndex].Key,
				UpperBound: ri.spans[ri.curIndex].EndKey,
			})
		if ri.reverse {
			ri.it.SeekLT(storage.MakeMVCCMetadataKey(ri.spans[ri.curIndex].EndKey))
		} else {
			ri.it.SeekGE(storage.MakeMVCCMetadataKey(ri.spans[ri.curIndex].Key))
		}
		if valid, err := ri.it.Valid(); valid || err != nil {
			ri.err = err
			return
		}
		if ri.reverse {
			ri.curIndex--
		} else {
			ri.curIndex++
		}
	}
}

// Close the underlying iterator.
func (ri *ReplicaMVCCDataIterator) Close() {
	if ri.it != nil {
		ri.it.Close()
		ri.it = nil
	}
}

// Next advances to the next key in the iteration.
func (ri *ReplicaMVCCDataIterator) Next() {
	if ri.reverse {
		panic("Next called on reverse iterator")
	}
	ri.it.Next()
	valid, err := ri.it.Valid()
	if err != nil {
		ri.err = err
		return
	}
	if !valid {
		ri.curIndex++
		ri.tryCloseAndCreateIter()
	}
}

// Prev advances the iterator one key backwards.
func (ri *ReplicaMVCCDataIterator) Prev() {
	if !ri.reverse {
		panic("Prev called on forward iterator")
	}
	ri.it.Prev()
	valid, err := ri.it.Valid()
	if err != nil {
		ri.err = err
		return
	}
	if !valid {
		ri.curIndex--
		ri.tryCloseAndCreateIter()
	}
}

// Valid returns true if the iterator currently points to a valid value.
func (ri *ReplicaMVCCDataIterator) Valid() (bool, error) {
	if ri.err != nil {
		return false, ri.err
	}
	if ri.it == nil {
		return false, nil
	}
	return true, nil
}

// Key returns the current key. Only called in tests.
func (ri *ReplicaMVCCDataIterator) Key() storage.MVCCKey {
	return ri.it.Key()
}

// Value returns the current value. Only called in tests.
func (ri *ReplicaMVCCDataIterator) Value() []byte {
	return ri.it.Value()
}

// UnsafeKey returns the same value as Key, but the memory is invalidated on
// the next call to {Next,Prev,Close}.
func (ri *ReplicaMVCCDataIterator) UnsafeKey() storage.MVCCKey {
	return ri.it.UnsafeKey()
}

// UnsafeValue returns the same value as Value, but the memory is invalidated on
// the next call to {Next,Prev,Close}.
func (ri *ReplicaMVCCDataIterator) UnsafeValue() []byte {
	return ri.it.UnsafeValue()
}
