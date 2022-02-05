// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package raftlog

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
)

type storageIterI interface {
	SeekGE(key storage.MVCCKey)
	Valid() (bool, error)
	Next()
	Close()
	UnsafeValue() []byte
}

type ReaderI interface {
	NewMVCCIterator(storage.MVCCIterKind, storage.IterOptions) storage.MVCCIterator
}

// An Iterator inspects the raft log. After creation, SeekGE should be invoked,
// followed by Next (and SeekGE again if desired, etc). The iterator must no
// longer be used after any of its method has returned an error. The Iterator
// must eventually be Close'd to release the resources associated to it.
type Iterator struct {
	eng       ReaderI
	prefixBuf keys.RangeIDPrefixBuf

	iter storageIterI
	// TODO(tbg): we're not reusing memory here. Since all of our allocs come
	// from protobuf marshaling, this is hard to avoid but we can do a little
	// better and at least avoid a few allocations.
	entry *Entry
}

// NewIterator initializes an Iterator that reads the raft log for the given
// RangeID from the provided Reader.
//
// Callers that can afford allocating a closure may prefer using Visit.
func NewIterator(rangeID roachpb.RangeID, eng ReaderI) *Iterator {
	prefixBuf := keys.MakeRangeIDPrefixBuf(rangeID)
	return &Iterator{
		eng:       eng,
		prefixBuf: prefixBuf,
		iter: eng.NewMVCCIterator(storage.MVCCKeyIterKind, storage.IterOptions{
			// TODO: could accept a tighter upper bound in NewIterator.
			UpperBound: prefixBuf.RaftLogPrefix().PrefixEnd(),
		}),
	}
}

// Close releases the resources associated with this Iterator.
func (it *Iterator) Close() {
	if it.iter != nil {
		it.iter.Close()
	}
}

func (it *Iterator) load() (bool, error) {
	ok, err := it.iter.Valid()
	if err != nil || !ok {
		return false, err
	}

	it.entry, err = NewEntryFromRawValue(it.iter.UnsafeValue())
	if err != nil {
		return false, err
	}

	return true, nil
}

// SeekGE positions the Iterator at the first raft log with index greater than
// or equal to idx. Returns (true, nil) on success, (false, nil) if no such
// Entry exists.
func (it *Iterator) SeekGE(idx uint64) (bool, error) {
	it.iter.SeekGE(storage.MakeMVCCMetadataKey(it.prefixBuf.RaftLogKey(idx)))
	return it.load()
}

// Next returns (true, nil) when the (in ascending index order) next entry is
// available via Entry. It returns (false, nil) if there are no more
// entries.
// Note that Iterator does not check for gaps in the log.
func (it *Iterator) Next() (bool, error) {
	it.iter.Next()
	return it.load()
}

// Entry returns the Entry the iterator is currently positioned at.
func (it *Iterator) Entry() *Entry {
	return it.entry
}

// Visit invokes fn with the raft log entries in ascending index order.
// The closure may return iterutil.StopIteration(), which will stop iteration
// without returning an error.
func Visit(
	ctx context.Context,
	rangeID roachpb.RangeID,
	eng ReaderI,
	lo, hi uint64, // lo is inclusive, hi exclusive
	fn func(context.Context, *Entry) error,
) error {
	it := NewIterator(rangeID, eng)
	defer it.Close()
	ok, err := it.SeekGE(lo)
	if err != nil {
		return err
	}
	for ; ok; ok, err = it.Next() {
		ent := it.Entry()
		if ent.Index >= hi {
			return nil
		}
		if err := fn(ctx, ent); err != nil {
			if errors.Is(err, iterutil.StopIteration()) {
				return nil
			}
			return err
		}
	}
	return err
}
