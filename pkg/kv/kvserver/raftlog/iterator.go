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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

// An Iterator inspects the raft log. After creation, Seek should be invoked.
// Then, after each call to Valid() which returns (true, nil), an Entry is
// available via the UnsafeEntry method; the Entry becomes invalid once the
// Iterator is advanced via Next or Seek (however, pointers inside of Entry will
// not be re-used and can thus be held on to). Valid() returns (false, nil) when
// no more entries are in the search window last provided to Seek; the Iterator
// may be Seek'ed again. The Iterator must eventually be Close'd.
type Iterator struct {
	eng     storage.Reader
	prefBuf keys.RangeIDPrefixBuf

	iter  storage.MVCCIterator
	entry Entry
}

// NewIterator initializes an Iterator that reads the raft log for the given
// RangeID from the provided Reader.
func NewIterator(rangeID roachpb.RangeID, eng storage.Reader) *Iterator {
	return &Iterator{
		eng:     eng,
		prefBuf: keys.MakeRangeIDPrefixBuf(rangeID),
	}
}

// Close releases the resources associated to this Iterator.
func (it *Iterator) Close() {
	if it.iter != nil {
		it.iter.Close()
	}
}

// Seek sets up the Iterator to read the raft log from indices lo (inclusive)
// to hi (exclusive).
func (it *Iterator) Seek(lo, hi uint64) {
	it.iter = it.eng.NewMVCCIterator(storage.MVCCKeyIterKind, storage.IterOptions{
		UpperBound: it.prefBuf.RaftLogKey(hi), // exclusive
	})

	it.iter.SeekGE(storage.MakeMVCCMetadataKey(it.prefBuf.RaftLogKey(lo)))
}

// Next advances the Iterator. This must follow a call to Valid that returned
// (true, nil).
func (it *Iterator) Next() {
	it.iter.Next()
}

// Valid returns (true, nil) when another entry is available via UnsafeEntry.
// It returns (false, nil) when the search window passed to the previous call
// to Seek is exhausted; note that this does not imply that all possible Entries
// in the iteration window actually existed. When an error is returned, the
// Iterator must no longer be used.
func (it *Iterator) Valid() (bool, error) {
	ok, err := it.iter.Valid()
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}

	if err := it.entry.LoadFromRawValue(it.iter.UnsafeValue()); err != nil {
		return false, err
	}

	return true, nil
}

// UnsafeEntry returns the Entry the iterator is currently positioned at. This
// Entry is only valid for use until Seek or Next are invoked. Memory referenced
// from within the Entry can be held on to.
//
// TODO(tbg): this is not the most useful contract - reusing only the Entry
// doesn't buy us as much, reusing `Entry.Data` is the really useful part.
func (it *Iterator) UnsafeEntry() *Entry {
	return &it.entry
}
