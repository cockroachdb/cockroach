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

type storageIter interface {
	SeekGE(key storage.MVCCKey)
	Valid() (bool, error)
	Next()
	Close()
	UnsafeValue() []byte
}

// Reader is the subset of storage.Reader relevant for accessing the raft log.
//
// The raft log is a contiguous sequence of indexes (i.e. no holes) which may be
// empty.
type Reader interface {
	NewMVCCIterator(storage.MVCCIterKind, storage.IterOptions) storage.MVCCIterator
}

// An Iterator inspects the raft log. After creation, SeekGE should be invoked,
// followed by Next (and SeekGE again if desired, etc). The iterator must no
// longer be used after any of its method has returned an error. The Iterator
// must eventually be Close'd to release the resources associated to it.
//
// TODO(sep-raft-log): the interface here isn't optimal and might be overkill:
// - asymmetry between lo (passed to SeekGE) and hi (specified at creation time)
// - do we ever need to re-seek this iterator anyway?
// - will all uses use the Visit pattern anyway?
//
// Revisit this.
type Iterator struct {
	eng       Reader
	prefixBuf keys.RangeIDPrefixBuf

	iter storageIter
	// TODO(tbg): we're not reusing memory here. Since all of our allocs come
	// from protobuf marshaling, this is hard to avoid but we can do a little
	// better and at least avoid a few allocations.
	entry *Entry
}

// IterOptions are options to NewIterator.
type IterOptions struct {
	// Hi ensures the Iterator never seeks to any Entry with index >= Hi. This is
	// useful when the caller is interested in a slice [Lo, Hi) of the raft log.
	Hi uint64
}

// NewIterator initializes an Iterator that reads the raft log for the given
// RangeID from the provided Reader.
//
// Callers that can afford allocating a closure may prefer using Visit.
func NewIterator(rangeID roachpb.RangeID, eng Reader, opts IterOptions) *Iterator {
	// TODO(tbg): can pool these most of the things below, incl. the *Iterator.
	prefixBuf := keys.MakeRangeIDPrefixBuf(rangeID)
	var upperBound roachpb.Key
	if opts.Hi == 0 {
		upperBound = prefixBuf.RaftLogPrefix().PrefixEnd()
	} else {
		upperBound = prefixBuf.RaftLogKey(opts.Hi)
	}
	return &Iterator{
		eng:       eng,
		prefixBuf: prefixBuf,
		iter: eng.NewMVCCIterator(storage.MVCCKeyIterKind, storage.IterOptions{
			UpperBound: upperBound,
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
//
// Note that a valid raft log has no gaps, and that the Iterator does not
// validate that.
func (it *Iterator) Next() (bool, error) {
	it.iter.Next()
	return it.load()
}

// Entry returns the Entry the iterator is currently positioned at. This
// must only be called after a prior successful call to SeekGE or Next.
//
// The caller may invoke `(*Entry).Release` if it no longer wishes to access the
// current Entry.
func (it *Iterator) Entry() *Entry {
	return it.entry
}

// Visit invokes fn with the raft log entries whose indexes fall into [lo, hi)
// in ascending index order. For example, if there are log entries for indexes
// 6, 7, and 8, then [lo, hi)=[1, 8) will visit 6 and 7, and [lo, hi)=[7,9) will
// visit 7 and 8.
//
// The closure may return iterutil.StopIteration(), which will stop iteration
// without returning an error.
//
// The closure may invoke `(*Entry).Release` if it is no longer going to access
// any memory in the current Entry.
func Visit(
	ctx context.Context,
	rangeID roachpb.RangeID,
	eng Reader,
	lo, hi uint64,
	fn func(context.Context, *Entry) error,
) error {
	it := NewIterator(rangeID, eng, IterOptions{Hi: hi})
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
