// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftlog

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
)

type storageIter interface {
	SeekGE(key storage.MVCCKey)
	Valid() (bool, error)
	Next()
	Close()
	UnsafeValue() ([]byte, error)
}

// Reader is the subset of storage.Reader relevant for accessing the raft log.
//
// The raft log is a contiguous sequence of indexes (i.e. no holes) which may be
// empty.
type Reader interface {
	NewMVCCIterator(
		context.Context, storage.MVCCIterKind, storage.IterOptions) (storage.MVCCIterator, error)
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
	entry raftpb.Entry
}

// IterOptions are options to NewIterator.
type IterOptions struct {
	// Hi ensures the Iterator never seeks to any entry with index >= Hi. This is
	// useful when the caller is interested in a slice [Lo, Hi) of the raft log.
	Hi kvpb.RaftIndex
}

// NewIterator initializes an Iterator that reads the raft log for the given
// RangeID from the provided Reader.
//
// Callers that can afford allocating a closure may prefer using Visit.
func NewIterator(
	ctx context.Context, rangeID roachpb.RangeID, eng Reader, opts IterOptions,
) (*Iterator, error) {
	// TODO(tbg): can pool these most of the things below, incl. the *Iterator.
	prefixBuf := keys.MakeRangeIDPrefixBuf(rangeID)
	var upperBound roachpb.Key
	if opts.Hi == 0 {
		upperBound = prefixBuf.RaftLogPrefix().PrefixEnd()
	} else {
		upperBound = prefixBuf.RaftLogKey(opts.Hi)
	}
	iter, err := eng.NewMVCCIterator(ctx, storage.MVCCKeyIterKind, storage.IterOptions{
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}
	return &Iterator{
		eng:       eng,
		prefixBuf: prefixBuf,
		iter:      iter,
	}, nil
}

// Close releases the resources associated with this Iterator.
func (it *Iterator) Close() {
	it.iter.Close()
}

func (it *Iterator) load() (bool, error) {
	if ok, err := it.iter.Valid(); err != nil || !ok {
		return false, err
	}
	v, err := it.iter.UnsafeValue()
	if err != nil {
		return false, err
	}
	if it.entry, err = RaftEntryFromRawValue(v); err != nil {
		return false, err
	}
	return true, nil
}

// SeekGE positions the Iterator at the first raft log with index greater than
// or equal to idx. Returns (true, nil) on success, (false, nil) if no such
// entry exists.
func (it *Iterator) SeekGE(idx kvpb.RaftIndex) (bool, error) {
	it.iter.SeekGE(storage.MakeMVCCMetadataKey(it.prefixBuf.RaftLogKey(idx)))
	return it.load()
}

// Next returns (true, nil) when the (in ascending index order) next entry is
// available via Entry(). It returns (false, nil) if there are no more entries.
//
// NB: a valid raft log has no gaps, but the Iterator does not validate that.
func (it *Iterator) Next() (bool, error) {
	it.iter.Next()
	return it.load()
}

// Entry returns the raft entry the iterator is currently positioned at. This
// must only be called after a prior successful call to SeekGE or Next.
func (it *Iterator) Entry() raftpb.Entry {
	return it.entry
}

// Visit invokes fn with the raft log entries whose indexes fall into [lo, hi)
// in ascending index order. For example, if there are log entries for indexes
// 6, 7, and 8, then [lo, hi)=[1, 8) will visit 6 and 7, and [lo, hi)=[7,9) will
// visit 7 and 8.
//
// The closure may return iterutil.StopIteration(), which will stop iteration
// without returning an error.
func Visit(
	ctx context.Context,
	eng Reader,
	rangeID roachpb.RangeID,
	lo, hi kvpb.RaftIndex,
	fn func(raftpb.Entry) error,
) error {
	it, err := NewIterator(ctx, rangeID, eng, IterOptions{Hi: hi})
	if err != nil {
		return err
	}
	defer it.Close()
	ok, err := it.SeekGE(lo)
	if err != nil {
		return err
	}
	for ; ok; ok, err = it.Next() {
		if err := fn(it.Entry()); err != nil {
			return iterutil.Map(err)
		}
	}
	return err
}
