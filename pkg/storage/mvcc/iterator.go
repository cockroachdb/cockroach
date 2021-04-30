// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package mvcc

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// SimpleMVCCIterator is an interface for iterating over key/value pairs in an
// engine. SimpleMVCCIterator implementations are thread safe unless otherwise
// noted. SimpleMVCCIterator is a subset of the functionality offered by MVCCIterator.
type SimpleMVCCIterator interface {
	// Close frees up resources held by the iterator.
	Close()
	// SeekGE advances the iterator to the first key in the engine which
	// is >= the provided key.
	SeekGE(key MVCCKey)
	// Valid must be called after any call to Seek(), Next(), Prev(), or
	// similar methods. It returns (true, nil) if the iterator points to
	// a valid key (it is undefined to call Key(), Value(), or similar
	// methods unless Valid() has returned (true, nil)). It returns
	// (false, nil) if the iterator has moved past the end of the valid
	// range, or (false, err) if an error has occurred. Valid() will
	// never return true with a non-nil error.
	Valid() (bool, error)
	// Next advances the iterator to the next key/value in the
	// iteration. After this call, Valid() will be true if the
	// iterator was not positioned at the last key.
	Next()
	// NextKey advances the iterator to the next MVCC key. This operation is
	// distinct from Next which advances to the next version of the current key
	// or the next key if the iterator is currently located at the last version
	// for a key. NextKey must not be used to switch iteration direction from
	// reverse iteration to forward iteration.
	NextKey()
	// UnsafeKey returns the same value as Key, but the memory is invalidated on
	// the next call to {Next,NextKey,Prev,SeekGE,SeekLT,Close}.
	UnsafeKey() MVCCKey
	// UnsafeValue returns the same value as Value, but the memory is
	// invalidated on the next call to {Next,NextKey,Prev,SeekGE,SeekLT,Close}.
	UnsafeValue() []byte
}

// IteratorStats is returned from (MVCCIterator).Stats.
type IteratorStats struct {
	InternalDeleteSkippedCount int
	TimeBoundNumSSTs           int
}

// MVCCIterator is an interface for iterating over key/value pairs in an
// engine. It is used for iterating over the key space that can have multiple
// versions, and if often also used (due to historical reasons) for iterating
// over the key space that never has multiple versions (i.e.,
// MVCCKey.Timestamp.IsEmpty()).
//
// MVCCIterator implementations are thread safe unless otherwise noted.
type MVCCIterator interface {
	SimpleMVCCIterator

	// SeekLT advances the iterator to the first key in the engine which
	// is < the provided key.
	SeekLT(key MVCCKey)
	// Prev moves the iterator backward to the previous key/value
	// in the iteration. After this call, Valid() will be true if the
	// iterator was not positioned at the first key.
	Prev()

	// SeekIntentGE is a specialized version of SeekGE(MVCCKey{Key: key}), when
	// the caller expects to find an intent, and additionally has the txnUUID
	// for the intent it is looking for. When running with separated intents,
	// this can optimize the behavior of the underlying Engine for write heavy
	// keys by avoiding the need to iterate over many deleted intents.
	SeekIntentGE(key roachpb.Key, txnUUID uuid.UUID)

	// Key returns the current key.
	Key() MVCCKey
	// UnsafeRawKey returns the current raw key which could be an encoded
	// MVCCKey, or the more general EngineKey (for a lock table key).
	// This is a low-level and dangerous method since it will expose the
	// raw key of the lock table, i.e., the intentInterleavingIter will not
	// hide the difference between interleaved and separated intents.
	// Callers should be very careful when using this. This is currently
	// only used by callers who are iterating and deleting all data in a
	// range.
	UnsafeRawKey() []byte
	// UnsafeRawMVCCKey returns a serialized MVCCKey. The memory is invalidated
	// on the next call to {Next,NextKey,Prev,SeekGE,SeekLT,Close}. If the
	// iterator is currently positioned at a separated intent (when
	// intentInterleavingIter is used), it makes that intent look like an
	// interleaved intent key, i.e., an MVCCKey with an empty timestamp. This is
	// currently used by callers who pass around key information as a []byte --
	// this seems avoidable, and we should consider cleaning up the callers.
	UnsafeRawMVCCKey() []byte
	// Value returns the current value as a byte slice.
	Value() []byte
	// ValueProto unmarshals the value the iterator is currently
	// pointing to using a protobuf decoder.
	ValueProto(msg protoutil.Message) error
	// When Key() is positioned on an intent, returns true iff this intent
	// (represented by MVCCMetadata) is a separated lock/intent. This is a
	// low-level method that should not be called from outside the storage
	// package. It is part of the exported interface because there are structs
	// outside the package that wrap and implement Iterator.
	IsCurIntentSeparated() bool
	// ComputeStats scans the underlying engine from start to end keys and
	// computes stats counters based on the values. This method is used after a
	// range is split to recompute stats for each subrange. The start key is
	// always adjusted to avoid counting local keys in the event stats are being
	// recomputed for the first range (i.e. the one with start key == KeyMin).
	// The nowNanos arg specifies the wall time in nanoseconds since the
	// epoch and is used to compute the total age of all intents.
	ComputeStats(start, end roachpb.Key, nowNanos int64) (enginepb.MVCCStats, error)
	// FindSplitKey finds a key from the given span such that the left side of
	// the split is roughly targetSize bytes. The returned key will never be
	// chosen from the key ranges listed in keys.NoSplitSpans and will always
	// sort equal to or after minSplitKey.
	//
	// DO NOT CALL directly (except in wrapper MVCCIterator implementations). Use the
	// package-level MVCCFindSplitKey instead. For correct operation, the caller
	// must set the upper bound on the iterator before calling this method.
	FindSplitKey(start, end, minSplitKey roachpb.Key, targetSize int64) (MVCCKey, error)
	// CheckForKeyCollisions checks whether any keys collide between the iterator
	// and the encoded SST data specified, within the provided key range. Returns
	// stats on skipped KVs, or an error if a collision is found.
	CheckForKeyCollisions(sstData []byte, start, end roachpb.Key) (enginepb.MVCCStats, error)
	// SetUpperBound installs a new upper bound for this iterator. The caller
	// can modify the parameter after this function returns. This must not be a
	// nil key. When Reader.ConsistentIterators is true, prefer creating a new
	// iterator.
	//
	// Due to the rare use, we are limiting this method to not switch an
	// iterator from a global key upper-bound to a local key upper-bound (it
	// simplifies some code in intentInterleavingIter) or vice versa. Iterator
	// reuse already happens under-the-covers for most Reader implementations
	// when constructing a new iterator, and that is a much cleaner solution.
	//
	// TODO(sumeer): this method is rarely used and is a source of complexity
	// since intentInterleavingIter needs to fiddle with the bounds of its
	// underlying iterators when this is called. Currently only used by
	// pebbleBatch.ClearIterRange to modify the upper bound of the iterator it
	// is given: this use is unprincipled and there is a comment in that code
	// about it. The caller is already usually setting the bounds accurately,
	// and in some cases the callee is tightening the upper bound. Remove that
	// use case and remove this from the interface.
	SetUpperBound(roachpb.Key)
	// Stats returns statistics about the iterator.
	Stats() IteratorStats
	// SupportsPrev returns true if MVCCIterator implementation supports reverse
	// iteration with Prev() or SeekLT().
	SupportsPrev() bool
}
