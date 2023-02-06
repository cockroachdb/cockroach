// Copyright 2014 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/storage/pebbleiter"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	prometheusgo "github.com/prometheus/client_model/go"
)

// DefaultStorageEngine represents the default storage engine to use.
var DefaultStorageEngine enginepb.EngineType

func init() {
	_ = DefaultStorageEngine.Set(envutil.EnvOrDefaultString("COCKROACH_STORAGE_ENGINE", "pebble"))
}

// SimpleMVCCIterator is an interface for iterating over key/value pairs in an
// engine. SimpleMVCCIterator implementations are thread safe unless otherwise
// noted. SimpleMVCCIterator is a subset of the functionality offered by
// MVCCIterator.
//
// API invariants are asserted via assertSimpleMVCCIteratorInvariants().
//
// The iterator exposes both point keys and range keys. Range keys are only
// emitted when enabled via IterOptions.KeyTypes. Currently, all range keys are
// MVCC range tombstones, and this is enforced during writes.
//
// Range keys and point keys exist separately in Pebble. A specific key position
// can have both a point key and multiple range keys overlapping it. Their
// properties are accessed via:
//
// HasPointAndRange(): Key types present at the current position.
// UnsafeKey():        Current position (point key if any).
// UnsafeValue():      Current point key value (if any).
// RangeBounds():      Start,end bounds of range keys at current position.
// RangeKeys():        All range keys/values overlapping current position.
//
// Consider the following point keys and range keys:
//
//	4: a4  b4
//	3: [-------)
//	2: [-------)
//	1:     b1  c1
//	   a   b   c
//
// Range keys cover a span between two roachpb.Key bounds (start inclusive, end
// exclusive) and contain timestamp/value pairs. They overlap *all* point key
// versions within their key bounds regardless of timestamp. For example, when
// the iterator is positioned on b@4, it will also expose [a-c)@3 and [a-c)@2.
//
// During iteration with IterKeyTypePointsAndRanges, range keys are emitted at
// their start key and at every overlapping point key. For example, iterating
// across the above span would emit this sequence:
//
// UnsafeKey HasPointAndRange UnsafeValue RangeKeys
// a         false,true       -           [a-c)@3 [a-c)@2
// a@4       true,true        a4          [a-c)@3 [a-c)@2
// b@4       true,true        b4          [a-c)@3 [a-c)@2
// b@1       true,true        b1          [a-c)@3 [a-c)@2
// c@1       true,false       c1          -
//
// MVCCIterator reverse iteration yields the above sequence in reverse.
// Notably, bare range keys are still emitted at their start key (not end key),
// so they will be emitted last in this example.
//
// When using SeekGE within range key bounds, the iterator may land on the bare
// range key first, unless seeking exactly to an existing point key. E.g.:
//
// SeekGE UnsafeKey HasPointAndRange UnsafeValue RangeKeys
// b      b         false,true       -           [a-c)@3 [a-c)@2
// b@5    b@5       false,true       -           [a-c)@3 [a-c)@2
// b@4    b@4       true,true        b@4         [a-c)@3 [a-c)@2
// b@3    b@3       false,true       -           [a-c)@3 [a-c)@2
//
// Note that intents (with timestamp 0) encode to a bare roachpb.Key, so they
// will be colocated with a range key start bound. For example, if there was an
// intent on a in the above example, then both SeekGE(a) and forward iteration
// would land on a@0 and [a-c)@3,[a-c)@2 simultaneously, instead of the bare
// range keys first.
//
// Range keys do not have a stable, discrete identity, and should be
// considered a continuum: they may be merged or fragmented by other range key
// writes, split and merged along with CRDB ranges, partially removed by GC,
// and truncated by iterator bounds.
//
// Range keys are fragmented by Pebble such that all overlapping range keys
// between two keys form a stack of range key fragments at different timestamps.
// For example, writing [a-e)@1 and [c-g)@2 will yield this fragment structure:
//
//	2:     |---|---|
//	1: |---|---|
//	   a   c   e   g
//
// Fragmentation makes all range key properties local, which avoids incurring
// unnecessary access costs across SSTs and CRDB ranges. It is deterministic
// on the current range key state, and does not depend on write history.
// Stacking allows easy access to all range keys overlapping a point key.
//
// For more information on MVCC range keys, see this tech note:
// https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/mvcc-range-tombstones.md
type SimpleMVCCIterator interface {
	// Close frees up resources held by the iterator.
	Close()
	// SeekGE advances the iterator to the first key in the engine which is >= the
	// provided key. This may be in the middle of a bare range key straddling the
	// seek key.
	SeekGE(key MVCCKey)
	// Valid must be called after any call to Seek(), Next(), Prev(), or
	// similar methods. It returns (true, nil) if the iterator points to
	// a valid key (it is undefined to call Key(), Value(), or similar
	// methods unless Valid() has returned (true, nil)). It returns
	// (false, nil) if the iterator has moved past the end of the valid
	// range, or (false, err) if an error has occurred. Valid() will
	// never return true with a non-nil error.
	Valid() (bool, error)
	// Next advances the iterator to the next key in the iteration. After this
	// call, Valid() will be true if the iterator was not positioned at the last
	// key.
	Next()
	// NextKey advances the iterator to the next MVCC key. This operation is
	// distinct from Next which advances to the next version of the current key
	// or the next key if the iterator is currently located at the last version
	// for a key. NextKey must not be used to switch iteration direction from
	// reverse iteration to forward iteration.
	//
	// If NextKey() lands on a bare range key, it is possible that there exists a
	// versioned point key at the start key too. Calling NextKey() again would
	// skip over this point key, since the start key was already emitted. If the
	// caller wants to see it, it must call Next() to check for it. Note that
	// this is not the case with intents: they don't have a timestamp, so the
	// encoded key is identical to the range key's start bound, and they will
	// be emitted together at that position.
	NextKey()
	// UnsafeKey returns the current key position. This may be a point key, or
	// the current position inside a range key (typically the start key
	// or the seek key when using SeekGE within its bounds).
	//
	// The memory is invalidated on the next call to {Next,NextKey,Prev,SeekGE,
	// SeekLT,Close}. Use Key() if this is undesirable.
	UnsafeKey() MVCCKey
	// UnsafeValue returns the current point key value as a byte slice.
	// This must only be called when it is known that the iterator is positioned
	// at a point value, i.e. HasPointAndRange has returned (true, *). If
	// possible, use MVCCValueLenAndIsTombstone() instead.
	//
	// The memory is invalidated on the next call to {Next,NextKey,Prev,SeekGE,SeekLT,Close}.
	// Use Value() if that is undesirable.
	UnsafeValue() ([]byte, error)
	// MVCCValueLenAndIsTombstone should be called only for MVCC (i.e.,
	// UnsafeKey().IsValue()) point values, when the actual point value is not
	// needed, for example when updating stats and making GC decisions, and it
	// is sufficient for the caller to know the length (len(UnsafeValue()), and
	// whether the underlying MVCCValue is a tombstone
	// (MVCCValue.IsTombstone()). This is an optimization that can allow the
	// underlying storage layer to avoid retrieving the value.
	// REQUIRES: HasPointAndRange() has returned (true, *).
	MVCCValueLenAndIsTombstone() (int, bool, error)
	// ValueLen can be called for MVCC or non-MVCC values, when only the value
	// length is needed. This is an optimization that can allow the underlying
	// storage layer to avoid retrieving the value.
	// REQUIRES: HasPointAndRange() has returned (true, *).
	ValueLen() int
	// HasPointAndRange returns whether the current iterator position has a point
	// key and/or a range key. Must check Valid() first. At least one of these
	// will always be true for a valid iterator. For details on range keys, see
	// comment on SimpleMVCCIterator.
	HasPointAndRange() (bool, bool)
	// RangeBounds returns the range bounds for the current range key, or an
	// empty span if there are none. The returned keys are valid until the
	// range key changes, see RangeKeyChanged().
	RangeBounds() roachpb.Span
	// RangeKeys returns a stack of all range keys (with different timestamps) at
	// the current key position. When at a point key, it will return all range
	// keys overlapping that point key. The stack is valid until the range key
	// changes, see RangeKeyChanged().
	//
	// For details on range keys, see SimpleMVCCIterator comment, or tech note:
	// https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/mvcc-range-tombstones.md
	RangeKeys() MVCCRangeKeyStack
	// RangeKeyChanged returns true if the previous seek or step moved to a
	// different range key (or none at all). Requires a valid iterator, but an
	// exhausted iterator is considered to have had no range keys when calling
	// this after repositioning.
	RangeKeyChanged() bool
}

// IteratorStats is returned from {MVCCIterator,EngineIterator}.Stats.
type IteratorStats struct {
	// Iteration stats. We directly expose pebble.IteratorStats. Callers
	// may want to aggregate and interpret these in the following manner:
	// - Aggregate {Forward,Reverse}SeekCount, {Forward,Reverse}StepCount.
	// - Interpret the four aggregated stats as follows:
	//   - {SeekCount,StepCount}[InterfaceCall]: We can refer to these simply as
	//     {SeekCount,StepCount} in logs/metrics/traces. These represents
	//     explicit calls by the implementation of MVCCIterator, in response to
	//     the caller of MVCCIterator. A high count relative to the unique MVCC
	//     keys returned suggests there are many versions for the same key.
	//   - {SeekCount,StepCount}[InternalIterCall]: We can refer to these simply
	//     as {InternalSeekCount,InternalStepCount}. If these are significantly
	//     larger than the ones in the preceding bullet, it suggests that there
	//     are lots of uncompacted deletes or stale Pebble-versions (not to be
	//     confused with MVCC versions) that need to be compacted away. This
	//     should be very rare, but has been observed.
	Stats pebble.IteratorStats
}

// MVCCIterator is an interface for iterating over key/value pairs in an
// engine. It is used for iterating over the key space that can have multiple
// versions, and if often also used (due to historical reasons) for iterating
// over the key space that never has multiple versions (i.e.,
// MVCCKey.Timestamp.IsEmpty()).
//
// MVCCIterator implementations are thread safe unless otherwise noted. API
// invariants are asserted via assertMVCCIteratorInvariants().
//
// For details on range keys and iteration, see comment on SimpleMVCCIterator.
type MVCCIterator interface {
	SimpleMVCCIterator

	// SeekLT advances the iterator to the first key in the engine which is < the
	// provided key. Unlike SeekGE, when calling SeekLT within range key bounds
	// this will not land on the seek key, but rather on the closest point key
	// overlapping the range key or the range key's start bound.
	SeekLT(key MVCCKey)
	// Prev moves the iterator backward to the previous key in the iteration.
	// After this call, Valid() will be true if the iterator was not positioned at
	// the first key.
	Prev()

	// SeekIntentGE is a specialized version of SeekGE(MVCCKey{Key: key}), when
	// the caller expects to find an intent, and additionally has the txnUUID
	// for the intent it is looking for. When running with separated intents,
	// this can optimize the behavior of the underlying Engine for write heavy
	// keys by avoiding the need to iterate over many deleted intents.
	SeekIntentGE(key roachpb.Key, txnUUID uuid.UUID)

	// Key is like UnsafeKey, but returns memory now owned by the caller.
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
	// Value is like UnsafeValue, but returns memory owned by the caller.
	Value() ([]byte, error)
	// ValueProto unmarshals the value the iterator is currently
	// pointing to using a protobuf decoder.
	ValueProto(msg protoutil.Message) error
	// FindSplitKey finds a key from the given span such that the left side of the
	// split is roughly targetSize bytes. It only considers MVCC point keys, not
	// range keys. The returned key will never be chosen from the key ranges
	// listed in keys.NoSplitSpans and will always sort equal to or after
	// minSplitKey.
	//
	// DO NOT CALL directly (except in wrapper MVCCIterator implementations). Use the
	// package-level MVCCFindSplitKey instead. For correct operation, the caller
	// must set the upper bound on the iterator before calling this method.
	FindSplitKey(start, end, minSplitKey roachpb.Key, targetSize int64) (MVCCKey, error)
	// Stats returns statistics about the iterator.
	Stats() IteratorStats
	// IsPrefix returns true if the MVCCIterator is a prefix iterator, i.e.
	// created with IterOptions.Prefix enabled.
	IsPrefix() bool
	// UnsafeLazyValue is only for use inside the storage package. It exposes
	// the LazyValue at the current iterator position, and hence delays fetching
	// the actual value. It is exposed for reverse scans that need to search for
	// the most recent relevant version, and can't know whether the current
	// value is that version, and need to step back to make that determination.
	//
	// REQUIRES: Valid() returns true.
	UnsafeLazyValue() pebble.LazyValue
}

// EngineIterator is an iterator over key-value pairs where the key is
// an EngineKey.
type EngineIterator interface {
	// Close frees up resources held by the iterator.
	Close()
	// SeekEngineKeyGE advances the iterator to the first key in the engine
	// which is >= the provided key.
	SeekEngineKeyGE(key EngineKey) (valid bool, err error)
	// SeekEngineKeyLT advances the iterator to the first key in the engine
	// which is < the provided key.
	SeekEngineKeyLT(key EngineKey) (valid bool, err error)
	// NextEngineKey advances the iterator to the next key/value in the
	// iteration. After this call, valid will be true if the iterator was not
	// originally positioned at the last key. Note that unlike
	// MVCCIterator.NextKey, this method does not skip other versions with the
	// same EngineKey.Key.
	// TODO(sumeer): change MVCCIterator.Next() to match the
	// return values, change all its callers, and rename this
	// to Next().
	NextEngineKey() (valid bool, err error)
	// PrevEngineKey moves the iterator backward to the previous key/value in
	// the iteration. After this call, valid will be true if the iterator was
	// not originally positioned at the first key.
	PrevEngineKey() (valid bool, err error)
	// HasPointAndRange returns whether the iterator is positioned on a point or
	// range key (shared with MVCCIterator interface).
	HasPointAndRange() (bool, bool)
	// EngineRangeBounds returns the current range key bounds.
	EngineRangeBounds() (roachpb.Span, error)
	// EngineRangeKeys returns the engine range keys at the current position.
	EngineRangeKeys() []EngineRangeKeyValue
	// RangeKeyChanged returns true if the previous seek or step moved to a
	// different range key (or none at all). This includes an exhausted iterator.
	RangeKeyChanged() bool
	// UnsafeEngineKey returns the same value as EngineKey, but the memory is
	// invalidated on the next call to {Next,NextKey,Prev,SeekGE,SeekLT,Close}.
	// REQUIRES: latest positioning function returned valid=true.
	UnsafeEngineKey() (EngineKey, error)
	// EngineKey returns the current key.
	// REQUIRES: latest positioning function returned valid=true.
	EngineKey() (EngineKey, error)
	// UnsafeRawEngineKey returns the current raw (encoded) key corresponding to
	// EngineKey. This is a low-level method and callers should avoid using
	// it. This is currently only used by intentInterleavingIter to implement
	// UnsafeRawKey.
	UnsafeRawEngineKey() []byte
	// UnsafeValue returns the same value as Value, but the memory is
	// invalidated on the next call to {Next,NextKey,Prev,SeekGE,SeekLT,Close}.
	// REQUIRES: latest positioning function returned valid=true.
	UnsafeValue() ([]byte, error)
	// Value returns the current value as a byte slice.
	// REQUIRES: latest positioning function returned valid=true.
	Value() ([]byte, error)
	// GetRawIter is a low-level method only for use in the storage package,
	// that returns the underlying pebble Iterator.
	GetRawIter() pebbleiter.Iterator
	// SeekEngineKeyGEWithLimit is similar to SeekEngineKeyGE, but takes an
	// additional exclusive upper limit parameter. The limit is semantically
	// best-effort, and is an optimization to avoid O(n^2) iteration behavior in
	// some pathological situations (uncompacted deleted locks).
	SeekEngineKeyGEWithLimit(key EngineKey, limit roachpb.Key) (state pebble.IterValidityState, err error)
	// SeekEngineKeyLTWithLimit is similar to SeekEngineKeyLT, but takes an
	// additional inclusive lower limit parameter. The limit is semantically
	// best-effort, and is an optimization to avoid O(n^2) iteration behavior in
	// some pathological situations (uncompacted deleted locks).
	SeekEngineKeyLTWithLimit(key EngineKey, limit roachpb.Key) (state pebble.IterValidityState, err error)
	// NextEngineKeyWithLimit is similar to NextEngineKey, but takes an
	// additional exclusive upper limit parameter. The limit is semantically
	// best-effort, and is an optimization to avoid O(n^2) iteration behavior in
	// some pathological situations (uncompacted deleted locks).
	NextEngineKeyWithLimit(limit roachpb.Key) (state pebble.IterValidityState, err error)
	// PrevEngineKeyWithLimit is similar to PrevEngineKey, but takes an
	// additional inclusive lower limit parameter. The limit is semantically
	// best-effort, and is an optimization to avoid O(n^2) iteration behavior in
	// some pathological situations (uncompacted deleted locks).
	PrevEngineKeyWithLimit(limit roachpb.Key) (state pebble.IterValidityState, err error)
	// Stats returns statistics about the iterator.
	Stats() IteratorStats
}

// IterOptions contains options used to create an {MVCC,Engine}Iterator.
//
// For performance, every {MVCC,Engine}Iterator must specify either Prefix or
// UpperBound.
type IterOptions struct {
	// If Prefix is true, Seek will use the user-key prefix of the supplied
	// {MVCC,Engine}Key (the Key field) to restrict which sstables are searched,
	// but iteration (using Next) over keys without the same user-key prefix
	// will not work correctly (keys may be skipped).
	Prefix bool
	// LowerBound gives this iterator an inclusive lower bound. Attempts to
	// SeekReverse or Prev to a key that is strictly less than the bound will
	// invalidate the iterator.
	LowerBound roachpb.Key
	// UpperBound gives this iterator an exclusive upper bound. Attempts to Seek
	// or Next to a key that is greater than or equal to the bound will invalidate
	// the iterator. UpperBound must be provided unless Prefix is true, in which
	// case the end of the prefix will be used as the upper bound.
	UpperBound roachpb.Key
	// If WithStats is true, the iterator accumulates performance
	// counters over its lifetime which can be queried via `Stats()`.
	WithStats bool
	// MinTimestampHint and MaxTimestampHint, if set, indicate that keys outside
	// of the time range formed by [MinTimestampHint, MaxTimestampHint] do not
	// need to be presented by the iterator. The underlying iterator may be able
	// to efficiently skip over keys outside of the hinted time range, e.g., when
	// an SST indicates that it contains no keys within the time range. Intents
	// will not be visible to such iterators at all. This is only relevant for
	// MVCCIterators.
	//
	// Note that time bound hints are strictly a performance optimization, and
	// iterators with time bounds hints will frequently return keys outside of the
	// [start, end] time range. If you must guarantee that you never see a key
	// outside of the time bounds, perform your own filtering.
	//
	// NB: The iterator may surface stale data. Pebble range tombstones do not have
	// timestamps and thus may be ignored entirely depending on whether their SST
	// happens to satisfy the filter. Furthermore, keys outside the timestamp
	// range may be stale and must be ignored -- for example, consider a key foo@5
	// written in an SST with timestamp range [3-7], and then a non-MVCC removal
	// or update of this key in a different SST with timestamp range [3-5]. Using
	// an iterator with range [6-9] would surface the old foo@5 key because it
	// would return all keys in the old [3-7] SST but not take into account the
	// separate [3-5] SST where foo@5 was removed or updated. See also:
	// https://github.com/cockroachdb/pebble/issues/1786
	//
	// NB: Range keys are not currently subject to timestamp filtering due to
	// complications with MVCCIncrementalIterator. See:
	// https://github.com/cockroachdb/cockroach/issues/86260
	//
	// Currently, the only way to correctly use such an iterator is to use it in
	// concert with an iterator without timestamp hints, as done by
	// MVCCIncrementalIterator.
	MinTimestampHint, MaxTimestampHint hlc.Timestamp
	// KeyTypes specifies the types of keys to surface: point and/or range keys.
	// Use HasPointAndRange() to determine which key type is present at a given
	// iterator position, and RangeBounds() and RangeKeys() to access range keys.
	// Defaults to IterKeyTypePointsOnly. For more details on range keys, see
	// comment on SimpleMVCCIterator.
	KeyTypes IterKeyType
	// RangeKeyMaskingBelow enables masking (hiding) of point keys by range keys.
	// Any range key with a timestamp at or below RangeKeyMaskingBelow
	// will mask point keys below it, preventing them from being surfaced.
	// Consider the following example:
	//
	// 4          o---------------o    RangeKeyMaskingBelow=4 emits b3
	// 3      b3      d3               RangeKeyMaskingBelow=3 emits b3,d3,f2
	// 2  o---------------o   f2       RangeKeyMaskingBelow=2 emits b3,d3,f2
	// 1  a1  b1          o-------o    RangeKeyMaskingBelow=1 emits a1,b3,b1,d3,f2
	//    a   b   c   d   e   f   g
	//
	// Range keys themselves are not affected by the masking, and will be
	// emitted as normal.
	RangeKeyMaskingBelow hlc.Timestamp
	// useL6Filters allows the caller to opt into reading filter blocks for
	// L6 sstables. Only for use with Prefix = true. Helpful if a lot of prefix
	// Seeks are expected in quick succession, that are also likely to not
	// yield a single key. Filter blocks in L6 can be relatively large, often
	// larger than data blocks, so the benefit of loading them in the cache
	// is minimized if the probability of the key existing is not low or if
	// this is a one-time Seek (where loading the data block directly is better).
	useL6Filters bool
}

// IterKeyType configures which types of keys an iterator should surface.
//
// TODO(erikgrinaker): Combine this with MVCCIterKind somehow.
type IterKeyType = pebble.IterKeyType

const (
	// IterKeyTypePointsOnly iterates over point keys only.
	IterKeyTypePointsOnly = pebble.IterKeyTypePointsOnly
	// IterKeyTypePointsAndRanges iterates over both point and range keys.
	IterKeyTypePointsAndRanges = pebble.IterKeyTypePointsAndRanges
	// IterKeyTypeRangesOnly iterates over only range keys.
	IterKeyTypeRangesOnly = pebble.IterKeyTypeRangesOnly
)

// MVCCIterKind is used to inform Reader about the kind of iteration desired
// by the caller.
type MVCCIterKind int

// "Intent" refers to non-inline meta, that can be interleaved or separated.
const (
	// MVCCKeyAndIntentsIterKind specifies that intents must be seen, and appear
	// interleaved with keys, even if they are in a separated lock table.
	// Iterators of this kind are not allowed to span from local to global keys,
	// since the physical layout has the separated lock table in-between the
	// local and global keys. These iterators do strict error checking and panic
	// if the caller seems that to be trying to violate this constraint.
	// Specifically:
	// - If both bounds are set they must not span from local to global.
	// - Any bound (lower or upper), constrains the iterator for its lifetime to
	//   one of local or global keys. The iterator will not tolerate a seek that
	//   violates this constraint.
	// We could, with significant code complexity, not constrain an iterator for
	// its lifetime, and allow a seek that specifies a global (local) key to
	// change the constraint to global (local). This would allow reuse of the
	// same iterator with a large global upper-bound. But a Next call on the
	// highest local key (Prev on the lowest global key) would still not be able
	// to transparently skip over the intermediate lock table. We deem that
	// behavior to be more surprising and bug-prone (for the caller), than being
	// strict.
	MVCCKeyAndIntentsIterKind MVCCIterKind = iota
	// MVCCKeyIterKind specifies that the caller does not need to see intents.
	// Any interleaved intents may be seen, but no correctness properties are
	// derivable from such partial knowledge of intents. NB: this is a performance
	// optimization when iterating over (a) MVCC keys where the caller does
	// not need to see intents, (b) a key space that is known to not have multiple
	// versions (and therefore will never have intents), like the raft log.
	MVCCKeyIterKind
)

// Reader is the read interface to an engine's data. Certain implementations
// of Reader guarantee consistency of the underlying engine state across the
// different iterators created by NewMVCCIterator, NewEngineIterator:
//   - pebbleSnapshot, because it uses an engine snapshot.
//   - pebbleReadOnly, pebbleBatch: when the IterOptions do not specify a
//     timestamp hint (see IterOptions). Note that currently the engine state
//     visible here is not as of the time of the Reader creation. It is the time
//     when the first iterator is created, or earlier if
//     PinEngineStateForIterators is called.
//
// The ConsistentIterators method returns true when this consistency is
// guaranteed by the Reader.
// TODO(sumeer): this partial consistency can be a source of bugs if future
// code starts relying on it, but rarely uses a Reader that does not guarantee
// it. Can we enumerate the current cases where KV uses Engine as a Reader?
type Reader interface {
	// Close closes the reader, freeing up any outstanding resources. Note that
	// various implementations have slightly different behaviors. In particular,
	// Distinct() batches release their parent batch for future use while
	// Engines, Snapshots and Batches free the associated C++ resources.
	Close()
	// Closed returns true if the reader has been closed or is not usable.
	// Objects backed by this reader (e.g. Iterators) can check this to ensure
	// that they are not using a closed engine. Intended for use within package
	// engine; exported to enable wrappers to exist in other packages.
	Closed() bool
	// MVCCIterate scans from the start key to the end key (exclusive), invoking
	// the function f on each key value pair. The inputs are copies, and safe to
	// retain beyond the function call. It supports interleaved iteration over
	// point and/or range keys, providing any overlapping range keys for each
	// point key if requested. If f returns an error or if the scan itself
	// encounters an error, the iteration will stop and return the error.
	//
	// Note that this method is not expected take into account the timestamp of
	// the end key; all MVCCKeys at end.Key are considered excluded in the
	// iteration.
	MVCCIterate(start, end roachpb.Key, iterKind MVCCIterKind, keyTypes IterKeyType,
		f func(MVCCKeyValue, MVCCRangeKeyStack) error) error
	// NewMVCCIterator returns a new instance of an MVCCIterator over this engine.
	// The caller must invoke Close() on it when done to free resources.
	//
	// Write visibility semantics:
	//
	// 1. An iterator has a consistent view of the reader as of the time of its
	//    creation. Subsequent writes are never visible to it.
	//
	// 2. All iterators on readers with ConsistentIterators=true have a consistent
	//    view of the _engine_ (not reader) as of the time of the first iterator
	//    creation or PinEngineStateForIterators call: newer engine writes are
	//    never visible. The opposite holds for ConsistentIterators=false: new
	//    iterators see the most recent engine state at the time of their creation.
	//
	// 3. Iterators on unindexed batches never see batch writes, but satisfy
	//    ConsistentIterators for engine write visibility.
	//
	// 4. Iterators on indexed batches see all batch writes as of their creation
	//    time, but they satisfy ConsistentIterators for engine writes.
	NewMVCCIterator(iterKind MVCCIterKind, opts IterOptions) MVCCIterator
	// NewEngineIterator returns a new instance of an EngineIterator over this
	// engine. The caller must invoke EngineIterator.Close() when finished
	// with the iterator to free resources. The caller can change IterOptions
	// after this function returns.
	NewEngineIterator(opts IterOptions) EngineIterator
	// ConsistentIterators returns true if the Reader implementation guarantees
	// that the different iterators constructed by this Reader will see the same
	// underlying Engine state. This is not true about Batch writes: new iterators
	// will see new writes made to the batch, existing iterators won't.
	ConsistentIterators() bool
	// SupportsRangeKeys returns true if the Reader implementation supports
	// range keys.
	//
	// TODO(erikgrinaker): Remove this after 22.2.
	SupportsRangeKeys() bool

	// PinEngineStateForIterators ensures that the state seen by iterators
	// without timestamp hints (see IterOptions) is pinned and will not see
	// future mutations. It can be called multiple times on a Reader in which
	// case the state seen will be either:
	// - As of the first call.
	// - For a Reader returned by Engine.NewSnapshot, the pinned state is as of
	//   the time the snapshot was taken.
	// So the semantics that are true for all Readers is that the pinned state
	// is somewhere in the time interval between the creation of the Reader and
	// the first call to PinEngineStateForIterators.
	// REQUIRES: ConsistentIterators returns true.
	PinEngineStateForIterators() error
}

// Writer is the write interface to an engine's data.
type Writer interface {
	// ApplyBatchRepr atomically applies a set of batched updates. Created by
	// calling Repr() on a batch. Using this method is equivalent to constructing
	// and committing a batch whose Repr() equals repr. If sync is true, the
	// batch is synchronously written to disk. It is an error to specify
	// sync=true if the Writer is a Batch.
	//
	// It is safe to modify the contents of the arguments after ApplyBatchRepr
	// returns.
	ApplyBatchRepr(repr []byte, sync bool) error

	// ClearMVCC removes the point key with the given MVCCKey from the db. It does
	// not affect range keys. It requires that the timestamp is non-empty (see
	// ClearUnversioned or ClearIntent if the timestamp is empty). Note that clear
	// actually removes entries from the storage engine, rather than inserting
	// MVCC tombstones.
	//
	// It is safe to modify the contents of the arguments after it returns.
	ClearMVCC(key MVCCKey) error
	// ClearUnversioned removes an unversioned item from the db. It is for use
	// with inline metadata (not intents) and other unversioned keys (like
	// Range-ID local keys). It does not affect range keys.
	//
	// It is safe to modify the contents of the arguments after it returns.
	ClearUnversioned(key roachpb.Key) error
	// ClearIntent removes an intent from the db. Unlike ClearMVCC and
	// ClearUnversioned, this is a higher-level method that may make changes in
	// parts of the key space that are not only a function of the input, and may
	// choose to use a single-clear under the covers. txnDidNotUpdateMeta allows
	// for performance optimization when set to true, and has semantics defined in
	// MVCCMetadata.TxnDidNotUpdateMeta (it can be conservatively set to false).
	//
	// It is safe to modify the contents of the arguments after it returns.
	//
	// TODO(sumeer): after the full transition to separated locks, measure the
	// cost of a PutIntent implementation, where there is an existing intent,
	// that does a <single-clear, put> pair. If there isn't a performance
	// decrease, we can stop tracking txnDidNotUpdateMeta and still optimize
	// ClearIntent by always doing single-clear.
	ClearIntent(key roachpb.Key, txnDidNotUpdateMeta bool, txnUUID uuid.UUID) error
	// ClearEngineKey removes the given point key from the engine. It does not
	// affect range keys.  Note that clear actually removes entries from the
	// storage engine. This is a general-purpose and low-level method that should
	// be used sparingly, only when the other Clear* methods are not applicable.
	//
	// It is safe to modify the contents of the arguments after it returns.
	ClearEngineKey(key EngineKey) error

	// ClearRawRange removes point and/or range keys from start (inclusive) to end
	// (exclusive) using Pebble range tombstones. It can be applied to a range
	// consisting of MVCCKeys or the more general EngineKeys -- it simply uses the
	// roachpb.Key parameters as the Key field of an EngineKey. This implies that
	// it does not clear intents unless the intent lock table is targeted
	// explicitly.
	//
	// Similar to the other Clear* methods, this method actually removes entries
	// from the storage engine. It is safe to modify the contents of the arguments
	// after it returns.
	ClearRawRange(start, end roachpb.Key, pointKeys, rangeKeys bool) error
	// ClearMVCCRange removes MVCC point and/or range keys (including intents)
	// from start (inclusive) to end (exclusive) using Pebble range tombstones.
	//
	// Similar to the other Clear* methods, this method actually removes entries
	// from the storage engine. It is safe to modify the contents of the arguments
	// after it returns.
	ClearMVCCRange(start, end roachpb.Key, pointKeys, rangeKeys bool) error
	// ClearMVCCVersions removes MVCC point key versions from start (inclusive) to
	// end (exclusive) using a Pebble range tombstone. It is meant for efficiently
	// clearing a subset of versions of a key, since the parameters are MVCCKeys
	// and not roachpb.Keys, but it can also be used across multiple keys. It will
	// ignore intents and range keys, leaving them in place.
	//
	// Similar to the other Clear* methods, this method actually removes entries
	// from the storage engine. It is safe to modify the contents of the arguments
	// after it returns.
	ClearMVCCVersions(start, end MVCCKey) error
	// ClearMVCCIteratorRange removes all point and/or range keys in the given
	// span using an MVCC iterator, by clearing individual keys (including
	// intents).
	//
	// Similar to the other Clear* methods, this method actually removes entries
	// from the storage engine. It is safe to modify the contents of the arguments
	// after it returns.
	//
	// TODO(erikgrinaker): This should be a separate function rather than an
	// interface method, but we keep it for now to make use of UnsafeRawKey() when
	// clearing keys.
	ClearMVCCIteratorRange(start, end roachpb.Key, pointKeys, rangeKeys bool) error

	// ClearMVCCRangeKey deletes an MVCC range key from start (inclusive) to end
	// (exclusive) at the given timestamp. For any range key that straddles the
	// start and end boundaries, only the segments within the boundaries will be
	// cleared. Range keys at other timestamps are unaffected.  Clears are
	// idempotent.
	//
	// This method is primarily intended for MVCC garbage collection and similar
	// internal use.
	ClearMVCCRangeKey(rangeKey MVCCRangeKey) error

	// PutMVCCRangeKey writes an MVCC range key. It will replace any overlapping
	// range keys at the given timestamp (even partial overlap). Only MVCC range
	// tombstones, i.e. an empty value, are currently allowed (other kinds will
	// need additional handling in MVCC APIs and elsewhere, e.g. stats and GC).
	//
	// Range keys must be accessed using special iterator options and methods,
	// see SimpleMVCCIterator.RangeKeys() for details.
	//
	// For more information on MVCC range keys, see this tech note:
	// https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/mvcc-range-tombstones.md
	PutMVCCRangeKey(MVCCRangeKey, MVCCValue) error

	// PutRawMVCCRangeKey is like PutMVCCRangeKey, but accepts an encoded
	// MVCCValue. It can be used to avoid decoding and immediately re-encoding an
	// MVCCValue, but should generally be avoided due to the lack of type safety.
	//
	// It is safe to modify the contents of the arguments after PutRawMVCCRangeKey
	// returns.
	PutRawMVCCRangeKey(MVCCRangeKey, []byte) error

	// PutEngineRangeKey sets the given range key to the values provided. This is
	// a general-purpose and low-level method that should be used sparingly, only
	// when the other Put* methods are not applicable.
	//
	// It is safe to modify the contents of the arguments after it returns.
	PutEngineRangeKey(start, end roachpb.Key, suffix, value []byte) error

	// ClearEngineRangeKey clears the given range key. This is a general-purpose
	// and low-level method that should be used sparingly, only when the other
	// Clear* methods are not applicable.
	//
	// It is safe to modify the contents of the arguments after it returns.
	ClearEngineRangeKey(start, end roachpb.Key, suffix []byte) error

	// Merge is a high-performance write operation used for values which are
	// accumulated over several writes. Multiple values can be merged
	// sequentially into a single key; a subsequent read will return a "merged"
	// value which is computed from the original merged values. We only
	// support Merge for keys with no version.
	//
	// Merge currently provides specialized behavior for three data types:
	// integers, byte slices, and time series observations. Merged integers are
	// summed, acting as a high-performance accumulator.  Byte slices are simply
	// concatenated in the order they are merged. Time series observations
	// (stored as byte slices with a special tag on the roachpb.Value) are
	// combined with specialized logic beyond that of simple byte slices.
	//
	//
	// It is safe to modify the contents of the arguments after Merge returns.
	Merge(key MVCCKey, value []byte) error

	// PutMVCC sets the given key to the value provided. It requires that the
	// timestamp is non-empty (see {PutUnversioned,PutIntent} if the timestamp
	// is empty).
	//
	// It is safe to modify the contents of the arguments after PutMVCC returns.
	PutMVCC(key MVCCKey, value MVCCValue) error
	// PutRawMVCC is like PutMVCC, but it accepts an encoded MVCCValue. It
	// can be used to avoid decoding and immediately re-encoding an MVCCValue,
	// but should generally be avoided due to the lack of type safety.
	//
	// It is safe to modify the contents of the arguments after PutRawMVCC
	// returns.
	PutRawMVCC(key MVCCKey, value []byte) error
	// PutUnversioned sets the given key to the value provided. It is for use
	// with inline metadata (not intents) and other unversioned keys (like
	// Range-ID local keys).
	//
	// It is safe to modify the contents of the arguments after Put returns.
	PutUnversioned(key roachpb.Key, value []byte) error
	// PutIntent puts an intent at the given key to the value provided. This is
	// a higher-level method that may make changes in parts of the key space
	// that are not only a function of the input key, and may explicitly clear
	// the preceding intent. txnDidNotUpdateMeta defines what happened prior to
	// this put, and allows for performance optimization when set to true, and
	// has semantics defined in MVCCMetadata.TxnDidNotUpdateMeta (it can be
	// conservatively set to false).
	//
	// It is safe to modify the contents of the arguments after Put returns.
	PutIntent(ctx context.Context, key roachpb.Key, value []byte, txnUUID uuid.UUID) error
	// PutEngineKey sets the given key to the value provided. This is a
	// general-purpose and low-level method that should be used sparingly,
	// only when the other Put* methods are not applicable.
	//
	// It is safe to modify the contents of the arguments after Put returns.
	PutEngineKey(key EngineKey, value []byte) error

	// LogData adds the specified data to the RocksDB WAL. The data is
	// uninterpreted by RocksDB (i.e. not added to the memtable or sstables).
	//
	// It is safe to modify the contents of the arguments after LogData returns.
	LogData(data []byte) error
	// LogLogicalOp logs the specified logical mvcc operation with the provided
	// details to the writer, if it has logical op logging enabled. For most
	// Writer implementations, this is a no-op.
	LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails)

	// SingleClearEngineKey removes the most recent write to the item from the db
	// with the given key. Whether older writes of the item will come back
	// to life if not also removed with SingleClear is undefined. See the
	// following:
	//   https://github.com/facebook/rocksdb/wiki/Single-Delete
	// for details on the SingleDelete operation that this method invokes. Note
	// that clear actually removes entries from the storage engine, rather than
	// inserting MVCC tombstones. This is a low-level interface that must not be
	// called from outside the storage package. It is part of the interface
	// because there are structs that wrap Writer and implement the Writer
	// interface, that are not part of the storage package.
	//
	// It is safe to modify the contents of the arguments after it returns.
	SingleClearEngineKey(key EngineKey) error

	// ShouldWriteLocalTimestamps is only for internal use in the storage package.
	// This method is temporary, to handle the transition from clusters where not
	// all nodes understand local timestamps.
	ShouldWriteLocalTimestamps(ctx context.Context) bool

	// BufferedSize returns the size of the underlying buffered writes if the
	// Writer implementation is buffered, and 0 if the Writer implementation is
	// not buffered. Buffered writers are expected to always give a monotonically
	// increasing size.
	BufferedSize() int
}

// ReadWriter is the read/write interface to an engine's data.
type ReadWriter interface {
	Reader
	Writer
}

// DurabilityRequirement is an advanced option. If in doubt, use
// StandardDurability.
//
// GuranteedDurability maps to pebble.IterOptions.OnlyReadGuaranteedDurable.
// This acknowledges the fact that we do not (without sacrificing correctness)
// sync the WAL for many writes, and there are some advanced cases
// (raftLogTruncator) that need visibility into what is guaranteed durable.
type DurabilityRequirement int8

const (
	// StandardDurability is what should normally be used.
	StandardDurability DurabilityRequirement = iota
	// GuaranteedDurability is an advanced option (only for raftLogTruncator).
	GuaranteedDurability
)

// Engine is the interface that wraps the core operations of a key/value store.
type Engine interface {
	ReadWriter
	// Attrs returns the engine/store attributes.
	Attrs() roachpb.Attributes
	// Capacity returns capacity details for the engine's available storage.
	Capacity() (roachpb.StoreCapacity, error)
	// Properties returns the low-level properties for the engine's underlying storage.
	Properties() roachpb.StoreProperties
	// Compact forces compaction over the entire database.
	Compact() error
	// Flush causes the engine to write all in-memory data to disk
	// immediately.
	Flush() error
	// GetMetrics retrieves metrics from the engine.
	GetMetrics() Metrics
	// GetEncryptionRegistries returns the file and key registries when encryption is enabled
	// on the store.
	GetEncryptionRegistries() (*EncryptionRegistries, error)
	// GetEnvStats retrieves stats about the engine's environment
	// For RocksDB, this includes details of at-rest encryption.
	GetEnvStats() (*EnvStats, error)
	// GetAuxiliaryDir returns a path under which files can be stored
	// persistently, and from which data can be ingested by the engine.
	//
	// Not thread safe.
	GetAuxiliaryDir() string
	// NewBatch returns a new instance of a batched engine which wraps
	// this engine. Batched engines accumulate all mutations and apply
	// them atomically on a call to Commit().
	NewBatch() Batch
	// NewReadOnly returns a new instance of a ReadWriter that wraps this
	// engine, and with the given durability requirement. This wrapper panics
	// when unexpected operations (e.g., write operations) are executed on it
	// and caches iterators to avoid the overhead of creating multiple iterators
	// for batched reads.
	//
	// All iterators created from a read-only engine are guaranteed to provide a
	// consistent snapshot of the underlying engine. See the comment on the
	// Reader interface and the Reader.ConsistentIterators method.
	NewReadOnly(durability DurabilityRequirement) ReadWriter
	// NewUnindexedBatch returns a new instance of a batched engine which wraps
	// this engine. It is unindexed, in that writes to the batch are not
	// visible to reads until after it commits. The batch accumulates all
	// mutations and applies them atomically on a call to Commit(). Read
	// operations return an error, unless writeOnly is set to false.
	//
	// When writeOnly is false, reads will be satisfied by reading from the
	// underlying engine, i.e., the caller does not see its own writes. This
	// setting should be used only when the caller is certain that this
	// optimization is correct, and beneficial. There are subtleties here -- see
	// the discussion on https://github.com/cockroachdb/cockroach/pull/57661 for
	// more details.
	//
	// TODO(sumeer): We should separate the writeOnly=true case into a
	// separate method, that returns a WriteBatch interface. Even better would
	// be not having an option to pass writeOnly=false, and have the caller
	// explicitly work with a separate WriteBatch and Reader.
	NewUnindexedBatch(writeOnly bool) Batch
	// NewSnapshot returns a new instance of a read-only snapshot
	// engine. Snapshots are instantaneous and, as long as they're
	// released relatively quickly, inexpensive. Snapshots are released
	// by invoking Close(). Note that snapshots must not be used after the
	// original engine has been stopped.
	NewSnapshot() Reader
	// Type returns engine type.
	Type() enginepb.EngineType
	// IngestExternalFiles atomically links a slice of files into the RocksDB
	// log-structured merge-tree.
	IngestExternalFiles(ctx context.Context, paths []string) error
	// IngestExternalFilesWithStats is a variant of IngestExternalFiles that
	// additionally returns ingestion stats.
	IngestExternalFilesWithStats(
		ctx context.Context, paths []string) (pebble.IngestOperationStats, error)
	// PreIngestDelay offers an engine the chance to backpressure ingestions.
	// When called, it may choose to block if the engine determines that it is in
	// or approaching a state where further ingestions may risk its health.
	PreIngestDelay(ctx context.Context)
	// ApproximateDiskBytes returns an approximation of the on-disk size for the given key span.
	ApproximateDiskBytes(from, to roachpb.Key) (uint64, error)
	// CompactRange ensures that the specified range of key value pairs is
	// optimized for space efficiency.
	CompactRange(start, end roachpb.Key) error
	// RegisterFlushCompletedCallback registers a callback that will be run for
	// every successful flush. Only one callback can be registered at a time, so
	// registering again replaces the previous callback. The callback must
	// return quickly and must not call any methods on the Engine in the context
	// of the callback since it could cause a deadlock (since the callback may
	// be invoked while holding mutexes).
	RegisterFlushCompletedCallback(cb func())
	// Filesystem functionality.
	fs.FS
	// CreateCheckpoint creates a checkpoint of the engine in the given directory,
	// which must not exist. The directory should be on the same file system so
	// that hard links can be used. If spans is not empty, the checkpoint excludes
	// SSTs that don't overlap with any of these key spans.
	CreateCheckpoint(dir string, spans []roachpb.Span) error

	// SetMinVersion is used to signal to the engine the current minimum
	// version that it must maintain compatibility with.
	SetMinVersion(version roachpb.Version) error

	// SetCompactionConcurrency is used to set the engine's compaction
	// concurrency. It returns the previous compaction concurrency.
	SetCompactionConcurrency(n uint64) uint64
}

// Batch is the interface for batch specific operations.
type Batch interface {
	// Iterators created on a batch can see some mutations performed after the
	// iterator creation. To guarantee that they see all the mutations, the
	// iterator has to be repositioned using a seek operation, after the
	// mutations were done.
	ReadWriter
	// Commit atomically applies any batched updates to the underlying
	// engine. This is a noop unless the batch was created via NewBatch(). If
	// sync is true, the batch is synchronously committed to disk.
	Commit(sync bool) error
	// CommitNoSyncWait atomically applies any batched updates to the underlying
	// engine and initiates a disk write, but does not wait for that write to
	// complete. The caller must call SyncWait to wait for the fsync to complete.
	// The caller must not Close the Batch without first calling SyncWait.
	CommitNoSyncWait() error
	// SyncWait waits for the disk write initiated by a call to CommitNoSyncWait
	// to complete.
	SyncWait() error
	// Empty returns whether the batch has been written to or not.
	Empty() bool
	// Count returns the number of memtable-modifying operations in the batch.
	Count() uint32
	// Len returns the size of the underlying representation of the batch.
	// Because of the batch header, the size of the batch is never 0 and should
	// not be used interchangeably with Empty. The method avoids the memory copy
	// that Repr imposes, but it still may require flushing the batch's mutations.
	Len() int
	// Repr returns the underlying representation of the batch and can be used to
	// reconstitute the batch on a remote node using Writer.ApplyBatchRepr().
	Repr() []byte
}

// Metrics is a set of Engine metrics. Most are contained in the embedded
// *pebble.Metrics struct, which has its own documentation.
type Metrics struct {
	*pebble.Metrics
	// WriteStallCount counts the number of times Pebble intentionally delayed
	// incoming writes. Currently, the only two reasons for this to happen are:
	// - "memtable count limit reached"
	// - "L0 file count limit exceeded"
	//
	// We do not split this metric across these two reasons, but they can be
	// distinguished in the pebble logs.
	WriteStallCount    int64
	WriteStallDuration time.Duration
	// DiskSlowCount counts the number of times Pebble records disk slowness.
	DiskSlowCount int64
	// DiskStallCount counts the number of times Pebble observes slow writes
	// on disk lasting longer than MaxSyncDuration (`storage.max_sync_duration`).
	DiskStallCount int64
}

// MetricsForInterval is a set of pebble.Metrics that need to be saved in order to
// compute metrics according to an interval.
type MetricsForInterval struct {
	WALFsyncLatency      prometheusgo.Metric
	FlushWriteThroughput pebble.ThroughputMetric
}

// NumSSTables returns the total number of SSTables in the LSM, aggregated
// across levels.
func (m *Metrics) NumSSTables() int64 {
	var num int64
	for _, lm := range m.Metrics.Levels {
		num += lm.NumFiles
	}
	return num
}

// IngestedBytes returns the sum of all ingested tables, aggregated across all
// levels of the LSM.
func (m *Metrics) IngestedBytes() uint64 {
	var ingestedBytes uint64
	for _, lm := range m.Metrics.Levels {
		ingestedBytes += lm.BytesIngested
	}
	return ingestedBytes
}

// CompactedBytes returns the sum of bytes read and written during
// compactions across all levels of the LSM.
func (m *Metrics) CompactedBytes() (read, written uint64) {
	for _, lm := range m.Metrics.Levels {
		read += lm.BytesRead
		written += lm.BytesCompacted
	}
	return read, written
}

// AsStoreStatsEvent converts a Metrics struct into an eventpb.StoreStats event,
// suitable for logging to the telemetry channel.
func (m *Metrics) AsStoreStatsEvent() eventpb.StoreStats {
	e := eventpb.StoreStats{
		CacheSize:                  m.BlockCache.Size,
		CacheCount:                 m.BlockCache.Count,
		CacheHits:                  m.BlockCache.Hits,
		CacheMisses:                m.BlockCache.Misses,
		CompactionCountDefault:     m.Compact.DefaultCount,
		CompactionCountDeleteOnly:  m.Compact.DeleteOnlyCount,
		CompactionCountElisionOnly: m.Compact.ElisionOnlyCount,
		CompactionCountMove:        m.Compact.MoveCount,
		CompactionCountRead:        m.Compact.ReadCount,
		CompactionCountRewrite:     m.Compact.RewriteCount,
		CompactionNumInProgress:    m.Compact.NumInProgress,
		CompactionMarkedFiles:      int64(m.Compact.MarkedFiles),
		FlushCount:                 m.Flush.Count,
		MemtableSize:               m.MemTable.Size,
		MemtableCount:              m.MemTable.Count,
		MemtableZombieCount:        m.MemTable.ZombieCount,
		MemtableZombieSize:         m.MemTable.ZombieSize,
		WalLiveCount:               m.WAL.Files,
		WalLiveSize:                m.WAL.Size,
		WalObsoleteCount:           m.WAL.ObsoleteFiles,
		WalObsoleteSize:            m.WAL.ObsoletePhysicalSize,
		WalPhysicalSize:            m.WAL.PhysicalSize,
		WalBytesIn:                 m.WAL.BytesIn,
		WalBytesWritten:            m.WAL.BytesWritten,
		TableObsoleteCount:         m.Table.ObsoleteCount,
		TableObsoleteSize:          m.Table.ObsoleteSize,
		TableZombieCount:           m.Table.ZombieCount,
		TableZombieSize:            m.Table.ZombieSize,
		RangeKeySetsCount:          m.Keys.RangeKeySetsCount,
	}
	for i, l := range m.Levels {
		if l.NumFiles == 0 {
			continue
		}
		e.Levels = append(e.Levels, eventpb.LevelStats{
			Level:           uint32(i),
			NumFiles:        l.NumFiles,
			SizeBytes:       l.Size,
			Score:           float32(l.Score),
			BytesIn:         l.BytesIn,
			BytesIngested:   l.BytesIngested,
			BytesMoved:      l.BytesMoved,
			BytesRead:       l.BytesRead,
			BytesCompacted:  l.BytesCompacted,
			BytesFlushed:    l.BytesFlushed,
			TablesCompacted: l.TablesCompacted,
			TablesFlushed:   l.TablesFlushed,
			TablesIngested:  l.TablesIngested,
			TablesMoved:     l.TablesMoved,
			NumSublevels:    l.Sublevels,
		})
	}
	return e
}

// EnvStats is a set of RocksDB env stats, including encryption status.
type EnvStats struct {
	// TotalFiles is the total number of files reported by rocksdb.
	TotalFiles uint64
	// TotalBytes is the total size of files reported by rocksdb.
	TotalBytes uint64
	// ActiveKeyFiles is the number of files using the active data key.
	ActiveKeyFiles uint64
	// ActiveKeyBytes is the size of files using the active data key.
	ActiveKeyBytes uint64
	// EncryptionType is an enum describing the active encryption algorithm.
	// See: ccl/storageccl/engineccl/enginepbccl/key_registry.proto
	EncryptionType int32
	// EncryptionStatus is a serialized enginepbccl/stats.proto::EncryptionStatus protobuf.
	EncryptionStatus []byte
}

// EncryptionRegistries contains the encryption-related registries:
// Both are serialized protobufs.
type EncryptionRegistries struct {
	// FileRegistry is the list of files with encryption status.
	// serialized storage/engine/enginepb/file_registry.proto::FileRegistry
	FileRegistry []byte
	// KeyRegistry is the list of keys, scrubbed of actual key data.
	// serialized ccl/storageccl/engineccl/enginepbccl/key_registry.proto::DataKeysRegistry
	KeyRegistry []byte
}

// GetIntent will look up an intent given a key. It there is no intent for a
// key, it will return nil rather than an error. Errors are returned for problem
// at the storage layer, problem decoding the key, problem unmarshalling the
// intent, missing transaction on the intent or multiple intents for this key.
func GetIntent(reader Reader, key roachpb.Key) (*roachpb.Intent, error) {
	// Translate this key from a regular key to one in the lock space so it can be
	// used for queries.
	lbKey, _ := keys.LockTableSingleKey(key, nil)

	iter := reader.NewEngineIterator(IterOptions{Prefix: true, LowerBound: lbKey})
	defer iter.Close()

	valid, err := iter.SeekEngineKeyGE(EngineKey{Key: lbKey})
	if err != nil {
		return nil, err
	}
	if !valid {
		return nil, nil
	}

	engineKey, err := iter.EngineKey()
	if err != nil {
		return nil, err
	}
	checkKey, err := keys.DecodeLockTableSingleKey(engineKey.Key)
	if err != nil {
		return nil, err
	}
	if !checkKey.Equal(key) {
		// This should not be possible, a key and using prefix match means that it
		// must match.
		return nil, errors.AssertionFailedf("key does not match expected %v != %v", checkKey, key)
	}
	var meta enginepb.MVCCMetadata
	v, err := iter.UnsafeValue()
	if err != nil {
		return nil, err
	}
	if err = protoutil.Unmarshal(v, &meta); err != nil {
		return nil, err
	}
	if meta.Txn == nil {
		return nil, errors.AssertionFailedf("txn is null for key %v, intent %v", key, meta)
	}
	intent := roachpb.MakeIntent(meta.Txn, key)

	hasNext, err := iter.NextEngineKey()
	if err != nil {
		// We expect false on the call to next, but not an error.
		return nil, err
	}
	// This should not be possible. There can only be one outstanding write
	// intent for a key and with prefix match we don't find additional names.
	if hasNext {
		engineKey, err := iter.EngineKey()
		if err != nil {
			return nil, err
		}
		return nil, errors.AssertionFailedf("unexpected additional key found %v while looking for %v", engineKey, key)
	}
	return &intent, nil
}

// Scan returns up to max point key/value objects from start (inclusive) to end
// (non-inclusive). Specify max=0 for unbounded scans. Since this code may use
// an intentInterleavingIter, the caller should not attempt a single scan to
// span local and global keys. See the comment in the declaration of
// intentInterleavingIter for details.
//
// NB: This function ignores MVCC range keys. It should only be used for tests.
func Scan(reader Reader, start, end roachpb.Key, max int64) ([]MVCCKeyValue, error) {
	var kvs []MVCCKeyValue
	err := reader.MVCCIterate(start, end, MVCCKeyAndIntentsIterKind, IterKeyTypePointsOnly,
		func(kv MVCCKeyValue, _ MVCCRangeKeyStack) error {
			if max != 0 && int64(len(kvs)) >= max {
				return iterutil.StopIteration()
			}
			kvs = append(kvs, kv)
			return nil
		})
	return kvs, err
}

// ScanIntents scans intents using only the separated intents lock table. It
// does not take interleaved intents into account at all.
func ScanIntents(
	ctx context.Context, reader Reader, start, end roachpb.Key, maxIntents int64, targetBytes int64,
) ([]roachpb.Intent, error) {
	intents := []roachpb.Intent{}

	if bytes.Compare(start, end) >= 0 {
		return intents, nil
	}

	ltStart, _ := keys.LockTableSingleKey(start, nil)
	ltEnd, _ := keys.LockTableSingleKey(end, nil)
	iter := reader.NewEngineIterator(IterOptions{LowerBound: ltStart, UpperBound: ltEnd})
	defer iter.Close()

	var meta enginepb.MVCCMetadata
	var intentBytes int64
	var ok bool
	var err error
	for ok, err = iter.SeekEngineKeyGE(EngineKey{Key: ltStart}); ok; ok, err = iter.NextEngineKey() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if maxIntents != 0 && int64(len(intents)) >= maxIntents {
			break
		}
		if targetBytes != 0 && intentBytes >= targetBytes {
			break
		}
		key, err := iter.EngineKey()
		if err != nil {
			return nil, err
		}
		lockedKey, err := keys.DecodeLockTableSingleKey(key.Key)
		if err != nil {
			return nil, err
		}
		v, err := iter.UnsafeValue()
		if err != nil {
			return nil, err
		}
		if err = protoutil.Unmarshal(v, &meta); err != nil {
			return nil, err
		}
		intents = append(intents, roachpb.MakeIntent(meta.Txn, lockedKey))
		intentBytes += int64(len(lockedKey)) + int64(len(v))
	}
	if err != nil {
		return nil, err
	}
	return intents, nil
}

// WriteSyncNoop carries out a synchronous no-op write to the engine.
func WriteSyncNoop(eng Engine) error {
	batch := eng.NewBatch()
	defer batch.Close()

	if err := batch.LogData(nil); err != nil {
		return err
	}

	if err := batch.Commit(true /* sync */); err != nil {
		return err
	}
	return nil
}

// ClearRangeWithHeuristic clears the keys from start (inclusive) to end
// (exclusive), including any range keys, but does not clear intents unless the
// lock table is targeted explicitly. Depending on the number of keys, it will
// either write a Pebble range tombstone or clear individual keys. If it uses
// a range tombstone, it will tighten the span to the first encountered key.
//
// pointKeyThreshold and rangeKeyThreshold specify the number of point/range
// keys respectively where it will switch from clearing individual keys to
// Pebble range tombstones (RANGEDEL or RANGEKEYDEL respectively). A threshold
// of 0 disables checking for and clearing that key type.
//
// NB: An initial scan will be done to determine the type of clear, so a large
// threshold will potentially involve scanning a large number of keys twice.
//
// TODO(erikgrinaker): Consider tightening the end of the range tombstone span
// too, by doing a SeekLT when we reach the threshold. It's unclear whether it's
// really worth it.
func ClearRangeWithHeuristic(
	r Reader, w Writer, start, end roachpb.Key, pointKeyThreshold, rangeKeyThreshold int,
) error {
	clearPointKeys := func(r Reader, w Writer, start, end roachpb.Key, threshold int) error {
		iter := r.NewEngineIterator(IterOptions{
			KeyTypes:   IterKeyTypePointsOnly,
			LowerBound: start,
			UpperBound: end,
		})
		defer iter.Close()

		// Scan, and drop a RANGEDEL if we reach the threshold. We tighten the span
		// to the first encountered key, since we can cheaply do so.
		var ok bool
		var err error
		var count int
		var firstKey roachpb.Key
		for ok, err = iter.SeekEngineKeyGE(EngineKey{Key: start}); ok; ok, err = iter.NextEngineKey() {
			count++
			if len(firstKey) == 0 {
				key, err := iter.UnsafeEngineKey()
				if err != nil {
					return err
				}
				firstKey = key.Key.Clone()
			}
			if count >= threshold {
				return w.ClearRawRange(firstKey, end, true /* pointKeys */, false /* rangeKeys */)
			}
		}
		if err != nil || count == 0 {
			return err
		}
		// Clear individual points.
		for ok, err = iter.SeekEngineKeyGE(EngineKey{Key: start}); ok; ok, err = iter.NextEngineKey() {
			key, err := iter.UnsafeEngineKey()
			if err != nil {
				return err
			}
			if err = w.ClearEngineKey(key); err != nil {
				return err
			}
		}
		return err
	}

	clearRangeKeys := func(r Reader, w Writer, start, end roachpb.Key, threshold int) error {
		iter := r.NewEngineIterator(IterOptions{
			KeyTypes:   IterKeyTypeRangesOnly,
			LowerBound: start,
			UpperBound: end,
		})
		defer iter.Close()

		// Scan, and drop a RANGEKEYDEL if we reach the threshold.
		var ok bool
		var err error
		var count int
		var firstKey roachpb.Key
		for ok, err = iter.SeekEngineKeyGE(EngineKey{Key: start}); ok; ok, err = iter.NextEngineKey() {
			count += len(iter.EngineRangeKeys())
			if len(firstKey) == 0 {
				bounds, err := iter.EngineRangeBounds()
				if err != nil {
					return err
				}
				firstKey = bounds.Key.Clone()
			}
			if count >= threshold {
				return w.ClearRawRange(firstKey, end, false /* pointKeys */, true /* rangeKeys */)
			}
		}
		if err != nil || count == 0 {
			return err
		}
		// Clear individual range keys.
		for ok, err = iter.SeekEngineKeyGE(EngineKey{Key: start}); ok; ok, err = iter.NextEngineKey() {
			bounds, err := iter.EngineRangeBounds()
			if err != nil {
				return err
			}
			for _, v := range iter.EngineRangeKeys() {
				if err := w.ClearEngineRangeKey(bounds.Key, bounds.EndKey, v.Version); err != nil {
					return err
				}
			}
		}
		return err
	}

	if pointKeyThreshold > 0 {
		if err := clearPointKeys(r, w, start, end, pointKeyThreshold); err != nil {
			return err
		}
	}

	if rangeKeyThreshold > 0 {
		if err := clearRangeKeys(r, w, start, end, rangeKeyThreshold); err != nil {
			return err
		}
	}

	return nil
}

var ingestDelayL0Threshold = settings.RegisterIntSetting(
	settings.TenantWritable,
	"rocksdb.ingest_backpressure.l0_file_count_threshold",
	"number of L0 files after which to backpressure SST ingestions",
	20,
)

var ingestDelayTime = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"rocksdb.ingest_backpressure.max_delay",
	"maximum amount of time to backpressure a single SST ingestion",
	time.Second*5,
)

// PreIngestDelay may choose to block for some duration if L0 has an excessive
// number of files in it or if PendingCompactionBytesEstimate is elevated. This
// it is intended to be called before ingesting a new SST, since we'd rather
// backpressure the bulk operation adding SSTs than slow down the whole RocksDB
// instance and impact all foreground traffic by adding too many files to it.
// After the number of L0 files exceeds the configured limit, it gradually
// begins delaying more for each additional file in L0 over the limit until
// hitting its configured (via settings) maximum delay. If the pending
// compaction limit is exceeded, it waits for the maximum delay.
func preIngestDelay(ctx context.Context, eng Engine, settings *cluster.Settings) {
	if settings == nil {
		return
	}
	metrics := eng.GetMetrics()
	targetDelay := calculatePreIngestDelay(settings, metrics.Metrics)

	if targetDelay == 0 {
		return
	}
	log.VEventf(ctx, 2, "delaying SST ingestion %s. %d L0 files, %d L0 Sublevels",
		targetDelay, metrics.Levels[0].NumFiles, metrics.Levels[0].Sublevels)

	select {
	case <-time.After(targetDelay):
	case <-ctx.Done():
	}
}

func calculatePreIngestDelay(settings *cluster.Settings, metrics *pebble.Metrics) time.Duration {
	maxDelay := ingestDelayTime.Get(&settings.SV)
	l0ReadAmpLimit := ingestDelayL0Threshold.Get(&settings.SV)

	const ramp = 10
	l0ReadAmp := metrics.Levels[0].NumFiles
	if metrics.Levels[0].Sublevels >= 0 {
		l0ReadAmp = int64(metrics.Levels[0].Sublevels)
	}

	if l0ReadAmp > l0ReadAmpLimit {
		delayPerFile := maxDelay / time.Duration(ramp)
		targetDelay := time.Duration(l0ReadAmp-l0ReadAmpLimit) * delayPerFile
		if targetDelay > maxDelay {
			return maxDelay
		}
		return targetDelay
	}
	return 0
}

// Helper function to implement Reader.MVCCIterate().
func iterateOnReader(
	reader Reader,
	start, end roachpb.Key,
	iterKind MVCCIterKind,
	keyTypes IterKeyType,
	f func(MVCCKeyValue, MVCCRangeKeyStack) error,
) error {
	if reader.Closed() {
		return errors.New("cannot call MVCCIterate on a closed batch")
	}
	if start.Compare(end) >= 0 {
		return nil
	}

	it := reader.NewMVCCIterator(iterKind, IterOptions{
		KeyTypes:   keyTypes,
		LowerBound: start,
		UpperBound: end,
	})
	defer it.Close()

	var rangeKeys MVCCRangeKeyStack // cached during iteration
	for it.SeekGE(MakeMVCCMetadataKey(start)); ; it.Next() {
		if ok, err := it.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}

		var kv MVCCKeyValue
		if hasPoint, _ := it.HasPointAndRange(); hasPoint {
			v, err := it.Value()
			if err != nil {
				return err
			}
			kv = MVCCKeyValue{Key: it.Key(), Value: v}
		}
		if !it.RangeBounds().Key.Equal(rangeKeys.Bounds.Key) {
			rangeKeys = it.RangeKeys().Clone()
		}

		if err := f(kv, rangeKeys); err != nil {
			return iterutil.Map(err)
		}
	}
	return nil
}

// assertSimpleMVCCIteratorInvariants asserts invariants in the
// SimpleMVCCIterator interface that should hold for all implementations,
// returning errors.AssertionFailedf for any violations. The iterator
// must be valid.
func assertSimpleMVCCIteratorInvariants(iter SimpleMVCCIterator) error {
	key := iter.UnsafeKey()

	// Keys can't be empty.
	if len(key.Key) == 0 {
		return errors.AssertionFailedf("valid iterator returned empty key")
	}

	// Can't be positioned in the lock table.
	if bytes.HasPrefix(key.Key, keys.LocalRangeLockTablePrefix) {
		return errors.AssertionFailedf("MVCC iterator positioned in lock table at %s", key)
	}

	// Any valid position must have either a point and/or range key.
	hasPoint, hasRange := iter.HasPointAndRange()
	if !hasPoint && !hasRange {
		// NB: MVCCIncrementalIterator can return hasPoint=false,hasRange=false
		// following a NextIgnoringTime() call. We explicitly allow this here.
		if incrIter, ok := iter.(*MVCCIncrementalIterator); !ok || !incrIter.ignoringTime {
			return errors.AssertionFailedf("valid iterator without point/range keys at %s", key)
		}
	}

	// Range key assertions.
	if hasRange {
		// Must have bounds. The MVCCRangeKey.Validate() call below will make
		// further bounds assertions.
		bounds := iter.RangeBounds()
		if len(bounds.Key) == 0 && len(bounds.EndKey) == 0 {
			return errors.AssertionFailedf("hasRange=true but empty range bounds at %s", key)
		}

		// Iterator position must be within range key bounds.
		if !bounds.ContainsKey(key.Key) {
			return errors.AssertionFailedf("iterator position %s outside range bounds %s", key, bounds)
		}

		// Bounds must match range key stack.
		rangeKeys := iter.RangeKeys()
		if !rangeKeys.Bounds.Equal(bounds) {
			return errors.AssertionFailedf("range bounds %s does not match range key %s",
				bounds, rangeKeys.Bounds)
		}

		// Must have range keys.
		if rangeKeys.IsEmpty() {
			return errors.AssertionFailedf("hasRange=true but no range key versions at %s", key)
		}

		for i, v := range rangeKeys.Versions {
			// Range key must be valid.
			rangeKey := rangeKeys.AsRangeKey(v)
			if err := rangeKey.Validate(); err != nil {
				return errors.NewAssertionErrorWithWrappedErrf(err, "invalid range key at %s", key)
			}
			// Range keys must be in descending timestamp order.
			if i > 0 && !v.Timestamp.Less(rangeKeys.Versions[i-1].Timestamp) {
				return errors.AssertionFailedf("range key %s not below version %s",
					rangeKey, rangeKeys.Versions[i-1].Timestamp)
			}
			// Range keys must currently be tombstones.
			if value, err := DecodeMVCCValue(v.Value); err != nil {
				return errors.NewAssertionErrorWithWrappedErrf(err, "invalid range key value at %s",
					rangeKey)
			} else if !value.IsTombstone() {
				return errors.AssertionFailedf("non-tombstone range key %s with value %x",
					rangeKey, value.Value.RawBytes)
			}
		}

	} else {
		// Bounds and range keys must be empty.
		if bounds := iter.RangeBounds(); !bounds.Equal(roachpb.Span{}) {
			return errors.AssertionFailedf("hasRange=false but RangeBounds=%s", bounds)
		}
		if r := iter.RangeKeys(); !r.IsEmpty() || !r.Bounds.Equal(roachpb.Span{}) {
			return errors.AssertionFailedf("hasRange=false but RangeKeys=%s", r)
		}
	}
	if hasPoint {
		value, err := iter.UnsafeValue()
		if err != nil {
			return err
		}
		valueLen := iter.ValueLen()
		if len(value) != valueLen {
			return errors.AssertionFailedf("length of UnsafeValue %d != ValueLen %d", len(value), valueLen)
		}
		if key.IsValue() {
			valueLen2, isTombstone, err := iter.MVCCValueLenAndIsTombstone()
			if err == nil {
				if len(value) != valueLen2 {
					return errors.AssertionFailedf("length of UnsafeValue %d != MVCCValueLenAndIsTombstone %d",
						len(value), valueLen2)
				}
				if v, err := DecodeMVCCValue(value); err == nil {
					if isTombstone != v.IsTombstone() {
						return errors.AssertionFailedf("isTombstone from MVCCValueLenAndIsTombstone %t != MVCCValue.IsTombstone %t",
							isTombstone, v.IsTombstone())
					}
					// Else err != nil. SimpleMVCCIterator is not responsile for data
					// corruption since it is possible that the implementation of
					// MVCCValueLenAndIsTombstone is fetching information from a
					// different part of the store than where the value is stored.
				}
			}
			// Else err != nil. Ignore, since SimpleMVCCIterator is not to be held
			// responsible for data corruption or tests writing non-MVCCValues.
		}
	}

	return nil
}

// assertMVCCIteratorInvariants asserts invariants in the MVCCIterator interface
// that should hold for all implementations, returning errors.AssertionFailedf
// for any violations. It calls through to assertSimpleMVCCIteratorInvariants().
// The iterator must be valid.
func assertMVCCIteratorInvariants(iter MVCCIterator) error {
	// Assert SimpleMVCCIterator invariants.
	if err := assertSimpleMVCCIteratorInvariants(iter); err != nil {
		return err
	}

	key := iter.Key()

	// Key must equal UnsafeKey.
	if u := iter.UnsafeKey(); !key.Equal(u) {
		return errors.AssertionFailedf("Key %s does not match UnsafeKey %s", key, u)
	}

	// UnsafeRawMVCCKey must match Key.
	if r, err := DecodeMVCCKey(iter.UnsafeRawMVCCKey()); err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(
			err, "failed to decode UnsafeRawMVCCKey at %s",
			key,
		)
	} else if !r.Equal(key) {
		return errors.AssertionFailedf("UnsafeRawMVCCKey %s does not match Key %s", r, key)
	}

	// UnsafeRawKey must either be an MVCC key matching Key, or a lock table key
	// that refers to it.
	if engineKey, ok := DecodeEngineKey(iter.UnsafeRawKey()); !ok {
		return errors.AssertionFailedf("failed to decode UnsafeRawKey as engine key at %s", key)
	} else if engineKey.IsMVCCKey() {
		if k, err := engineKey.ToMVCCKey(); err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err, "invalid UnsafeRawKey at %s", key)
		} else if !k.Equal(key) {
			return errors.AssertionFailedf("UnsafeRawKey %s does not match Key %s", k, key)
		}
	} else if engineKey.IsLockTableKey() {
		if k, err := engineKey.ToLockTableKey(); err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err, "invalid UnsafeRawKey at %s", key)
		} else if !k.Key.Equal(key.Key) {
			return errors.AssertionFailedf("UnsafeRawKey lock table key %s does not match Key %s", k, key)
		} else if !key.Timestamp.IsEmpty() {
			return errors.AssertionFailedf(
				"UnsafeRawKey lock table key %s for Key %s with non-zero timestamp", k, key,
			)
		}
	} else {
		return errors.AssertionFailedf("unknown type for engine key %s", engineKey)
	}

	// Value must equal UnsafeValue.
	u, err := iter.UnsafeValue()
	if err != nil {
		return err
	}
	v, err := iter.Value()
	if err != nil {
		return err
	}
	if !bytes.Equal(v, u) {
		return errors.AssertionFailedf("Value %x does not match UnsafeValue %x at %s", v, u, key)
	}

	// For prefix iterators, any range keys must be point-sized. We've already
	// asserted that the range key covers the iterator position.
	if iter.IsPrefix() {
		if _, hasRange := iter.HasPointAndRange(); hasRange {
			if bounds := iter.RangeBounds(); !bounds.EndKey.Equal(bounds.Key.Next()) {
				return errors.AssertionFailedf("prefix iterator with wide range key %s", bounds)
			}
		}
	}

	return nil
}

// ScanConflictingIntentsForDroppingLatchesEarly scans intents using only the
// separated intents lock table on behalf of a batch request trying to drop its
// latches early. If found, conflicting intents are added to the supplied
// `intents` slice, which indicates to the caller that evaluation should not
// proceed until the intents are resolved. Intents that don't conflict with the
// transaction referenced by txnID[1] at the supplied `ts` are ignored.
//
// The caller must supply the sequence number of the request on behalf of which
// the intents are being scanned. This is used to determine if the caller needs
// to consult intent history when performing a scan over the MVCC keyspace
// (indicated by the `needIntentHistory` return parameter). Intent history is
// required to read the correct provisional value when scanning if we encounter
// an intent written by the `txn` at a higher sequence number than the one
// supplied or at a higher timestamp than the `ts` supplied (regardless of the
// sequence number of the intent).
//
// [1] The supplied txnID may be empty (uuid.Nil) if the request on behalf of
// which the scan is being performed is non-transactional.
func ScanConflictingIntentsForDroppingLatchesEarly(
	ctx context.Context,
	reader Reader,
	txnID uuid.UUID,
	ts hlc.Timestamp,
	start, end roachpb.Key,
	seq enginepb.TxnSeq,
	intents *[]roachpb.Intent,
	maxIntents int64,
) (needIntentHistory bool, err error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	upperBoundUnset := len(end) == 0 // NB: Get requests do not set the end key.
	if !upperBoundUnset && bytes.Compare(start, end) >= 0 {
		return true, errors.AssertionFailedf("start key must be less than end key")
	}
	ltStart, _ := keys.LockTableSingleKey(start, nil)
	opts := IterOptions{LowerBound: ltStart}
	if upperBoundUnset {
		opts.Prefix = true
	} else {
		ltEnd, _ := keys.LockTableSingleKey(end, nil)
		opts.UpperBound = ltEnd
	}
	iter := reader.NewEngineIterator(opts)
	defer iter.Close()

	var meta enginepb.MVCCMetadata
	var ok bool
	for ok, err = iter.SeekEngineKeyGE(EngineKey{Key: ltStart}); ok; ok, err = iter.NextEngineKey() {
		if maxIntents != 0 && int64(len(*intents)) >= maxIntents {
			// Return early if we're done accumulating intents; make no claims about
			// not needing intent history.
			return true /* needsIntentHistory */, nil
		}
		v, err := iter.UnsafeValue()
		if err != nil {
			return false, err
		}
		if err = protoutil.Unmarshal(v, &meta); err != nil {
			return false, err
		}
		if meta.Txn == nil {
			return false, errors.Errorf("intent without transaction")
		}
		ownIntent := txnID != uuid.Nil && txnID == meta.Txn.ID
		if ownIntent {
			// If we ran into one of our own intents, check whether the intent has a
			// higher (or equal) sequence number or a higher (or equal) timestamp. If
			// either of these conditions is true, a corresponding scan over the MVCC
			// key space will need access to the key's intent history in order to read
			// the correct provisional value. So we set `needIntentHistory` to true.
			if seq <= meta.Txn.Sequence || ts.LessEq(meta.Timestamp.ToTimestamp()) {
				needIntentHistory = true
			}
			continue
		}
		if conflictingIntent := meta.Timestamp.ToTimestamp().LessEq(ts); !conflictingIntent {
			continue
		}
		key, err := iter.EngineKey()
		if err != nil {
			return false, err
		}
		lockedKey, err := keys.DecodeLockTableSingleKey(key.Key)
		if err != nil {
			return false, err
		}
		*intents = append(*intents, roachpb.MakeIntent(meta.Txn, lockedKey))
	}
	if err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return needIntentHistory, nil /* err */
}
