// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"bytes"
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/storage/pebbleiter"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/rangekey"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/redact"
	prometheusgo "github.com/prometheus/client_model/go"
)

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
	// UnsafeLazyValue is only for use inside the storage package. It exposes
	// the LazyValue at the current iterator position, and hence delays fetching
	// the actual value.
	// REQUIRES: latest positioning function returned valid=true.
	UnsafeLazyValue() pebble.LazyValue
	// Value returns the current value as a byte slice.
	// REQUIRES: latest positioning function returned valid=true.
	Value() ([]byte, error)
	// ValueLen returns the length of the current value. ValueLen should be
	// preferred when the actual value is not needed. In some circumstances, the
	// storage engine may be able to avoid loading the value.
	// REQUIRES: latest positioning function returned valid=true.
	ValueLen() int
	// CloneContext is a low-level method only for use in the storage package,
	// that provides sufficient context that the iterator may be cloned.
	CloneContext() CloneContext
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

// CloneContext is an opaque type encapsulating sufficient context to construct
// a clone of an existing iterator.
type CloneContext struct {
	rawIter pebbleiter.Iterator
	engine  *Pebble
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
	// MinTimestamp and MaxTimestamp, if set, indicate that only keys
	// within the time range formed by [MinTimestamp, MaxTimestamp] should be
	// returned. The underlying iterator may be able to efficiently skip over
	// keys outside of the hinted time range, e.g., when a block handle
	// indicates that the block contains no keys within the time range. Intents
	// will not be visible to such iterators at all. This is only relevant for
	// MVCCIterators.
	//
	// Note that time-bound iterators previously were only a performance
	// optimization but now guarantee that no keys outside of the [start, end]
	// time range will be returned.
	//
	// NB: Range keys are not currently subject to timestamp filtering due to
	// complications with MVCCIncrementalIterator. See:
	// https://github.com/cockroachdb/cockroach/issues/86260
	MinTimestamp, MaxTimestamp hlc.Timestamp
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
	// ReadCategory is used to map to a user-understandable category string, for
	// stats aggregation and metrics, and a Pebble-understandable QoS.
	ReadCategory fs.ReadCategory
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
	MVCCIterate(
		ctx context.Context, start, end roachpb.Key, iterKind MVCCIterKind, keyTypes IterKeyType,
		readCategory fs.ReadCategory, f func(MVCCKeyValue, MVCCRangeKeyStack) error,
	) error
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
	NewMVCCIterator(
		ctx context.Context, iterKind MVCCIterKind, opts IterOptions) (MVCCIterator, error)
	// NewEngineIterator returns a new instance of an EngineIterator over this
	// engine. The caller must invoke EngineIterator.Close() when finished
	// with the iterator to free resources. The caller can change IterOptions
	// after this function returns.
	NewEngineIterator(ctx context.Context, opts IterOptions) (EngineIterator, error)
	// ScanInternal allows a caller to inspect the underlying engine's InternalKeys
	// using a visitor pattern, while also allowing for keys in shared files to be
	// skipped if a visitor is provided for visitSharedFiles. Useful for
	// fast-replicating state from one Reader to another. Point keys are collapsed
	// such that only one internal key per user key is exposed, and rangedels and
	// range keys are collapsed and defragmented with each span being surfaced
	// exactly once, alongside the highest seqnum for a rangedel on that span
	// (for rangedels) or all coalesced rangekey.Keys in that span (for range
	// keys). A point key deleted by a rangedel will not be exposed, but the
	// rangedel would be exposed.
	//
	// Note that ScanInternal does not obey the guarantees indicated by
	// ConsistentIterators.
	ScanInternal(
		ctx context.Context, lower, upper roachpb.Key,
		visitPointKey func(key *pebble.InternalKey, value pebble.LazyValue, info pebble.IteratorLevel) error,
		visitRangeDel func(start, end []byte, seqNum pebble.SeqNum) error,
		visitRangeKey func(start, end []byte, keys []rangekey.Key) error,
		visitSharedFile func(sst *pebble.SharedSSTMeta) error,
		visitExternalFile func(sst *pebble.ExternalFile) error,
	) error
	// ConsistentIterators returns true if the Reader implementation guarantees
	// that the different iterators constructed by this Reader will see the same
	// underlying Engine state. This is not true about Batch writes: new iterators
	// will see new writes made to the batch, existing iterators won't.
	ConsistentIterators() bool

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
	PinEngineStateForIterators(readCategory fs.ReadCategory) error
}

// EventuallyFileOnlyReader is a specialized Reader that supports a method to
// wait on a transition to being a file-only reader that does not pin any
// keys in-memory.
type EventuallyFileOnlyReader interface {
	Reader
	// WaitForFileOnly blocks the calling goroutine until this reader has
	// transitioned to a file-only reader that does not pin any in-memory state.
	// If an error is returned, this transition did not succeed. The Duration
	// argument specifies how long to wait for before attempting a flush to
	// force a transition to a file-only snapshot.
	WaitForFileOnly(ctx context.Context, gracePeriodBeforeFlush time.Duration) error
}

// Writer is the write interface to an engine's data.
type Writer interface {
	// ApplyBatchRepr atomically applies a set of batched updates. Created by
	// calling Repr() on a batch. Using this method is equivalent to constructing
	// and committing a batch whose Repr() equals repr. If sync is true, the batch
	// is synchronously flushed to the OS and written to disk. It is an error to
	// specify sync=true if the Writer is a Batch.
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
	// If the caller knows the size of the value that is being cleared, they
	// should set ClearOptions.{ValueSizeKnown, ValueSize} accordingly to
	// improve the storage engine's ability to prioritize compactions.
	//
	// It is safe to modify the contents of the arguments after it returns.
	ClearMVCC(key MVCCKey, opts ClearOptions) error
	// ClearUnversioned removes an unversioned item from the db. It is for use
	// with inline metadata (not intents) and other unversioned keys (like
	// Range-ID local keys). It does not affect range keys.
	//
	// If the caller knows the size of the value that is being cleared, they
	// should set ClearOptions.{ValueSizeKnown, ValueSize} accordingly to
	// improve the storage engine's ability to prioritize compactions.
	//
	// It is safe to modify the contents of the arguments after it returns.
	ClearUnversioned(key roachpb.Key, opts ClearOptions) error
	// ClearEngineKey removes the given point key from the engine. It does not
	// affect range keys.  Note that clear actually removes entries from the
	// storage engine. This is a general-purpose and low-level method that should
	// be used sparingly, only when the other Clear* methods are not applicable.
	//
	// If the caller knows the size of the value that is being cleared, they
	// should set ClearOptions.{ValueSizeKnown, ValueSize} accordingly to
	// improve the storage engine's ability to prioritize compactions.
	//
	// It is safe to modify the contents of the arguments after it returns.
	ClearEngineKey(key EngineKey, opts ClearOptions) error

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

	// SingleClearEngineKey removes the most recent write to the item from the
	// db with the given key, using Pebble's SINGLEDEL operation. This
	// originally resembled the semantics of RocksDB
	// (https://github.com/facebook/rocksdb/wiki/Single-Delete), but was
	// strengthened in Pebble such that sequences (from more recent to older)
	// like SINGLEDEL#20, SET#17, DEL#15, ... work as intended since there has
	// been only one SET more recent than the last DEL. These also work if the
	// DEL is replaced by a RANGEDEL, since RANGEDELs are used extensively to
	// drop all the data for a replica, which may then be recreated in the
	// future. The behavior is non-deterministic and definitely not what the
	// caller wants if there are multiple SETs/MERGEs etc. immediately older
	// than the SINGLEDEL.
	//
	// Note that using SINGLEDEL requires the caller to not duplicate SETs
	// without knowing about it. That is, the caller cannot rely simply on
	// idempotent writes for correctness, if they are going to be later deleted
	// using SINGLEDEL. A current case where duplication without knowledge can
	// happen is sstable ingestion for "global" keys, say during import and
	// schema change. SSTable ingestion via the KV-layer's AddSSTable changes
	// the replicated state machine, but does not atomically update the
	// RangeAppliedState.RaftAppliedIndex, so on a node crash the SSTable
	// ingestion will be repeated due to replaying the Raft log. Hence,
	// SingleClearEngineKey must not be used for global keys e.g. do not
	// consider using it for MVCC GC.
	//
	// This operation actually removes entries from the storage engine, rather
	// than inserting MVCC tombstones. This is a low-level interface that must
	// not be called from outside the storage package. It is part of the
	// interface because there are structs that wrap Writer and implement the
	// Writer interface, that are not part of the storage package.
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

// InternalWriter is an extension of Writer that supports additional low-level
// methods to operate on internal keys in Pebble. These additional methods
// should only be used sparingly, when one of the high-level methods cannot
// achieve the same ends.
type InternalWriter interface {
	Writer
	// ClearRawEncodedRange is similar to ClearRawRange, except it takes pre-encoded
	// start, end keys and bypasses the EngineKey encoding step. It also only
	// operates on point keys; for range keys, use ClearEngineRangeKey or
	// PutInternalRangeKey.
	//
	// It is safe to modify the contents of the arguments after it returns.
	ClearRawEncodedRange(start, end []byte) error

	// PutInternalRangeKey adds an InternalRangeKey to this batch. This is a very
	// low-level method that should be used sparingly.
	//
	// It is safe to modify the contents of the arguments after it returns.
	PutInternalRangeKey(start, end []byte, key rangekey.Key) error
	// PutInternalPointKey adds a point InternalKey to this batch. This is a very
	// low-level method that should be used sparingly.
	//
	// It is safe to modify the contents of the arguments after it returns.
	PutInternalPointKey(key *pebble.InternalKey, value []byte) error
}

// ClearOptions holds optional parameters to methods that clear keys from the
// storage engine.
type ClearOptions struct {
	// ValueSizeKnown indicates whether the ValueSize carries a meaningful
	// value. If false, ValueSize is ignored.
	ValueSizeKnown bool
	// ValueSize may be provided to indicate the size of the existing KV
	// record's value that is being removed. ValueSize should be the encoded
	// value size that the storage engine observes. If the value is a
	// MVCCMetadata, ValueSize should be the length of the encoded MVCCMetadata.
	// If the value is a MVCCValue, ValueSize should be the length of the
	// encoded MVCCValue.
	//
	// Setting ValueSize and ValueSizeKnown improves the storage engine's
	// ability to estimate space amplification and prioritize compactions.
	// Without it, compaction heuristics rely on average value sizes which are
	// susceptible to over and under estimation.
	//
	// If the true value size is unknown, leave ValueSizeKnown false.
	// Correctness is not compromised if ValueSize is incorrect; the underlying
	// key will always be cleared regardless of whether its value size matches
	// the provided value.
	ValueSize uint32
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
	Reader
	Writer
	// Attrs returns the engine/store attributes.
	Attrs() roachpb.Attributes
	// Capacity returns capacity details for the engine's available storage.
	Capacity() (roachpb.StoreCapacity, error)
	// Properties returns the low-level properties for the engine's underlying storage.
	Properties() roachpb.StoreProperties
	// Compact forces compaction over the entire database.
	Compact() error
	// Env returns the filesystem environment used by the Engine.
	Env() *fs.Env
	// Flush causes the engine to write all in-memory data to disk
	// immediately.
	Flush() error
	// GetMetrics retrieves metrics from the engine.
	GetMetrics() Metrics
	// GetEncryptionRegistries returns the file and key registries when encryption is enabled
	// on the store.
	GetEncryptionRegistries() (*fs.EncryptionRegistries, error)
	// GetEnvStats retrieves stats about the engine's environment
	// For RocksDB, this includes details of at-rest encryption.
	GetEnvStats() (*fs.EnvStats, error)
	// GetAuxiliaryDir returns a path under which files can be stored
	// persistently, and from which data can be ingested by the engine.
	//
	// Not thread safe.
	GetAuxiliaryDir() string
	// NewBatch returns a new instance of a batched engine which wraps
	// this engine. Batched engines accumulate all mutations and apply
	// them atomically on a call to Commit().
	NewBatch() Batch
	// NewReader returns a new instance of a Reader that wraps this engine, and
	// with the given durability requirement. This wrapper caches iterators to
	// avoid the overhead of creating multiple iterators for batched reads.
	//
	// All iterators created from a read-only engine are guaranteed to provide a
	// consistent snapshot of the underlying engine. See the comment on the
	// Reader interface and the Reader.ConsistentIterators method.
	NewReader(durability DurabilityRequirement) Reader
	// NewReadOnly returns a new instance of a ReadWriter that wraps this
	// engine, and with the given durability requirement. This wrapper panics
	// when unexpected operations (e.g., write operations) are executed on it
	// and caches iterators to avoid the overhead of creating multiple iterators
	// for batched reads.
	//
	// All iterators created from a read-only engine are guaranteed to provide a
	// consistent snapshot of the underlying engine. See the comment on the
	// Reader interface and the Reader.ConsistentIterators method.
	//
	// TODO(sumeer,jackson): Remove this method and force the caller to operate
	// explicitly with a separate WriteBatch and Reader.
	NewReadOnly(durability DurabilityRequirement) ReadWriter
	// NewUnindexedBatch returns a new instance of a batched engine which wraps
	// this engine. It is unindexed, in that writes to the batch are not
	// visible to reads until after it commits. The batch accumulates all
	// mutations and applies them atomically on a call to Commit().
	//
	// Reads will be satisfied by reading from the underlying engine, i.e., the
	// caller does not see its own writes. This setting should be used only when
	// the caller is certain that this optimization is correct, and beneficial.
	// There are subtleties here -- see the discussion on
	// https://github.com/cockroachdb/cockroach/pull/57661 for more details.
	//
	// TODO(sumeer,jackson): Remove this method and force the caller to operate
	// explicitly with a separate WriteBatch and Reader.
	NewUnindexedBatch() Batch
	// NewWriteBatch returns a new write batch that will commit to the
	// underlying Engine. The batch accumulates all mutations and applies them
	// atomically on a call to Commit().
	NewWriteBatch() WriteBatch
	// NewSnapshot returns a new instance of a read-only snapshot engine. A
	// snapshot provides a consistent view of the database across multiple
	// iterators. If a caller only needs a single consistent iterator, they
	// should create an iterator directly off the engine instead.
	//
	// Acquiring a snapshot is instantaneous and is inexpensive if quickly
	// released. Snapshots are released by invoking Close(). Open snapshots
	// prevent compactions from reclaiming space or removing tombstones for any
	// keys written after the snapshot is acquired. This can be problematic
	// during rebalancing or large ingestions, so they should be used sparingly
	// and briefly.
	//
	// Note that snapshots must not be used after the original engine has been
	// stopped.
	NewSnapshot() Reader
	// NewEventuallyFileOnlySnapshot returns a new instance of a read-only
	// eventually file-only snapshot. This type of snapshot incurs lower write-amp
	// than a regular Snapshot opened with NewSnapshot, however it incurs a greater
	// space-amp on disk for the duration of this snapshot's lifetime. There's
	// also a chance that its conversion to a file-only snapshot could get
	// errored out if an excise operation were to conflict with one of the passed
	// in KeyRanges. Note that if no keyRanges are passed in, a file-only snapshot
	// is created from the start; this is usually not desirable as it makes no
	// deterministic guarantees about what will be readable (anything in memtables
	// will not be visible). Snapshot guarantees are only provided for keys
	// in the passed-in keyRanges; reads are not guaranteed to be consistent
	// outside of these bounds.
	NewEventuallyFileOnlySnapshot(keyRanges []roachpb.Span) EventuallyFileOnlyReader
	// IngestLocalFiles atomically links a slice of files into the RocksDB
	// log-structured merge-tree.
	IngestLocalFiles(ctx context.Context, paths []string) error
	// IngestLocalFilesWithStats is a variant of IngestLocalFiles that
	// additionally returns ingestion stats.
	IngestLocalFilesWithStats(
		ctx context.Context, paths []string) (pebble.IngestOperationStats, error)
	// IngestAndExciseFiles is a variant of IngestLocalFilesWithStats that excises
	// an ExciseSpan, and ingests either local or shared sstables or both. It also
	// takes the flag sstsContainExciseTombstone to signal that the exciseSpan
	// contains RANGEDELs and RANGEKEYDELs.
	//
	// NB: It is the caller's responsibility to ensure if
	// sstsContainExciseTombstone is set to true, the ingestion sstables must
	// contain a tombstone for the exciseSpan.
	IngestAndExciseFiles(
		ctx context.Context,
		paths []string,
		shared []pebble.SharedSSTMeta,
		external []pebble.ExternalFile,
		exciseSpan roachpb.Span,
		sstsContainExciseTombstone bool,
	) (pebble.IngestOperationStats, error)
	// IngestExternalFiles is a variant of IngestLocalFiles that takes external
	// files. These files can be referred to by multiple stores, but are not
	// modified or deleted by the Engine doing the ingestion.
	IngestExternalFiles(ctx context.Context, external []pebble.ExternalFile) (pebble.IngestOperationStats, error)

	// PreIngestDelay offers an engine the chance to backpressure ingestions.
	// When called, it may choose to block if the engine determines that it is in
	// or approaching a state where further ingestions may risk its health.
	PreIngestDelay(ctx context.Context)
	// ApproximateDiskBytes returns an approximation of the on-disk size and file
	// counts for the given key span, along with how many of those bytes are on
	// remote, as well as specifically external remote, storage.
	ApproximateDiskBytes(from, to roachpb.Key) (total, remote, external uint64, _ error)
	// ConvertFilesToBatchAndCommit converts local files with the given paths to
	// a WriteBatch and commits the batch with sync=true. The files represented
	// in paths must not be overlapping -- this is the same contract as
	// IngestLocalFiles*. Additionally, clearedSpans represents the spans which
	// must be deleted before writing the data contained in these paths.
	//
	// This method is expected to be used instead of IngestLocalFiles* or
	// IngestAndExciseFiles when the sum of the file sizes is small.
	//
	// TODO(sumeer): support this as an alternative to IngestAndExciseFiles.
	// This should be easy since we use NewSSTEngineIterator to read the ssts,
	// which supports multiple levels.
	ConvertFilesToBatchAndCommit(
		ctx context.Context, paths []string, clearedSpans []roachpb.Span,
	) error
	// CompactRange ensures that the specified range of key value pairs is
	// optimized for space efficiency.
	CompactRange(start, end roachpb.Key) error
	// ScanStorageInternalKeys returns key level statistics for each level of a pebble store (that overlap start and end).
	ScanStorageInternalKeys(start, end roachpb.Key, megabytesPerSecond int64) ([]enginepb.StorageInternalKeysMetrics, error)
	// GetTableMetrics returns information about sstables that overlap start and end.
	GetTableMetrics(start, end roachpb.Key) ([]enginepb.SSTableMetricsInfo, error)
	// RegisterFlushCompletedCallback registers a callback that will be run for
	// every successful flush. Only one callback can be registered at a time, so
	// registering again replaces the previous callback. The callback must
	// return quickly and must not call any methods on the Engine in the context
	// of the callback since it could cause a deadlock (since the callback may
	// be invoked while holding mutexes).
	RegisterFlushCompletedCallback(cb func())
	// CreateCheckpoint creates a checkpoint of the engine in the given directory,
	// which must not exist. The directory should be on the same file system so
	// that hard links can be used. If spans is not empty, the checkpoint excludes
	// SSTs that don't overlap with any of these key spans.
	CreateCheckpoint(dir string, spans []roachpb.Span) error

	// MinVersion is the minimum CockroachDB version that is compatible with this
	// store. For newly created stores, this matches the currently active cluster
	// version.
	// Must never return an empty version.
	MinVersion() roachpb.Version

	// SetMinVersion is used to signal to the engine the current minimum
	// version that it must maintain compatibility with.
	SetMinVersion(version roachpb.Version) error

	// SetCompactionConcurrency is used to set the engine's compaction
	// concurrency. It returns the previous compaction concurrency.
	SetCompactionConcurrency(n uint64) uint64

	// AdjustCompactionConcurrency adjusts the compaction concurrency up or down by
	// the passed delta, down to a minimum of 1.
	AdjustCompactionConcurrency(delta int64) uint64

	// SetStoreID informs the engine of the store ID, once it is known.
	// Used to show the store ID in logs and to initialize the shared object
	// creator ID (if shared object storage is configured).
	SetStoreID(ctx context.Context, storeID int32) error

	// GetStoreID is used to retrieve the configured store ID.
	GetStoreID() (int32, error)

	// Download informs the engine to download remote files corresponding to the
	// given span. The parameter copy controls how it is downloaded -- i.e. if it
	// just copies the backing bytes to a local file of if it rewrites the file
	// key-by-key to a new file.
	Download(ctx context.Context, span roachpb.Span, copy bool) error

	// RegisterDiskSlowCallback registers a callback that will be run when a
	// write operation on the disk has been seen to be slow. This callback
	// needs to be thread-safe as it could be called repeatedly in multiple threads
	// over a short period of time.
	RegisterDiskSlowCallback(cb func(info pebble.DiskSlowInfo))

	// RegisterLowDiskSpaceCallback registers a callback that will be run when a
	// disk is running out of space. This callback needs to be thread-safe as it
	// could be called repeatedly in multiple threads over a short period of time.
	RegisterLowDiskSpaceCallback(cb func(info pebble.LowDiskSpaceInfo))

	// GetPebbleOptions returns the options used when creating the engine. The
	// caller must not modify these.
	GetPebbleOptions() *pebble.Options
}

// Batch is the interface for batch specific operations.
type Batch interface {
	// Iterators created on a batch can see some mutations performed after the
	// iterator creation. To guarantee that they see all the mutations, the
	// iterator has to be repositioned using a seek operation, after the
	// mutations were done.
	Reader
	WriteBatch
	// NewBatchOnlyMVCCIterator returns a new instance of MVCCIterator that only
	// sees the mutations in the batch (not the engine). It does not interleave
	// intents, i.e., it is of kind MVCCKeyIterKind.
	//
	// REQUIRES: the batch is indexed.
	NewBatchOnlyMVCCIterator(ctx context.Context, opts IterOptions) (MVCCIterator, error)
}

// WriteBatch is the interface for write batch specific operations.
type WriteBatch interface {
	InternalWriter
	// Close closes the batch, freeing up any outstanding resources.
	Close()
	// Commit atomically applies any batched updates to the underlying engine. If
	// sync is true, the batch is synchronously flushed to the OS and committed to
	// disk. Otherwise, this call returns before the data is even flushed to the
	// OS, and it may be lost if the process terminates.
	//
	// This is a noop unless the batch was created via NewBatch().
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
	// CommitStats returns stats related to committing the batch. Should be
	// called after Batch.Commit. If CommitNoSyncWait is used, it should be
	// called after the call to SyncWait.
	CommitStats() BatchCommitStats
}

type BatchCommitStats struct {
	pebble.BatchCommitStats
}

// SafeFormat implements redact.SafeFormatter. It does not print the total
// duration.
func (stats BatchCommitStats) SafeFormat(p redact.SafePrinter, _ rune) {
	p.Printf("commit-wait %s", stats.CommitWaitDuration)
	if stats.WALQueueWaitDuration > 0 {
		p.Printf(" wal-q %s", stats.WALQueueWaitDuration)
	}
	if stats.MemTableWriteStallDuration > 0 {
		p.Printf(" mem-stall %s", stats.MemTableWriteStallDuration)
	}
	if stats.L0ReadAmpWriteStallDuration > 0 {
		p.Printf(" l0-stall %s", stats.L0ReadAmpWriteStallDuration)
	}
	if stats.WALRotationDuration > 0 {
		p.Printf(" wal-rot %s", stats.WALRotationDuration)
	}
	if stats.SemaphoreWaitDuration > 0 {
		p.Printf(" sem %s", stats.SemaphoreWaitDuration)
	}
}

// Metrics is a set of Engine metrics. Most are contained in the embedded
// *pebble.Metrics struct, which has its own documentation.
type Metrics struct {
	*pebble.Metrics
	Iterator         AggregatedIteratorStats
	BatchCommitStats AggregatedBatchCommitStats
	// DiskSlowCount counts the number of times Pebble records disk slowness.
	DiskSlowCount int64
	// DiskStallCount counts the number of times Pebble observes slow writes
	// on disk lasting longer than MaxSyncDuration (`storage.max_sync_duration`).
	DiskStallCount int64
	// SingleDelInvariantViolationCount counts the number of times a
	// SingleDelete was found to violate the invariant that it should only be
	// used when there is at most one older Set for it to delete.
	//
	// TODO(sumeer): remove, since can fire due to delete-only compactions.
	SingleDelInvariantViolationCount int64
	// SingleDelIneffectualCount counts the number of times a SingleDelete was
	// ineffectual, i.e., it was elided without deleting anything.
	//
	// TODO(sumeer): remove, since can fire due to delete-only compactions.
	SingleDelIneffectualCount int64
	// SharedStorageWriteBytes counts the number of bytes written to shared storage.
	SharedStorageWriteBytes int64
	// SharedStorageReadBytes counts the number of bytes read from shared storage.
	SharedStorageReadBytes int64
	// WriteStallCount counts the number of times Pebble intentionally delayed
	// incoming writes. Currently, the only two reasons for this to happen are:
	// - "memtable count limit reached"
	// - "L0 file count limit exceeded"
	//
	// We do not split this metric across these two reasons, but they can be
	// distinguished in the pebble logs.
	WriteStallCount    int64
	WriteStallDuration time.Duration

	// BlockLoadConcurrencyLimit is the current limit on the number of concurrent
	// sstable block reads.
	BlockLoadConcurrencyLimit int64
	// BlockLoadsInProgress is the (instantaneous) number of sstable blocks that
	// are being read from disk.
	BlockLoadsInProgress int64
	// BlockLoadsQueued is the cumulative total number of sstable block reads
	// that had to wait on the BlockLoadConcurrencyLimit.
	BlockLoadsQueued int64

	DiskWriteStats []vfs.DiskWriteStatsAggregate
}

// AggregatedIteratorStats holds cumulative stats, collected and summed over all
// of an engine's iterators.
type AggregatedIteratorStats struct {
	// BlockBytes holds the sum of sizes of all loaded blocks. If the block was
	// compressed, this is the compressed bytes. This value includes blocks that
	// were loaded from the cache, and bytes that needed to be read from
	// persistent storage.
	//
	// Currently, there may be some gaps in coverage. (At the time of writing,
	// 2nd-level index blocks are excluded.)
	BlockBytes uint64
	// BlockBytesInCache holds the subset of BlockBytes that were already in the
	// block cache, requiring no I/O.
	BlockBytesInCache uint64
	// BlockReadDuration accumulates the duration spent fetching blocks due to
	// block cache misses.
	//
	// Currently, there may be some gaps in coverage. (At the time of writing,
	// range deletion and range key blocks, meta index blocks and properties
	// blocks are all excluded.)
	BlockReadDuration time.Duration
	// ExternalSeeks is the total count of seeks in forward and backward
	// directions performed on pebble.Iterators.
	ExternalSeeks int
	// ExternalSteps is the total count of relative positioning operations (eg,
	// Nexts, Prevs, NextPrefix, NextWithLimit, etc) in forward and backward
	// directions performed on pebble.Iterators.
	ExternalSteps int
	// InternalSeeks is the total count of steps in forward and backward
	// directions performed on Pebble's internal iterator. If this is high
	// relative to ExternalSeeks, it's a good indication that there's an
	// accumulation of garbage within the LSM (NOT MVCC garbage).
	InternalSeeks int
	// InternalSteps is the total count of relative positioning operations (eg,
	// Nexts, Prevs, NextPrefix, etc) in forward and backward directions
	// performed on pebble's internal iterator. If this is high relative to
	// ExternalSteps, it's a good indication that there's an accumulation of
	// garbage within the LSM (NOT MVCC garbage).
	InternalSteps int
}

// AggregatedBatchCommitStats hold cumulative stats summed over all the
// batches that committed at the engine. Since these are durations, only the
// mean (over an interval) can be recovered. We can change some of these to
// histograms once we know which ones are more useful.
type AggregatedBatchCommitStats struct {
	Count uint64
	BatchCommitStats
}

// MetricsForInterval is a set of pebble.Metrics that need to be saved in order to
// compute metrics according to an interval. The metrics recorded here are
// cumulative values, that are used to subtract from, when the next cumulative
// values are received.
type MetricsForInterval struct {
	WALFsyncLatency                prometheusgo.Metric
	FlushWriteThroughput           pebble.ThroughputMetric
	WALFailoverWriteAndSyncLatency prometheusgo.Metric
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
		FlushIngestCount:           m.Flush.AsIngestCount,
		FlushIngestTableCount:      m.Flush.AsIngestTableCount,
		FlushIngestTableBytes:      m.Flush.AsIngestBytes,
		IngestCount:                m.Ingest.Count,
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

// GetIntent will look up an intent given a key. It there is no intent for a
// key, it will return nil rather than an error. Errors are returned for problem
// at the storage layer, problem decoding the key, problem unmarshalling the
// intent, missing transaction on the intent, or multiple intents for this key.
func GetIntent(ctx context.Context, reader Reader, key roachpb.Key) (*roachpb.Intent, error) {
	// Probe the lock table at key using a lock-table iterator.
	opts := LockTableIteratorOptions{
		Prefix: true,
		// Ignore Exclusive and Shared locks. We only care about intents.
		MatchMinStr: lock.Intent,
	}
	iter, err := NewLockTableIterator(ctx, reader, opts)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	seekKey, _ := keys.LockTableSingleKey(key, nil)
	valid, err := iter.SeekEngineKeyGE(EngineKey{Key: seekKey})
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
	ltKey, err := engineKey.ToLockTableKey()
	if err != nil {
		return nil, err
	}
	if !ltKey.Key.Equal(key) {
		// This should not be possible, a key and using prefix match means that it
		// must match.
		return nil, errors.AssertionFailedf("key does not match expected %v != %v", ltKey.Key, key)
	}
	if ltKey.Strength != lock.Intent {
		return nil, errors.AssertionFailedf("unexpected strength for LockTableKey %s: %v", ltKey.Strength, ltKey)
	}
	var meta enginepb.MVCCMetadata
	if err = iter.ValueProto(&meta); err != nil {
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
func Scan(
	ctx context.Context, reader Reader, start, end roachpb.Key, max int64,
) ([]MVCCKeyValue, error) {
	var kvs []MVCCKeyValue
	err := reader.MVCCIterate(ctx, start, end, MVCCKeyAndIntentsIterKind, IterKeyTypePointsOnly,
		fs.UnknownReadCategory,
		func(kv MVCCKeyValue, _ MVCCRangeKeyStack) error {
			if max != 0 && int64(len(kvs)) >= max {
				return iterutil.StopIteration()
			}
			kvs = append(kvs, kv)
			return nil
		})
	return kvs, err
}

// ScanLocks scans locks (shared, exclusive, and intent) using only the lock
// table keyspace. It does not scan over the MVCC keyspace.
func ScanLocks(
	ctx context.Context, reader Reader, start, end roachpb.Key, maxLocks, targetBytes int64,
) ([]roachpb.Lock, error) {
	var locks []roachpb.Lock

	if bytes.Compare(start, end) >= 0 {
		return locks, nil
	}

	ltStart, _ := keys.LockTableSingleKey(start, nil)
	ltEnd, _ := keys.LockTableSingleKey(end, nil)
	iter, err := NewLockTableIterator(ctx, reader, LockTableIteratorOptions{
		LowerBound:  ltStart,
		UpperBound:  ltEnd,
		MatchMinStr: lock.Shared, // all locks
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var meta enginepb.MVCCMetadata
	var lockBytes int64
	var ok bool
	for ok, err = iter.SeekEngineKeyGE(EngineKey{Key: ltStart}); ok; ok, err = iter.NextEngineKey() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if maxLocks != 0 && int64(len(locks)) >= maxLocks {
			break
		}
		if targetBytes != 0 && lockBytes >= targetBytes {
			break
		}
		key, err := iter.EngineKey()
		if err != nil {
			return nil, err
		}
		ltKey, err := key.ToLockTableKey()
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
		locks = append(locks, roachpb.MakeLock(meta.Txn, ltKey.Key, ltKey.Strength))
		lockBytes += int64(len(ltKey.Key)) + int64(len(v))
	}
	if err != nil {
		return nil, err
	}
	return locks, nil
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
	ctx context.Context,
	r Reader,
	w Writer,
	start, end roachpb.Key,
	pointKeyThreshold, rangeKeyThreshold int,
) error {
	clearPointKeys := func(r Reader, w Writer, start, end roachpb.Key, threshold int) error {
		iter, err := r.NewEngineIterator(ctx, IterOptions{
			KeyTypes:   IterKeyTypePointsOnly,
			LowerBound: start,
			UpperBound: end,
		})
		if err != nil {
			return err
		}
		defer iter.Close()

		// Scan, and drop a RANGEDEL if we reach the threshold. We tighten the span
		// to the first encountered key, since we can cheaply do so.
		var ok bool
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
			if err = w.ClearEngineKey(key, ClearOptions{
				ValueSizeKnown: true,
				ValueSize:      uint32(iter.ValueLen()),
			}); err != nil {
				return err
			}
		}
		return err
	}

	clearRangeKeys := func(r Reader, w Writer, start, end roachpb.Key, threshold int) error {
		iter, err := r.NewEngineIterator(ctx, IterOptions{
			KeyTypes:   IterKeyTypeRangesOnly,
			LowerBound: start,
			UpperBound: end,
		})
		if err != nil {
			return err
		}
		defer iter.Close()

		// Scan, and drop a RANGEKEYDEL if we reach the threshold.
		var ok bool
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
	settings.ApplicationLevel,
	"rocksdb.ingest_backpressure.l0_file_count_threshold",
	"number of L0 files after which to backpressure SST ingestions",
	20,
)

var ingestDelayTime = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"rocksdb.ingest_backpressure.max_delay",
	"maximum amount of time to backpressure a single SST ingestion",
	time.Second*5,
)

var preIngestDelayEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"pebble.pre_ingest_delay.enabled",
	"controls whether the pre-ingest delay mechanism is active",
	false,
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
	if !preIngestDelayEnabled.Get(&settings.SV) {
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
	ctx context.Context,
	reader Reader,
	start, end roachpb.Key,
	iterKind MVCCIterKind,
	keyTypes IterKeyType,
	readCategory fs.ReadCategory,
	f func(MVCCKeyValue, MVCCRangeKeyStack) error,
) error {
	if reader.Closed() {
		return errors.New("cannot call MVCCIterate on a closed batch")
	}
	if start.Compare(end) >= 0 {
		return nil
	}

	it, err := reader.NewMVCCIterator(ctx, iterKind, IterOptions{
		KeyTypes:     keyTypes,
		LowerBound:   start,
		UpperBound:   end,
		ReadCategory: readCategory,
	})
	if err != nil {
		return err
	}
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
			kv = MVCCKeyValue{Key: it.UnsafeKey().Clone(), Value: v}
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

	key := iter.UnsafeKey().Clone()

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

	// If the iterator position has a point key, Value must equal UnsafeValue.
	// NB: It's only valid to read an iterator's Value if the iterator is
	// positioned at a point key.
	if hasPoint, _ := iter.HasPointAndRange(); hasPoint {
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
// transaction referenced by txnID[1] at the supplied `ts` are ignored; so are
// {Shared,Exclusive} replicated locks, as they do not conflict with non-locking
// reads.
//
// The `needsIntentHistory` return value indicates whether the caller needs to
// consult intent history when performing a scan over the MVCC keyspace to
// read correct provisional values for at least one of the keys being scanned.
// Typically, this applies to all transactions that read their own writes.
//
// [1] The supplied txnID may be empty (uuid.Nil) if the request on behalf of
// which the scan is being performed is non-transactional.
func ScanConflictingIntentsForDroppingLatchesEarly(
	ctx context.Context,
	reader Reader,
	txnID uuid.UUID,
	ts hlc.Timestamp,
	start, end roachpb.Key,
	intents *[]roachpb.Intent,
	maxLockConflicts int64,
	targetLockConflictBytes int64,
) (needIntentHistory bool, err error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	upperBoundUnset := len(end) == 0 // NB: Get requests do not set the end key.
	if !upperBoundUnset && bytes.Compare(start, end) >= 0 {
		return true, errors.AssertionFailedf("start key must be less than end key")
	}
	ltStart, _ := keys.LockTableSingleKey(start, nil)
	opts := LockTableIteratorOptions{
		LowerBound: ltStart,
		// Ignore Exclusive and Shared locks; we only drop latches early for
		// non-locking reads, which do not conflict with Shared or
		// Exclusive[1] locks.
		//
		// [1] Specifically replicated Exclusive locks. Interaction with
		// unreplicated locks is governed by the ExclusiveLocksBlockNonLockingReads
		// cluster setting.
		MatchMinStr:  lock.Intent,
		ReadCategory: fs.BatchEvalReadCategory,
	}
	if upperBoundUnset {
		opts.Prefix = true
	} else {
		ltEnd, _ := keys.LockTableSingleKey(end, nil)
		opts.UpperBound = ltEnd
	}
	iter, err := NewLockTableIterator(ctx, reader, opts)
	if err != nil {
		return false, err
	}
	defer iter.Close()
	if log.ExpensiveLogEnabled(ctx, 3) {
		defer func() {
			ss := iter.Stats().Stats
			log.VEventf(ctx, 3, "lock table scan stats: %s", ss.String())
		}()
	}

	var meta enginepb.MVCCMetadata
	var ok bool
	intentSize := int64(0)
	for ok, err = iter.SeekEngineKeyGE(EngineKey{Key: ltStart}); ok; ok, err = iter.NextEngineKey() {
		if maxLockConflicts != 0 && int64(len(*intents)) >= maxLockConflicts {
			// Return early if we're done accumulating intents; make no claims about
			// not needing intent history.
			return true /* needsIntentHistory */, nil
		}
		if targetLockConflictBytes != 0 && intentSize >= targetLockConflictBytes {
			// Return early if we're exceed intent byte limits; make no claims about
			// not needing intent history.
			return true /* needsIntentHistory */, nil
		}
		err := iter.ValueProto(&meta)
		if err != nil {
			return false, err
		}
		if meta.Txn == nil {
			return false, errors.Errorf("intent without transaction")
		}
		ownIntent := txnID != uuid.Nil && txnID == meta.Txn.ID
		if ownIntent {
			// If we ran into one of our own intents, a corresponding scan over the
			// MVCC keyspace will need access to the key's intent history in order to
			// read the correct provisional value. As such, we set `needsIntentHistory`
			// to be true.
			//
			// This determination is more restrictive than it needs to be. A read
			// request needs access to the intent history when performing a scan over
			// the MVCC keyspace only if:
			// 1. The request is reading at a lower sequence number than the intent's
			// sequence number.
			// 2. OR the request is reading at a (strictly) lower timestamp than the
			// intent timestamp[1]. This can happen if the intent was pushed for some
			// reason.
			// 3. OR the found intent should be ignored because it was written as part
			// of a savepoint which was subsequently rolled back.
			// 4. OR the found intent and read request belong to different txn epochs.
			//
			// The conditions above mirror special case handling for intents by
			// pebbleMVCCScanner's getOne method. If we find scanning the lock table
			// twice (once during conflict resolution, and once when interleaving
			// intents during the MVCC read) is too expensive for transactions that
			// read their own writes, there's some optimizations to be had here by
			// being smarter about when we decide to interleave intents or not to.
			//
			// [1] Only relevant if the intent has a sequence number less than or
			// equal to the read request's sequence number. Otherwise, we need access
			// to the intent history to read the correct provisional value -- one
			// written at a lower or equal sequence number compared to the read
			// request's.
			needIntentHistory = true
			continue
		}
		if intentConflicts := meta.Timestamp.ToTimestamp().LessEq(ts); !intentConflicts {
			continue
		}
		key, err := iter.EngineKey()
		if err != nil {
			return false, err
		}
		ltKey, err := key.ToLockTableKey()
		if err != nil {
			return false, err
		}
		if ltKey.Strength != lock.Intent {
			return false, errors.AssertionFailedf("unexpected strength for LockTableKey %s", ltKey.Strength)
		}
		conflictingIntent := roachpb.MakeIntent(meta.Txn, ltKey.Key)
		intentSize += int64(conflictingIntent.Size())
		*intents = append(*intents, conflictingIntent)
	}
	if err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return needIntentHistory, nil /* err */
}
