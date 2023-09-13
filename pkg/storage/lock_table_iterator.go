// Copyright 2023 The Cockroach Authors.
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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

// LockTableIterator is an EngineIterator that iterates over locks in the lock
// table keyspace. It performs no translation of input or output keys or values,
// so it is used like a normal EngineIterator, with the limitation that it can
// only be used to iterate over the lock table keyspace.
//
// The benefit of using a LockTableIterator is that it performs filtering of the
// locks in the lock table, only returning locks that match the configured
// filtering criteria and transparently skipping past locks that do not. The
// filtering criteria is expressed as a logical disjunction of two configuration
// parameters, at least one of which must be set:
//
//   - MatchTxnID: if set, the iterator return locks held by this transaction.
//
//   - MatchMinStr: if set, the iterator returns locks held by any transaction
//     with this strength or stronger.
//
// Expressed abstractly as a SQL query, the filtering criteria is:
//
//	SELECT * FROM lock_table WHERE (MatchTxnID  != 0 AND txn_id = MatchTxnID)
//	                            OR (MatchMinStr != 0 AND strength >= MatchMinStr)
//
// Pushing this filtering logic into the iterator is a convenience for its
// users. It also allows the iterator to use its knowledge of the lock table
// keyspace structure to efficiently skip past locks that do not match the
// filtering criteria. It does this by seeking past many ignored locks when
// appropriate to avoid cases of O(ignored_locks) work, instead performing at
// most O(matching_locks + locked_keys) work.
//
// A common case where this matters is with shared locks. If the iterator is
// configured to ignore shared locks, a single key with a large number of shared
// locks can be skipped over with a single seek. Avoiding this unnecessary work
// is essential to avoiding quadratic behavior during shared lock acquisition
// and release.
type LockTableIterator struct {
	iter EngineIterator
	// If set, return locks with any strength held by this transaction.
	matchTxnID uuid.UUID
	// If set, return locks held by any transaction with this strength or
	// stronger.
	matchMinStr lock.Strength
}

var _ EngineIterator = &LockTableIterator{}

// LockTableIteratorOptions contains options used to create a LockTableIterator.
type LockTableIteratorOptions struct {
	// See IterOptions.Prefix.
	Prefix bool
	// See IterOptions.LowerBound.
	LowerBound roachpb.Key
	// See IterOptions.UpperBound.
	UpperBound roachpb.Key

	// If set, return locks with any strength held by this transaction.
	MatchTxnID uuid.UUID
	// If set, return locks held by any transaction with this strength or
	// stronger.
	MatchMinStr lock.Strength
}

// validate validates the LockTableIteratorOptions.
func (opts LockTableIteratorOptions) validate() error {
	if !opts.Prefix && len(opts.UpperBound) == 0 && len(opts.LowerBound) == 0 {
		return errors.AssertionFailedf("LockTableIterator must set prefix or upper bound or lower bound")
	}
	if len(opts.LowerBound) != 0 && !isLockTableKey(opts.LowerBound) {
		return errors.AssertionFailedf("LockTableIterator lower bound must be a lock table key")
	}
	if len(opts.UpperBound) != 0 && !isLockTableKey(opts.UpperBound) {
		return errors.AssertionFailedf("LockTableIterator upper bound must be a lock table key")
	}
	if opts.MatchTxnID == uuid.Nil && opts.MatchMinStr == 0 {
		return errors.AssertionFailedf("LockTableIterator must specify MatchTxnID, MatchMinStr, or both")
	}
	return nil
}

// toIterOptions converts the LockTableIteratorOptions to IterOptions.
func (opts LockTableIteratorOptions) toIterOptions() IterOptions {
	return IterOptions{
		Prefix:     opts.Prefix,
		LowerBound: opts.LowerBound,
		UpperBound: opts.UpperBound,
	}
}

var lockTableIteratorPool = sync.Pool{
	New: func() interface{} { return new(LockTableIterator) },
}

// NewLockTableIterator creates a new LockTableIterator.
func NewLockTableIterator(
	reader Reader, opts LockTableIteratorOptions,
) (*LockTableIterator, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}
	iter, err := reader.NewEngineIterator(opts.toIterOptions())
	if err != nil {
		return nil, err
	}
	ltIter := lockTableIteratorPool.Get().(*LockTableIterator)
	*ltIter = LockTableIterator{
		iter:        iter,
		matchTxnID:  opts.MatchTxnID,
		matchMinStr: opts.MatchMinStr,
	}
	return ltIter, nil
}

// SeekEngineKeyGE implements the EngineIterator interface.
func (i *LockTableIterator) SeekEngineKeyGE(key EngineKey) (valid bool, err error) {
	if err := checkLockTableKey(key.Key); err != nil {
		return false, err
	}
	valid, err = i.iter.SeekEngineKeyGE(key)
	if !valid || err != nil {
		return valid, err
	}
	state, err := i.advanceToMatchingLock(+1, nil)
	return state == pebble.IterValid, err
}

// SeekEngineKeyLT implements the EngineIterator interface.
func (i *LockTableIterator) SeekEngineKeyLT(key EngineKey) (valid bool, err error) {
	if err := checkLockTableKey(key.Key); err != nil {
		return false, err
	}
	valid, err = i.iter.SeekEngineKeyLT(key)
	if !valid || err != nil {
		return valid, err
	}
	state, err := i.advanceToMatchingLock(-1, nil)
	return state == pebble.IterValid, err
}

// NextEngineKey implements the EngineIterator interface.
func (i *LockTableIterator) NextEngineKey() (valid bool, err error) {
	valid, err = i.iter.NextEngineKey()
	if !valid || err != nil {
		return valid, err
	}
	state, err := i.advanceToMatchingLock(+1, nil)
	return state == pebble.IterValid, err
}

// PrevEngineKey implements the EngineIterator interface.
func (i *LockTableIterator) PrevEngineKey() (valid bool, err error) {
	valid, err = i.iter.PrevEngineKey()
	if !valid || err != nil {
		return valid, err
	}
	state, err := i.advanceToMatchingLock(-1, nil)
	return state == pebble.IterValid, err
}

// SeekEngineKeyGEWithLimit implements the EngineIterator interface.
func (i *LockTableIterator) SeekEngineKeyGEWithLimit(
	key EngineKey, limit roachpb.Key,
) (state pebble.IterValidityState, err error) {
	if err := checkLockTableKey(key.Key); err != nil {
		return 0, err
	}
	if err := checkLockTableKeyOrNil(limit); err != nil {
		return 0, err
	}
	state, err = i.iter.SeekEngineKeyGEWithLimit(key, limit)
	if state != pebble.IterValid || err != nil {
		return state, err
	}
	return i.advanceToMatchingLock(+1, limit)
}

// SeekEngineKeyLTWithLimit implements the EngineIterator interface.
func (i *LockTableIterator) SeekEngineKeyLTWithLimit(
	key EngineKey, limit roachpb.Key,
) (state pebble.IterValidityState, err error) {
	if err := checkLockTableKey(key.Key); err != nil {
		return 0, err
	}
	if err := checkLockTableKeyOrNil(limit); err != nil {
		return 0, err
	}
	state, err = i.iter.SeekEngineKeyLTWithLimit(key, limit)
	if state != pebble.IterValid || err != nil {
		return state, err
	}
	return i.advanceToMatchingLock(-1, limit)
}

// NextEngineKeyWithLimit implements the EngineIterator interface.
func (i *LockTableIterator) NextEngineKeyWithLimit(
	limit roachpb.Key,
) (state pebble.IterValidityState, err error) {
	if err := checkLockTableKeyOrNil(limit); err != nil {
		return 0, err
	}
	state, err = i.iter.NextEngineKeyWithLimit(limit)
	if state != pebble.IterValid || err != nil {
		return state, err
	}
	return i.advanceToMatchingLock(+1, limit)
}

// PrevEngineKeyWithLimit implements the EngineIterator interface.
func (i *LockTableIterator) PrevEngineKeyWithLimit(
	limit roachpb.Key,
) (state pebble.IterValidityState, err error) {
	if err := checkLockTableKeyOrNil(limit); err != nil {
		return 0, err
	}
	state, err = i.iter.PrevEngineKeyWithLimit(limit)
	if state != pebble.IterValid || err != nil {
		return state, err
	}
	return i.advanceToMatchingLock(-1, limit)
}

// advanceToMatchingLock advances the iterator to the next lock table key that
// matches the configured filtering criteria. If limit is non-nil, the iterator
// will stop advancing once it reaches the limit.
func (i *LockTableIterator) advanceToMatchingLock(
	dir int, limit roachpb.Key,
) (state pebble.IterValidityState, err error) {
	for {
		engineKey, err := i.iter.UnsafeEngineKey()
		if err != nil {
			return 0, err
		}
		str, txnID, err := engineKey.decodeLockTableKeyVersion()
		if err != nil {
			return 0, err
		}
		if i.matchingLock(str, txnID) {
			return pebble.IterValid, nil
		}

		// TODO(nvanbenschoten): implement a maxItersBeforeSeek-like algorithm
		// to skip over ignored locks and bound the work performed by the
		// iterator for ignored locks.

		if dir < 0 {
			state, err = i.iter.PrevEngineKeyWithLimit(limit)
		} else {
			state, err = i.iter.NextEngineKeyWithLimit(limit)
		}
		if state != pebble.IterValid || err != nil {
			return state, err
		}
	}
}

// matchingLock returns whether the lock table key with the provided strength
// and transaction ID matches the configured filtering criteria.
func (i *LockTableIterator) matchingLock(str lock.Strength, txnID uuid.UUID) bool {
	// Is this a lock held by the desired transaction?
	return (i.matchTxnID != uuid.Nil && i.matchTxnID == txnID) ||
		// Or, is this a lock with the desired strength or stronger?
		(i.matchMinStr != 0 && i.matchMinStr <= str)
}

// Close implements the EngineIterator interface.
func (i *LockTableIterator) Close() {
	i.iter.Close()
	*i = LockTableIterator{}
	lockTableIteratorPool.Put(i)
}

// HasPointAndRange implements the EngineIterator interface.
func (i *LockTableIterator) HasPointAndRange() (bool, bool) {
	return i.iter.HasPointAndRange()
}

// EngineRangeBounds implements the EngineIterator interface.
func (i *LockTableIterator) EngineRangeBounds() (roachpb.Span, error) {
	return i.iter.EngineRangeBounds()
}

// EngineRangeKeys implements the EngineIterator interface.
func (i *LockTableIterator) EngineRangeKeys() []EngineRangeKeyValue {
	return i.iter.EngineRangeKeys()
}

// RangeKeyChanged implements the EngineIterator interface.
func (i *LockTableIterator) RangeKeyChanged() bool {
	return i.iter.RangeKeyChanged()
}

// UnsafeEngineKey implements the EngineIterator interface.
func (i *LockTableIterator) UnsafeEngineKey() (EngineKey, error) {
	return i.iter.UnsafeEngineKey()
}

// EngineKey implements the EngineIterator interface.
func (i *LockTableIterator) EngineKey() (EngineKey, error) {
	return i.iter.EngineKey()
}

// UnsafeRawEngineKey implements the EngineIterator interface.
func (i *LockTableIterator) UnsafeRawEngineKey() []byte {
	return i.iter.UnsafeRawEngineKey()
}

// LockTableKeyVersion returns the strength and txn ID from the version of the
// current key.
func (i *LockTableIterator) LockTableKeyVersion() (lock.Strength, uuid.UUID, error) {
	k, err := i.iter.UnsafeEngineKey()
	if err != nil {
		return 0, uuid.UUID{}, errors.Wrap(err, "retrieving lock table key")
	}
	return k.decodeLockTableKeyVersion()
}

// UnsafeValue implements the EngineIterator interface.
func (i *LockTableIterator) UnsafeValue() ([]byte, error) {
	return i.iter.UnsafeValue()
}

// UnsafeLazyValue implements the EngineIterator interface.
func (i *LockTableIterator) UnsafeLazyValue() pebble.LazyValue {
	return i.iter.(*pebbleIterator).UnsafeLazyValue()
}

// Value implements the EngineIterator interface.
func (i *LockTableIterator) Value() ([]byte, error) {
	return i.iter.Value()
}

// ValueLen implements the EngineIterator interface.
func (i *LockTableIterator) ValueLen() int {
	return i.iter.ValueLen()
}

// ValueProto unmarshals the current value into the provided proto.
func (i *LockTableIterator) ValueProto(meta *enginepb.MVCCMetadata) error {
	v, err := i.iter.UnsafeValue()
	if err != nil {
		return errors.Wrap(err, "retrieving lock table value")
	}
	return protoutil.Unmarshal(v, meta)
}

// CloneContext implements the EngineIterator interface.
func (i *LockTableIterator) CloneContext() CloneContext {
	return i.iter.CloneContext()
}

// Stats implements the EngineIterator interface.
func (i *LockTableIterator) Stats() IteratorStats {
	return i.iter.Stats()
}

//gcassert:inline
func isLockTableKey(key roachpb.Key) bool {
	return bytes.HasPrefix(key, keys.LocalRangeLockTablePrefix)
}

var errNotLockTableKey = errors.New("LockTableIterator: key is not a lock table key")

//gcassert:inline
func checkLockTableKey(key roachpb.Key) error {
	if !isLockTableKey(key) {
		return errNotLockTableKey
	}
	return nil
}

//gcassert:inline
func checkLockTableKeyOrNil(key roachpb.Key) error {
	if len(key) == 0 {
		return nil
	}
	return checkLockTableKey(key)
}
