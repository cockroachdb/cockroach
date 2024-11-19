// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"bytes"
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
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
	iter   EngineIterator
	prefix bool
	// If set, return locks with any strength held by this transaction.
	matchTxnID uuid.UUID
	// If set, return locks held by any transaction with this strength or
	// stronger.
	matchMinStr lock.Strength
	// Used to avoid iterating over all shared locks on a key when not necessary,
	// given the filtering criteria. See the comment about "skip past locks" above
	// for details about why this is important.
	itersBeforeSeek lockTableItersBeforeSeekHelper
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
	// ReadCategory is used to map to a user-understandable category string, for
	// stats aggregation and metrics, and a Pebble-understandable QoS.
	ReadCategory fs.ReadCategory
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
	ctx context.Context, reader Reader, opts LockTableIteratorOptions,
) (*LockTableIterator, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}
	iter, err := reader.NewEngineIterator(ctx, opts.toIterOptions())
	if err != nil {
		return nil, err
	}
	ltIter := lockTableIteratorPool.Get().(*LockTableIterator)
	*ltIter = LockTableIterator{
		iter:            iter,
		prefix:          opts.Prefix,
		matchTxnID:      opts.MatchTxnID,
		matchMinStr:     opts.MatchMinStr,
		itersBeforeSeek: ltIter.itersBeforeSeek,
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
	defer i.itersBeforeSeek.reset()
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

		// We found a non-matching lock. Determine whether to step or seek past it.
		// We only ever seek if we found a shared lock, because no other locking
		// strength allows for multiple locks to be held by different transactions
		// on the same key.
		var seek bool
		if str == lock.Shared {
			seek = i.itersBeforeSeek.shouldSeek(engineKey.Key)
		}

		// Advance to the next key, either by stepping or seeking.
		if seek {
			ltKey, ltKeyErr := engineKey.ToLockTableKey()
			if ltKeyErr != nil {
				return 0, ltKeyErr
			}
			seekKeyBuf := &i.itersBeforeSeek.seekKeyBuf
			var seekKey EngineKey
			if dir < 0 {
				// If iterating backwards and searching for locks held by a specific
				// transaction, determine whether we have yet to reach key/shared/txnID
				// or have already passed it. If we have not yet passed it, seek to the
				// specific version, remembering to offset the txn ID by 1 to account
				// for the exclusive reverse seek. Otherwise, seek past the maximum
				// (first) txn ID to the previous locking strength (exclusive).
				// NOTE: Recall that txnIDs in the lock table key version are ordered in
				// reverse lexicographical order.
				if i.matchTxnID != uuid.Nil && bytes.Compare(txnID.GetBytes(), i.matchTxnID.GetBytes()) < 0 {
					// The subtraction cannot underflow because matchTxnID cannot be the
					// zero UUID if we are in this branch, with the iterator positioned
					// after the matchTxnID. Assert for good measure.
					if i.matchTxnID == uuid.Nil {
						panic("matchTxnID is unexpectedly the zero UUID")
					}
					ltKey.TxnUUID = uuid.FromUint128(i.matchTxnID.ToUint128().Sub(1))
					seekKey, *seekKeyBuf = ltKey.ToEngineKey(*seekKeyBuf)
				} else {
					ltKey.TxnUUID = uuid.Max
					seekKey, *seekKeyBuf = ltKey.ToEngineKey(*seekKeyBuf)
				}
				state, err = i.iter.SeekEngineKeyLTWithLimit(seekKey, limit)
			} else {
				// If iterating forwards and searching for locks held by a specific
				// transaction, determine whether we have yet to reach /key/shared/txnID
				// or have already passed it. If we have not yet passed it, seek to the
				// specific version. Otherwise, seek to the next key prefix.
				// NOTE: Recall that txnIDs in the lock table key version are ordered in
				// reverse lexicographical order.
				// NOTE: Recall that shared locks are ordered last for a given key.
				if i.matchTxnID != uuid.Nil && bytes.Compare(txnID.GetBytes(), i.matchTxnID.GetBytes()) > 0 {
					ltKey.TxnUUID = i.matchTxnID
					seekKey, *seekKeyBuf = ltKey.ToEngineKey(*seekKeyBuf)
				} else {
					// Seek to the next key prefix (locks on the next user key).
					// Unlike the two reverse iteration cases and the forward
					// iteration case where we have yet to reach /key/shared/txnID,
					// this case deserves special consideration.
					if i.prefix {
						// If we are configured as a prefix iterator, do not seek to
						// the next key prefix. Instead, return the IterExhausted
						// state. This is more than just an optimization. Seeking to
						// the next key prefix would move the underlying iterator
						// (which is also configured for prefix iteration) to the
						// next key prefix, if such a key prefix exists.
						//
						// This case could be decoupled from the itersBeforeSeek
						// optimization. When performing prefix iteration, we could
						// immediately detect cases where there are no more possible
						// matching locks in the key prefix and return an exhausted
						// state, instead of waiting until we decide to seek to do
						// so. It's not clear that this additional complexity and
						// code duplication is worth it, so we don't do it for now.
						return pebble.IterExhausted, nil
					}
					// TODO(nvanbenschoten): for now, we call SeekEngineKeyGEWithLimit
					// with the prefix of the next lock table key. If EngineIterator
					// exposed an interface that called NextPrefix(), we could use that
					// instead. This will require adding a NextPrefixWithLimit() method
					// to pebble.
					var seekKeyPrefix roachpb.Key
					seekKeyPrefix, *seekKeyBuf = keys.LockTableSingleNextKey(ltKey.Key, *seekKeyBuf)
					seekKey = EngineKey{Key: seekKeyPrefix}
				}
				state, err = i.iter.SeekEngineKeyGEWithLimit(seekKey, limit)
			}
		} else {
			if dir < 0 {
				state, err = i.iter.PrevEngineKeyWithLimit(limit)
			} else {
				state, err = i.iter.NextEngineKeyWithLimit(limit)
			}
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
	*i = LockTableIterator{
		itersBeforeSeek: i.itersBeforeSeek,
	}
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

// UnsafeLockTableKey returns the current key as an unsafe LockTableKey.
// TODO(nvanbenschoten): use this more widely.
func (i *LockTableIterator) UnsafeLockTableKey() (LockTableKey, error) {
	k, err := i.iter.UnsafeEngineKey()
	if err != nil {
		return LockTableKey{}, errors.Wrap(err, "retrieving lock table key")
	}
	return k.ToLockTableKey()
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

// defaultLockTableItersBeforeSeek is the default value for the
// lockTableItersBeforeSeek metamorphic value.
const defaultLockTableItersBeforeSeek = 5

// lockTableItersBeforeSeek is the number of iterations to perform across the
// shared locks on a single user key before seeking past them. This is used to
// avoid iterating over all shared locks on a key when not necessary, given the
// filtering criteria.
var lockTableItersBeforeSeek = metamorphic.ConstantWithTestRange(
	"lock-table-iters-before-seek",
	defaultLockTableItersBeforeSeek, /* defaultValue */
	0,                               /* min */
	3,                               /* max */
)

// DisableMetamorphicLockTableItersBeforeSeek disables the metamorphic value for
// the duration of a test, resetting it at the end.
func DisableMetamorphicLockTableItersBeforeSeek(t interface {
	Helper()
	Cleanup(func())
}) {
	t.Helper()
	prev := lockTableItersBeforeSeek
	lockTableItersBeforeSeek = defaultLockTableItersBeforeSeek
	t.Cleanup(func() {
		lockTableItersBeforeSeek = prev
	})
}

// lockTableItersBeforeSeekHelper is a helper struct that keeps track of the
// number of iterations performed across the shared locks on a single user key
// while searching for matching locks in the lock table. It is used to determine
// when to seek past the shared locks to avoid O(ignored_locks) work.
//
// This is similar to the dynamic itersBeforeSeek algorithm that is used by
// pebbleMVCCScanner when scanning over mvcc versions for a key. However, we
// don't adaptively adjust the number of itersBeforeSeek as we go. Instead, we
// reset the iteration counter to lockTableItersBeforeSeek (default: 5) on each
// new key prefix. Doing something more sophisticated introduces complexity and
// it's not clear that this is worth it.
//
// The zero value is ready to use.
type lockTableItersBeforeSeekHelper struct {
	curItersBeforeSeek int
	curKeyPrefix       roachpb.Key

	// Buffers that avoids allocations.
	keyPrefixBuf []byte
	seekKeyBuf   []byte
}

func (h *lockTableItersBeforeSeekHelper) reset() {
	// Clearing the curKeyPrefix ensures that the next call to shouldSeek() will
	// save the new key prefix and reset curItersBeforeSeek. This is why the zero
	// value of the struct is ready to use.
	h.curKeyPrefix = nil
}

func (h *lockTableItersBeforeSeekHelper) shouldSeek(keyPrefix roachpb.Key) bool {
	if h.alwaysSeek() {
		return true
	}
	if !h.curKeyPrefix.Equal(keyPrefix) {
		// New key prefix (or curKeyPrefix was nil). Save it and reset the iteration
		// count.
		h.saveKeyPrefix(keyPrefix)
		h.curItersBeforeSeek = lockTableItersBeforeSeek
	} else {
		// Same key prefix as before. Check if we should seek.
		if h.curItersBeforeSeek == 0 {
			return true
		}
	}
	h.curItersBeforeSeek--
	return false
}

func (h *lockTableItersBeforeSeekHelper) alwaysSeek() bool {
	// Only returns true in tests when the metamorphic value is set to 0.
	return lockTableItersBeforeSeek == 0
}

func (h *lockTableItersBeforeSeekHelper) saveKeyPrefix(keyPrefix roachpb.Key) {
	h.keyPrefixBuf = append(h.keyPrefixBuf[:0], keyPrefix...)
	h.curKeyPrefix = h.keyPrefixBuf
}
