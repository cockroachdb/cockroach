// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// Fixed length slice for all supported lock strengths for replicated locks. May
// be used to iterate supported lock strengths in strength order (strongest to
// weakest).
var replicatedLockStrengths = [...]lock.Strength{lock.Intent, lock.Exclusive, lock.Shared}

func init() {
	if replicatedLockStrengths[0] != lock.MaxStrength {
		panic("replicatedLockStrengths[0] != lock.MaxStrength; update replicatedLockStrengths?")
	}
}

// replicatedLockStrengthToIndexMap returns a mapping between (strength, index)
// pairs that can be used to index into the lockTableScanner.ownLocks array.
//
// Trying to use a lock strength that isn't supported with replicated locks to
// index into the lockTableScanner.ownLocks array will cause a runtime error.
var replicatedLockStrengthToIndexMap = func() (m [lock.MaxStrength + 1]int) {
	// Initialize all to -1.
	for str := range m {
		m[str] = -1
	}
	// Set the indices of the valid strengths.
	for i, str := range replicatedLockStrengths {
		m[str] = i
	}
	return m
}()

// strongerOrEqualStrengths returns all supported lock strengths for replicated
// locks that are as strong or stronger than the provided strength. The returned
// slice is ordered from strongest to weakest.
func strongerOrEqualStrengths(str lock.Strength) []lock.Strength {
	return replicatedLockStrengths[:replicatedLockStrengthToIndexMap[str]+1]
}

// minConflictLockStrength returns the minimum lock strength that conflicts with
// the provided lock strength.
func minConflictLockStrength(str lock.Strength) (lock.Strength, error) {
	switch str {
	case lock.None:
		// Don't conflict with any locks held by other transactions.
		return lock.None, nil
	case lock.Shared:
		return lock.Exclusive, nil
	case lock.Exclusive, lock.Intent:
		return lock.Shared, nil
	default:
		return 0, errors.AssertionFailedf(
			"lockTableKeyScanner: unexpected lock strength %s", str.String())
	}
}

// lockTableKeyScanner is used to scan a single key in the replicated lock
// table. It searches for locks on the key that conflict with a (transaction,
// lock strength) pair and for locks that the transaction has already acquired
// on the key.
//
// The purpose of a lockTableKeyScanner is to determine whether a transaction
// can acquire a lock on a key or perform an MVCC mutation on a key, and if so,
// what lock table keys the transaction should write to perform the operation.
type lockTableKeyScanner struct {
	iter *LockTableIterator
	// The transaction attempting to acquire a lock. The ID will be zero if a
	// non-transactional request is attempting to perform an MVCC mutation.
	txnID uuid.UUID
	// Stop adding conflicting locks and abort scan once the maxConflicts limit
	// is reached. Ignored if zero.
	maxConflicts int64
	// Stop adding conflicting locks and abort scan once the targetBytesPerConflict
	// limit is reached via collected intent size. Ignored if zero.
	targetBytesPerConflict int64

	// Stores any error returned. If non-nil, iteration short circuits.
	err error
	// Stores any locks that conflict with the transaction and locking strength.
	conflicts []roachpb.Lock
	// Stores the total byte size of conflicts.
	conflictBytes int64
	// Stores any locks that the transaction has already acquired.
	ownLocks [len(replicatedLockStrengths)]*enginepb.MVCCMetadata

	// Avoids heap allocations.
	ltKeyBuf     []byte
	ltValue      enginepb.MVCCMetadata
	firstOwnLock enginepb.MVCCMetadata
}

var lockTableKeyScannerPool = sync.Pool{
	New: func() interface{} { return new(lockTableKeyScanner) },
}

// newLockTableKeyScanner creates a new lockTableKeyScanner.
//
// txnID corresponds to the ID of the transaction attempting to acquire locks.
// If txnID is valid (non-empty), locks held by the transaction with any
// strength will be accumulated into the ownLocks array. Otherwise, if txnID is
// empty, the request is non-transactional and no locks will be accumulated into
// the ownLocks array.
//
// str is the strength of the lock that the transaction (or non-transactional
// request) is attempting to acquire. The scanner will search for locks held by
// other transactions that conflict with this strength[1].
//
// maxConflicts is the maximum number of conflicting locks that the scanner
// should accumulate before returning an error. If maxConflicts is zero, the
// scanner will accumulate all conflicting locks.
//
// [1] It's valid to pass in lock.None for str. lock.None doesn't conflict with
// any other replicated locks; as such, passing lock.None configures the scanner
// to only return locks from the supplied txnID.
func newLockTableKeyScanner(
	ctx context.Context,
	reader Reader,
	txnID uuid.UUID,
	str lock.Strength,
	maxConflicts int64,
	targetBytesPerConflict int64,
	readCategory fs.ReadCategory,
) (*lockTableKeyScanner, error) {
	minConflictStr, err := minConflictLockStrength(str)
	if err != nil {
		return nil, err
	}
	iter, err := NewLockTableIterator(ctx, reader, LockTableIteratorOptions{
		Prefix:       true,
		MatchTxnID:   txnID,
		MatchMinStr:  minConflictStr,
		ReadCategory: readCategory,
	})
	if err != nil {
		return nil, err
	}
	s := lockTableKeyScannerPool.Get().(*lockTableKeyScanner)
	s.iter = iter
	s.txnID = txnID
	s.maxConflicts = maxConflicts
	s.targetBytesPerConflict = targetBytesPerConflict
	return s, nil
}

func (s *lockTableKeyScanner) close() {
	s.iter.Close()
	*s = lockTableKeyScanner{ltKeyBuf: s.ltKeyBuf}
	lockTableKeyScannerPool.Put(s)
}

// scan scans the lock table at the provided key for locks held by other
// transactions that conflict with the configured locking strength and for locks
// of any strength that the configured transaction has already acquired.
func (s *lockTableKeyScanner) scan(key roachpb.Key) error {
	s.resetScanState()
	for ok := s.seek(key); ok; ok = s.getOneAndAdvance() {
	}
	return s.afterScan()
}

// resetScanState resets the scanner's state before a scan.
func (s *lockTableKeyScanner) resetScanState() {
	s.err = nil
	s.conflicts = nil
	s.conflictBytes = 0
	for i := range s.ownLocks {
		s.ownLocks[i] = nil
	}
	s.ltValue.Reset()
	s.firstOwnLock.Reset()
}

// afterScan returns any error encountered during the scan.
func (s *lockTableKeyScanner) afterScan() error {
	if s.err != nil {
		return s.err
	}
	if len(s.conflicts) != 0 {
		return &kvpb.LockConflictError{Locks: s.conflicts}
	}
	return nil
}

// seek seeks the iterator to the first lock table key associated with the
// provided key. Returns true if the scanner should continue scanning, false
// if not.
func (s *lockTableKeyScanner) seek(key roachpb.Key) bool {
	var ltKey roachpb.Key
	ltKey, s.ltKeyBuf = keys.LockTableSingleKey(key, s.ltKeyBuf)
	valid, err := s.iter.SeekEngineKeyGE(EngineKey{Key: ltKey})
	if err != nil {
		s.err = err
	}
	return valid
}

// getOneAndAdvance consumes the current lock table key and value and advances
// the iterator. Returns true if the scanner should continue scanning, false if
// not.
func (s *lockTableKeyScanner) getOneAndAdvance() bool {
	ltKey, ok := s.getLockTableKey()
	if !ok {
		return false
	}
	ltValue, ok := s.getLockTableValue()
	if !ok {
		return false
	}
	if !s.consumeLockTableKeyValue(ltKey, ltValue) {
		return false
	}
	return s.advance()
}

// advance advances the iterator to the next lock table key.
func (s *lockTableKeyScanner) advance() bool {
	valid, err := s.iter.NextEngineKey()
	if err != nil {
		s.err = err
	}
	return valid
}

// getLockTableKey decodes the current lock table key.
func (s *lockTableKeyScanner) getLockTableKey() (LockTableKey, bool) {
	ltEngKey, err := s.iter.UnsafeEngineKey()
	if err != nil {
		s.err = err
		return LockTableKey{}, false
	}
	ltKey, err := ltEngKey.ToLockTableKey()
	if err != nil {
		s.err = err
		return LockTableKey{}, false
	}
	return ltKey, true
}

// getLockTableValue decodes the current lock table values.
func (s *lockTableKeyScanner) getLockTableValue() (*enginepb.MVCCMetadata, bool) {
	err := s.iter.ValueProto(&s.ltValue)
	if err != nil {
		s.err = err
		return nil, false
	}
	return &s.ltValue, true
}

// consumeLockTableKeyValue consumes the current lock table key and value, which
// is either a conflicting lock or a lock held by the scanning transaction.
func (s *lockTableKeyScanner) consumeLockTableKeyValue(
	ltKey LockTableKey, ltValue *enginepb.MVCCMetadata,
) bool {
	if ltValue.Txn == nil {
		s.err = errors.AssertionFailedf("unexpectedly found non-transactional lock: %v", ltValue)
		return false
	}
	if ltKey.TxnUUID != ltValue.Txn.ID {
		s.err = errors.AssertionFailedf("lock table key (%+v) and value (%+v) txn ID mismatch", ltKey, ltValue)
		return false
	}
	if ltKey.TxnUUID == s.txnID {
		return s.consumeOwnLock(ltKey, ltValue)
	}
	return s.consumeConflictingLock(ltKey, ltValue)
}

// consumeOwnLock consumes a lock held by the scanning transaction.
func (s *lockTableKeyScanner) consumeOwnLock(
	ltKey LockTableKey, ltValue *enginepb.MVCCMetadata,
) bool {
	var ltValueCopy *enginepb.MVCCMetadata
	if s.firstOwnLock.Txn == nil {
		// This is the first lock held by the transaction that we've seen, so
		// we can avoid the heap allocation.
		ltValueCopy = &s.firstOwnLock
	} else {
		ltValueCopy = new(enginepb.MVCCMetadata)
	}
	// NOTE: this will alias internal pointer fields of ltValueCopy with those
	// in ltValue, but this will not lead to issues when ltValue is updated by
	// the next call to getLockTableValue, because its internal fields will be
	// reset by protoutil.Unmarshal before unmarshalling.
	*ltValueCopy = *ltValue
	s.ownLocks[replicatedLockStrengthToIndexMap[ltKey.Strength]] = ltValueCopy
	return true
}

// consumeConflictingLock consumes a conflicting lock.
func (s *lockTableKeyScanner) consumeConflictingLock(
	ltKey LockTableKey, ltValue *enginepb.MVCCMetadata,
) bool {
	conflict := roachpb.MakeLock(ltValue.Txn, ltKey.Key.Clone(), ltKey.Strength)
	conflictSize := int64(conflict.Size())
	s.conflictBytes += conflictSize
	s.conflicts = append(s.conflicts, conflict)
	if s.maxConflicts != 0 && s.maxConflicts == int64(len(s.conflicts)) {
		return false
	}
	if s.targetBytesPerConflict != 0 && s.conflictBytes >= s.targetBytesPerConflict {
		return false
	}
	return true
}

// foundOwn returns the lock table value for the provided strength if the
// transaction has already acquired a lock of that strength. Returns nil if not.
func (s *lockTableKeyScanner) foundOwn(str lock.Strength) *enginepb.MVCCMetadata {
	return s.ownLocks[replicatedLockStrengthToIndexMap[str]]
}
