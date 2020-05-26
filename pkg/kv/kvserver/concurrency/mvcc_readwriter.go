// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package concurrency

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// Implements a reader-writer for use by a request that operates on MVCC key-values.
// It hides MVCCMetadata from the request processing, and updating of MVCCStats.
// It assumes no conflicting locks can be found since the lockTable has already
// been used for concurrency control.
// It replaces most of the complex code in the storage package, in mvcc.go and
// pebble_mvcc_scanner.go. And the code duplication across the various functions
// in those files is also eliminated -- callers doing a get, scan or put will
// all use MVCCReadWriter both for iterating and when needed a Put. The caller
// needs to obey a contract when doing Put: the iterator must be positioned at
// the most recent version of the key being Put, if the key has any version.
//
// The following interface is not for intent resolution which is also currently
// implemented in mvcc.go. There is not any simplification possible there since
// the complexity there is in handling of epochs, changing the timestamp of
// the provisional value, handling savepoint rollbacks. We'll simply lift that
// implementation out of mvcc.go and into the lockTable.
type MVCCReadWriter interface {
	// SeekGE advances the iterator to the first key which is >= the provided
	// key.
	SeekGE(key roachpb.Key)
	// SeekLT advances the iterator to the first key which is < the provided key.
	SeekLT(key roachpb.Key)
	// Valid must be called after any call to SeekGE(), SeekLT(), Next(),
	// Prev(). It returns (true, nil) if the iterator points to a valid key. It
	// returns (false, nil) if the iterator has moved past the end of the valid
	// range, or (false, err) if an error has occurred. Valid() will never
	// return true with a non-nil error.
	Valid() (bool, error)
	// NextKey advances the iterator to the next key/value in the
	// iteration. After this call, Valid() will be true if the
	// iterator was not previously positioned at the last key.
	NextKey()
	// PrevKey moves the iterator backward to the previous key/value
	// in the iteration. After this call, Valid() will be true if the
	// iterator was not previously positioned at the first key.
	PrevKey()
	// UnsafeKey returns the Key, but the memory is invalidated on
	// the next call to {NextKey,PrevKey,SeekGE,SeekLT,Close}.
	UnsafeKey() storage.MVCCKey
	// UnsafeValue returns the same value as Value, but the memory is
	// invalidated on the next call to {NextKey,PrevKey,SeekGE,SeekLT,Close}.
	UnsafeValue() []byte
	// IsMyKeyValue() returns true iff the key-value that is being returned by
	// Unsafe{Key, Value} was written by the transaction that is using this
	// MVCCReadWriter.
	IsMyKeyValue() bool
	// SetUpperBound installs a new upper bound for this iterator.
	SetUpperBound(roachpb.Key)

	// Put sets the given key to the value provided. The caller must have
	// positioned the iterator at the most recent version of key.key, if there
	// are any versions. And SetUpperBound() must have been called to ensure
	// the iterator is invalid if there was no version.
	// Note that the most recent version can be an uncommitted version from the
	// same txn.
	// If there is a most recent version the caller must set key.Timestamp to:
	// - !IsMyKeyValue(): > UnsafeKey().Timestamp
	// - IsMyKeyValue(): >= UnsafeKey().Timestamp.
	// The txnMeta parameter must be consistent with this Put.
	//
	// It is safe to modify the contents of the arguments after Put returns.
	Put(key storage.MVCCKey, value []byte, txnMeta *enginepb.TxnMeta) error

	// This closes the MVCCReadWriter. It does not commit anything written to
	// the storage.ReadWriter that the MVCCReadWriter was initialized with. It
	// is the caller's responsibility to commit if it wishes to.
	Close()
}

// TODO: implement itersBeforeSeek optimization
// TODO: implement Reverse iteration.
// TODO: add call to storage.Writer.LogLogicalOp in Put
// TODO: implement an optional optimistic mode when intentAwareReadWriter.readOnly
// is true. Will need to create intentIter even in the request is non-transactional
// and return a distinguishable error that the caller can use to retry
// pessimistically.

// Implementation of MVCCReadWriter.
//
// - It currently does not interact directly with lockTable, since we do not
//   allow multiple intents to exist for the same key in the lockTable. This
//   will eventually change, and this implementation will use the lockTable's
//   in-memory state of intents that are committed to also adjust the
//   timestamps of the provisional values from other transactions that have
//   committed but whose intents have not yet been removed. We do not expect
//   this implementation will inform the lockTable of new intents it is adding
//   since those intents have not yet being committed via state machine
//   replication.
// -
type intentAwareReadWriter struct {
	lowerBound roachpb.Key
	upperBound roachpb.Key

	writer storage.Writer
	// Non-nil for a transactional read or write.
	txn *roachpb.Transaction
	// The stats to update for a Put. nil when readOnly is true.
	ms *enginepb.MVCCStats
	// Set to true, if there will never be a Put.
	readOnly bool
	// If txn != nil, this is equal to txn.ReadTimestamp.
	// NB: There is no configured writeTimestamp. As mentioned earlier,
	// it is the caller's responsibility to set an appropriate
	// timestamp in the Put call.
	readTimestamp hlc.Timestamp
	// Set when readOnly && txn != nil && readTimestamp < txn.MaxTimestamp
	checkUncertainty bool
	// Set when !readOnly or forceFindMostRecent, so that the iterator is
	// positioned at the most recent committed value when this txn does not
	// already have an intent. Note that for !readOnly, and when this txn
	// already has an intent, it is the caller's responsibility to ensure that
	// rw.txn.Sequence < rw.curMeta.Txn.Sequence since earlier seqs of the txn
	// should not be executing after later seqs.
	findMostRecent bool

	// The iterator in the key-value space.
	valueIter                 storage.Iterator
	valueIterAlreadyAtNextKey bool
	// The iterator in the lock space for reading replicated exclusive locks
	// that also function as intents.
	intentIter storage.Iterator
	// When the intentIter is valid, the decoded key, from the key-value
	// space is placed in lockedKey. Depending on the iteration direction, this
	// key is equal to or ahead of the key at which valueIter is positioned (when
	// !valueIterAlreadyAtNextKey).
	lockedKey       roachpb.Key
	lockedKeyIsMine bool
	// True when intentIter and valueIter are at the same key.
	itersAreEqual bool
	// True iff itersAreEqual and the intent is for the same transaction.
	posOnOwnIntent bool
	// When posOnOwnIntent is true, the MVCCMetadata is decoded into curMeta
	curMeta enginepb.MVCCMetadata
	// If posOnOwnIntent && valueFromIntent, the caller has read an older version
	// written by this txn. That value is placed in intentValue.
	// If posOnOwnIntent && !valueFromIntent, the valueIterValueIsMine represents
	// whether the value at which valueIter is positioned is written by this
	// txn.
	valueFromIntent      bool
	intentValue          []byte
	valueIterValueIsMine bool

	// notValid is true when the iterator is no longer positioned at
	// a valid entry. err != nil implies notValid, but the reverse is not
	// true, since the caller can change direction or seek again.
	notValid bool
	err      error

	keyBuf          []byte
	metaBuf         []byte
	implicitOldMeta enginepb.MVCCMetadata
	newMeta         enginepb.MVCCMetadata
}

var _ MVCCReadWriter = &intentAwareReadWriter{}

// intentAwareReadWriter is only for request processing.
// Do not set opts.{Min, Max}TimestampHint -- they are not supported. For time
// bound iterators, which are request agnostic, there will be a different
// interface and implementation that uses the fact that the lockTable has already
// been used to confirm that there are no intents below the MaxTimestampHint.
func CreateMVCCReadWriter(
	reader storage.Reader,
	writer storage.Writer,
	opts storage.IterOptions,
	txn *roachpb.Transaction,
	ms *enginepb.MVCCStats,
	readTimestamp hlc.Timestamp,
	forceFindMostRecent bool,
) *intentAwareReadWriter {
	if opts.LowerBound == nil || opts.UpperBound == nil {
		panic("we need bounds to not traverse into non-MVCC parts of the key space")
	}
	var intentIter storage.Iterator
	if txn != nil {
		intentOpts := opts
		intentOpts.LowerBound = keys.MakeLockTableKeyPrefix(opts.LowerBound)
		intentOpts.UpperBound = keys.MakeLockTableKeyPrefix(opts.UpperBound)
		intentIter = reader.NewIterator(intentOpts)
	}
	mvccRW := &intentAwareReadWriter{
		writer:           writer,
		txn:              txn,
		ms:               ms,
		readOnly:         ms == nil,
		readTimestamp:    readTimestamp,
		checkUncertainty: ms == nil && txn != nil && readTimestamp.Less(txn.MaxTimestamp),
		findMostRecent:   ms != nil || forceFindMostRecent,
		valueIter:        reader.NewIterator(opts),
		intentIter:       intentIter,
	}
	return mvccRW
}

func (rw *intentAwareReadWriter) SeekGE(key roachpb.Key) {
	rw.notValid = false
	if rw.txn != nil {
		// Position the intentIter.
		intentSeekKey := keys.LockTableKeyExclusive(key, rw.txn.ID)
		// It is odd to use an interface that expects an MVCCKey when we are
		// seeking in a key space that is not MVCC
		rw.intentIter.SeekGE(storage.MVCCKey{Key: intentSeekKey})
		if err := rw.tryDecodeLockedKey(); err != nil {
			return
		}
	}
	// Position at the youngest version of key.
	rw.valueIter.SeekGE(storage.MVCCKey{Key: key})
	valid, err := rw.valueIter.Valid()
	if err != nil || !valid {
		rw.err = err
		rw.notValid = true
		return
	}
	rw.itersAreEqual = false
	rw.posOnOwnIntent = false
	rw.valueIterAlreadyAtNextKey = false
	// Iterate forward until we have extracted a value or there is an error
	// or nothing left to read.
	for !rw.extractValue() && rw.nextKey() {
	}
}

// Must be called with a valid valueIter.
// Returns true when successful, or done (due to error or nothing to read).
// Returns false if stepped to the next key without finding a version to extract.
func (rw *intentAwareReadWriter) extractValue() bool {
	rw.valueFromIntent = false
	rw.valueIterValueIsMine = false
	rw.itersAreEqual = rw.lockedKey != nil && rw.lockedKey.Equal(rw.valueIter.UnsafeKey().Key)
	// Find existing intent for this txn, if any.
	if rw.txn != nil {
		if !rw.itersAreEqual || !rw.lockedKeyIsMine {
			// No intent, or not my intent. If it is not my intent, we've already dealt
			// with it in lockTable, with one of two cases:
			// - this must be a non-locking read-only request and the intent must be at
			//   a higher timestamp.
			// - read-only requests that are acquiring unreplicated locks set
			//   forceFindMostRecent but they have seen conflicting intents.
			return rw.extractOtherFromValueIter()
		}
		// Found our own intent.
		if err := rw.intentIter.ValueProto(&rw.curMeta); err != nil {
			rw.err = err
			rw.notValid = true
			return true
		}
		rw.posOnOwnIntent = true
		// Don't need to check uncertainty, or find more recent.
		if rw.txn.Epoch < rw.curMeta.Txn.Epoch {
			// We're reading our own txn's intent but the current txn has
			// an earlier epoch than the intent. Return an error so that the
			// earlier incarnation of our transaction aborts (presumably
			// this is some operation that was retried).
			rw.err = errors.Errorf("failed to read with epoch %d due to a write intent with epoch %d",
				rw.txn.Epoch, rw.curMeta.Txn.Epoch)
			rw.notValid = true
			return true
		}
		if rw.txn.Epoch == rw.curMeta.Txn.Epoch {
			if rw.txn.Sequence >= rw.curMeta.Txn.Sequence && !enginepb.TxnSeqIsIgnored(rw.curMeta.Txn.Sequence, rw.txn.IgnoredSeqNums) {
				// We're reading our own txn's intent at an equal or higher sequence.
				// Note that we read at the intent timestamp, not at our read timestamp
				// as the intent timestamp may have been pushed forward by another
				// transaction. Txn's always need to read their own writes.
				rw.extractOwnFromValueIter(hlc.Timestamp(rw.curMeta.Timestamp))
				return true
			}
			// We're reading our own txn's intent at a lower sequence than is
			// currently present in the provisional value. This means the intent
			// we're seeing was written at a higher sequence than the read and that
			// there may or may not be earlier versions of the intent (with lower
			// sequence numbers) that we should read. If there exists a value in the
			// intent history that has a sequence number equal to or less than the
			// read sequence, read that value.
			if found := rw.getFromIntentHistory(); found {
				return true
			}
			// If no value in the intent history has a sequence number equal to
			// or less than the read, we must ignore the intents laid down by the
			// transaction all together. We ignore the intent by insisting that the
			// timestamp we're reading at is a historical timestamp < the intent
			// timestamp.
			return rw.extractOtherUsingTsFromValueIter(hlc.Timestamp(rw.curMeta.Timestamp).Prev())
		}
		// We're reading our own txn's intent but the current txn has a
		// later epoch than the intent. This can happen if the txn was
		// restarted and an earlier iteration wrote the value we're now
		// reading. In this case, we ignore the intent and read the
		// previous value as if the transaction were starting fresh.
		return rw.extractOtherUsingTsFromValueIter(hlc.Timestamp(rw.curMeta.Timestamp).Prev())
	}
	// There is no intent.
	return rw.extractOtherFromValueIter()
}

// Must be called with a valid valueIter. Returns true if successful or
// iterator is done (due to error or nothing to read). Return false if
// could not find a version to read and the iterator is not done.
func (rw *intentAwareReadWriter) extractOtherFromValueIter() bool {
	if rw.findMostRecent {
		// Already positioned at latest version
		return true
	}
	copiedToKeyBuf := false
	if rw.checkUncertainty {
		for {
			valueTS := rw.valueIter.UnsafeKey().Timestamp
			if rw.txn.MaxTimestamp.Less(valueTS) {
				if !copiedToKeyBuf {
					copiedToKeyBuf = true
					rw.keyBuf = append(rw.keyBuf[:0], rw.valueIter.UnsafeKey().Key...)
				}
				if !rw.nextVersionValueIter() {
					return rw.notValid
				}
			} else if valueTS.LessEq(rw.txn.MaxTimestamp) && rw.readTimestamp.Less(valueTS) {
				rw.err = roachpb.NewReadWithinUncertaintyIntervalError(
					rw.readTimestamp, valueTS, rw.txn)
				rw.notValid = true
				return true
			} else {
				break
			}
		}
	}
	for rw.readTimestamp.Less(rw.valueIter.UnsafeKey().Timestamp) {
		if !copiedToKeyBuf {
			copiedToKeyBuf = true
			rw.keyBuf = append(rw.keyBuf[:0], rw.valueIter.UnsafeKey().Key...)
		}
		if !rw.nextVersionValueIter() {
			return rw.notValid
		}
	}
	return true
}

// Returns false if there is an error, or there are no more versions of the key.
// rw.keyBuf must already contain a saved version of the current key.
func (rw *intentAwareReadWriter) nextVersionValueIter() bool {
	rw.valueIter.Next()
	valid, err := rw.valueIter.Valid()
	if err != nil || !valid {
		rw.err = err
		rw.notValid = true
		return false
	}
	if !rw.valueIter.UnsafeKey().Key.Equal(rw.keyBuf) {
		rw.valueIterAlreadyAtNextKey = true
		return false
	}
	return true
}

// It is an error if there is no value at ts.
func (rw *intentAwareReadWriter) extractOwnFromValueIter(ts hlc.Timestamp) {
	// The intent ts must be the most recent version, which we are already it.
	if rw.valueIter.UnsafeKey().Timestamp != ts {
		rw.err = errors.Errorf("corruption or bug: found ts %s instead of expected %s",
			rw.valueIter.UnsafeKey().Timestamp, ts)
		rw.notValid = true
	}
	rw.valueIterValueIsMine = true
	return
}

// Returns true when successful, or done (due to error or nothing to read).
// Returns false if stepped to the next key without finding a version to extract.
func (rw *intentAwareReadWriter) extractOtherUsingTsFromValueIter(ts hlc.Timestamp) bool {
	copiedToKeyBuf := false
	for {
		valueTS := rw.valueIter.UnsafeKey().Timestamp
		if valueTS.LessEq(ts) {
			return true
		}
		if !copiedToKeyBuf {
			copiedToKeyBuf = true
			rw.keyBuf = append(rw.keyBuf[:0], rw.valueIter.UnsafeKey().Key...)
		}
		if !rw.nextVersionValueIter() {
			return rw.notValid
		}
	}
}

func (rw *intentAwareReadWriter) getFromIntentHistory() bool {
	intentHistory := rw.curMeta.IntentHistory
	// upIdx is the index of the first intent in intentHistory with a sequence
	// number greater than our transaction's sequence number. Subtract 1 from it
	// to get the index of the intent with the highest sequence number that is
	// still less than or equal to p.txnSeq.
	upIdx := sort.Search(len(intentHistory), func(i int) bool {
		return intentHistory[i].Sequence > rw.txn.Sequence
	})
	// If the candidate intent has a sequence number that is ignored by this txn,
	// iterate backward along the sorted intent history until we come across an
	// intent which isn't ignored.
	//
	// TODO(itsbilal): Explore if this iteration can be improved through binary
	// search.
	for upIdx > 0 && enginepb.TxnSeqIsIgnored(
		rw.curMeta.IntentHistory[upIdx-1].Sequence, rw.txn.IgnoredSeqNums) {
		upIdx--
	}
	if upIdx == 0 {
		// It is possible that no intent exists such that the sequence is less
		// than the read sequence, and is not ignored by this transaction.
		// In this case, we cannot read a value from the intent history.
		return false
	}
	intent := &rw.curMeta.IntentHistory[upIdx-1]
	rw.valueFromIntent = true
	rw.intentValue = intent.Value
	return true
}

func (rw *intentAwareReadWriter) NextKey() {
	success := rw.nextKey()
	for success && !rw.extractValue() {
		success = rw.nextKey()
	}
}

// Moves to the next key. Returns true if successful, and false if there is an
// error or nothing left to read.
func (rw *intentAwareReadWriter) nextKey() bool {
	if !rw.valueIterAlreadyAtNextKey {
		rw.valueIter.NextKey()
	} else {
		rw.valueIterAlreadyAtNextKey = false
	}
	valid, err := rw.valueIter.Valid()
	if err != nil || !valid {
		rw.err = err
		rw.notValid = true
		return false
	}
	if rw.itersAreEqual {
		rw.intentIter.Next()
		rw.itersAreEqual = false
		rw.posOnOwnIntent = false
		if err := rw.tryDecodeLockedKey(); err != nil {
			return false
		}
	}
	// Else the intentIter was already ahead of the valueIter, so nothing
	// to do.
	return true
}

func (rw *intentAwareReadWriter) tryDecodeLockedKey() error {
	valid, err := rw.intentIter.Valid()
	if err != nil {
		rw.err = err
		rw.notValid = true
		return err
	}
	if !valid {
		rw.lockedKey = nil
		return nil
	}
	var txnID uuid.UUID
	rw.lockedKey, _, txnID, err = keys.DecodeLockTableKey(rw.intentIter.UnsafeKey().Key)
	if err != nil {
		rw.err = err
		rw.notValid = true
		return err
	}
	rw.lockedKeyIsMine = rw.txn != nil && txnID == rw.txn.ID
	return nil
}

func (rw *intentAwareReadWriter) Valid() (bool, error) {
	return rw.notValid, rw.err
}

func (rw *intentAwareReadWriter) UnsafeKey() storage.MVCCKey {
	if rw.valueFromIntent {
		return storage.MVCCKey{
			Key:       rw.intentIter.Key().Key,
			Timestamp: hlc.Timestamp(rw.curMeta.Timestamp),
		}
	}
	return rw.valueIter.Key()
}

func (rw *intentAwareReadWriter) UnsafeValue() []byte {
	if rw.valueFromIntent {
		return rw.intentValue
	}
	return rw.valueIter.UnsafeValue()
}

func (rw *intentAwareReadWriter) IsMyKeyValue() bool {
	return rw.valueFromIntent || rw.valueIterValueIsMine
}

func (rw *intentAwareReadWriter) SetUpperBound(key roachpb.Key) {
	rw.valueIter.SetUpperBound(key)
	if rw.txn != nil {
		rw.valueIter.SetUpperBound(keys.LockTableKeyExclusive(key, rw.txn.ID))
	}
}

func (rw *intentAwareReadWriter) Put(
	key storage.MVCCKey, value []byte, txnMeta *enginepb.TxnMeta,
) error {
	rw.newMeta = enginepb.MVCCMetadata{}
	if rw.posOnOwnIntent && txnMeta.Epoch == rw.curMeta.Txn.Epoch {
		rw.newMeta.IntentHistory = rw.curMeta.IntentHistory
	}
	if rw.valueIterValueIsMine {
		// Latest value is not rolled back in a savepoint rollback.
		rw.newMeta.AddToIntentHistory(rw.curMeta.Txn.Sequence, rw.valueIter.UnsafeValue())
	}
	rw.newMeta.Txn = txnMeta
	rw.newMeta.Timestamp = hlc.LegacyTimestamp(key.Timestamp)
	rw.newMeta.KeyBytes = storage.MVCCVersionTimestampSize
	rw.newMeta.ValBytes = int64(len(value))
	rw.newMeta.Deleted = value == nil
	var metaKeySize, metaValSize int64
	fakeMetaKey := storage.MVCCKey{Key: key.Key}
	if rw.newMeta.Txn != nil {
		if err := rw.marshalMeta(&rw.newMeta); err != nil {
			return err
		}
		var metaKey storage.MVCCKey
		if rw.posOnOwnIntent {
			metaKey = rw.intentIter.Key()
		} else {
			metaKey = storage.MVCCKey{Key: keys.LockTableKeyExclusive(key.Key, rw.txn.ID)}
		}
		if err := rw.writer.Put(metaKey, rw.metaBuf); err != nil {
			return err
		}
		metaKeySize = int64(fakeMetaKey.EncodedSize())
		metaValSize = int64(len(rw.metaBuf))
	} else {
		// Per-key stats count the full-key once and MVCCVersionTimestampSize for
		// each versioned value. We maintain that accounting even when the MVCC
		// metadata is implicit.
		metaKeySize = int64(fakeMetaKey.EncodedSize())
	}

	// Write the mvcc value.
	if rw.posOnOwnIntent && hlc.Timestamp(rw.curMeta.Timestamp) != key.Timestamp {
		if err := rw.writer.Clear(
			storage.MVCCKey{Key: key.Key, Timestamp: hlc.Timestamp(rw.curMeta.Timestamp)}); err != nil {
			return err
		}
	}
	if err := rw.writer.Put(key, value); err != nil {
		return err
	}

	// Update MVCC stats.
	if rw.ms != nil {
		var oldMeta *enginepb.MVCCMetadata
		var prevValSize, origMetaKeySize, origMetaValSize int64
		origMetaKeySize = int64(fakeMetaKey.EncodedSize())
		if rw.posOnOwnIntent {
			oldMeta = &rw.curMeta
			// We are replacing our own write intent. If we are not writing at the
			// same timestamp we must take care with MVCCStats: If the older write
			// intent has a version underneath it, we need to read its size because
			// its GCBytesAge contribution may change as we move the intent above
			// it. A similar phenomenon occurs in intent resolution.
			if hlc.Timestamp(oldMeta.Timestamp).Less(key.Timestamp) {
				if !rw.valueFromIntent && !rw.valueIterValueIsMine {
					// We already tried to read the previous value written by a
					// different transaction.
					if !rw.notValid {
						prevValSize = int64(len(rw.valueIter.UnsafeValue()))
					}
				} else {
					// rw.valueFroIntent || rw.valueIterValueIsMine.
					// The valueIter is positioned at our provisional value.
					if rw.nextVersionValueIter() {
						prevValSize = int64(len(rw.valueIter.UnsafeValue()))
					}
				}
			}
			origMetaValSize = int64(len(rw.intentIter.UnsafeValue()))
		} else {
			rw.initImplicitOldMeta()
			oldMeta = &rw.implicitOldMeta
			origMetaValSize = 0
		}
		rw.ms.Add(updateStatsOnPut(key.Key, prevValSize, origMetaKeySize, origMetaValSize,
			metaKeySize, metaValSize, oldMeta, &rw.newMeta))
	}
	return nil
}

func (rw *intentAwareReadWriter) initImplicitOldMeta() {
	rw.implicitOldMeta.Reset()
	// For values, the size of keys is always accounted for as
	// MVCCVersionTimestampSize. The size of the metadata key is
	// accounted for separately.
	if valid, _ := rw.Valid(); valid {
		rw.implicitOldMeta.KeyBytes = storage.MVCCVersionTimestampSize
		rw.implicitOldMeta.ValBytes = int64(len(rw.UnsafeValue()))
		rw.implicitOldMeta.Timestamp = hlc.LegacyTimestamp(rw.UnsafeKey().Timestamp)
	}
	rw.implicitOldMeta.Deleted = rw.implicitOldMeta.ValBytes == 0
}

func (rw *intentAwareReadWriter) marshalMeta(meta *enginepb.MVCCMetadata) error {
	size := meta.Size()
	if cap(rw.metaBuf) < size {
		rw.metaBuf = make([]byte, size)
	} else {
		rw.metaBuf = rw.metaBuf[:size]
	}
	_, err := protoutil.MarshalTo(meta, rw.metaBuf)
	if err != nil {
		return err
	}
	return nil
}

// isSysLocal returns whether the whether the key is system-local.
func isSysLocal(key roachpb.Key) bool {
	return key.Compare(keys.LocalMax) < 0
}

// This is copied from mvcc.go. This is too complicated and I am
// inclined towards changing how stats are kept, but I am unclear
// on compatibility issues that may arise
//
// updateStatsOnPut updates stat counters for a newly put value,
// including both the metadata key & value bytes and the mvcc
// versioned value's key & value bytes. If the value is not a
// deletion tombstone, updates the live stat counters as well.
// If this value is an intent, updates the intent counters.
func updateStatsOnPut(
	key roachpb.Key,
	prevValSize int64,
	origMetaKeySize, origMetaValSize, metaKeySize, metaValSize int64,
	orig, meta *enginepb.MVCCMetadata,
) enginepb.MVCCStats {
	var ms enginepb.MVCCStats

	if isSysLocal(key) {
		// Handling system-local keys is straightforward because
		// we don't track ageable quantities for them (we
		// could, but don't). Remove the contributions from the
		// original, if any, and add in the new contributions.
		if orig != nil {
			ms.SysBytes -= origMetaKeySize + origMetaValSize
			if orig.Txn != nil {
				// If the original value was an intent, we're replacing the
				// intent. Note that since it's a system key, it doesn't affect
				// IntentByte, IntentCount, and correspondingly, IntentAge.
				ms.SysBytes -= orig.KeyBytes + orig.ValBytes
			}
			ms.SysCount--
		}
		ms.SysBytes += meta.KeyBytes + meta.ValBytes + metaKeySize + metaValSize
		ms.SysCount++
		return ms
	}

	// Handle non-sys keys. This follows the same scheme: if there was a previous
	// value, perhaps even an intent, subtract its contributions, and then add the
	// new contributions. The complexity here is that we need to properly update
	// GCBytesAge and IntentAge, which don't follow the same semantics. The difference
	// between them is that an intent accrues IntentAge from its own timestamp on,
	// while GCBytesAge is accrued by versions according to the following rules:
	// 1. a (non-tombstone) value that is shadowed by a newer write accrues age at
	// 	  the point in time at which it is shadowed (i.e. the newer write's timestamp).
	// 2. a tombstone value accrues age at its own timestamp (note that this means
	//    the tombstone's own contribution only -- the actual write that was deleted
	//    is then shadowed by this tombstone, and will thus also accrue age from
	//    the tombstone's value on, as per 1).
	//
	// This seems relatively straightforward, but only because it omits pesky
	// details, which have been relegated to the comments below.

	// Remove current live counts for this key.
	if orig != nil {
		ms.KeyCount--

		// Move the (so far empty) stats to the timestamp at which the
		// previous entry was created, which is where we wish to reclassify
		// its contributions.
		ms.AgeTo(orig.Timestamp.WallTime)

		// If the original metadata for this key was an intent, subtract
		// its contribution from stat counters as it's being replaced.
		if orig.Txn != nil {
			// Subtract counts attributable to intent we're replacing.
			ms.ValCount--
			ms.IntentBytes -= (orig.KeyBytes + orig.ValBytes)
			ms.IntentCount--
		}

		// If the original intent is a deletion, we're removing the intent. This
		// means removing its contribution at the *old* timestamp because it has
		// accrued GCBytesAge that we need to offset (rule 2).
		//
		// Note that there is a corresponding block for the case of a non-deletion
		// (rule 1) below, at meta.Timestamp.
		if orig.Deleted {
			ms.KeyBytes -= origMetaKeySize
			ms.ValBytes -= origMetaValSize

			if orig.Txn != nil {
				ms.KeyBytes -= orig.KeyBytes
				ms.ValBytes -= orig.ValBytes
			}
		}

		// Rule 1 implies that sometimes it's not only the old meta and the new meta
		// that matter, but also the version below both of them. For example, take
		// a version at t=1 and an intent over it at t=2 that is now being replaced
		// (t=3). Then orig.Timestamp will be 2, and meta.Timestamp will be 3, but
		// rule 1 tells us that for the interval [2,3) we have already accrued
		// GCBytesAge for the version at t=1 that is now moot, because the intent
		// at t=2 is moving to t=3; we have to emit a GCBytesAge offset to that effect.
		//
		// The code below achieves this by making the old version live again at
		// orig.Timestamp, and then marking it as shadowed at meta.Timestamp below.
		// This only happens when that version wasn't a tombstone, in which case it
		// contributes from its own timestamp on anyway, and doesn't need adjustment.
		//
		// Note that when meta.Timestamp equals orig.Timestamp, the computation is
		// moot, which is something our callers may exploit (since retrieving the
		// previous version is not for free).
		prevIsValue := prevValSize > 0

		if prevIsValue {
			// If the previous value (exists and) was not a deletion tombstone, make it
			// live at orig.Timestamp. We don't have to do anything if there is a
			// previous value that is a tombstone: according to rule two its age
			// contributions are anchored to its own timestamp, so moving some values
			// higher up doesn't affect the contributions tied to that key.
			ms.LiveBytes += storage.MVCCVersionTimestampSize + prevValSize
		}

		// Note that there is an interesting special case here: it's possible that
		// meta.Timestamp.WallTime < orig.Timestamp.WallTime. This wouldn't happen
		// outside of tests (due to our semantics of txn.ReadTimestamp, which never
		// decreases) but it sure does happen in randomized testing. An earlier
		// version of the code used `Forward` here, which is incorrect as it would be
		// a no-op and fail to subtract out the intent bytes/GC age incurred due to
		// removing the meta entry at `orig.Timestamp` (when `orig != nil`).
		ms.AgeTo(meta.Timestamp.WallTime)

		if prevIsValue {
			// Make the previous non-deletion value non-live again, as explained in the
			// sibling block above.
			ms.LiveBytes -= storage.MVCCVersionTimestampSize + prevValSize
		}

		// If the original version wasn't a deletion, it becomes non-live at meta.Timestamp
		// as this is where it is shadowed.
		if !orig.Deleted {
			ms.LiveBytes -= orig.KeyBytes + orig.ValBytes
			ms.LiveBytes -= origMetaKeySize + origMetaValSize
			ms.LiveCount--

			ms.KeyBytes -= origMetaKeySize
			ms.ValBytes -= origMetaValSize

			if orig.Txn != nil {
				ms.KeyBytes -= orig.KeyBytes
				ms.ValBytes -= orig.ValBytes
			}
		}
	} else {
		ms.AgeTo(meta.Timestamp.WallTime)
	}

	// If the new version isn't a deletion tombstone, add it to live counters.
	if !meta.Deleted {
		ms.LiveBytes += meta.KeyBytes + meta.ValBytes + metaKeySize + metaValSize
		ms.LiveCount++
	}
	ms.KeyBytes += meta.KeyBytes + metaKeySize
	ms.ValBytes += meta.ValBytes + metaValSize
	ms.KeyCount++
	ms.ValCount++
	if meta.Txn != nil {
		ms.IntentBytes += meta.KeyBytes + meta.ValBytes
		ms.IntentCount++
	}

	return ms
}

func (rw *intentAwareReadWriter) Close() {
	rw.valueIter.Close()
	if rw.txn != nil {
		rw.intentIter.Close()
	}
}

func (rw *intentAwareReadWriter) SeekLT(key roachpb.Key) {
	// TODO
}

func (rw *intentAwareReadWriter) PrevKey() {
	// TODO
}
