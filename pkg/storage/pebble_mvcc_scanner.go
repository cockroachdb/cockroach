// Copyright 2019 The Cockroach Authors.
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
	"encoding/binary"
	"sort"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/observedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

const (
	maxItersBeforeSeek = 10
)

// Struct to store MVCCScan / MVCCGet in the same binary format as that
// expected by MVCCScanDecodeKeyValue.
type pebbleResults struct {
	count int64
	bytes int64
	repr  []byte
	bufs  [][]byte
}

func (p *pebbleResults) clear() {
	*p = pebbleResults{}
}

// The repr that MVCCScan / MVCCGet expects to provide as output goes:
// <valueLen:Uint32><keyLen:Uint32><Key><Value>
// This function adds to repr in that format.
func (p *pebbleResults) put(key []byte, value []byte) {
	// Key value lengths take up 8 bytes (2 x Uint32).
	const kvLenSize = 8
	const minSize = 16
	const maxSize = 128 << 20 // 128 MB

	// We maintain a list of buffers, always encoding into the last one (a.k.a.
	// pebbleResults.repr). The size of the buffers is exponentially increasing,
	// capped at maxSize. The exponential increase allows us to amortize the
	// cost of the allocation over multiple put calls. If this (key, value) pair
	// needs capacity greater than maxSize, we allocate exactly the size needed.
	lenKey := len(key)
	lenToAdd := kvLenSize + lenKey + len(value)
	if len(p.repr)+lenToAdd > cap(p.repr) {
		newSize := 2 * cap(p.repr)
		if newSize == 0 || newSize > maxSize {
			// If the previous buffer exceeded maxSize, we don't double its capacity
			// for next allocation, and instead reset the exponential increase, in
			// case we had a stray huge key-value.
			newSize = minSize
		}
		if lenToAdd >= maxSize {
			newSize = lenToAdd
		} else {
			for newSize < lenToAdd && newSize < maxSize {
				newSize *= 2
			}
		}
		if len(p.repr) > 0 {
			p.bufs = append(p.bufs, p.repr)
		}
		p.repr = nonZeroingMakeByteSlice(newSize)[:0]
	}

	startIdx := len(p.repr)
	p.repr = p.repr[:startIdx+lenToAdd]
	binary.LittleEndian.PutUint32(p.repr[startIdx:], uint32(len(value)))
	binary.LittleEndian.PutUint32(p.repr[startIdx+4:], uint32(lenKey))
	copy(p.repr[startIdx+kvLenSize:], key)
	copy(p.repr[startIdx+kvLenSize+lenKey:], value)
	p.count++
	p.bytes += int64(lenToAdd)
}

func (p *pebbleResults) finish() [][]byte {
	if len(p.repr) > 0 {
		p.bufs = append(p.bufs, p.repr)
		p.repr = nil
	}
	return p.bufs
}

// Go port of mvccScanner in libroach/mvcc.h. Stores all variables relating to
// one MVCCGet / MVCCScan call.
type pebbleMVCCScanner struct {
	parent  MVCCIterator
	reverse bool
	peeked  bool
	// Iteration bounds. Does not contain MVCC timestamp.
	start, end roachpb.Key
	// Timestamp with which MVCCScan/MVCCGet was called.
	ts hlc.Timestamp
	// Max number of keys to return. Note that targetBytes below is implemented
	// by mutating maxKeys. (In particular, one must not assume that if maxKeys
	// is zero initially it will always be zero).
	maxKeys int64
	// Stop adding keys once p.result.bytes matches or exceeds this threshold,
	// if nonzero.
	targetBytes int64
	// Stop adding intents and abort scan once maxIntents threshold is reached.
	// This limit is only applicable to consistent scans since they return
	// intents as an error.
	// Not used in inconsistent scans.
	// Ignored if zero.
	maxIntents int64
	// Transaction epoch and sequence number.
	txn               *roachpb.Transaction
	txnEpoch          enginepb.TxnEpoch
	txnSequence       enginepb.TxnSeq
	txnIgnoredSeqNums []enginepb.IgnoredSeqNumRange
	// Uncertainty related fields.
	localUncertaintyLimit  hlc.Timestamp
	globalUncertaintyLimit hlc.Timestamp
	checkUncertainty       bool
	// Metadata object for unmarshalling intents.
	meta enginepb.MVCCMetadata
	// Bools copied over from MVCC{Scan,Get}Options. See the comment on the
	// package level MVCCScan for what these mean.
	inconsistent, tombstones bool
	failOnMoreRecent         bool
	isGet                    bool
	keyBuf                   []byte
	savedBuf                 []byte
	// cur* variables store the "current" record we're pointing to. Updated in
	// updateCurrent. Note that the timestamp can be clobbered in the case of
	// adding an intent from the intent history but is otherwise meaningful.
	curUnsafeKey MVCCKey
	curRawKey    []byte
	curValue     []byte
	results      pebbleResults
	intents      pebble.Batch
	// mostRecentTS stores the largest timestamp observed that is equal to or
	// above the scan timestamp. Only applicable if failOnMoreRecent is true. If
	// set and no other error is hit, a WriteToOld error will be returned from
	// the scan.
	mostRecentTS hlc.Timestamp
	// Stores any error returned. If non-nil, iteration short circuits.
	err error
	// Number of iterations to try before we do a Seek/SeekReverse. Stays within
	// [1, maxItersBeforeSeek] and defaults to maxItersBeforeSeek/2 .
	itersBeforeSeek int
}

// Pool for allocating pebble MVCC Scanners.
var pebbleMVCCScannerPool = sync.Pool{
	New: func() interface{} {
		return &pebbleMVCCScanner{}
	},
}

func (p *pebbleMVCCScanner) release() {
	// Discard most memory references before placing in pool.
	*p = pebbleMVCCScanner{
		keyBuf: p.keyBuf,
	}
	pebbleMVCCScannerPool.Put(p)
}

// init sets bounds on the underlying pebble iterator, and initializes other
// fields not set by the calling method.
func (p *pebbleMVCCScanner) init(txn *roachpb.Transaction, localUncertaintyLimit hlc.Timestamp) {
	p.itersBeforeSeek = maxItersBeforeSeek / 2

	if txn != nil {
		p.txn = txn
		p.txnEpoch = txn.Epoch
		p.txnSequence = txn.Sequence
		p.txnIgnoredSeqNums = txn.IgnoredSeqNums

		p.localUncertaintyLimit = localUncertaintyLimit
		p.globalUncertaintyLimit = txn.GlobalUncertaintyLimit
		// We must check uncertainty even if p.ts.Less(localUncertaintyLimit)
		// because the local uncertainty limit cannot be applied to values with
		// synthetic timestamps.
		p.checkUncertainty = p.ts.Less(p.globalUncertaintyLimit)
	}
}

// get iterates exactly once and adds one KV to the result set.
func (p *pebbleMVCCScanner) get() {
	p.isGet = true
	p.parent.SeekGE(MVCCKey{Key: p.start})
	if !p.updateCurrent() {
		return
	}
	p.getAndAdvance()
	p.maybeFailOnMoreRecent()
}

// scan iterates until maxKeys records are in results, or the underlying
// iterator is exhausted, or an error is encountered.
func (p *pebbleMVCCScanner) scan() (*roachpb.Span, error) {
	p.isGet = false
	if p.reverse {
		if !p.iterSeekReverse(MVCCKey{Key: p.end}) {
			return nil, p.err
		}
	} else {
		if !p.iterSeek(MVCCKey{Key: p.start}) {
			return nil, p.err
		}
	}

	for p.getAndAdvance() {
	}
	p.maybeFailOnMoreRecent()

	var resume *roachpb.Span
	if p.maxKeys > 0 && p.results.count == p.maxKeys && p.advanceKey() {
		if p.reverse {
			// curKey was not added to results, so it needs to be included in the
			// resume span.
			//
			// NB: this is equivalent to:
			//  append(roachpb.Key(nil), p.curKey.Key...).Next()
			// but with half the allocations.
			curKey := p.curUnsafeKey.Key
			curKeyCopy := make(roachpb.Key, len(curKey), len(curKey)+1)
			copy(curKeyCopy, curKey)
			resume = &roachpb.Span{
				Key:    p.start,
				EndKey: curKeyCopy.Next(),
			}
		} else {
			resume = &roachpb.Span{
				Key:    append(roachpb.Key(nil), p.curUnsafeKey.Key...),
				EndKey: p.end,
			}
		}
	}
	return resume, p.err
}

// Increments itersBeforeSeek while ensuring it stays <= maxItersBeforeSeek
func (p *pebbleMVCCScanner) incrementItersBeforeSeek() {
	p.itersBeforeSeek++
	if p.itersBeforeSeek > maxItersBeforeSeek {
		p.itersBeforeSeek = maxItersBeforeSeek
	}
}

// Decrements itersBeforeSeek while ensuring it stays positive.
func (p *pebbleMVCCScanner) decrementItersBeforeSeek() {
	p.itersBeforeSeek--
	if p.itersBeforeSeek < 1 {
		p.itersBeforeSeek = 1
	}
}

// Try to read from the current value's intent history. Assumes p.meta has been
// unmarshalled already. Returns found = true if a value was found and returned.
func (p *pebbleMVCCScanner) getFromIntentHistory() (value []byte, found bool) {
	intentHistory := p.meta.IntentHistory
	// upIdx is the index of the first intent in intentHistory with a sequence
	// number greater than our transaction's sequence number. Subtract 1 from it
	// to get the index of the intent with the highest sequence number that is
	// still less than or equal to p.txnSeq.
	upIdx := sort.Search(len(intentHistory), func(i int) bool {
		return intentHistory[i].Sequence > p.txnSequence
	})
	// If the candidate intent has a sequence number that is ignored by this txn,
	// iterate backward along the sorted intent history until we come across an
	// intent which isn't ignored.
	//
	// TODO(itsbilal): Explore if this iteration can be improved through binary
	// search.
	for upIdx > 0 && enginepb.TxnSeqIsIgnored(p.meta.IntentHistory[upIdx-1].Sequence, p.txnIgnoredSeqNums) {
		upIdx--
	}
	if upIdx == 0 {
		// It is possible that no intent exists such that the sequence is less
		// than the read sequence, and is not ignored by this transaction.
		// In this case, we cannot read a value from the intent history.
		return nil, false
	}
	intent := &p.meta.IntentHistory[upIdx-1]
	return intent.Value, true
}

// Returns a write too old error if an error is not already set on the scanner
// and a more recent value was found during the scan.
func (p *pebbleMVCCScanner) maybeFailOnMoreRecent() {
	if p.err != nil || p.mostRecentTS.IsEmpty() {
		return
	}
	// The txn can't write at the existing timestamp, so we provide the error
	// with the timestamp immediately after it.
	p.err = roachpb.NewWriteTooOldError(p.ts, p.mostRecentTS.Next())
	p.results.clear()
	p.intents.Reset()
}

// Returns whether a value with the specified timestamp is within the
// transaction's uncertainty interval and is considered uncertain.
//
// REQUIRES: p.uncertaintyCheck == true
// REQUIRES: p.ts < ts
func (p *pebbleMVCCScanner) isUncertainValue(ts hlc.Timestamp) bool {
	return observedts.IsUncertain(p.localUncertaintyLimit, p.globalUncertaintyLimit, ts)
}

// Returns an uncertainty error with the specified timestamp and p.txn.
func (p *pebbleMVCCScanner) uncertaintyError(ts hlc.Timestamp) bool {
	p.err = roachpb.NewReadWithinUncertaintyIntervalError(p.ts, ts, p.localUncertaintyLimit, p.txn)
	p.results.clear()
	p.intents.Reset()
	return false
}

// Emit a tuple and return true if we have reason to believe iteration can
// continue.
func (p *pebbleMVCCScanner) getAndAdvance() bool {
	if !p.curUnsafeKey.Timestamp.IsEmpty() {
		// ts < read_ts
		if p.curUnsafeKey.Timestamp.Less(p.ts) {
			// 1. Fast path: there is no intent and our read timestamp is newer
			// than the most recent version's timestamp.
			return p.addAndAdvance(p.curRawKey, p.curValue)
		}

		// ts == read_ts
		if p.curUnsafeKey.Timestamp.EqOrdering(p.ts) {
			if p.failOnMoreRecent {
				// 2. Our txn's read timestamp is equal to the most recent
				// version's timestamp and the scanner has been configured to
				// throw a write too old error on equal or more recent versions.
				// Merge the current timestamp with the maximum timestamp we've
				// seen so we know to return an error, but then keep scanning so
				// that we can return the largest possible time.
				p.mostRecentTS.Forward(p.curUnsafeKey.Timestamp)
				return p.advanceKey()
			}

			// 3. There is no intent and our read timestamp is equal to the most
			// recent version's timestamp.
			return p.addAndAdvance(p.curRawKey, p.curValue)
		}

		// ts > read_ts
		if p.failOnMoreRecent {
			// 4. Our txn's read timestamp is less than the most recent
			// version's timestamp and the scanner has been configured to
			// throw a write too old error on equal or more recent versions.
			// Merge the current timestamp with the maximum timestamp we've
			// seen so we know to return an error, but then keep scanning so
			// that we can return the largest possible time.
			p.mostRecentTS.Forward(p.curUnsafeKey.Timestamp)
			return p.advanceKey()
		}

		if p.checkUncertainty {
			// 5. Our txn's read timestamp is less than the max timestamp
			// seen by the txn. We need to check for clock uncertainty
			// errors.
			if p.isUncertainValue(p.curUnsafeKey.Timestamp) {
				return p.uncertaintyError(p.curUnsafeKey.Timestamp)
			}

			// This value is not within the reader's uncertainty window, but
			// there could be other uncertain committed values, so seek and
			// check uncertainty using globalUncertaintyLimit.
			return p.seekVersion(p.globalUncertaintyLimit, true)
		}

		// 6. Our txn's read timestamp is greater than or equal to the
		// max timestamp seen by the txn so clock uncertainty checks are
		// unnecessary. We need to seek to the desired version of the
		// value (i.e. one with a timestamp earlier than our read
		// timestamp).
		return p.seekVersion(p.ts, false)
	}

	if len(p.curValue) == 0 {
		p.err = errors.Errorf("zero-length mvcc metadata")
		return false
	}
	err := protoutil.Unmarshal(p.curValue, &p.meta)
	if err != nil {
		p.err = errors.Errorf("unable to decode MVCCMetadata: %s", err)
		return false
	}
	if len(p.meta.RawBytes) != 0 {
		// 7. Emit immediately if the value is inline.
		return p.addAndAdvance(p.curRawKey, p.meta.RawBytes)
	}

	if p.meta.Txn == nil {
		p.err = errors.Errorf("intent without transaction")
		return false
	}
	metaTS := p.meta.Timestamp.ToTimestamp()

	// metaTS is the timestamp of an intent value, which we may or may
	// not end up ignoring, depending on factors codified below. If we do ignore
	// the intent then we want to read at a lower timestamp that's strictly
	// below the intent timestamp (to skip the intent), but also does not exceed
	// our read timestamp (to avoid erroneously picking up future committed
	// values); this timestamp is prevTS.
	prevTS := p.ts
	if metaTS.LessEq(p.ts) {
		prevTS = metaTS.Prev()
	}

	ownIntent := p.txn != nil && p.meta.Txn.ID.Equal(p.txn.ID)
	conflictingIntent := metaTS.LessEq(p.ts) || p.failOnMoreRecent

	if !ownIntent && !conflictingIntent {
		// 8. The key contains an intent, but we're reading below the intent.
		// Seek to the desired version, checking for uncertainty if necessary.
		//
		// Note that if we own the intent (i.e. we're reading transactionally)
		// we want to read the intent regardless of our read timestamp and fall
		// into case 11 below.
		if p.checkUncertainty {
			if p.isUncertainValue(metaTS) {
				return p.uncertaintyError(metaTS)
			}
			// The intent is not within the uncertainty window, but there could
			// be an uncertain committed value, so seek and check uncertainty
			// using globalUncertaintyLimit.
			return p.seekVersion(p.globalUncertaintyLimit, true)
		}
		return p.seekVersion(p.ts, false)
	}

	if p.inconsistent {
		// 9. The key contains an intent and we're doing an inconsistent
		// read at a timestamp newer than the intent. We ignore the
		// intent by insisting that the timestamp we're reading at is a
		// historical timestamp < the intent timestamp. However, we
		// return the intent separately; the caller may want to resolve
		// it.
		if p.maxKeys > 0 && p.results.count == p.maxKeys {
			// TODO(oleg): This check seems broken and it should never
			// reach this point with results count reaching max. We
			// always get out of getAndAdvance either by addAndAdvance or
			// by seekVersion which is also calling addAndAdvance.
			//
			// We've already retrieved the desired number of keys and now
			// we're adding the resume key. We don't want to add the
			// intent here as the intents should only correspond to KVs
			// that lie before the resume key.
			return false
		}
		p.err = p.intents.Set(p.curRawKey, p.curValue, nil)
		if p.err != nil {
			return false
		}

		return p.seekVersion(prevTS, false)
	}

	if !ownIntent {
		// 10. The key contains an intent which was not written by our
		// transaction and either:
		// - our read timestamp is equal to or newer than that of the
		//   intent
		// - our read timestamp is older than that of the intent but
		//   the intent is in our transaction's uncertainty interval
		// - our read timestamp is older than that of the intent but
		//   we want to fail on more recent writes
		// Note that this will trigger an error higher up the stack. We
		// continue scanning so that we can return all of the intents
		// in the scan range.
		p.err = p.intents.Set(p.curRawKey, p.curValue, nil)
		if p.err != nil {
			return false
		}
		// Limit number of intents returned in write intent error.
		if p.maxIntents > 0 && int64(p.intents.Count()) >= p.maxIntents {
			return false
		}
		return p.advanceKey()
	}

	if p.txnEpoch == p.meta.Txn.Epoch {
		if p.txnSequence >= p.meta.Txn.Sequence && !enginepb.TxnSeqIsIgnored(p.meta.Txn.Sequence, p.txnIgnoredSeqNums) {
			// 11. We're reading our own txn's intent at an equal or higher sequence.
			// Note that we read at the intent timestamp, not at our read timestamp
			// as the intent timestamp may have been pushed forward by another
			// transaction. Txn's always need to read their own writes.
			return p.seekVersion(metaTS, false)
		}

		// 12. We're reading our own txn's intent at a lower sequence than is
		// currently present in the intent. This means the intent we're seeing
		// was written at a higher sequence than the read and that there may or
		// may not be earlier versions of the intent (with lower sequence
		// numbers) that we should read. If there exists a value in the intent
		// history that has a sequence number equal to or less than the read
		// sequence, read that value.
		if value, found := p.getFromIntentHistory(); found {
			// If we're adding a value due to a previous intent, we want to populate
			// the timestamp as of current metaTimestamp. Note that this may be
			// controversial as this maybe be neither the write timestamp when this
			// intent was written. However, this was the only case in which a value
			// could have been returned from a read without an MVCC timestamp.
			//
			// Note: this assumes that it is safe to corrupt curKey here because we're
			// about to advance. If this proves to be a problem later, we can extend
			// addAndAdvance to take an MVCCKey explicitly.
			p.curUnsafeKey.Timestamp = metaTS
			p.keyBuf = EncodeKeyToBuf(p.keyBuf[:0], p.curUnsafeKey)
			return p.addAndAdvance(p.keyBuf, value)
		}
		// 13. If no value in the intent history has a sequence number equal to
		// or less than the read, we must ignore the intents laid down by the
		// transaction all together. We ignore the intent by insisting that the
		// timestamp we're reading at is a historical timestamp < the intent
		// timestamp.
		return p.seekVersion(prevTS, false)
	}

	if p.txnEpoch < p.meta.Txn.Epoch {
		// 14. We're reading our own txn's intent but the current txn has
		// an earlier epoch than the intent. Return an error so that the
		// earlier incarnation of our transaction aborts (presumably
		// this is some operation that was retried).
		p.err = errors.Errorf("failed to read with epoch %d due to a write intent with epoch %d",
			p.txnEpoch, p.meta.Txn.Epoch)
		return false
	}

	// 15. We're reading our own txn's intent but the current txn has a
	// later epoch than the intent. This can happen if the txn was
	// restarted and an earlier iteration wrote the value we're now
	// reading. In this case, we ignore the intent and read the
	// previous value as if the transaction were starting fresh.
	return p.seekVersion(prevTS, false)
}

// nextKey advances to the next user key.
func (p *pebbleMVCCScanner) nextKey() bool {
	p.keyBuf = append(p.keyBuf[:0], p.curUnsafeKey.Key...)

	for i := 0; i < p.itersBeforeSeek; i++ {
		if !p.iterNext() {
			return false
		}
		if !bytes.Equal(p.curUnsafeKey.Key, p.keyBuf) {
			p.incrementItersBeforeSeek()
			return true
		}
	}

	p.decrementItersBeforeSeek()
	// We're pointed at a different version of the same key. Fall back to
	// seeking to the next key. We append a NUL to account for the "next-key".
	// Note that we cannot rely on curUnsafeKey.Key being unchanged even though
	// we are at a different version of the same key -- the underlying
	// MVCCIterator is free to mutate the backing for p.curUnsafeKey.Key
	// arbitrarily. Therefore we use p.keyBuf here which we have handy.
	p.keyBuf = append(p.keyBuf, 0)
	return p.iterSeek(MVCCKey{Key: p.keyBuf})
}

// backwardLatestVersion backs up the iterator to the latest version for the
// specified key. The parameter i is used to maintain iteration count between
// the loop here and the caller (usually prevKey). Returns false if the
// iterator was exhausted. Assumes that the iterator is currently positioned at
// the oldest version of key.
func (p *pebbleMVCCScanner) backwardLatestVersion(key []byte, i int) bool {
	p.keyBuf = append(p.keyBuf[:0], key...)

	for ; i < p.itersBeforeSeek; i++ {
		peekedKey, ok := p.iterPeekPrev()
		if !ok {
			// No previous entry exists, so we're at the latest version of key.
			return true
		}
		if !bytes.Equal(peekedKey, p.keyBuf) {
			p.incrementItersBeforeSeek()
			return true
		}
		if !p.iterPrev() {
			return false
		}
	}

	// We're still not pointed to the latest version of the key. Fall back to
	// seeking to the latest version. Note that we cannot rely on key being
	// unchanged even though we are at a different version of the same key --
	// the underlying MVCCIterator is free to mutate the backing for key
	// arbitrarily. Therefore we use p.keyBuf here which we have handy.
	p.decrementItersBeforeSeek()
	return p.iterSeek(MVCCKey{Key: p.keyBuf})
}

// prevKey advances to the newest version of the user key preceding the
// specified key. Assumes that the iterator is currently positioned at
// key or 1 record after key.
func (p *pebbleMVCCScanner) prevKey(key []byte) bool {
	p.keyBuf = append(p.keyBuf[:0], key...)

	for i := 0; i < p.itersBeforeSeek; i++ {
		peekedKey, ok := p.iterPeekPrev()
		if !ok {
			return false
		}
		if !bytes.Equal(peekedKey, p.keyBuf) {
			return p.backwardLatestVersion(peekedKey, i+1)
		}
		if !p.iterPrev() {
			return false
		}
	}

	p.decrementItersBeforeSeek()
	return p.iterSeekReverse(MVCCKey{Key: p.keyBuf})
}

// advanceKey advances to the next key in the iterator's direction.
func (p *pebbleMVCCScanner) advanceKey() bool {
	if p.isGet {
		return false
	}
	if p.reverse {
		return p.prevKey(p.curUnsafeKey.Key)
	}
	return p.nextKey()
}

// advanceKeyAtEnd advances to the next key when the iterator's end has been
// reached.
func (p *pebbleMVCCScanner) advanceKeyAtEnd() bool {
	if p.reverse {
		// Iterating to the next key might have caused the iterator to reach the
		// end of the key space. If that happens, back up to the very last key.
		p.peeked = false
		p.parent.SeekLT(MVCCKey{Key: p.end})
		if !p.updateCurrent() {
			return false
		}
		return p.advanceKey()
	}
	// We've reached the end of the iterator and there is nothing left to do.
	return false
}

// advanceKeyAtNewKey advances to the key after the specified key, assuming we
// have just reached the specified key.
func (p *pebbleMVCCScanner) advanceKeyAtNewKey(key []byte) bool {
	if p.reverse {
		// We've advanced to the next key but need to move back to the previous
		// key.
		return p.prevKey(key)
	}
	// We're already at the new key so there is nothing to do.
	return true
}

// Adds the specified key and value to the result set, excluding tombstones unless
// p.tombstones is true. Advances to the next key unless we've reached the max
// results limit.
func (p *pebbleMVCCScanner) addAndAdvance(rawKey []byte, val []byte) bool {
	// Don't include deleted versions len(val) == 0, unless we've been instructed
	// to include tombstones in the results.
	if len(val) > 0 || p.tombstones {
		p.results.put(rawKey, val)
		if p.targetBytes > 0 && p.results.bytes >= p.targetBytes {
			// When the target bytes are met or exceeded, stop producing more
			// keys. We implement this by reducing maxKeys to the current
			// number of keys.
			//
			// TODO(bilal): see if this can be implemented more transparently.
			p.maxKeys = p.results.count
		}
		if p.maxKeys > 0 && p.results.count == p.maxKeys {
			return false
		}
	}
	return p.advanceKey()
}

// Seeks to the latest revision of the current key that's still less than or
// equal to the specified timestamp, adds it to the result set, then moves onto
// the next user key.
func (p *pebbleMVCCScanner) seekVersion(seekTS hlc.Timestamp, uncertaintyCheck bool) bool {
	seekKey := MVCCKey{Key: p.curUnsafeKey.Key, Timestamp: seekTS}
	p.keyBuf = EncodeKeyToBuf(p.keyBuf[:0], seekKey)
	origKey := p.keyBuf[:len(p.curUnsafeKey.Key)]
	// We will need seekKey below, if the next's don't suffice. Even though the
	// MVCCIterator will be at a different version of the same key, it is free
	// to mutate the backing for p.curUnsafeKey.Key in an arbitrary manner. So
	// assign to this copy, to make it stable.
	seekKey.Key = origKey

	for i := 0; i < p.itersBeforeSeek; i++ {
		if !p.iterNext() {
			return p.advanceKeyAtEnd()
		}
		if !bytes.Equal(p.curUnsafeKey.Key, origKey) {
			p.incrementItersBeforeSeek()
			return p.advanceKeyAtNewKey(origKey)
		}
		if p.curUnsafeKey.Timestamp.LessEq(seekTS) {
			p.incrementItersBeforeSeek()
			if !uncertaintyCheck || p.curUnsafeKey.Timestamp.LessEq(p.ts) {
				return p.addAndAdvance(p.curRawKey, p.curValue)
			}
			// Iterate through uncertainty interval. Though we found a value in
			// the interval, it may not be uncertainty. This is because seekTS
			// is set to the transaction's global uncertainty limit, so we are
			// seeking based on the worst-case uncertainty, but values with a
			// time in the range (localUncertaintyLimit, globalUncertaintyLimit]
			// are only uncertain if their timestamps are synthetic. Meanwhile,
			// any value with a time in the range (ts, localUncertaintyLimit]
			// is uncertain.
			if p.isUncertainValue(p.curUnsafeKey.Timestamp) {
				return p.uncertaintyError(p.curUnsafeKey.Timestamp)
			}
		}
	}

	p.decrementItersBeforeSeek()
	if !p.iterSeek(seekKey) {
		return p.advanceKeyAtEnd()
	}
	for {
		if !bytes.Equal(p.curUnsafeKey.Key, origKey) {
			return p.advanceKeyAtNewKey(origKey)
		}
		if !uncertaintyCheck || p.curUnsafeKey.Timestamp.LessEq(p.ts) {
			return p.addAndAdvance(p.curRawKey, p.curValue)
		}
		// Iterate through uncertainty interval. See the comment above about why
		// a value in this interval is not necessarily cause for an uncertainty
		// error.
		if p.isUncertainValue(p.curUnsafeKey.Timestamp) {
			return p.uncertaintyError(p.curUnsafeKey.Timestamp)
		}
		if !p.iterNext() {
			return p.advanceKeyAtEnd()
		}
	}
}

// Updates cur{RawKey, Key, TS} to match record the iterator is pointing to.
func (p *pebbleMVCCScanner) updateCurrent() bool {
	if !p.iterValid() {
		return false
	}

	p.curRawKey = p.parent.UnsafeRawMVCCKey()

	var err error
	p.curUnsafeKey, err = DecodeMVCCKey(p.curRawKey)
	if err != nil {
		panic(err)
	}
	p.curValue = p.parent.UnsafeValue()
	return true
}

func (p *pebbleMVCCScanner) iterValid() bool {
	if valid, err := p.parent.Valid(); !valid {
		// Defensive: unclear if p.err can already be non-nil here, but
		// regardless, don't overwrite unless we have a non-nil err.
		if err != nil {
			p.err = err
		}
		return false
	}
	return true
}

// iterSeek seeks to the latest revision of the specified key (or a greater key).
func (p *pebbleMVCCScanner) iterSeek(key MVCCKey) bool {
	p.clearPeeked()
	p.parent.SeekGE(key)
	return p.updateCurrent()
}

// iterSeekReverse seeks to the latest revision of the key before the specified key.
func (p *pebbleMVCCScanner) iterSeekReverse(key MVCCKey) bool {
	p.clearPeeked()
	p.parent.SeekLT(key)
	if !p.updateCurrent() {
		// We have seeked to before the start key. Return.
		return false
	}

	if p.curUnsafeKey.Timestamp.IsEmpty() {
		// We landed on an intent or inline value.
		return true
	}
	// We landed on a versioned value, we need to back up to find the
	// latest version.
	return p.backwardLatestVersion(p.curUnsafeKey.Key, 0)
}

// iterNext advances to the next MVCC key.
func (p *pebbleMVCCScanner) iterNext() bool {
	if p.reverse && p.peeked {
		// If we have peeked at the previous entry, we need to advance the iterator
		// twice.
		p.peeked = false
		if !p.iterValid() {
			// We were peeked off the beginning of iteration. Seek to the first
			// entry, and then advance one step.
			p.parent.SeekGE(MVCCKey{Key: p.start})
			if !p.iterValid() {
				return false
			}
			p.parent.Next()
			return p.updateCurrent()
		}
		p.parent.Next()
		if !p.iterValid() {
			return false
		}
	}
	p.parent.Next()
	return p.updateCurrent()
}

// iterPrev advances to the previous MVCC Key.
func (p *pebbleMVCCScanner) iterPrev() bool {
	if p.peeked {
		p.peeked = false
		return p.updateCurrent()
	}
	p.parent.Prev()
	return p.updateCurrent()
}

// Peek the previous key and store the result in peekedKey. Note that this
// moves the iterator backward, while leaving p.cur{key,value,rawKey} untouched
// and therefore out of sync. iterPrev and iterNext take this into account.
func (p *pebbleMVCCScanner) iterPeekPrev() ([]byte, bool) {
	if !p.peeked {
		p.peeked = true
		// We need to save a copy of the current iterator key and value and adjust
		// curRawKey, curKey and curValue to point to this saved data. We use a
		// single buffer for this purpose: savedBuf.
		p.savedBuf = append(p.savedBuf[:0], p.curRawKey...)
		p.savedBuf = append(p.savedBuf, p.curValue...)
		p.curRawKey = p.savedBuf[:len(p.curRawKey)]
		p.curValue = p.savedBuf[len(p.curRawKey):]
		// The raw key is always a prefix of the encoded MVCC key. Take advantage of this to
		// sub-slice the raw key directly, instead of calling SplitMVCCKey.
		p.curUnsafeKey.Key = p.curRawKey[:len(p.curUnsafeKey.Key)]

		// With the current iterator state saved we can move the iterator to the
		// previous entry.
		p.parent.Prev()
		if !p.iterValid() {
			// The iterator is now invalid, but note that this case is handled in
			// both iterNext and iterPrev. In the former case, we'll position the
			// iterator at the first entry, and in the latter iteration will be done.
			return nil, false
		}
	} else if !p.iterValid() {
		return nil, false
	}

	peekedKey := p.parent.UnsafeKey()
	return peekedKey.Key, true
}

// Clear the peeked flag. Call this before any iterator operations.
func (p *pebbleMVCCScanner) clearPeeked() {
	if p.reverse {
		p.peeked = false
	}
}

func (p *pebbleMVCCScanner) intentsRepr() []byte {
	if p.intents.Count() == 0 {
		return nil
	}
	return p.intents.Repr()
}
