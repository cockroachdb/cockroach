// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package engine

import (
	"bytes"
	"encoding/binary"
	"sort"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
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
	repr  []byte
	bufs  [][]byte
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
	// capped at maxSize.
	lenToAdd := kvLenSize + len(key) + len(value)
	if len(p.repr)+lenToAdd > cap(p.repr) {
		newSize := 2 * cap(p.repr)
		if newSize == 0 {
			newSize = minSize
		}
		for newSize < lenToAdd && newSize < maxSize {
			newSize *= 2
		}
		if len(p.repr) > 0 {
			p.bufs = append(p.bufs, p.repr)
		}
		p.repr = nonZeroingMakeByteSlice(newSize)[:0]
	}

	startIdx := len(p.repr)
	p.repr = p.repr[:startIdx+lenToAdd]
	binary.LittleEndian.PutUint32(p.repr[startIdx:], uint32(len(value)))
	binary.LittleEndian.PutUint32(p.repr[startIdx+4:], uint32(len(key)))
	copy(p.repr[startIdx+kvLenSize:], key)
	copy(p.repr[startIdx+kvLenSize+len(key):], value)
	p.count++
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
	parent  *pebble.Iterator
	reverse bool
	// Iteration bounds. Does not contain MVCC timestamp.
	start, end roachpb.Key
	// Timestamp with which MVCCScan/MVCCGet was called.
	ts hlc.Timestamp
	// Max number of keys to return.
	maxKeys int64
	// Reference to transaction record, could be nil.
	txn *roachpb.Transaction
	// Metadata object for unmarshalling intents.
	meta enginepb.MVCCMetadata
	// Bools copied over from MVCC{Scan,Get}Options. See the comment on the
	// package level MVCCScan for what these mean.
	inconsistent, tombstones    bool
	ignoreSeq, checkUncertainty bool
	keyBuf                      []byte
	// cur* variables store the "current" record we're pointing to. Updated in
	// updateCurrent. Note that curRawKey = the full encoded MVCC key, while
	// curKey = the user-key part of curRawKey (i.e. excluding the timestamp).
	curRawKey, curKey, curValue []byte
	curTS                       hlc.Timestamp
	results                     pebbleResults
	intents                     pebble.Batch
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

// init sets bounds on the underlying pebble iterator, and initializes other
// fields not set by the calling method.
func (p *pebbleMVCCScanner) init() {
	p.itersBeforeSeek = maxItersBeforeSeek / 2

	if p.txn != nil {
		p.checkUncertainty = p.ts.Less(p.txn.MaxTimestamp)
	}
}

// seekReverse seeks to the latest revision of the key before the specified key.
func (p *pebbleMVCCScanner) seekReverse(key roachpb.Key) {
	mvccKey := MVCCKey{key, hlc.Timestamp{}}
	p.keyBuf = EncodeKeyToBuf(p.keyBuf[:0], mvccKey)

	if !p.parent.SeekLT(p.keyBuf) {
		// We have seeked to before the start key. Return.
		return
	}

	mvccKey, err := DecodeMVCCKey(p.parent.Key())
	if err != nil {
		p.err = nil
		return
	}
	// Seek to the earliest revision of mvccKey.
	mvccKey.Timestamp = hlc.Timestamp{}
	p.keyBuf = EncodeKeyToBuf(p.keyBuf[:0], mvccKey)
	p.parent.SeekGE(p.keyBuf)
	p.updateCurrent()
}

// seek seeks to the latest revision of the specified key (or a greater key).
func (p *pebbleMVCCScanner) seek(key roachpb.Key) {
	p.keyBuf = EncodeKeyToBuf(p.keyBuf[:0], MVCCKey{key, hlc.Timestamp{}})
	p.parent.SeekGE(p.keyBuf)
	p.updateCurrent()
}

// scan iterates until maxKeys records are in results, or the underlying
// iterator is exhausted, or an error is encountered.
func (p *pebbleMVCCScanner) scan() {
	if p.reverse {
		p.seekReverse(p.end)
	} else {
		p.seek(p.start)
	}

	for p.results.count < p.maxKeys && p.getAndAdvance() {
	}

	if p.results.count < p.maxKeys || !p.parent.Valid() {
		// Either the iterator was exhausted or an error was encountered. This
		// means there's no point in having a resumeSpan. Set all current variables
		// to their zero values so the caller doesn't create a resumeSpan.
		p.curRawKey = nil
		p.curKey = nil
		p.curTS = hlc.Timestamp{}
	}
}

// get iterates exactly once and adds one KV to the result set.
func (p *pebbleMVCCScanner) get() {
	p.keyBuf = EncodeKeyToBuf(p.keyBuf[:0], MVCCKey{p.start, hlc.Timestamp{}})
	p.parent.SeekPrefixGE(p.keyBuf)
	p.updateCurrent()
	p.getAndAdvance()
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

// Updates cur{RawKey, Key, TS} to match record the iterator is pointing to.
func (p *pebbleMVCCScanner) updateCurrent() {
	if !p.parent.Valid() {
		return
	}

	p.curRawKey = append(p.curRawKey[:0], p.parent.Key()...)
	p.curValue = append(p.curValue[:0], p.parent.Value()...)

	mvccKey, err := DecodeMVCCKey(p.curRawKey)
	if err != nil {
		p.err = err
		return
	}

	p.curKey = mvccKey.Key
	p.curTS = mvccKey.Timestamp
}

// Advance to the next key in the iterator's direction.
func (p *pebbleMVCCScanner) advanceKey() {
	if p.reverse {
		p.prevKey()
	} else {
		p.nextKey()
	}
}

// Advance to the newest iteration of the previous user key (where user key =
// part of the MVCC Key that precedes the timestamp).
func (p *pebbleMVCCScanner) prevKey() {
	iterCount := p.itersBeforeSeek
	mvccKey, err := DecodeMVCCKey(p.parent.Key())
	gotToPrevious := false

	for iterCount >= 0 && p.parent.Valid() && err == nil && bytes.Equal(mvccKey.Key, p.curKey) {
		p.parent.Prev()
		mvccKey, err = DecodeMVCCKey(p.parent.Key())
		iterCount--

		if err == nil && !bytes.Equal(mvccKey.Key, p.curKey) && !gotToPrevious {
			// We've backed up to the previous key, but not its latest revision.
			// Update current then keep going until we get to the latest version of
			// that key.
			gotToPrevious = true
			p.updateCurrent()
		}
	}

	if err != nil {
		p.err = err
		return
	}

	if bytes.Equal(mvccKey.Key, p.curKey) {
		// We have to seek.
		if !gotToPrevious {
			// Seek to the latest revision of the key before p.curKey.
			p.seekReverse(p.curKey)
		} else {
			// p.curKey is already one key before where it was at the start of this
			// function. Just seek to the latest revision of it.
			p.seek(p.curKey)
		}

		p.decrementItersBeforeSeek()
		return
	}

	if gotToPrevious {
		// We made it to the record preceding the record we're finding, since we
		// encountered two changes in mvccKey.Key. Go to the next key to get to the
		// correct record.
		p.parent.Next()
	}

	p.incrementItersBeforeSeek()
	p.updateCurrent()
}

// Advances to the next user key.
func (p *pebbleMVCCScanner) nextKey() {
	if !p.parent.Valid() {
		return
	}

	iterCount := p.itersBeforeSeek
	mvccKey, err := DecodeMVCCKey(p.parent.Key())

	for iterCount >= 0 && p.parent.Valid() && err == nil && bytes.Equal(mvccKey.Key, p.curKey) {
		p.parent.Next()
		mvccKey, err = DecodeMVCCKey(p.parent.Key())
	}

	if err != nil {
		p.err = err
		return
	}

	if bytes.Equal(mvccKey.Key, p.curKey) {
		// We have to seek. Append a null byte to the current key. Note that
		// appending to p.curKey could invalidate p.curRawKey, since p.curKey is
		// usually a sub-slice of the latter. But if we're just seeking to the next
		// key right afterward (seek calls updateCurrent), this is not an issue.
		p.curKey = append(p.curKey, 0x00)
		p.seek(p.curKey)

		p.decrementItersBeforeSeek()
		return
	}

	p.incrementItersBeforeSeek()
	p.updateCurrent()
}

// Seeks to the latest revision of the current key that's still less than or
// equal to the specified timestamp, adds it to the result set, then moves onto
// the next user key.
func (p *pebbleMVCCScanner) seekVersion(ts hlc.Timestamp, uncertaintyCheck bool) {
	mvccKey := MVCCKey{Key: p.curKey, Timestamp: ts}
	p.keyBuf = EncodeKeyToBuf(p.keyBuf[:0], mvccKey)
	origKey := p.keyBuf[:len(p.curKey)]

	iterCount := p.itersBeforeSeek
	for iterCount >= 0 && MVCCKeyCompare(p.curRawKey, p.keyBuf) < 0 {
		if !p.parent.Valid() {
			// For reverse iterations, go back a key.
			if p.reverse {
				p.prevKey()
			}
			return
		}
		p.parent.Next()
		p.updateCurrent()
		iterCount--
	}

	if iterCount < 0 {
		p.parent.SeekGE(p.keyBuf)
		p.updateCurrent()

		p.decrementItersBeforeSeek()
		return
	}
	p.incrementItersBeforeSeek()

	if !p.parent.Valid() {
		// For reverse iterations, go back a key.
		if p.reverse {
			p.prevKey()
		}
		return
	}
	if !bytes.Equal(p.curKey, origKey) {
		// Could not find a value - we moved past to the next key.
		if p.reverse {
			p.prevKey()
		}
		return
	}

	if !(p.curTS.Less(ts) || p.curTS.Equal(ts)) {
		if ts == (hlc.Timestamp{}) {
			// Zero timestamps come at the start. This case means there's
			// no value at the zero timestamp, and we're sitting at a nonzero
			// timestamp for the same key. Skip to the next key.
			p.advanceKey()
			return
		}
		// Potential ordering issue - this should never happen.
		panic("timestamps encountered out of order")
	}

	if uncertaintyCheck && p.ts.Less(p.curTS) {
		p.err = p.uncertaintyError(p.curTS)
		return
	}

	// Check to ensure we don't unintentionally add an intent to results.
	if p.curTS != (hlc.Timestamp{}) {
		p.addKV(p.curRawKey, p.curValue)
	}
	p.advanceKey()
}

// Adds the specified value to the result set, excluding tombstones unless
// p.tombstones is true.
func (p *pebbleMVCCScanner) addKV(key []byte, val []byte) {
	if len(val) > 0 || p.tombstones {
		p.results.put(key, val)
	}
}

// Returns an uncertainty error with the specified timestamp and p.txn.
func (p *pebbleMVCCScanner) uncertaintyError(ts hlc.Timestamp) error {
	if ts.WallTime == 0 && ts.Logical == 0 {
		return nil
	}

	return roachpb.NewReadWithinUncertaintyIntervalError(
		p.ts, ts, p.txn)
}

// Try to read from the current value's intent history. Assumes p.meta has been
// unmarshalled already. Returns true if a value was read and added to the
// result set.
func (p *pebbleMVCCScanner) getFromIntentHistory() bool {
	intentHistory := p.meta.IntentHistory
	// upIdx is the index of the first intent in intentHistory with a sequence
	// number greater than our transaction's sequence number. Subtract 1 from it
	// to get the index of the intent with the highest sequence number that is
	// still less than or equal to p.txnSeq.
	upIdx := sort.Search(len(intentHistory), func(i int) bool {
		return intentHistory[i].Sequence > p.txn.Sequence
	})
	if upIdx == 0 {
		// It is possible that no intent exists such that the sequence is less
		// than the read sequence. In this case, we cannot read a value from the
		// intent history.
		return false
	}
	intent := p.meta.IntentHistory[upIdx-1]
	p.addKV(p.curRawKey, intent.Value)
	return true
}

// Emit a tuple (using p.addKV) and return true if we have reason to believe
// iteration can continue.
func (p *pebbleMVCCScanner) getAndAdvance() bool {
	if !p.parent.Valid() {
		return false
	}
	p.err = nil

	mvccKey := MVCCKey{p.curKey, p.curTS}
	if mvccKey.IsValue() {
		if !p.ts.Less(p.curTS) {
			// 1. Fast path: there is no intent and our read timestamp is newer than
			// the most recent version's timestamp.
			p.addKV(p.curRawKey, p.curValue)
			p.advanceKey()
			return true
		}

		if p.checkUncertainty {
			// 2. Our txn's read timestamp is less than the max timestamp
			// seen by the txn. We need to check for clock uncertainty
			// errors.
			if !p.txn.MaxTimestamp.Less(p.curTS) {
				p.err = p.uncertaintyError(p.curTS)
				return false
			}

			p.seekVersion(p.txn.MaxTimestamp, true)
			return p.err == nil
		}

		// 3. Our txn's read timestamp is greater than or equal to the
		// max timestamp seen by the txn so clock uncertainty checks are
		// unnecessary. We need to seek to the desired version of the
		// value (i.e. one with a timestamp earlier than our read
		// timestamp).
		p.seekVersion(p.ts, false)
		return p.err == nil
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
		// 4. Emit immediately if the value is inline.
		p.addKV(p.curRawKey, p.meta.RawBytes)
		p.advanceKey()
		return true
	}

	if p.meta.Txn == nil {
		p.err = errors.Errorf("intent without transaction")
		return false
	}
	metaTS := hlc.Timestamp(p.meta.Timestamp)

	// metaTS is the timestamp of an intent value, which we may or may
	// not end up ignoring, depending on factors codified below. If we do ignore
	// the intent then we want to read at a lower timestamp that's strictly
	// below the intent timestamp (to skip the intent), but also does not exceed
	// our read timestamp (to avoid erroneously picking up future committed
	// values); this timestamp is prevTS.
	prevTS := p.ts
	if !p.ts.Less(metaTS) {
		prevTS = metaTS.Prev()
	}

	ownIntent := p.txn != nil && p.meta.Txn.ID.Equal(p.txn.ID)
	maxVisibleTS := p.ts
	if p.checkUncertainty {
		maxVisibleTS = p.txn.MaxTimestamp
	}

	if maxVisibleTS.Less(metaTS) && !ownIntent {
		// 5. The key contains an intent, but we're reading before the
		// intent. Seek to the desired version. Note that if we own the
		// intent (i.e. we're reading transactionally) we want to read
		// the intent regardless of our read timestamp and fall into
		// case 8 below.
		p.seekVersion(p.ts, false)
		return p.err == nil
	}

	if p.inconsistent {
		// 6. The key contains an intent and we're doing an inconsistent
		// read at a timestamp newer than the intent. We ignore the
		// intent by insisting that the timestamp we're reading at is a
		// historical timestamp < the intent timestamp. However, we
		// return the intent separately; the caller may want to resolve
		// it.
		if p.results.count == p.maxKeys {
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

		p.seekVersion(prevTS, false)
		return p.err == nil
	}

	if !ownIntent {
		// 7. The key contains an intent which was not written by our
		// transaction and our read timestamp is newer than that of the
		// intent. Note that this will trigger an error on the Go
		// side. We continue scanning so that we can return all of the
		// intents in the scan range.
		p.err = p.intents.Set(p.curRawKey, p.curValue, nil)
		if p.err != nil {
			return false
		}
		p.advanceKey()
		return p.err == nil
	}

	if p.txn != nil && p.txn.Epoch == p.meta.Txn.Epoch {
		if p.ignoreSeq || (p.txn.Sequence >= p.meta.Txn.Sequence) {
			// 8. We're reading our own txn's intent at an equal or higher sequence.
			// Note that we read at the intent timestamp, not at our read timestamp
			// as the intent timestamp may have been pushed forward by another
			// transaction. Txn's always need to read their own writes.
			p.seekVersion(metaTS, false)
		} else {
			// 9. We're reading our own txn's intent at a lower sequence than is
			// currently present in the intent. This means the intent we're seeing
			// was written at a higher sequence than the read and that there may or
			// may not be earlier versions of the intent (with lower sequence
			// numbers) that we should read. If there exists a value in the intent
			// history that has a sequence number equal to or less than the read
			// sequence, read that value.
			found := p.getFromIntentHistory()
			if found {
				p.advanceKey()
				return true
			}
			// 10. If no value in the intent history has a sequence number equal to
			// or less than the read, we must ignore the intents laid down by the
			// transaction all together. We ignore the intent by insisting that the
			// timestamp we're reading at is a historical timestamp < the intent
			// timestamp.
			p.seekVersion(prevTS, false)
		}
		return p.err == nil
	}

	if p.txn != nil && (p.txn.Epoch < p.meta.Txn.Epoch) {
		// 11. We're reading our own txn's intent but the current txn has
		// an earlier epoch than the intent. Return an error so that the
		// earlier incarnation of our transaction aborts (presumably
		// this is some operation that was retried).
		p.err = errors.Errorf("failed to read with epoch %d due to a write intent with epoch %d",
			p.txn.Epoch, p.meta.Txn.Epoch)
	}

	// 12. We're reading our own txn's intent but the current txn has a
	// later epoch than the intent. This can happen if the txn was
	// restarted and an earlier iteration wrote the value we're now
	// reading. In this case, we ignore the intent and read the
	// previous value as if the transaction were starting fresh.
	p.seekVersion(prevTS, false)
	return p.err == nil
}
