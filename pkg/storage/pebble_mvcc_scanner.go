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
	"context"
	"encoding/binary"
	"sort"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/uncertainty"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

// Key value lengths take up 8 bytes (2 x Uint32).
const kvLenSize = 8

var maxItersBeforeSeek = util.ConstantWithMetamorphicTestRange(
	"mvcc-max-iters-before-seek",
	10, /* defaultValue */
	0,  /* min */
	3,  /* max */
)

// Struct to store MVCCScan / MVCCGet in the same binary format as that
// expected by MVCCScanDecodeKeyValue.
type pebbleResults struct {
	count int64
	bytes int64
	repr  []byte
	bufs  [][]byte

	// lastOffsets is a ring buffer that keeps track of byte offsets for the last
	// N KV pairs. It is used to discard a partial SQL row at the end of the
	// result via maybeTrimPartialLastRows() -- such rows can span multiple KV
	// pairs. The length of lastOffsets is interpreted as the maximum expected SQL
	// row size (i.e.  number of column families).
	//
	// lastOffsets is initialized with a fixed length giving the N number of last
	// KV pair offsets to track. lastOffsetIdx contains the index in lastOffsets
	// where the next KV byte offset will be written, wrapping around to 0 when it
	// reaches the end of lastOffsets.
	//
	// The lastOffsets values are byte offsets in p.repr and p.bufs. The latest
	// lastOffset (i.e. the one at lastOffsetIdx-1) will be an offset in p.repr.
	// When iterating backwards through the ring buffer and crossing a byte offset
	// of 0, the next iterated byte offset in the ring buffer (at i-1) will then
	// point to the previous buffer in p.bufs.
	//
	// Actual and default 0 values in the slice are disambiguated when iterating
	// backwards through p.repr and p.bufs. If we iterate to the start of all byte
	// buffers without iterating through all of lastOffsets (i.e. when there are
	// fewer KV pairs than the length of lastOffsets), then we must be at the start
	// of lastOffsets, and any 0 values at the end are of no interest.
	lastOffsetsEnabled bool // NB: significantly faster than checking lastOffsets != nil
	lastOffsets        []int
	lastOffsetIdx      int
}

func (p *pebbleResults) clear() {
	*p = pebbleResults{}
}

// The repr that MVCCScan / MVCCGet expects to provide as output goes:
// <valueLen:Uint32><keyLen:Uint32><Key><Value>
// This function adds to repr in that format.
// - maxNewSize, if positive, indicates the maximum capacity for a new repr that
// can be allocated. It is assumed that maxNewSize (when positive) is sufficient
// for the new key-value pair.
func (p *pebbleResults) put(
	ctx context.Context, key []byte, value []byte, memAccount *mon.BoundAccount, maxNewSize int,
) error {
	const minSize = 16
	const maxSize = 128 << 20 // 128 MB

	// We maintain a list of buffers, always encoding into the last one (a.k.a.
	// pebbleResults.repr). The size of the buffers is exponentially increasing,
	// capped at maxSize. The exponential increase allows us to amortize the
	// cost of the allocation over multiple put calls. If this (key, value) pair
	// needs capacity greater than maxSize, we allocate exactly the size needed.
	lenKey := len(key)
	lenValue := len(value)
	lenToAdd := p.sizeOf(lenKey, lenValue)
	if len(p.repr)+lenToAdd > cap(p.repr) {
		// Exponential increase by default, while ensuring that we respect
		// - a hard lower bound of lenToAdd
		// - a soft upper bound of maxSize
		// - a hard upper bound of maxNewSize (if set).
		if maxNewSize > 0 && maxNewSize < lenToAdd {
			// Hard upper bound is greater than hard lower bound - this is a
			// violation of our assumptions.
			return errors.AssertionFailedf("maxNewSize %dB is not sufficient, %dB required", maxNewSize, lenToAdd)
		}
		// Exponential growth to ensure newSize >= lenToAdd.
		newSize := 2 * cap(p.repr)
		if newSize == 0 || newSize > maxSize {
			// If the previous buffer exceeded maxSize, we don't double its
			// capacity for next allocation, and instead reset the exponential
			// increase, in case we had a stray huge key-value.
			newSize = minSize
		}
		for newSize < lenToAdd {
			newSize *= 2
		}
		// Respect soft upper-bound before hard lower-bound, since it could be
		// lower than hard lower-bound.
		if newSize > maxSize {
			newSize = maxSize
		}
		// Respect hard upper-bound.
		if maxNewSize > 0 && newSize > maxNewSize {
			newSize = maxNewSize
		}
		// Now respect hard lower-bound.
		if newSize < lenToAdd {
			newSize = lenToAdd
		}
		if len(p.repr) > 0 {
			p.bufs = append(p.bufs, p.repr)
		}
		if err := memAccount.Grow(ctx, int64(newSize)); err != nil {
			return err
		}
		p.repr = nonZeroingMakeByteSlice(newSize)[:0]
	}

	startIdx := len(p.repr)
	p.repr = p.repr[:startIdx+lenToAdd]
	binary.LittleEndian.PutUint32(p.repr[startIdx:], uint32(lenValue))
	binary.LittleEndian.PutUint32(p.repr[startIdx+4:], uint32(lenKey))
	copy(p.repr[startIdx+kvLenSize:], key)
	copy(p.repr[startIdx+kvLenSize+lenKey:], value)
	p.count++
	p.bytes += int64(lenToAdd)

	// If we're tracking KV offsets, update the ring buffer.
	if p.lastOffsetsEnabled {
		p.lastOffsets[p.lastOffsetIdx] = startIdx
		p.lastOffsetIdx++
		// NB: Branching is significantly faster than modulo in benchmarks, likely
		// because of a high branch prediction hit rate.
		if p.lastOffsetIdx == len(p.lastOffsets) {
			p.lastOffsetIdx = 0
		}
	}

	return nil
}

func (p *pebbleResults) sizeOf(lenKey, lenValue int) int {
	return kvLenSize + lenKey + lenValue
}

// continuesFirstRow returns true if the given key belongs to the same SQL row
// as the first KV pair in the result (or if the result is empty). If either
// key is not a valid SQL row key, returns false.
func (p *pebbleResults) continuesFirstRow(key roachpb.Key) bool {
	repr := p.repr
	if len(p.bufs) > 0 {
		repr = p.bufs[0]
	}
	if len(repr) == 0 {
		return true // no rows in the result
	}

	rowPrefix := getRowPrefix(key)
	if rowPrefix == nil {
		return false
	}
	return bytes.Equal(rowPrefix, getRowPrefix(extractResultKey(repr)))
}

// lastRowHasFinalColumnFamily returns true if the last key in the result is the
// maximum column family ID of the row (i.e. when it equals len(lastOffsets)-1).
// If so, we know that the row is complete. However, the inverse is not true:
// the final column families of the row may be omitted, in which case the caller
// has to scan to the next key to find out whether the row is complete.
func (p *pebbleResults) lastRowHasFinalColumnFamily() bool {
	if !p.lastOffsetsEnabled || p.count == 0 {
		return false
	}

	lastOffsetIdx := p.lastOffsetIdx - 1 // p.lastOffsetIdx is where next offset would be stored
	if lastOffsetIdx < 0 {
		lastOffsetIdx = len(p.lastOffsets) - 1
	}
	lastOffset := p.lastOffsets[lastOffsetIdx]

	key := extractResultKey(p.repr[lastOffset:])
	colFamilyID, err := keys.DecodeFamilyKey(key)
	if err != nil {
		return false
	}
	return int(colFamilyID) == len(p.lastOffsets)-1
}

// maybeTrimPartialLastRow removes the last KV pairs from the result that are part
// of the same SQL row as the given key, returning the earliest key removed. The
// row cannot be made up of more KV pairs than given by len(lastOffsets),
// otherwise an error is returned. Must be called before finish().
func (p *pebbleResults) maybeTrimPartialLastRow(nextKey roachpb.Key) (roachpb.Key, error) {
	if !p.lastOffsetsEnabled || len(p.repr) == 0 {
		return nil, nil
	}
	trimRowPrefix := getRowPrefix(nextKey)
	if trimRowPrefix == nil {
		return nil, nil
	}

	var firstTrimmedKey roachpb.Key

	// We're iterating backwards through the p.lastOffsets ring buffer, starting
	// at p.lastOffsetIdx-1 (which is where the last KV was stored). The loop
	// condition simply makes sure we limit the number of iterations to the size
	// of the ring buffer, to prevent wrapping around.
	for i := 0; i < len(p.lastOffsets); i++ {
		lastOffsetIdx := p.lastOffsetIdx - 1 // p.lastOffsetIdx is where next offset would be stored
		if lastOffsetIdx < 0 {
			lastOffsetIdx = len(p.lastOffsets) - 1
		}
		lastOffset := p.lastOffsets[lastOffsetIdx]

		// The remainder of repr from the offset is now a single KV.
		repr := p.repr[lastOffset:]
		key := extractResultKey(repr)
		rowPrefix := getRowPrefix(key)

		// If the prefix belongs to a different row, we're done trimming.
		if !bytes.Equal(rowPrefix, trimRowPrefix) {
			return firstTrimmedKey, nil
		}

		// Remove this KV pair.
		p.repr = p.repr[:lastOffset]
		p.count--
		p.bytes -= int64(len(repr))
		firstTrimmedKey = key

		p.lastOffsetIdx = lastOffsetIdx
		p.lastOffsets[lastOffsetIdx] = 0

		if len(p.repr) == 0 {
			if len(p.bufs) == 0 {
				// The entire result set was trimmed, so we're done.
				return firstTrimmedKey, nil
			}
			// Pop the last buf back into repr.
			p.repr = p.bufs[len(p.bufs)-1]
			p.bufs = p.bufs[:len(p.bufs)-1]
		}
	}
	return nil, errors.Errorf("row exceeds expected max size (%d): %s", len(p.lastOffsets), nextKey)
}

func (p *pebbleResults) finish() [][]byte {
	if len(p.repr) > 0 {
		p.bufs = append(p.bufs, p.repr)
		p.repr = nil
	}
	return p.bufs
}

// getRowPrefix decodes a SQL row prefix from the given key. Returns nil if the
// key is not a valid SQL row, or if the prefix is the entire key.
func getRowPrefix(key roachpb.Key) []byte {
	if len(key) == 0 {
		return nil
	}
	n, err := keys.GetRowPrefixLength(key)
	if err != nil || n <= 0 || n >= len(key) {
		return nil
	}
	return key[:n]
}

// extractResultKey takes in a binary KV result representation, finds the raw
// key, decodes it as an MVCC key, and returns the key (without timestamp).
// Returns nil if the key could not be decoded. repr must be a valid, non-empty
// KV representation, otherwise this may panic.
func extractResultKey(repr []byte) roachpb.Key {
	keyLen := binary.LittleEndian.Uint32(repr[4:8])
	key, ok := DecodeEngineKey(repr[8 : 8+keyLen])
	if !ok {
		return nil
	}
	return key.Key
}

// pebbleMVCCScanner handles MVCCScan / MVCCGet using a Pebble iterator. If any
// MVCC range tombstones are encountered, it synthesizes MVCC point tombstones
// by switching to a pointSynthesizingIter.
type pebbleMVCCScanner struct {
	parent MVCCIterator
	// pointIter is a point synthesizing iterator that wraps and replaces parent
	// when an MVCC range tombstone is encountered. A separate reference to it is
	// kept in order to release it back to its pool when the scanner is done.
	pointIter *PointSynthesizingIter
	// memAccount is used to account for the size of the scan results.
	memAccount *mon.BoundAccount
	// lockTable is used to determine whether keys are locked in the in-memory
	// lock table when scanning with the skipLocked option.
	lockTable LockTableView
	reverse   bool
	peeked    bool
	// Iteration bounds. Does not contain MVCC timestamp.
	start, end roachpb.Key
	// Timestamp with which MVCCScan/MVCCGet was called.
	ts hlc.Timestamp
	// Max number of keys to return.
	maxKeys int64
	// Stop adding keys once p.result.bytes matches or exceeds this threshold,
	// if nonzero.
	targetBytes int64
	// If true, return an empty result if the first result exceeds targetBytes.
	allowEmpty bool
	// If set, don't return partial SQL rows (spanning multiple KV pairs) when
	// hitting a limit. Partial rows at the end of the result will be trimmed. If
	// allowEmpty is false, and the partial row is the first row in the result,
	// the row will instead be completed by fetching additional KV pairs.
	//
	// Requires init() to have been called with trackLastOffsets set to the
	// maximum number of KV pairs in a row.
	wholeRows bool
	// Stop adding intents and abort scan once maxIntents threshold is reached.
	// This limit is only applicable to consistent scans since they return
	// intents as an error.
	// Not used in inconsistent scans.
	// Ignored if zero.
	maxIntents int64
	// Resume fields describe the resume span to return. resumeReason must be set
	// to a non-zero value to return a resume span, the others are optional.
	resumeReason    roachpb.ResumeReason
	resumeKey       roachpb.Key // if unset, falls back to p.advanceKey()
	resumeNextBytes int64       // set when targetBytes is exceeded
	// Transaction epoch and sequence number.
	txn               *roachpb.Transaction
	txnEpoch          enginepb.TxnEpoch
	txnSequence       enginepb.TxnSeq
	txnIgnoredSeqNums []enginepb.IgnoredSeqNumRange
	// Uncertainty related fields.
	uncertainty      uncertainty.Interval
	checkUncertainty bool
	// Metadata object for unmarshalling intents.
	meta enginepb.MVCCMetadata
	// Bools copied over from MVCC{Scan,Get}Options. See the comment on the
	// package level MVCCScan for what these mean.
	inconsistent     bool
	skipLocked       bool
	tombstones       bool
	failOnMoreRecent bool
	isGet            bool
	keyBuf           []byte
	savedBuf         []byte
	// cur* variables store the "current" record we're pointing to. Updated in
	// updateCurrent. Note that the timestamp can be clobbered in the case of
	// adding an intent from the intent history but is otherwise meaningful.
	curUnsafeKey   MVCCKey
	curRawKey      []byte
	curUnsafeValue MVCCValue
	curRawValue    []byte
	results        pebbleResults
	intents        pebble.Batch
	// mostRecentTS stores the largest timestamp observed that is equal to or
	// above the scan timestamp. Only applicable if failOnMoreRecent is true. If
	// set and no other error is hit, a WriteToOld error will be returned from
	// the scan. mostRecentKey is one of the keys (not necessarily at
	// mostRecentTS) that was more recent than the scan.
	mostRecentTS  hlc.Timestamp
	mostRecentKey roachpb.Key
	// Stores any error returned. If non-nil, iteration short circuits.
	err error
	// Number of iterations to try before we do a Seek/SeekReverse. Stays within
	// [0, maxItersBeforeSeek] and defaults to maxItersBeforeSeek/2 .
	itersBeforeSeek int
}

// Pool for allocating pebble MVCC Scanners.
var pebbleMVCCScannerPool = sync.Pool{
	New: func() interface{} {
		return &pebbleMVCCScanner{}
	},
}

func (p *pebbleMVCCScanner) release() {
	if p.pointIter != nil {
		p.pointIter.release()
	}
	// Discard most memory references before placing in pool.
	*p = pebbleMVCCScanner{
		keyBuf: p.keyBuf,
	}
	pebbleMVCCScannerPool.Put(p)
}

// init sets bounds on the underlying pebble iterator, and initializes other
// fields not set by the calling method.
func (p *pebbleMVCCScanner) init(
	txn *roachpb.Transaction, ui uncertainty.Interval, trackLastOffsets int,
) {
	p.itersBeforeSeek = maxItersBeforeSeek / 2
	if trackLastOffsets > 0 {
		p.results.lastOffsetsEnabled = true
		p.results.lastOffsets = make([]int, trackLastOffsets)
	}

	if txn != nil {
		p.txn = txn
		p.txnEpoch = txn.Epoch
		p.txnSequence = txn.Sequence
		p.txnIgnoredSeqNums = txn.IgnoredSeqNums
	}

	p.uncertainty = ui
	// We must check uncertainty even if p.ts >= local_uncertainty_limit
	// because the local uncertainty limit cannot be applied to values with
	// synthetic timestamps. We are only able to skip uncertainty checks if
	// p.ts >= global_uncertainty_limit.
	p.checkUncertainty = p.ts.Less(p.uncertainty.GlobalLimit)
}

// get iterates exactly once and adds one KV to the result set.
func (p *pebbleMVCCScanner) get(ctx context.Context) {
	p.isGet = true

	// The iterator may already be positioned on a range key that SeekGE hits, in
	// which case RangeKeyChanged() wouldn't fire, so we enable point synthesis
	// here if needed. We check this before SeekGE, because in the typical case
	// this will be a new, unpositioned iterator, which allows omitting the
	// HasPointAndRange() call.
	if ok, _ := p.parent.Valid(); ok {
		if _, hasRange := p.parent.HasPointAndRange(); hasRange {
			p.enablePointSynthesis()
		}
	}

	p.parent.SeekGE(MVCCKey{Key: p.start})
	if !p.updateCurrent() {
		return
	}
	p.getAndAdvance(ctx)
	p.maybeFailOnMoreRecent()
}

// scan iterates until a limit is exceeded, the underlying iterator is
// exhausted, or an error is encountered. If a limit was exceeded, it returns a
// resume span, resume reason, and for targetBytes the size of the next result.
func (p *pebbleMVCCScanner) scan(
	ctx context.Context,
) (*roachpb.Span, roachpb.ResumeReason, int64, error) {
	if p.wholeRows && !p.results.lastOffsetsEnabled {
		return nil, 0, 0, errors.AssertionFailedf("cannot use wholeRows without trackLastOffsets")
	}
	p.isGet = false

	// The iterator may already be positioned on a range key that the seek hits,
	// in which case RangeKeyChanged() wouldn't fire, so we enable point synthesis
	// here if needed. We check this before seeking, because in the typical case
	// this will be a new, unpositioned iterator, which allows omitting the
	// HasPointAndRange() call.
	if ok, _ := p.parent.Valid(); ok {
		if _, hasRange := p.parent.HasPointAndRange(); hasRange {
			p.enablePointSynthesis()
		}
	}

	if p.reverse {
		if !p.iterSeekReverse(MVCCKey{Key: p.end}) {
			return nil, 0, 0, p.err
		}
	} else {
		if !p.iterSeek(MVCCKey{Key: p.start}) {
			return nil, 0, 0, p.err
		}
	}

	for p.getAndAdvance(ctx) {
	}
	p.maybeFailOnMoreRecent()

	if p.err != nil {
		return nil, 0, 0, p.err
	}

	if p.resumeReason != 0 {
		resumeKey := p.resumeKey
		if len(resumeKey) == 0 {
			if !p.advanceKey() {
				return nil, 0, 0, nil // nothing to resume
			}
			resumeKey = p.curUnsafeKey.Key
		}

		var resumeSpan *roachpb.Span
		if p.reverse {
			// NB: this is equivalent to:
			//  append(roachpb.Key(nil), resumeKey...).Next()
			// but with half the allocations.
			resumeKeyCopy := make(roachpb.Key, len(resumeKey), len(resumeKey)+1)
			copy(resumeKeyCopy, resumeKey)
			resumeSpan = &roachpb.Span{
				Key:    p.start,
				EndKey: resumeKeyCopy.Next(),
			}
		} else {
			resumeSpan = &roachpb.Span{
				Key:    append(roachpb.Key(nil), resumeKey...),
				EndKey: p.end,
			}
		}
		return resumeSpan, p.resumeReason, p.resumeNextBytes, nil
	}
	return nil, 0, 0, nil
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
	if p.itersBeforeSeek < 0 {
		p.itersBeforeSeek = 0
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
	p.err = roachpb.NewWriteTooOldError(p.ts, p.mostRecentTS.Next(), p.mostRecentKey)
	p.results.clear()
	p.intents.Reset()
}

// Returns an uncertainty error with the specified timestamp and p.txn.
func (p *pebbleMVCCScanner) uncertaintyError(ts hlc.Timestamp) bool {
	p.err = roachpb.NewReadWithinUncertaintyIntervalError(
		p.ts, ts, p.uncertainty.LocalLimit.ToTimestamp(), p.txn)
	p.results.clear()
	p.intents.Reset()
	return false
}

// Emit a tuple and return true if we have reason to believe iteration can
// continue.
func (p *pebbleMVCCScanner) getAndAdvance(ctx context.Context) bool {
	if !p.curUnsafeKey.Timestamp.IsEmpty() {
		if extended, valid := p.tryDecodeCurrentValueSimple(); !valid {
			return false
		} else if extended {
			if !p.decodeCurrentValueExtended() {
				return false
			}
		}

		// ts < read_ts
		if p.curUnsafeKey.Timestamp.Less(p.ts) {
			// 1. Fast path: there is no intent and our read timestamp is newer
			// than the most recent version's timestamp.
			return p.addAndAdvance(ctx, p.curUnsafeKey.Key, p.curRawKey, p.curUnsafeValue.Value.RawBytes)
		}

		// ts == read_ts
		if p.curUnsafeKey.Timestamp.EqOrdering(p.ts) {
			if p.failOnMoreRecent {
				// 2. Our txn's read timestamp is equal to the most recent
				// version's timestamp and the scanner has been configured to
				// throw a write too old error on equal or more recent versions.

				if p.skipLocked {
					if locked, ok := p.isKeyLockedByConflictingTxn(ctx, p.curRawKey); !ok {
						return false
					} else if locked {
						// 2a. the scanner was configured to skip locked keys, and
						// this key was locked, so we can advance past it without
						// raising the write too old error.
						return p.advanceKey()
					}
				}

				// 2b. We need to raise a write too old error. Merge the current
				// timestamp with the maximum timestamp we've seen so we know to
				// return an error, but then keep scanning so that we can return
				// the largest possible time.
				p.mostRecentTS.Forward(p.curUnsafeKey.Timestamp)
				if len(p.mostRecentKey) == 0 {
					p.mostRecentKey = append(p.mostRecentKey, p.curUnsafeKey.Key...)
				}
				return p.advanceKey()
			}

			// 3. There is no intent and our read timestamp is equal to the most
			// recent version's timestamp.
			return p.addAndAdvance(ctx, p.curUnsafeKey.Key, p.curRawKey, p.curUnsafeValue.Value.RawBytes)
		}

		// ts > read_ts
		if p.failOnMoreRecent {
			// 4. Our txn's read timestamp is less than the most recent
			// version's timestamp and the scanner has been configured to
			// throw a write too old error on equal or more recent versions.

			if p.skipLocked {
				if locked, ok := p.isKeyLockedByConflictingTxn(ctx, p.curRawKey); !ok {
					return false
				} else if locked {
					// 4a. the scanner was configured to skip locked keys, and
					// this key was locked, so we can advance past it without
					// raising the write too old error.
					return p.advanceKey()
				}
			}

			// 4b. We need to raise a write too old error. Merge the current
			// timestamp with the maximum timestamp we've seen so we know to
			// return an error, but then keep scanning so that we can return
			// the largest possible time.
			p.mostRecentTS.Forward(p.curUnsafeKey.Timestamp)
			if len(p.mostRecentKey) == 0 {
				p.mostRecentKey = append(p.mostRecentKey, p.curUnsafeKey.Key...)
			}
			return p.advanceKey()
		}

		if p.checkUncertainty {
			// 5. Our txn's read timestamp is less than the max timestamp
			// seen by the txn. We need to check for clock uncertainty
			// errors.
			localTS := p.curUnsafeValue.GetLocalTimestamp(p.curUnsafeKey.Timestamp)
			if p.uncertainty.IsUncertain(p.curUnsafeKey.Timestamp, localTS) {
				return p.uncertaintyError(p.curUnsafeKey.Timestamp)
			}

			// This value is not within the reader's uncertainty window, but
			// there could be other uncertain committed values, so seek and
			// check uncertainty using the uncertainty interval's GlobalLimit.
			return p.seekVersion(ctx, p.uncertainty.GlobalLimit, true)
		}

		// 6. Our txn's read timestamp is greater than or equal to the
		// max timestamp seen by the txn so clock uncertainty checks are
		// unnecessary. We need to seek to the desired version of the
		// value (i.e. one with a timestamp earlier than our read
		// timestamp).
		return p.seekVersion(ctx, p.ts, false)
	}

	if !p.decodeCurrentMetadata() {
		return false
	}
	if len(p.meta.RawBytes) != 0 {
		// 7. Emit immediately if the value is inline.
		return p.addAndAdvance(ctx, p.curUnsafeKey.Key, p.curRawKey, p.meta.RawBytes)
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
	if !ownIntent {
		conflictingIntent := metaTS.LessEq(p.ts) || p.failOnMoreRecent
		if !conflictingIntent {
			// 8. The key contains an intent, but we're reading below the intent.
			// Seek to the desired version, checking for uncertainty if necessary.
			//
			// Note that if we own the intent (i.e. we're reading transactionally)
			// we want to read the intent regardless of our read timestamp and fall
			// into case 11 below.
			if p.checkUncertainty {
				// The intent's provisional value may be within the uncertainty window.
				// Or there could be a different, uncertain committed value in the
				// window. To detect either case, seek to and past the uncertainty
				// interval's global limit and check uncertainty as we scan.
				return p.seekVersion(ctx, p.uncertainty.GlobalLimit, true)
			}
			return p.seekVersion(ctx, p.ts, false)
		}

		if p.inconsistent {
			// 9. The key contains an intent and we're doing an inconsistent
			// read at a timestamp newer than the intent. We ignore the
			// intent by insisting that the timestamp we're reading at is a
			// historical timestamp < the intent timestamp. However, we
			// return the intent separately; the caller may want to resolve
			// it.
			//
			// p.intents is a pebble.Batch which grows its byte slice capacity in
			// chunks to amortize allocations. The memMonitor is under-counting here
			// by only accounting for the key and value bytes.
			if !p.addCurIntent(ctx) {
				return false
			}
			return p.seekVersion(ctx, prevTS, false)
		}

		if p.skipLocked {
			// 10. The scanner has been configured with the skipLocked option. Ignore
			// intents written by other transactions and seek to the next key.
			// However, we return the intent separately if we have room; the caller
			// may want to resolve it. Unlike below, this intent will not result in
			// a WriteIntentError because MVCC{Scan,Get}Options.errOnIntents returns
			// false when skipLocked in enabled.
			if p.maxIntents == 0 || int64(p.intents.Count()) < p.maxIntents {
				if !p.addCurIntent(ctx) {
					return false
				}
			}
			return p.advanceKey()
		}

		// 11. The key contains an intent which was not written by our
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
		if !p.addCurIntent(ctx) {
			return false
		}
		// Limit number of intents returned in write intent error.
		if p.maxIntents > 0 && int64(p.intents.Count()) >= p.maxIntents {
			p.resumeReason = roachpb.RESUME_INTENT_LIMIT
			return false
		}
		return p.advanceKey()
	}

	if p.txnEpoch == p.meta.Txn.Epoch {
		if p.txnSequence >= p.meta.Txn.Sequence && !enginepb.TxnSeqIsIgnored(p.meta.Txn.Sequence, p.txnIgnoredSeqNums) {
			// 12. We're reading our own txn's intent at an equal or higher sequence.
			// Note that we read at the intent timestamp, not at our read timestamp
			// as the intent timestamp may have been pushed forward by another
			// transaction. Txn's always need to read their own writes.
			return p.seekVersion(ctx, metaTS, false)
		}

		// 13. We're reading our own txn's intent at a lower sequence than is
		// currently present in the intent. This means the intent we're seeing
		// was written at a higher sequence than the read and that there may or
		// may not be earlier versions of the intent (with lower sequence
		// numbers) that we should read. If there exists a value in the intent
		// history that has a sequence number equal to or less than the read
		// sequence, read that value.
		if intentValueRaw, found := p.getFromIntentHistory(); found {
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
			p.keyBuf = EncodeMVCCKeyToBuf(p.keyBuf[:0], p.curUnsafeKey)
			p.curUnsafeValue, p.err = DecodeMVCCValue(intentValueRaw)
			if p.err != nil {
				return false
			}
			return p.addAndAdvance(ctx, p.curUnsafeKey.Key, p.keyBuf, p.curUnsafeValue.Value.RawBytes)
		}
		// 14. If no value in the intent history has a sequence number equal to
		// or less than the read, we must ignore the intents laid down by the
		// transaction all together. We ignore the intent by insisting that the
		// timestamp we're reading at is a historical timestamp < the intent
		// timestamp.
		return p.seekVersion(ctx, prevTS, false)
	}

	if p.txnEpoch < p.meta.Txn.Epoch {
		// 15. We're reading our own txn's intent but the current txn has
		// an earlier epoch than the intent. Return an error so that the
		// earlier incarnation of our transaction aborts (presumably
		// this is some operation that was retried).
		p.err = errors.Errorf("failed to read with epoch %d due to a write intent with epoch %d",
			p.txnEpoch, p.meta.Txn.Epoch)
		return false
	}

	// 16. We're reading our own txn's intent but the current txn has a
	// later epoch than the intent. This can happen if the txn was
	// restarted and an earlier iteration wrote the value we're now
	// reading. In this case, we ignore the intent and read the
	// previous value as if the transaction were starting fresh.
	return p.seekVersion(ctx, prevTS, false)
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
func (p *pebbleMVCCScanner) addAndAdvance(
	ctx context.Context, key roachpb.Key, rawKey []byte, rawValue []byte,
) bool {
	// Don't include deleted versions len(val) == 0, unless we've been instructed
	// to include tombstones in the results.
	if len(rawValue) == 0 && !p.tombstones {
		return p.advanceKey()
	}

	// If the scanner has been configured with the skipLocked option, don't
	// include locked keys in the result set. Consult the in-memory lock table to
	// determine whether this is locked with an unreplicated lock. Replicated
	// locks will be represented as intents, which will be skipped over in
	// getAndAdvance.
	if p.skipLocked {
		if locked, ok := p.isKeyLockedByConflictingTxn(ctx, rawKey); !ok {
			return false
		} else if locked {
			return p.advanceKey()
		}
	}

	// Check if adding the key would exceed a limit.
	if p.targetBytes > 0 && p.results.bytes+int64(p.results.sizeOf(len(rawKey), len(rawValue))) > p.targetBytes {
		p.resumeReason = roachpb.RESUME_BYTE_LIMIT
		p.resumeNextBytes = int64(p.results.sizeOf(len(rawKey), len(rawValue)))

	} else if p.maxKeys > 0 && p.results.count >= p.maxKeys {
		p.resumeReason = roachpb.RESUME_KEY_LIMIT
	}

	var mustPutKey bool
	if p.resumeReason != 0 {
		// If we exceeded a limit, but we're not allowed to return an empty result,
		// then make sure we include the first key in the result. If wholeRows is
		// enabled, then also make sure we complete the first SQL row.
		if !p.allowEmpty &&
			(p.results.count == 0 || (p.wholeRows && p.results.continuesFirstRow(key))) {
			p.resumeReason = 0
			p.resumeNextBytes = 0
			mustPutKey = true
		} else {
			p.resumeKey = key

			// If requested, remove any partial SQL rows from the end of the result.
			if p.wholeRows {
				trimmedKey, err := p.results.maybeTrimPartialLastRow(key)
				if err != nil {
					p.err = err
					return false
				}
				if trimmedKey != nil {
					p.resumeKey = trimmedKey
				}
			}
			return false
		}
	}

	// We are here due to one of the following cases:
	// A. No limits were exceeded
	// B. Limits were exceeded, but we need to put a key, so mustPutKey = true.
	//
	// For B we will never set maxNewSize.
	// For A, we may set maxNewSize, but we already know that
	//   p.targetBytes >= p.results.bytes + lenToAdd
	// so maxNewSize will be sufficient.
	var maxNewSize int
	if p.targetBytes > 0 && p.targetBytes > p.results.bytes && !mustPutKey {
		// INVARIANT: !mustPutKey => maxNewSize is sufficient for key-value
		// pair.
		maxNewSize = int(p.targetBytes - p.results.bytes)
	}
	if err := p.results.put(ctx, rawKey, rawValue, p.memAccount, maxNewSize); err != nil {
		p.err = errors.Wrapf(err, "scan with start key %s", p.start)
		return false
	}

	// Check if we hit the key limit just now to avoid scanning further before
	// checking the key limit above on the next iteration. This has a small cost
	// (~0.5% for large scans), but avoids the potentially large cost of scanning
	// lots of garbage before the next key -- especially when maxKeys is small.
	if p.maxKeys > 0 && p.results.count >= p.maxKeys {
		// If we're not allowed to return partial SQL rows, check whether the last
		// KV pair in the result has the maximum column family ID of the row. If so,
		// we can return early. However, if it doesn't then we can't know yet
		// whether the row is complete or not, because the final column families of
		// the row may have been omitted (if they are all NULL values) -- to find
		// out, we must continue scanning to the next key and handle it above.
		if !p.wholeRows || p.results.lastRowHasFinalColumnFamily() {
			p.resumeReason = roachpb.RESUME_KEY_LIMIT
			return false
		}
	}
	return p.advanceKey()
}

// Seeks to the latest revision of the current key that's still less than or
// equal to the specified timestamp, adds it to the result set, then moves onto
// the next user key.
func (p *pebbleMVCCScanner) seekVersion(
	ctx context.Context, seekTS hlc.Timestamp, uncertaintyCheck bool,
) bool {
	if seekTS.IsEmpty() {
		// If the seek timestamp is empty, we've already seen all versions of this
		// key, so seek to the next key. Seeking to version zero of the current key
		// would be incorrect, as version zero is stored before all other versions.
		return p.advanceKey()
	}

	seekKey := MVCCKey{Key: p.curUnsafeKey.Key, Timestamp: seekTS}
	p.keyBuf = EncodeMVCCKeyToBuf(p.keyBuf[:0], seekKey)
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
			if extended, valid := p.tryDecodeCurrentValueSimple(); !valid {
				return false
			} else if extended {
				if !p.decodeCurrentValueExtended() {
					return false
				}
			}
			if !uncertaintyCheck || p.curUnsafeKey.Timestamp.LessEq(p.ts) {
				return p.addAndAdvance(ctx, p.curUnsafeKey.Key, p.curRawKey, p.curUnsafeValue.Value.RawBytes)
			}
			// Iterate through uncertainty interval. Though we found a value in
			// the interval, it may not be uncertainty. This is because seekTS
			// is set to the transaction's global uncertainty limit, so we are
			// seeking based on the worst-case uncertainty, but values with a
			// time in the range (uncertainty.LocalLimit, uncertainty.GlobalLimit]
			// are only uncertain if their timestamps are synthetic. Meanwhile,
			// any value with a time in the range (ts, uncertainty.LocalLimit]
			// is uncertain.
			localTS := p.curUnsafeValue.GetLocalTimestamp(p.curUnsafeKey.Timestamp)
			if p.uncertainty.IsUncertain(p.curUnsafeKey.Timestamp, localTS) {
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
		if extended, valid := p.tryDecodeCurrentValueSimple(); !valid {
			return false
		} else if extended {
			if !p.decodeCurrentValueExtended() {
				return false
			}
		}
		if !uncertaintyCheck || p.curUnsafeKey.Timestamp.LessEq(p.ts) {
			return p.addAndAdvance(ctx, p.curUnsafeKey.Key, p.curRawKey, p.curUnsafeValue.Value.RawBytes)
		}
		// Iterate through uncertainty interval. See the comment above about why
		// a value in this interval is not necessarily cause for an uncertainty
		// error.
		localTS := p.curUnsafeValue.GetLocalTimestamp(p.curUnsafeKey.Timestamp)
		if p.uncertainty.IsUncertain(p.curUnsafeKey.Timestamp, localTS) {
			return p.uncertaintyError(p.curUnsafeKey.Timestamp)
		}
		if !p.iterNext() {
			return p.advanceKeyAtEnd()
		}
	}
}

// Updates cur{RawKey, UnsafeKey, RawValue} to match record the iterator is
// pointing to. Callers should call decodeCurrent{Metadata, Value} to decode
// the raw value if they need it.
func (p *pebbleMVCCScanner) updateCurrent() bool {
	if !p.iterValid() {
		return false
	}

	p.curRawKey = p.parent.UnsafeRawMVCCKey()

	var err error
	p.curUnsafeKey, err = DecodeMVCCKey(p.curRawKey)
	if err != nil {
		p.err = errors.Wrap(err, "unable to decode MVCCKey")
		return false
	}
	p.curRawValue = p.parent.UnsafeValue()

	// Reset decoded value to avoid bugs.
	if util.RaceEnabled {
		p.meta = enginepb.MVCCMetadata{}
		p.curUnsafeValue = MVCCValue{}
	}
	return true
}

// enablePointSynthesis wraps p.parent with a pointSynthesizingIter, which
// synthesizes MVCC point tombstones for MVCC range tombstones and never emits
// range keys itself. p.parent must be valid.
//
// gcassert:inline
func (p *pebbleMVCCScanner) enablePointSynthesis() {
	if util.RaceEnabled {
		if ok, _ := p.parent.Valid(); !ok {
			panic(errors.AssertionFailedf("enablePointSynthesis called with invalid iter"))
		}
		if p.pointIter != nil {
			panic(errors.AssertionFailedf("enablePointSynthesis called when already enabled"))
		}
		if _, hasRange := p.parent.HasPointAndRange(); !hasRange {
			panic(errors.AssertionFailedf("enablePointSynthesis called on non-range-key position %s",
				p.parent.UnsafeKey()))
		}
	}
	p.pointIter = NewPointSynthesizingIterAtParent(p.parent)
	p.parent = p.pointIter
	if util.RaceEnabled {
		if ok, _ := p.parent.Valid(); !ok {
			panic(errors.AssertionFailedf("invalid pointSynthesizingIter with valid iter"))
		}
	}
}

func (p *pebbleMVCCScanner) decodeCurrentMetadata() bool {
	if len(p.curRawValue) == 0 {
		p.err = errors.Errorf("zero-length mvcc metadata")
		return false
	}
	err := protoutil.Unmarshal(p.curRawValue, &p.meta)
	if err != nil {
		p.err = errors.Wrap(err, "unable to decode MVCCMetadata")
		return false
	}
	return true
}

//gcassert:inline
func (p *pebbleMVCCScanner) tryDecodeCurrentValueSimple() (extended, valid bool) {
	var simple bool
	p.curUnsafeValue, simple, p.err = tryDecodeSimpleMVCCValue(p.curRawValue)
	return !simple, p.err == nil
}

//gcassert:inline
func (p *pebbleMVCCScanner) decodeCurrentValueExtended() bool {
	p.curUnsafeValue, p.err = decodeExtendedMVCCValue(p.curRawValue)
	return p.err == nil
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
	// Since iterValid() is called after every iterator positioning operation, it
	// is convenient to check for any range keys and enable point synthesis here.
	if p.parent.RangeKeyChanged() {
		p.enablePointSynthesis()
	}
	if util.RaceEnabled && p.pointIter == nil {
		if _, hasRange := p.parent.HasPointAndRange(); hasRange {
			panic(errors.AssertionFailedf("range key did not trigger point synthesis at %s",
				p.parent.UnsafeKey()))
		}
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
		p.savedBuf = append(p.savedBuf, p.curRawValue...)
		p.curRawKey = p.savedBuf[:len(p.curRawKey)]
		p.curRawValue = p.savedBuf[len(p.curRawKey):]
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

// isKeyLockedByConflictingTxn consults the in-memory lock table to determine
// whether the provided key is locked with an unreplicated lock by a different
// txn. When p.skipLocked, this method should be called before adding a key to
// the scan's result set or throwing a write too old error on behalf of a key.
// If the key is locked, skipLocked instructs the scan to skip over it instead.
func (p *pebbleMVCCScanner) isKeyLockedByConflictingTxn(
	ctx context.Context, rawKey []byte,
) (locked, ok bool) {
	key, _, err := enginepb.DecodeKey(rawKey)
	if err != nil {
		p.err = err
		return false, false
	}
	strength := lock.None
	if p.failOnMoreRecent {
		strength = lock.Exclusive
	}
	if ok, txn := p.lockTable.IsKeyLockedByConflictingTxn(key, strength); ok {
		// The key is locked, so ignore it. However, we return the lock holder
		// separately if we have room; the caller may want to resolve it.
		if p.maxIntents == 0 || int64(p.intents.Count()) < p.maxIntents {
			if !p.addKeyAndMetaAsIntent(ctx, key, txn) {
				return false, false
			}
		}
		return true, true
	}
	return false, true
}

// addCurIntent adds the key-value pair that the scanner is currently
// pointing to as an intent to the intents set.
func (p *pebbleMVCCScanner) addCurIntent(ctx context.Context) bool {
	return p.addRawIntent(ctx, p.curRawKey, p.curRawValue)
}

// addKeyAndMetaAsIntent adds the key and transaction meta as an intent to
// the intents set.
func (p *pebbleMVCCScanner) addKeyAndMetaAsIntent(
	ctx context.Context, key roachpb.Key, txn *enginepb.TxnMeta,
) bool {
	mvccKey := MakeMVCCMetadataKey(key)
	mvccVal := enginepb.MVCCMetadata{Txn: txn}
	encodedKey := EncodeMVCCKey(mvccKey)
	encodedVal, err := protoutil.Marshal(&mvccVal)
	if err != nil {
		p.err = err
		return false
	}
	return p.addRawIntent(ctx, encodedKey, encodedVal)
}

func (p *pebbleMVCCScanner) addRawIntent(ctx context.Context, key, value []byte) bool {
	// p.intents is a pebble.Batch which grows its byte slice capacity in
	// chunks to amortize allocations. The memMonitor is under-counting here
	// by only accounting for the key and value bytes.
	if p.err = p.memAccount.Grow(ctx, int64(len(key)+len(value))); p.err != nil {
		p.err = errors.Wrapf(p.err, "scan with start key %s", p.start)
		return false
	}
	p.err = p.intents.Set(key, value, nil)
	return p.err == nil
}

func (p *pebbleMVCCScanner) intentsRepr() []byte {
	if p.intents.Count() == 0 {
		return nil
	}
	return p.intents.Repr()
}
