// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"sort"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/uncertainty"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

var maxItersBeforeSeek = metamorphic.ConstantWithTestRange(
	"mvcc-max-iters-before-seek",
	10, /* defaultValue */
	0,  /* min */
	3,  /* max */
)

// MVCCDecodingStrategy controls if and how the fetcher should decode MVCC
// timestamps from returned KV's.
type MVCCDecodingStrategy int

const (
	// MVCCDecodingNotRequired is used when timestamps aren't needed.
	MVCCDecodingNotRequired MVCCDecodingStrategy = iota
	// MVCCDecodingRequired is used when timestamps are needed.
	MVCCDecodingRequired
)

// results abstracts away a result set where pebbleMVCCScanner put()'s KVs into.
type results interface {
	// clear clears the results so that its memory could be GCed.
	clear()
	// sizeInfo returns several pieces of information about the current size of
	// the results:
	// - the number of KVs currently in the results,
	// - the current memory footprint of the results in bytes,
	// - the increment for how much the memory footprint of the results will
	//   increase (in bytes) if a KV (with the corresponding lengths of the key
	//   and the value parts) is put into it.
	//
	// Note that we chose to squash all these things into a single method rather
	// than defining a separate method for each parameter out of performance
	// considerations.
	sizeInfo(lenKey, lenValue int) (numKeys, numBytes, numBytesInc int64)
	// put adds a KV into the results. An error is returned if the memory
	// reservation is denied by the memory account.
	put(_ context.Context, mvccKey []byte, value []byte, memAccount *mon.BoundAccount, maxNewSize int) error
	// continuesFirstRow returns true if the given key belongs to the same SQL
	// row as the first KV pair in the result. If the given key is not a valid
	// SQL row key, returns false.
	//
	// This method is called _after_ having called put() with no error at least
	// once, meaning that at least one key is in the results.
	//
	// Only called when wholeRows option is enabled.
	continuesFirstRow(key roachpb.Key) bool
	// maybeTrimPartialLastRow removes the last KV pairs from the result that
	// are part of the same SQL row as the given key, returning the earliest key
	// removed.
	//
	// pebbleMVCCScanner.getOne can call this method only _before_ calling
	// put(). This constraint ensures that for the singleResults implementation
	// of this interface, when this method is called, there is no buffered KV
	// (i.e. there is no KV that has been `put` into the results but not yet
	// returned on the NextKVer.NextKV call). This allows for the singleResults
	// to synchronize with the colfetcher.cFetcher (via the
	// storage.FirstKeyOfRowGetter) to obtain the first key of the SQL row if
	// the given key belongs to that row.
	//
	// Only called when wholeRows option is enabled.
	maybeTrimPartialLastRow(key roachpb.Key) (roachpb.Key, error)
	// lastRowHasFinalColumnFamily returns true if the last key in the result is
	// the maximum column family ID of the row. If so, we know that the row is
	// complete. However, the inverse is not true: the final column families of
	// the row may be omitted, in which case the caller has to scan to the next
	// key to find out whether the row is complete.
	//
	// This method is called _after_ having called put() with no error at least
	// once, meaning that at least one key is in the results.
	//
	// Only called when wholeRows option is enabled.
	lastRowHasFinalColumnFamily(reverse bool) bool
}

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

// clear implements the results interface.
func (p *pebbleResults) clear() {
	*p = pebbleResults{}
}

// Key value lengths take up 8 bytes (2 x Uint32).
const pebbleResultsKVLenSize = 8

func pebbleResultsKVSizeOf(lenKey, lenValue int) int {
	return pebbleResultsKVLenSize + lenKey + lenValue
}

// sizeInfo implements the results interface.
func (p *pebbleResults) sizeInfo(lenKey, lenValue int) (numKeys, numBytes, numBytesInc int64) {
	numKeys = p.count
	numBytes = p.bytes
	numBytesInc = int64(pebbleResultsKVSizeOf(lenKey, lenValue))
	return numKeys, numBytes, numBytesInc
}

// put implements the results interface.
//
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
	lenToAdd := pebbleResultsKVSizeOf(lenKey, lenValue)
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
	copy(p.repr[startIdx+pebbleResultsKVLenSize:], key)
	copy(p.repr[startIdx+pebbleResultsKVLenSize+lenKey:], value)
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

// continuesFirstRow implements the results interface.
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

// keyHasFinalColumnFamily returns whether the given key corresponds to the last
// column family in a SQL row. Returns false if the key is not a valid SQL key.
func keyHasFinalColumnFamily(key roachpb.Key, maxFamilyID uint32, reverse bool) bool {
	colFamilyID, err := keys.DecodeFamilyKey(key)
	if err != nil {
		return false
	}
	if reverse {
		return colFamilyID == 0
	}
	return colFamilyID == maxFamilyID
}

// lastRowHasFinalColumnFamily implements the results interface.
func (p *pebbleResults) lastRowHasFinalColumnFamily(reverse bool) bool {
	lastOffsetIdx := p.lastOffsetIdx - 1 // p.lastOffsetIdx is where next offset would be stored
	if lastOffsetIdx < 0 {
		lastOffsetIdx = len(p.lastOffsets) - 1
	}
	lastOffset := p.lastOffsets[lastOffsetIdx]
	key := extractResultKey(p.repr[lastOffset:])
	return keyHasFinalColumnFamily(key, uint32(len(p.lastOffsets)-1), reverse)
}

// maybeTrimPartialLastRow implements the results interface.
//
// The row cannot be made up of more KV pairs than given by len(lastOffsets),
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

// pebbleMVCCScanner handles MVCCScan / MVCCGet using a Pebble iterator.
type pebbleMVCCScanner struct {
	parent MVCCIterator
	// memAccount is used to account for the size of the scan results.
	memAccount *mon.BoundAccount
	// unlimitedMemAcc will back the memAccount field above when the scanner is
	// retrieved from its pool. The account is cleared as the scanner returns to
	// the pool (see release); it's fine to "leak" the account if the scanner is
	// not returned to the pool, since it's an unlimited account.
	// When a custom mem account should be used instead, memAccount should be
	// overridden.
	unlimitedMemAcc mon.BoundAccount
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
	wholeRows bool
	// decodeMVCCHeaders is set by callers who expect to be able
	// to read the full MVCCValueHeader off of
	// curUnsafeValue. Used by mvccGet.
	decodeMVCCHeaders bool
	// Stop adding intents and abort scan once maxLockConflicts threshold is
	// reached. This limit is only applicable to consistent scans since they
	// return intents as an error.
	// Not used in inconsistent scans.
	// Ignored if zero.
	maxLockConflicts int64
	// Resume fields describe the resume span to return. resumeReason must be set
	// to a non-zero value to return a resume span, the others are optional.
	resumeReason    kvpb.ResumeReason
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
	inconsistent bool
	skipLocked   bool
	tombstones   bool
	// rawMVCCValues instructs the scanner to return the full
	// extended encoding of any returned value. This includes the
	// MVCCValueHeader.
	rawMVCCValues    bool
	failOnMoreRecent bool
	keyBuf           []byte
	savedBuf         []byte
	lazyFetcherBuf   pebble.LazyFetcher
	lazyValueBuf     []byte
	// cur* variables store the "current" record we're pointing to. Updated in
	// updateCurrent. Note that the timestamp can be clobbered in the case of
	// adding an intent from the intent history but is otherwise meaningful.
	curUnsafeKey      MVCCKey
	curRawKey         []byte
	curUnsafeValue    MVCCValue
	curRawValue       pebble.LazyValue
	curRangeKeys      MVCCRangeKeyStack
	savedRangeKeys    MVCCRangeKeyStack
	savedRangeKeyVers MVCCRangeKeyVersion
	results           results
	intents           pebble.Batch
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
	// [0, maxItersBeforeSeek] and defaults to maxItersBeforeSeek/2.
	itersBeforeSeek int
	// machine is the state machine for how the iterator should be advanced in
	// order to handle scans and reverse scans.
	machine struct {
		// fn indicates the advance function that needs to be called next.
		fn advanceFn
		// origKey is a temporary buffer used to store the "original key" when
		// advancing the iterator at the new key. It is backed by keyBuf.
		origKey []byte
	}
	// alloc holds fields embedded within the scanner struct only to reduce
	// allocations in common cases.
	alloc struct {
		// Typically pebbleMVCCScanner.results points to pebbleResults.
		// Embedding the pebbleResults within the pebbleMVCCScanner avoids an
		// extra allocation, at the cost of higher allocated bytes when we use a
		// different implementation of the results interface.
		pebbleResults pebbleResults
	}
}

type advanceFn int

const (
	_ advanceFn = iota

	// "Forward" advance states, used for non-reverse scans.
	//
	// advanceKeyForward indicates that the iterator needs to be advanced to the
	// next key.
	advanceKeyForward
	// advanceKeyAtEndForward indicates that the iterator has reached the end in
	// the forward direction.
	advanceKeyAtEndForward
	// advanceKeyAtNewKeyForward indicates that the iterator has just reached a
	// new key.
	advanceKeyAtNewKeyForward

	// "Reverse" advance states, used for reverse scans.
	//
	// advanceKeyReverse indicates that the iterator needs to be advanced to the
	// previous key.
	advanceKeyReverse
	// advanceKeyAtEndReverse indicates that the iterator has reached the end in
	// the reverse direction.
	advanceKeyAtEndReverse
	// advanceKeyAtNewKeyReverse indicates that the iterator needs to be
	// advanced to the key before the key that has just been reached.
	advanceKeyAtNewKeyReverse
)

// Pool for allocating pebble MVCC Scanners.
var pebbleMVCCScannerPool = sync.Pool{
	New: func() interface{} {
		mvccScanner := &pebbleMVCCScanner{
			unlimitedMemAcc: *mon.NewStandaloneUnlimitedAccount(),
		}
		mvccScanner.memAccount = &mvccScanner.unlimitedMemAcc
		return mvccScanner
	},
}

func (p *pebbleMVCCScanner) release() {
	// Release all bytes from the unlimited memory account (but keep
	// the account intact).
	p.unlimitedMemAcc.Empty(context.Background())
	// Discard most memory references before placing in pool.
	*p = pebbleMVCCScanner{
		keyBuf:          p.keyBuf,
		memAccount:      &p.unlimitedMemAcc,
		unlimitedMemAcc: p.unlimitedMemAcc,
		// NB: This clears p.alloc.pebbleResults too, which should be maintained
		// to avoid delaying GC of contained byte slices and avoid accidental
		// misuse.
	}
	pebbleMVCCScannerPool.Put(p)
}

// init sets bounds on the underlying pebble iterator, and initializes other
// fields not set by the calling method.
func (p *pebbleMVCCScanner) init(
	txn *roachpb.Transaction, ui uncertainty.Interval, results results,
) {
	p.itersBeforeSeek = maxItersBeforeSeek / 2
	p.results = results

	if txn != nil {
		p.txn = txn
		p.txnEpoch = txn.Epoch
		p.txnSequence = txn.Sequence
		p.txnIgnoredSeqNums = txn.IgnoredSeqNums
	}

	p.uncertainty = ui
	// We must check uncertainty even if p.ts >= local_uncertainty_limit
	// because the local uncertainty limit cannot be applied to values with
	// future-time timestamps with earlier local timestamps. We are only able
	// to skip uncertainty checks if p.ts >= global_uncertainty_limit.
	//
	// We disable checkUncertainty when the scanner is configured with failOnMoreRecent.
	// This avoids cases in which a scan would have failed with a WriteTooOldError
	// but instead gets an unexpected ReadWithinUncertaintyIntervalError
	// See:
	// https://github.com/cockroachdb/cockroach/issues/119681
	p.checkUncertainty = p.ts.Less(p.uncertainty.GlobalLimit) && !p.failOnMoreRecent
}

// get seeks to the start key exactly once and adds one KV to the result set.
func (p *pebbleMVCCScanner) get(ctx context.Context) {
	p.parent.SeekGE(MVCCKey{Key: p.start})
	if !p.iterValid() {
		return
	}

	// Unlike scans, if tombstones are enabled, we synthesize point tombstones
	// for MVCC range tombstones even if there is no existing point key below
	// it. These are often needed for e.g. conflict checks. However, both
	// processRangeKeys and getOne may need to advance the iterator,
	// moving away from range key we originally landed on. If we're in tombstone
	// mode and there's a range key, save the most recent visible value so that
	// we can use it to synthesize a tombstone if we fail to find a KV.
	var hadMVCCRangeTombstone bool
	if p.tombstones {
		if _, hasRange := p.parent.HasPointAndRange(); hasRange {
			rangeKeys := p.parent.RangeKeys()
			if rkv, ok := rangeKeys.FirstAtOrBelow(p.ts); ok {
				hadMVCCRangeTombstone = true
				rkv.CloneInto(&p.savedRangeKeyVers)
			}
		}
	}

	var added bool
	if p.processRangeKeys(true /* seeked */, false /* reverse */) {
		if p.updateCurrent() {
			_, added = p.getOne(ctx)
		}
	}
	p.maybeFailOnMoreRecent()

	// In tombstone mode, if there was no existing point key we may need to
	// synthesize a point tombstone if we saved a range key before
	// Unlike scans, if tombstones are enabled, we synthesize point tombstones
	// for MVCC range tombstones even if there is no existing point key below
	// it. These are often needed for e.g. conflict checks.
	if p.tombstones && hadMVCCRangeTombstone && !added && p.err == nil {
		p.addSynthetic(ctx, p.start, p.savedRangeKeyVers)
	}
}

// seekToStartOfScan positions the scanner at the initial key.
func (p *pebbleMVCCScanner) seekToStartOfScan() (ok bool) {
	if p.reverse {
		if !p.iterSeekReverse(MVCCKey{Key: p.end}) {
			p.maybeFailOnMoreRecent() // may have seen a conflicting range key
			return false
		}
		p.machine.fn = advanceKeyReverse
	} else {
		if !p.iterSeek(MVCCKey{Key: p.start}) {
			p.maybeFailOnMoreRecent() // may have seen a conflicting range key
			return false
		}
		p.machine.fn = advanceKeyForward
	}
	return true
}

// advance advances the iterator according to the current state of the state
// machine.
func (p *pebbleMVCCScanner) advance() bool {
	switch p.machine.fn {
	case advanceKeyForward:
		return p.advanceKeyForward()
	case advanceKeyAtEndForward:
		// We've reached the end of the iterator and there is
		// nothing left to do.
		return false
	case advanceKeyAtNewKeyForward:
		// We're already at the new key so there is nothing to do.
		p.machine.fn = advanceKeyForward
		return true
	case advanceKeyReverse:
		return p.advanceKeyReverse()
	case advanceKeyAtEndReverse:
		p.machine.fn = advanceKeyReverse
		return p.advanceKeyAtEndReverse()
	case advanceKeyAtNewKeyReverse:
		p.machine.fn = advanceKeyReverse
		return p.advanceKeyAtNewKeyReverse()
	default:
		p.err = errors.AssertionFailedf("unexpected advanceFn: %d", p.machine.fn)
		return false
	}
}

// scan iterates until a limit is exceeded, the underlying iterator is
// exhausted, or an error is encountered. If a limit was exceeded, it returns a
// resume span, resume reason, and for targetBytes the size of the next result.
func (p *pebbleMVCCScanner) scan(
	ctx context.Context,
) (*roachpb.Span, kvpb.ResumeReason, int64, error) {
	if p.wholeRows && !p.results.(*pebbleResults).lastOffsetsEnabled {
		return nil, 0, 0, errors.AssertionFailedf("cannot use wholeRows without trackLastOffsets")
	}
	if !p.seekToStartOfScan() {
		return nil, 0, 0, p.err
	}
	for ok := true; ok; {
		ok, _ = p.getOne(ctx)
		if ok {
			ok = p.advance()
		}
	}
	return p.afterScan()
}

// afterScan checks whether some limit was exceeded during the scan, and if so,
// it returns a resume span, resume reason, and for targetBytes the size of the
// next result.
func (p *pebbleMVCCScanner) afterScan() (*roachpb.Span, kvpb.ResumeReason, int64, error) {
	p.maybeFailOnMoreRecent()

	if p.err != nil {
		return nil, 0, 0, p.err
	}

	if p.resumeReason != 0 {
		resumeKey := p.resumeKey
		if len(resumeKey) == 0 {
			if p.reverse {
				if !p.advanceKeyReverse() {
					return nil, 0, 0, nil // nothing to resume
				}
			} else {
				if !p.advanceKeyForward() {
					return nil, 0, 0, nil // nothing to resume
				}
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

// Increments itersBeforeSeek while ensuring it stays <= maxItersBeforeSeek.
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
		if maxItersBeforeSeek > 0 {
			p.itersBeforeSeek = 1
		} else if p.itersBeforeSeek < 0 {
			// maxItersBeforeSeek == 0 && p.itersBeforeSeek < 0.
			p.itersBeforeSeek = 0
		}
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
	p.err = kvpb.NewWriteTooOldError(p.ts, p.mostRecentTS.Next(), p.mostRecentKey)
	p.results.clear()
	p.intents.Reset()
}

// Returns an uncertainty error with the specified value and local timestamps,
// along with context about the reader.
func (p *pebbleMVCCScanner) uncertaintyError(
	valueTs hlc.Timestamp, localTs hlc.ClockTimestamp,
) (ok bool) {
	p.err = kvpb.NewReadWithinUncertaintyIntervalError(
		p.ts, p.uncertainty.LocalLimit, p.txn, valueTs, localTs)
	p.results.clear()
	p.intents.Reset()
	return false
}

// Get one tuple into the result set. This method will make at most one
// 'results.put' call regardless of whether 'put' returns an error or not.
// - ok indicates whether the iteration should continue.
// - added indicates whether a tuple was included into the result set.
// (ok=true, added=false) indicates that the current key was skipped for some
// reason, but the iteration should continue.
// (ok=false, added=true) indicates that the KV was included into the result but
// the iteration should stop.
//
// The scanner must be positioned on a point key, possibly with an overlapping
// range key. Range keys are processed separately in processRangeKeys().
func (p *pebbleMVCCScanner) getOne(ctx context.Context) (ok, added bool) {
	if !p.curUnsafeKey.Timestamp.IsEmpty() {
		// Range key where read ts >= range key ts >= point key ts. Synthesize a
		// point tombstone for it. Range key conflict checks are done in
		// processRangeKeys().
		if rkv, ok := p.coveredByRangeKey(p.curUnsafeKey.Timestamp); ok {
			return p.addSynthetic(ctx, p.curUnsafeKey.Key, rkv)
		}

		// We are eagerly fetching and decoding the value, even though it may be
		// too recent. With some care, this could be optimized to be lazy.
		v, valid := p.getFromLazyValue()
		if !valid {
			return false, false
		}

		uncertaintyCheckRequired := p.checkUncertainty && !p.curUnsafeKey.Timestamp.LessEq(p.ts)
		if !p.mvccHeaderRequired(uncertaintyCheckRequired) {
			if !p.decodeCurrentValueIgnoringHeader(v) {
				return false, false
			}
		} else if extended, valid := p.tryDecodeCurrentValueSimple(v); !valid {
			return false, false
		} else if extended {
			if !p.decodeCurrentValueExtended(v) {
				return false, false
			}
		}

		// ts < read_ts
		if p.curUnsafeKey.Timestamp.Less(p.ts) {
			// 1. Fast path: there is no intent and our read timestamp is newer
			// than the most recent version's timestamp.
			return p.add(ctx, p.curUnsafeKey.Key, p.curRawKey, p.curUnsafeValue.Value.RawBytes, v)
		}

		// ts == read_ts
		if p.curUnsafeKey.Timestamp == p.ts {
			if p.failOnMoreRecent {
				// 2. Our txn's read timestamp is equal to the most recent
				// version's timestamp and the scanner has been configured to
				// throw a write too old error on equal or more recent versions.

				if p.skipLocked {
					if locked, ok := p.isKeyLockedByConflictingTxn(ctx, p.curRawKey); !ok {
						return false, false
					} else if locked {
						// 2a. the scanner was configured to skip locked keys, and
						// this key was locked, so we can advance past it without
						// raising the write too old error.
						return true /* ok */, false
					}
				}

				// 2b. We need to raise a write too old error. Merge the current
				// timestamp with the maximum timestamp we've seen so we know to
				// return an error, but then keep scanning so that we can return
				// the largest possible time.
				if p.mostRecentTS.Forward(p.curUnsafeKey.Timestamp) {
					p.mostRecentKey = append(p.mostRecentKey[:0], p.curUnsafeKey.Key...)
				}
				return true /* ok */, false
			}

			// 3. There is no intent and our read timestamp is equal to the most
			// recent version's timestamp.
			return p.add(ctx, p.curUnsafeKey.Key, p.curRawKey, p.curUnsafeValue.Value.RawBytes, v)
		}

		// ts > read_ts
		if p.failOnMoreRecent {
			// 4. Our txn's read timestamp is less than the most recent
			// version's timestamp and the scanner has been configured to
			// throw a write too old error on equal or more recent versions.

			if p.skipLocked {
				if locked, ok := p.isKeyLockedByConflictingTxn(ctx, p.curRawKey); !ok {
					return false, false
				} else if locked {
					// 4a. the scanner was configured to skip locked keys, and
					// this key was locked, so we can advance past it without
					// raising the write too old error.
					return true /* ok */, false
				}
			}

			// 4b. We need to raise a write too old error. Merge the current
			// timestamp with the maximum timestamp we've seen so we know to
			// return an error, but then keep scanning so that we can return
			// the largest possible time.
			if p.mostRecentTS.Forward(p.curUnsafeKey.Timestamp) {
				p.mostRecentKey = append(p.mostRecentKey[:0], p.curUnsafeKey.Key...)
			}
			return true /* ok */, false
		}

		if p.checkUncertainty {
			// 5. Our txn's read timestamp is less than the max timestamp
			// seen by the txn. We need to check for clock uncertainty
			// errors.
			localTS := p.curUnsafeValue.GetLocalTimestamp(p.curUnsafeKey.Timestamp)
			if p.uncertainty.IsUncertain(p.curUnsafeKey.Timestamp, localTS) {
				return p.uncertaintyError(p.curUnsafeKey.Timestamp, localTS), false
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
		return false, false
	}
	if len(p.meta.RawBytes) != 0 {
		// 7. Emit immediately if the value is inline.
		//
		// TODO(ssd): We error if we find an inline when
		// ReturnRawMVCCValues is set. Anyone scanning with
		// that option set should not be encountering inline
		// values.
		//
		// https://github.com/cockroachdb/cockroach/issues/131667
		return p.add(ctx, p.curUnsafeKey.Key, p.curRawKey, p.meta.RawBytes, p.meta.RawBytes)
	}

	if p.meta.Txn == nil {
		p.err = errors.Errorf("intent without transaction")
		return false, false
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
				return false, false
			}
			return p.seekVersion(ctx, prevTS, false)
		}

		if p.skipLocked {
			// 10. The scanner has been configured with the skipLocked option. Ignore
			// intents written by other transactions and seek to the next key.
			// However, we return the intent separately if we have room; the caller
			// may want to resolve it. Unlike below, this intent will not result in
			// a LockConflictError because MVCC{Scan,Get}Options.errOnIntents returns
			// false when skipLocked in enabled.
			if p.maxLockConflicts == 0 || int64(p.intents.Count()) < p.maxLockConflicts {
				if !p.addCurIntent(ctx) {
					return false, false
				}
			}
			return true /* ok */, false
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
			return false, false
		}
		// Limit number of intents returned in lock conflict error.
		if p.maxLockConflicts > 0 && int64(p.intents.Count()) >= p.maxLockConflicts {
			p.resumeReason = kvpb.RESUME_INTENT_LIMIT
			return false, false
		}
		return true /* ok */, false
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
			// add to take an MVCCKey explicitly.
			p.curUnsafeKey.Timestamp = metaTS
			p.keyBuf = EncodeMVCCKeyToBuf(p.keyBuf[:0], p.curUnsafeKey)
			p.curUnsafeValue, p.err = DecodeMVCCValue(intentValueRaw)
			if p.err != nil {
				return false, false
			}
			return p.add(ctx, p.curUnsafeKey.Key, p.keyBuf, p.curUnsafeValue.Value.RawBytes, intentValueRaw)
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
		return false, false
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
	if p.reverse && p.peeked {
		// If the parent iterator is in reverse because we've peeked, then we
		// can step the iterator once to land back onto the current key before
		// we fallthrough to call NextKey.
		if !p.iterNext() {
			return false
		}
		// Fallthrough to NextKey.
	}
	p.parent.NextKey()
	if !p.iterValid() {
		return false
	}
	if !p.processRangeKeys(false /* seeked */, false /* reverse */) {
		return false
	}
	return p.updateCurrent()
}

// backwardLatestVersion backs up the iterator to the latest version for the
// specified key. The parameter i is used to maintain iteration count between
// the loop here and the caller (usually prevKey). Returns false if the
// iterator was exhausted. Assumes that the iterator is currently positioned at
// the oldest version of key.
func (p *pebbleMVCCScanner) backwardLatestVersion(key []byte, i int) bool {
	p.keyBuf = append(p.keyBuf[:0], key...)

	for ; i < p.itersBeforeSeek; i++ {
		peekedKey, hasPoint, ok := p.iterPeekPrev()
		if !ok {
			// No previous entry exists, so we're at the latest version of key.
			return true
		}
		// We may peek a bare range key with the same start bound as the point key,
		// in which case we're also positioned on the latest point key version.
		if !bytes.Equal(peekedKey, p.keyBuf) || !hasPoint {
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
// specified key. Assumes that the iterator is currently positioned at key or 1
// record after key and that the key is "safe" (i.e. it's a copy and not the
// "current" key directly).
func (p *pebbleMVCCScanner) prevKey(key []byte) bool {
	for i := 0; i < p.itersBeforeSeek; i++ {
		if !p.iterPrev() {
			return false
		}
		if !bytes.Equal(p.curUnsafeKey.Key, key) {
			return p.backwardLatestVersion(p.curUnsafeKey.Key, i+1)
		}
	}

	p.decrementItersBeforeSeek()
	return p.iterSeekReverse(MVCCKey{Key: key})
}

// advanceKeyForward advances to the next key in the forward direction.
func (p *pebbleMVCCScanner) advanceKeyForward() bool {
	return p.nextKey()
}

// advanceKeyReverse advances to the next key in the reverse direction.
func (p *pebbleMVCCScanner) advanceKeyReverse() bool {
	// Make a copy to satisfy the contract of prevKey.
	p.keyBuf = append(p.keyBuf[:0], p.curUnsafeKey.Key...)
	return p.prevKey(p.keyBuf)
}

// setAdvanceKeyAtEnd updates the machine to the corresponding "advance key at
// end" state.
func (p *pebbleMVCCScanner) setAdvanceKeyAtEnd() {
	if p.reverse {
		p.machine.fn = advanceKeyAtEndReverse
	} else {
		p.machine.fn = advanceKeyAtEndForward
	}
}

// advanceKeyAtEndReverse advances to the next key when the iterator's end has
// been reached in the reverse direction.
func (p *pebbleMVCCScanner) advanceKeyAtEndReverse() bool {
	// Iterating to the next key might have caused the iterator to reach the
	// end of the key space. If that happens, back up to the very last key.
	p.peeked = false
	p.parent.SeekLT(MVCCKey{Key: p.end})
	if !p.iterValid() {
		return false
	}
	if !p.processRangeKeys(true /* seeked */, true /* reverse */) {
		return false
	}
	if !p.updateCurrent() {
		return false
	}
	return p.advanceKeyReverse()
}

// setAdvanceKeyAtNewKey updates the machine to the corresponding "advance key
// at new key" state.
func (p *pebbleMVCCScanner) setAdvanceKeyAtNewKey(origKey []byte) {
	p.machine.origKey = origKey
	if p.reverse {
		p.machine.fn = advanceKeyAtNewKeyReverse
	} else {
		p.machine.fn = advanceKeyAtNewKeyForward
	}
}

// advanceKeyAtNewKeyReverse advances to the key after the key stored in
// p.machine.origKey in the reverse direction, assuming we have just reached the
// key after p.machine.origKey in the forward direction.
func (p *pebbleMVCCScanner) advanceKeyAtNewKeyReverse() bool {
	// We've advanced to the next key but need to move back to the previous key.
	// Note that we already made the copy in seekVersion, so we can just use the
	// key as is.
	return p.prevKey(p.machine.origKey)
}

// IncludeStartKeyIntoErr wraps with the given error to include the provided
// start key of the scan as an additional detail.
func IncludeStartKeyIntoErr(startKey roachpb.Key, err error) error {
	return errors.Wrapf(err, "scan with start key %s", startKey)
}

// Adds the specified key and value to the result set, excluding
// tombstones unless p.tombstones is true. If p.rawMVCCValues is true,
// then the mvccRawBytes argument will be added to the results set
// instead.
//
//   - ok indicates whether the iteration should continue. This can be false
//     because we hit an error or reached some limit.
//   - added indicates whether the key and value were included into the result
//     set.
func (p *pebbleMVCCScanner) add(
	ctx context.Context, key roachpb.Key, rawKey []byte, rawValue []byte, mvccRawBytes []byte,
) (ok, added bool) {
	// Don't include deleted versions len(val) == 0, unless we've been instructed
	// to include tombstones in the results.
	if len(rawValue) == 0 && !p.tombstones {
		return true /* ok */, false
	}
	if p.rawMVCCValues {
		rawValue = mvccRawBytes
	}

	// If the scanner has been configured with the skipLocked option, don't
	// include locked keys in the result set. Consult the in-memory lock table to
	// determine whether this is locked with an unreplicated lock. Replicated
	// locks will be represented as intents, which will be skipped over in
	// getAndAdvance.
	if p.skipLocked {
		if locked, ok := p.isKeyLockedByConflictingTxn(ctx, rawKey); !ok {
			return false, false
		} else if locked {
			return true /* ok */, false
		}
	}

	numKeys, numBytes, numBytesInc := p.results.sizeInfo(len(rawKey), len(rawValue))

	// Check if adding the key would exceed a limit.
	if p.targetBytes > 0 && numBytes+numBytesInc > p.targetBytes {
		p.resumeReason = kvpb.RESUME_BYTE_LIMIT
		p.resumeNextBytes = numBytesInc

	} else if p.maxKeys > 0 && numKeys >= p.maxKeys {
		p.resumeReason = kvpb.RESUME_KEY_LIMIT
	}

	var mustPutKey bool
	if p.resumeReason != 0 {
		// If we exceeded a limit, but we're not allowed to return an empty result,
		// then make sure we include the first key in the result. If wholeRows is
		// enabled, then also make sure we complete the first SQL row.
		if !p.allowEmpty &&
			(numKeys == 0 || (p.wholeRows && p.results.continuesFirstRow(key))) {
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
					return false, false
				}
				if trimmedKey != nil {
					p.resumeKey = trimmedKey
				}
			}
			return false, false
		}
	}

	// We are here due to one of the following cases:
	// A. No limits were exceeded
	// B. Limits were exceeded, but we need to put a key, so mustPutKey = true.
	//
	// For B we will never set maxNewSize.
	// For A, we may set maxNewSize, but we already know that
	//   p.targetBytes >= numBytes + numBytesInc
	// so maxNewSize will be sufficient.
	var maxNewSize int
	if p.targetBytes > 0 && p.targetBytes > numBytes && !mustPutKey {
		// INVARIANT: !mustPutKey => maxNewSize is sufficient for key-value
		// pair.
		maxNewSize = int(p.targetBytes - numBytes)
	}
	if err := p.results.put(ctx, rawKey, rawValue, p.memAccount, maxNewSize); err != nil {
		p.err = IncludeStartKeyIntoErr(p.start, err)
		return false, false
	}
	numKeys++

	// Check if we hit the key limit just now to avoid scanning further before
	// checking the key limit above on the next iteration. This has a small cost
	// (~0.5% for large scans), but avoids the potentially large cost of scanning
	// lots of garbage before the next key -- especially when maxKeys is small.
	if p.maxKeys > 0 && numKeys >= p.maxKeys {
		// If we're not allowed to return partial SQL rows, check whether the last
		// KV pair in the result has the maximum column family ID of the row. If so,
		// we can return early. However, if it doesn't then we can't know yet
		// whether the row is complete or not, because the final column families of
		// the row may have been omitted (if they are all NULL values) -- to find
		// out, we must continue scanning to the next key and handle it above.
		if !p.wholeRows || p.results.lastRowHasFinalColumnFamily(p.reverse) {
			p.resumeReason = kvpb.RESUME_KEY_LIMIT
			return false /* ok */, true /* added */
		}
	}
	return true /* ok */, true /* added */
}

// addSynthetic adds a synthetic point key for the given range key version.
func (p *pebbleMVCCScanner) addSynthetic(
	ctx context.Context, key roachpb.Key, version MVCCRangeKeyVersion,
) (ok, added bool) {
	p.keyBuf = EncodeMVCCKeyToBuf(p.keyBuf[:0], MVCCKey{Key: key, Timestamp: version.Timestamp})
	var value MVCCValue
	var simple bool
	value, simple, p.err = tryDecodeSimpleMVCCValue(version.Value)
	if !simple && p.err == nil {
		value, p.err = decodeExtendedMVCCValue(version.Value, p.decodeMVCCHeaders)
	}
	if p.err != nil {
		return false, false
	}

	return p.add(ctx, key, p.keyBuf, value.Value.RawBytes, version.Value)
}

// Seeks to the latest revision of the current key that's still less than or
// equal to the specified timestamp and adds it to the result set.
//   - ok indicates whether the iteration should continue.
//   - added indicates whether the key and value were included into the result
//     set.
func (p *pebbleMVCCScanner) seekVersion(
	ctx context.Context, seekTS hlc.Timestamp, uncertaintyCheck bool,
) (ok, added bool) {
	if seekTS.IsEmpty() {
		// If the seek timestamp is empty, we've already seen all versions of this
		// key, so seek to the next key. Seeking to version zero of the current key
		// would be incorrect, as version zero is stored before all other versions.
		return true /* ok */, false
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
			p.setAdvanceKeyAtEnd()
			return true /* ok */, false
		}
		if !bytes.Equal(p.curUnsafeKey.Key, origKey) {
			p.incrementItersBeforeSeek()
			p.setAdvanceKeyAtNewKey(origKey)
			return true /* ok */, false
		}
		if p.curUnsafeKey.Timestamp.LessEq(seekTS) {
			p.incrementItersBeforeSeek()
			v, valid := p.getFromLazyValue()
			if !valid {
				return false, false
			}
			uncertaintyCheckRequired := uncertaintyCheck && !p.curUnsafeKey.Timestamp.LessEq(p.ts)
			if !p.mvccHeaderRequired(uncertaintyCheckRequired) {
				if !p.decodeCurrentValueIgnoringHeader(v) {
					return false, false
				}
			} else if extended, valid := p.tryDecodeCurrentValueSimple(v); !valid {
				return false, false
			} else if extended {
				if !p.decodeCurrentValueExtended(v) {
					return false, false
				}
			}
			if !uncertaintyCheckRequired {
				if rkv, ok := p.coveredByRangeKey(p.curUnsafeKey.Timestamp); ok {
					return p.addSynthetic(ctx, p.curUnsafeKey.Key, rkv)
				}
				return p.add(ctx, p.curUnsafeKey.Key, p.curRawKey, p.curUnsafeValue.Value.RawBytes, v)
			}
			// Iterate through uncertainty interval. Though we found a value in
			// the interval, it may not be uncertainty. This is because seekTS
			// is set to the transaction's global uncertainty limit, so we are
			// seeking based on the worst-case uncertainty, but values with a
			// time in the range (uncertainty.LocalLimit, uncertainty.GlobalLimit]
			// are only uncertain if they have an earlier local timestamp that is
			// before uncertainty.LocalLimit. Meanwhile, any value with a time in
			// the range (ts, uncertainty.LocalLimit] is uncertain.
			localTS := p.curUnsafeValue.GetLocalTimestamp(p.curUnsafeKey.Timestamp)
			if p.uncertainty.IsUncertain(p.curUnsafeKey.Timestamp, localTS) {
				return p.uncertaintyError(p.curUnsafeKey.Timestamp, localTS), false
			}
		}
	}

	p.decrementItersBeforeSeek()
	if !p.iterSeek(seekKey) {
		p.setAdvanceKeyAtEnd()
		return true /* ok */, false
	}
	for {
		if !bytes.Equal(p.curUnsafeKey.Key, origKey) {
			p.setAdvanceKeyAtNewKey(origKey)
			return true /* ok */, false
		}
		v, valid := p.getFromLazyValue()
		if !valid {
			return false, false
		}

		uncertaintyCheckRequired := uncertaintyCheck && !p.curUnsafeKey.Timestamp.LessEq(p.ts)
		if !p.mvccHeaderRequired(uncertaintyCheckRequired) {
			if !p.decodeCurrentValueIgnoringHeader(v) {
				return false, false
			}
		} else if extended, valid := p.tryDecodeCurrentValueSimple(v); !valid {
			return false, false
		} else if extended {
			if !p.decodeCurrentValueExtended(v) {
				return false, false
			}
		}
		if !uncertaintyCheckRequired {
			if rkv, ok := p.coveredByRangeKey(p.curUnsafeKey.Timestamp); ok {
				return p.addSynthetic(ctx, p.curUnsafeKey.Key, rkv)
			}
			return p.add(ctx, p.curUnsafeKey.Key, p.curRawKey, p.curUnsafeValue.Value.RawBytes, v)
		}
		// Iterate through uncertainty interval. See the comment above about why
		// a value in this interval is not necessarily cause for an uncertainty
		// error.
		localTS := p.curUnsafeValue.GetLocalTimestamp(p.curUnsafeKey.Timestamp)
		if p.uncertainty.IsUncertain(p.curUnsafeKey.Timestamp, localTS) {
			return p.uncertaintyError(p.curUnsafeKey.Timestamp, localTS), false
		}
		if !p.iterNext() {
			p.setAdvanceKeyAtEnd()
			return true /* ok */, false
		}
	}
}

// coveredByRangeKey returns the topmost range key at the current position
// between the given timestamp and the read timestamp p.ts, if any.
//
// gcassert:inline
func (p *pebbleMVCCScanner) coveredByRangeKey(ts hlc.Timestamp) (rkv MVCCRangeKeyVersion, ok bool) {
	// This code is a bit odd to fit it within the mid-stack inlining budget. We
	// can't use p.curRangeKeys.IsEmpty(), nor early returns.
	if len(p.curRangeKeys.Versions) > 0 {
		rkv, ok = p.doCoveredByRangeKey(ts)
	}
	return rkv, ok
}

// doCoveredByRangeKey is a helper for coveredByRangeKey to allow mid-stack
// inlining.  It is only called when there are range keys present.
func (p *pebbleMVCCScanner) doCoveredByRangeKey(ts hlc.Timestamp) (MVCCRangeKeyVersion, bool) {
	// In the common case when tombstones are disabled, range key masking will be
	// enabled and so the point key will generally always be above the upper range
	// key (unless we're reading in the past). We fast-path this here.
	if p.tombstones || ts.LessEq(p.curRangeKeys.Newest()) {
		if rkv, ok := p.curRangeKeys.FirstAtOrBelow(p.ts); ok && ts.LessEq(rkv.Timestamp) {
			return rkv, true
		}
	}
	return MVCCRangeKeyVersion{}, false
}

// processRangeKeys will check for any newly encountered MVCC range keys (as
// determined by RangeKeyChanged), perform conflict checks for them, decode them
// into p.curRangeKeys, and skip across bare range keys until positioned on a
// point key or exhausted iterator. It must be called after every iterator
// positioning operation, to make sure it sees the RangeKeyChanged signal.
// Requires a valid iterator. Returns true if iteration can continue.
//
// seeked must be set to true following an iterator seek operation. In the
// forward direction, bare range keys are only possible with RangeKeyChanged or
// SeekGE, which allows omitting HasPointAndRange calls in the common Next case.
// It's also required to handle the case where the scanner is given a used
// iterator that may already be positioned on a range key such that the initial
// seek won't trigger RangeKeyChanged.
//
// reverse must be set to true if the previous iterator operation was a reverse
// operation (SeekLT or Prev). This determines the direction to skip in, and
// also requires checking for bare range keys after every step, since we'll
// land on them last.
func (p *pebbleMVCCScanner) processRangeKeys(seeked bool, reverse bool) bool {

	// Look for new range keys to process, and step across bare range keys until
	// we land on a point key (or exhaust the iterator).
	for {
		// In the forward direction, we can only land on a bare range key when
		// RangeKeyChanged fires (at its start bound) or when we SeekGE within it.
		rangeKeyChanged := p.parent.RangeKeyChanged()
		if !rangeKeyChanged && !reverse && !seeked {
			return true
		}

		// We fast-path the common no-range-key case.
		hasPoint, hasRange := p.parent.HasPointAndRange()
		if !hasRange {
			p.curRangeKeys = MVCCRangeKeyStack{}
			return true
		}

		// Process new range keys. On the initial seek it's possible that we're
		// given an iterator that's already positioned on a range key, so
		// RangeKeyChanged won't fire -- we handle that case here as well.
		if rangeKeyChanged || (seeked && p.curRangeKeys.IsEmpty()) {
			p.curRangeKeys = p.parent.RangeKeys()

			// Check for conflicts with range keys at or above the read timestamp.
			// We don't need to handle e.g. skipLocked, because range keys don't
			// currently have intents.
			if p.failOnMoreRecent {
				if key := p.parent.UnsafeKey(); !hasPoint || !key.Timestamp.IsEmpty() {
					if newest := p.curRangeKeys.Newest(); p.ts.LessEq(newest) {
						if p.mostRecentTS.Forward(newest) {
							p.mostRecentKey = append(p.mostRecentKey[:0], key.Key...)
						}
					}
				}
			}

			// Check if any of the range keys are in the uncertainty interval.
			if p.checkUncertainty {
				for _, version := range p.curRangeKeys.Versions {
					if version.Timestamp.LessEq(p.ts) {
						break
					}
					var value MVCCValue
					var simple bool
					value, simple, p.err = tryDecodeSimpleMVCCValue(version.Value)
					if !simple && p.err == nil {
						value, p.err = decodeExtendedMVCCValue(version.Value, true)
					}
					if p.err != nil {
						return false
					}
					localTS := value.GetLocalTimestamp(version.Timestamp)
					if p.uncertainty.IsUncertain(version.Timestamp, localTS) {
						return p.uncertaintyError(version.Timestamp, localTS)
					}
				}
			}
		}

		// If we're on a point key we're done, otherwise keep stepping.
		if hasPoint {
			return true
		}
		if !reverse {
			p.parent.Next()
		} else {
			p.parent.Prev()
		}
		if !p.iterValid() {
			return false
		}
	}
}

// Updates cur{RawKey, UnsafeKey, RawValue} to match record the iterator is
// pointing to. Callers should call decodeCurrent{Metadata, Value} to decode
// the raw value if they need it.
//
// Must only be called with a valid iterator.
func (p *pebbleMVCCScanner) updateCurrent() bool {
	p.curRawKey = p.parent.UnsafeRawMVCCKey()

	var err error
	p.curUnsafeKey, err = DecodeMVCCKey(p.curRawKey)
	if err != nil {
		p.err = errors.Wrap(err, "unable to decode MVCCKey")
		return false
	}
	p.curRawValue = p.parent.UnsafeLazyValue()

	// Reset decoded value to avoid bugs.
	if util.RaceEnabled {
		p.meta = enginepb.MVCCMetadata{}
		p.curUnsafeValue = MVCCValue{}
	}
	return true
}

func (p *pebbleMVCCScanner) getFromLazyValue() (v []byte, valid bool) {
	v, callerOwned, err := p.curRawValue.Value(p.lazyValueBuf)
	if err != nil {
		p.err = err
		return nil, false
	}
	if callerOwned {
		p.lazyValueBuf = v[:0]
	}
	return v, true
}

func (p *pebbleMVCCScanner) decodeCurrentMetadata() bool {
	val, valid := p.getFromLazyValue()
	if !valid {
		return false
	}
	if len(val) == 0 {
		p.err = errors.Errorf("zero-length mvcc metadata")
		return false
	}
	err := protoutil.Unmarshal(val, &p.meta)
	if err != nil {
		p.err = errors.Wrap(err, "unable to decode MVCCMetadata")
		return false
	}
	return true
}

// mvccHeaderRequired returns true if the caller should fully
// unmarshal the MVCCValueHeader when parsing an MVCCValue.
//
// The passed bool indicates whether the caller needs the
// MVCCValueHeader because they are going to do an uncertainty check,
// which may require the LocalTimestamp stored in the MVCCValueHeader
//
//gcassert:inline
func (p *pebbleMVCCScanner) mvccHeaderRequired(uncertaintyCheckRequired bool) bool {
	return uncertaintyCheckRequired || p.decodeMVCCHeaders
}

//gcassert:inline
func (p *pebbleMVCCScanner) decodeCurrentValueIgnoringHeader(v []byte) bool {
	p.curUnsafeValue, p.err = decodeMVCCValueIgnoringHeader(v)
	return p.err == nil
}

//gcassert:inline
func (p *pebbleMVCCScanner) tryDecodeCurrentValueSimple(v []byte) (extended bool, valid bool) {
	var simple bool
	p.curUnsafeValue, simple, p.err = tryDecodeSimpleMVCCValue(v)
	return !simple, p.err == nil
}

//gcassert:inline
func (p *pebbleMVCCScanner) decodeCurrentValueExtended(v []byte) bool {
	p.curUnsafeValue, p.err = decodeExtendedMVCCValue(v, true)
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
	return true
}

// iterSeek seeks to the latest revision of the specified key (or a greater key).
func (p *pebbleMVCCScanner) iterSeek(key MVCCKey) bool {
	p.clearPeeked()
	p.parent.SeekGE(key)
	if !p.iterValid() {
		return false
	}
	if !p.processRangeKeys(true /* seeked */, false /* reverse */) {
		return false
	}
	return p.updateCurrent()
}

// iterSeekReverse seeks to the latest revision of the key before the specified key.
func (p *pebbleMVCCScanner) iterSeekReverse(key MVCCKey) bool {
	p.clearPeeked()
	p.parent.SeekLT(key)
	if !p.iterValid() {
		return false
	}
	if !p.processRangeKeys(true /* seeked */, true /* reverse */) {
		return false
	}
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
		// to get back to the current entry. If we've peeked off the beginning,
		// it's okay to Next: Pebble will reposition to the first visible key.
		p.peeked = false
		p.parent.Next()
		// We don't need to process range key changes here, because curRangeKeys
		// already contains the range keys at this position from before the peek.
		if buildutil.CrdbTestBuild {
			p.assertOwnedRangeKeys()
		}
		if !p.iterValid() {
			return false
		}
	}
	// Step forward from the current entry.
	p.parent.Next()
	if !p.iterValid() {
		return false
	}
	if !p.processRangeKeys(false /* seeked */, false /* reverse */) {
		return false
	}
	return p.updateCurrent()
}

// iterPrev advances to the previous MVCC Key.
func (p *pebbleMVCCScanner) iterPrev() bool {
	if p.peeked {
		p.peeked = false
	} else {
		p.parent.Prev()
	}
	if !p.iterValid() {
		return false
	}
	if !p.processRangeKeys(false /* seeked */, true /* reverse */) {
		return false
	}
	return p.updateCurrent()
}

// Peek the previous key and store the result in peekedKey, also returning
// whether the peeked key had a point key or only a bare range key. Note that
// this moves the iterator backward, while leaving p.cur{key,value,rawKey,RangeKeys}
// untouched and therefore out of sync. iterPrev and iterNext take this into
// account.
//
// NB: Unlike iterPrev() and iterNext(), iterPeekPrev() will not skip across
// bare range keys: we have to do conflict checks on any new range keys when we
// step onto them, which may happen on the next positioning operation. The
// returned hasPoint value will indicate whether the peeked position is a bare
// range key or not.
func (p *pebbleMVCCScanner) iterPeekPrev() ([]byte, bool, bool) {
	if !p.peeked {
		p.peeked = true
		// We need to save a copy of the current iterator key and value and adjust
		// curRawKey, curKey and curValue to point to this saved data. We use a
		// single buffer for this purpose: savedBuf.
		p.savedBuf = append(p.savedBuf[:0], p.curRawKey...)
		p.curRawValue, p.savedBuf = p.curRawValue.Clone(p.savedBuf, &p.lazyFetcherBuf)
		p.curRawKey = p.savedBuf[:len(p.curRawKey)]
		// The raw key is always a prefix of the encoded MVCC key. Take advantage of this to
		// sub-slice the raw key directly, instead of calling SplitMVCCKey.
		p.curUnsafeKey.Key = p.curRawKey[:len(p.curUnsafeKey.Key)]
		// We need to save copies of the current range keys too, but we can avoid
		// this if we already saved them previously (if cur and saved share memory).
		if curStart := p.curRangeKeys.Bounds.Key; len(curStart) > 0 {
			savedStart := p.savedRangeKeys.Bounds.Key
			if len(curStart) != len(savedStart) || &curStart[0] != &savedStart[0] {
				p.curRangeKeys.CloneInto(&p.savedRangeKeys)
				p.curRangeKeys = p.savedRangeKeys
			}
		}

		// With the current iterator state saved we can move the iterator to the
		// previous entry.
		p.parent.Prev()
		if !p.iterValid() {
			// The iterator is now invalid, but note that this case is handled in
			// both iterNext and iterPrev. In the former case, we'll position the
			// iterator at the first entry, and in the latter iteration will be done.
			return nil, false, false
		}
	} else if !p.iterValid() {
		return nil, false, false
	}

	peekedKey := p.parent.UnsafeKey()

	// We may land on a bare range key without RangeKeyChanged firing, but only at
	// its start bound where the timestamp must be empty. HasPointAndRange() is
	// not cheap, so we only check it when necessary.
	hasPoint := true
	if peekedKey.Timestamp.IsEmpty() {
		hasPoint, _ = p.parent.HasPointAndRange()
	}

	return peekedKey.Key, hasPoint, true
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
	ok, txn, err := p.lockTable.IsKeyLockedByConflictingTxn(ctx, key)
	if err != nil {
		p.err = err
		return false, false
	}
	if ok {
		// The key is locked or reserved, so ignore it.
		if txn != nil && (p.maxLockConflicts == 0 || int64(p.intents.Count()) < p.maxLockConflicts) {
			// However, if the key is locked, we return the lock holder separately
			// (if we have room); the caller may want to resolve it.
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
	v, valid := p.getFromLazyValue()
	if !valid {
		return false
	}
	return p.addRawIntent(ctx, p.curRawKey, v)
}

// addKeyAndMetaAsIntent adds the key and transaction meta as an intent to
// the intents set.
func (p *pebbleMVCCScanner) addKeyAndMetaAsIntent(
	ctx context.Context, key roachpb.Key, txn *enginepb.TxnMeta,
) bool {
	if txn == nil {
		p.err = errors.AssertionFailedf("nil txn passed to addKeyAndMetaAsIntent")
		return false
	}
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
		p.err = IncludeStartKeyIntoErr(p.start, p.err)
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

// assertOwnedRangeKeys asserts that p.curRangeKeys is empty, or backed by
// p.savedRangeKeys's buffers.
func (p *pebbleMVCCScanner) assertOwnedRangeKeys() {
	if p.curRangeKeys.IsEmpty() {
		return
	}
	// NB: We compare on the EndKey in case the start key is /Min, the empty
	// key.
	if &p.curRangeKeys.Bounds.EndKey[0] != &p.savedRangeKeys.Bounds.EndKey[0] {
		panic(errors.AssertionFailedf("current range keys are not scanner-owned"))
	}
}
