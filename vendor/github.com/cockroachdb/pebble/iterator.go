// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"io"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/fastrand"
	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/redact"
)

// iterPos describes the state of the internal iterator, in terms of whether it
// is at the position returned to the user (cur), one ahead of the position
// returned (next for forward iteration and prev for reverse iteration). The cur
// position is split into two states, for forward and reverse iteration, since
// we need to differentiate for switching directions.
//
// There is subtlety in what is considered the current position of the Iterator.
// The internal iterator exposes a sequence of internal keys. There is not
// always a single internalIterator position corresponding to the position
// returned to the user. Consider the example:
//
//    a.MERGE.9 a.MERGE.8 a.MERGE.7 a.SET.6 b.DELETE.9 b.DELETE.5 b.SET.4
//    \                                   /
//      \       Iterator.Key() = 'a'    /
//
// The Iterator exposes one valid position at user key 'a' and the two exhausted
// positions at the beginning and end of iteration. The underlying
// internalIterator contains 7 valid positions and 2 exhausted positions.
//
// Iterator positioning methods must set iterPos to iterPosCur{Foward,Backward}
// iff the user key at the current internalIterator position equals the
// Iterator.Key returned to the user. This guarantees that a call to nextUserKey
// or prevUserKey will advance to the next or previous iterator position.
// iterPosCur{Forward,Backward} does not make any guarantee about the internal
// iterator position among internal keys with matching user keys, and it will
// vary subtly depending on the particular key kinds encountered. In the above
// example, the iterator returning 'a' may set iterPosCurForward if the internal
// iterator is positioned at any of a.MERGE.9, a.MERGE.8, a.MERGE.7 or a.SET.6.
//
// When setting iterPos to iterPosNext or iterPosPrev, the internal iterator
// must be advanced to the first internalIterator position at a user key greater
// (iterPosNext) or less (iterPosPrev) than the key returned to the user. An
// internalIterator position that's !Valid() must also be considered greater or
// less—depending on the direction of iteration—than the last valid Iterator
// position.
type iterPos int8

const (
	iterPosCurForward iterPos = 0
	iterPosNext       iterPos = 1
	iterPosPrev       iterPos = -1
	iterPosCurReverse iterPos = -2

	// For limited iteration. When the iterator is at iterPosCurForwardPaused
	// - Next*() call should behave as if the internal iterator is already
	//   at next (akin to iterPosNext).
	// - Prev*() call should behave as if the internal iterator is at the
	//   current key (akin to iterPosCurForward).
	//
	// Similar semantics apply to CurReversePaused.
	iterPosCurForwardPaused iterPos = 2
	iterPosCurReversePaused iterPos = -3
)

// Approximate gap in bytes between samples of data read during iteration.
// This is multiplied with a default ReadSamplingMultiplier of 1 << 4 to yield
// 1 << 20 (1MB). The 1MB factor comes from:
// https://github.com/cockroachdb/pebble/issues/29#issuecomment-494477985
const readBytesPeriod uint64 = 1 << 16

var errReversePrefixIteration = errors.New("pebble: unsupported reverse prefix iteration")

// IteratorMetrics holds per-iterator metrics. These do not change over the
// lifetime of the iterator.
type IteratorMetrics struct {
	// The read amplification experienced by this iterator. This is the sum of
	// the memtables, the L0 sublevels and the non-empty Ln levels. Higher read
	// amplification generally results in slower reads, though allowing higher
	// read amplification can also result in faster writes.
	ReadAmp int
}

// IteratorStatsKind describes the two kind of iterator stats.
type IteratorStatsKind int8

const (
	// InterfaceCall represents calls to Iterator.
	InterfaceCall IteratorStatsKind = iota
	// InternalIterCall represents calls by Iterator to its internalIterator.
	InternalIterCall
	// NumStatsKind is the number of kinds, and is used for array sizing.
	NumStatsKind
)

// IteratorStats contains iteration stats.
type IteratorStats struct {
	// ForwardSeekCount includes SeekGE, SeekPrefixGE, First.
	ForwardSeekCount [NumStatsKind]int
	// ReverseSeek includes SeekLT, Last.
	ReverseSeekCount [NumStatsKind]int
	// ForwardStepCount includes Next.
	ForwardStepCount [NumStatsKind]int
	// ReverseStepCount includes Prev.
	ReverseStepCount [NumStatsKind]int
	InternalStats    InternalIteratorStats
}

var _ redact.SafeFormatter = &IteratorStats{}

// InternalIteratorStats contains miscellaneous stats produced by internal
// iterators.
type InternalIteratorStats = base.InternalIteratorStats

// Iterator iterates over a DB's key/value pairs in key order.
//
// An iterator must be closed after use, but it is not necessary to read an
// iterator until exhaustion.
//
// An iterator is not goroutine-safe, but it is safe to use multiple iterators
// concurrently, with each in a dedicated goroutine.
//
// It is also safe to use an iterator concurrently with modifying its
// underlying DB, if that DB permits modification. However, the resultant
// key/value pairs are not guaranteed to be a consistent snapshot of that DB
// at a particular point in time.
//
// If an iterator encounters an error during any operation, it is stored by
// the Iterator and surfaced through the Error method. All absolute
// positioning methods (eg, SeekLT, SeekGT, First, Last, etc) reset any
// accumulated error before positioning. All relative positioning methods (eg,
// Next, Prev) return without advancing if the iterator has an accumulated
// error.
type Iterator struct {
	opts      IterOptions
	merge     Merge
	comparer  base.Comparer
	iter      internalIterator
	pointIter internalIterator
	readState *readState
	// rangeKey holds iteration state specific to iteration over range keys.
	// The range key field may be nil if the Iterator has never been configured
	// to iterate over range keys. Its non-nilness cannot be used to determine
	// if the Iterator is currently iterating over range keys: For that, consult
	// the IterOptions using opts.rangeKeys(). If non-nil, its rangeKeyIter
	// field is guaranteed to be non-nil too.
	rangeKey *iteratorRangeKeyState
	// rangeKeyMasking holds state for range-key masking of point keys.
	rangeKeyMasking rangeKeyMasking
	err             error
	// When iterValidityState=IterValid, key represents the current key, which
	// is backed by keyBuf.
	key         []byte
	keyBuf      []byte
	value       []byte
	valueBuf    []byte
	valueCloser io.Closer
	// boundsBuf holds two buffers used to store the lower and upper bounds.
	// Whenever the Iterator's bounds change, the new bounds are copied into
	// boundsBuf[boundsBufIdx]. The two bounds share a slice to reduce
	// allocations. opts.LowerBound and opts.UpperBound point into this slice.
	boundsBuf    [2][]byte
	boundsBufIdx int
	// iterKey, iterValue reflect the latest position of iter, except when
	// SetBounds is called. In that case, these are explicitly set to nil.
	iterKey             *InternalKey
	iterValue           []byte
	alloc               *iterAlloc
	getIterAlloc        *getIterAlloc
	prefixOrFullSeekKey []byte
	readSampling        readSampling
	stats               IteratorStats
	externalReaders     [][]*sstable.Reader

	// Following fields used when constructing an iterator stack, eg, in Clone
	// and SetOptions or when re-fragmenting a batch's range keys/range dels.
	// Non-nil if this Iterator includes a Batch.
	batch            *Batch
	newIters         tableNewIters
	newIterRangeKey  keyspan.TableNewSpanIter
	lazyCombinedIter lazyCombinedIter
	seqNum           uint64
	// batchSeqNum is used by Iterators over indexed batches to detect when the
	// underlying batch has been mutated. The batch beneath an indexed batch may
	// be mutated while the Iterator is open, but new keys are not surfaced
	// until the next call to SetOptions.
	batchSeqNum uint64
	// batch{PointIter,RangeDelIter,RangeKeyIter} are used when the Iterator is
	// configured to read through an indexed batch. If a batch is set, these
	// iterators will be included within the iterator stack regardless of
	// whether the batch currently contains any keys of their kind. These
	// pointers are used during a call to SetOptions to refresh the Iterator's
	// view of its indexed batch.
	batchPointIter    batchIter
	batchRangeDelIter keyspan.Iter
	batchRangeKeyIter keyspan.Iter

	// Keeping the bools here after all the 8 byte aligned fields shrinks the
	// sizeof this struct by 24 bytes.

	// INVARIANT:
	// iterValidityState==IterAtLimit <=>
	//  pos==iterPosCurForwardPaused || pos==iterPosCurReversePaused
	iterValidityState IterValidityState
	// Set to true by SetBounds, SetOptions. Causes the Iterator to appear
	// exhausted externally, while preserving the correct iterValidityState for
	// the iterator's internal state. Preserving the correct internal validity
	// is used for SeekPrefixGE(..., trySeekUsingNext), and SeekGE/SeekLT
	// optimizations after "no-op" calls to SetBounds and SetOptions.
	requiresReposition bool
	// The position of iter. When this is iterPos{Prev,Next} the iter has been
	// moved past the current key-value, which can only happen if
	// iterValidityState=IterValid, i.e., there is something to return to the
	// client for the current position.
	pos iterPos
	// Relates to the prefixOrFullSeekKey field above.
	hasPrefix bool
	// Used for deriving the value of SeekPrefixGE(..., trySeekUsingNext),
	// and SeekGE/SeekLT optimizations
	lastPositioningOp lastPositioningOpKind
	// Used for an optimization in external iterators to reduce the number of
	// merging levels.
	forwardOnly bool
	// closePointIterOnce is set to true if this point iter can only be Close()d
	// once, _and_ closing i.iter and then i.pointIter would close i.pointIter
	// twice. This is necessary to track if the point iter is an internal iterator
	// that could release its resources to a pool on Close(), making it harder for
	// that iterator to make its own closes idempotent.
	//
	// TODO(bilal): Update SetOptions to always close out point key iterators when
	// they won't be used, so that Close() doesn't need to default to closing
	// point iterators twice.
	closePointIterOnce bool
	// Used in some tests to disable the random disabling of seek optimizations.
	forceEnableSeekOpt bool
}

// cmp is a convenience shorthand for the i.comparer.Compare function.
func (i *Iterator) cmp(a, b []byte) int {
	return i.comparer.Compare(a, b)
}

// split is a convenience shorthand for the i.comparer.Split function.
func (i *Iterator) split(a []byte) int {
	return i.comparer.Split(a)
}

// equal is a convenience shorthand for the i.comparer.Equal function.
func (i *Iterator) equal(a, b []byte) bool {
	return i.comparer.Equal(a, b)
}

// iteratorRangeKeyState holds an iterator's range key iteration state.
type iteratorRangeKeyState struct {
	opts  *IterOptions
	cmp   base.Compare
	split base.Split
	// rangeKeyIter holds the range key iterator stack that iterates over the
	// merged spans across the entirety of the LSM.
	rangeKeyIter keyspan.FragmentIterator
	iiter        keyspan.InterleavingIter
	// stale is set to true when the range key state recorded here (in start,
	// end and keys) may not be in sync with the current range key at the
	// interleaving iterator's current position.
	//
	// When the interelaving iterator passes over a new span, it invokes the
	// SpanChanged hook defined on the `rangeKeyMasking` type,  which sets stale
	// to true if the span is non-nil.
	//
	// The parent iterator may not be positioned over the interleaving
	// iterator's current position (eg, i.iterPos = iterPos{Next,Prev}), so
	// {keys,start,end} are only updated to the new range key during a call to
	// Iterator.saveRangeKey.
	stale bool
	// updated is used to signal to the Iterator client whether the state of
	// range keys has changed since the previous iterator position through the
	// `RangeKeyChanged` method. It's set to true during an Iterator positioning
	// operation that changes the state of the current range key. Each Iterator
	// positioning operation sets it back to false before executing.
	updated bool
	// prevPosHadRangeKey records whether the previous Iterator position had a
	// range key (HasPointAndRage() = (_, true)). It's updated at the beginning
	// of each new Iterator positioning operation. It's required by saveRangeKey to
	// to set `updated` appropriately: Without this record of the previous iterator
	// state, it's ambiguous whether an iterator only temporarily stepped onto a
	// position without a range key.
	prevPosHadRangeKey bool
	// rangeKeyOnly is set to true if at the current iterator position there is
	// no point key, only a range key start boundary.
	rangeKeyOnly bool
	// hasRangeKey is true when the current iterator position has a covering
	// range key (eg, a range key with bounds [<lower>,<upper>) such that
	// <lower> ≤ Key() < <upper>).
	hasRangeKey bool
	// start and end are the [start, end) boundaries of the current range keys.
	start []byte
	end   []byte
	// keys is sorted by Suffix ascending.
	keys []RangeKeyData
	// buf is used to save range-key data before moving the range-key iterator.
	// Start and end boundaries, suffixes and values are all copied into buf.
	buf []byte

	// iterConfig holds fields that are used for the construction of the
	// iterator stack, but do not need to be directly accessed during iteration.
	// This struct is bundled within the iteratorRangeKeyState struct to reduce
	// allocations.
	iterConfig rangekey.UserIteratorConfig
}

func (i *iteratorRangeKeyState) init(cmp base.Compare, split base.Split, opts *IterOptions) {
	i.cmp = cmp
	i.split = split
	i.opts = opts
}

var iterRangeKeyStateAllocPool = sync.Pool{
	New: func() interface{} {
		return &iteratorRangeKeyState{}
	},
}

// isEphemeralPosition returns true iff the current iterator position is
// ephemeral, and won't be visited during subsequent relative positioning
// operations.
//
// The iterator position resulting from a SeekGE or SeekPrefixGE that lands on a
// straddling range key without a coincident point key is such a position.
func (i *Iterator) isEphemeralPosition() bool {
	return i.opts.rangeKeys() && i.rangeKey != nil && i.rangeKey.rangeKeyOnly &&
		!i.equal(i.rangeKey.start, i.key)
}

type lastPositioningOpKind int8

const (
	unknownLastPositionOp lastPositioningOpKind = iota
	seekPrefixGELastPositioningOp
	seekGELastPositioningOp
	seekLTLastPositioningOp
)

// Limited iteration mode. Not for use with prefix iteration.
//
// SeekGE, SeekLT, Prev, Next have WithLimit variants, that pause the iterator
// at the limit in a best-effort manner. The client should behave correctly
// even if the limits are ignored. These limits are not "deep", in that they
// are not passed down to the underlying collection of internalIterators. This
// is because the limits are transient, and apply only until the next
// iteration call. They serve mainly as a way to bound the amount of work when
// two (or more) Iterators are being coordinated at a higher level.
//
// In limited iteration mode:
// - Avoid using Iterator.Valid if the last call was to a *WithLimit() method.
//   The return value from the *WithLimit() method provides a more precise
//   disposition.
// - The limit is exclusive for forward and inclusive for reverse.
//
//
// Limited iteration mode & range keys
//
// Limited iteration interacts with range-key iteration. When range key
// iteration is enabled, range keys are interleaved at their start boundaries.
// Limited iteration must ensure that if a range key exists within the limit,
// the iterator visits the range key.
//
// During forward limited iteration, this is trivial: An overlapping range key
// must have a start boundary less than the limit, and the range key's start
// boundary will be interleaved and found to be within the limit.
//
// During reverse limited iteration, the tail of the range key may fall within
// the limit. The range key must be surfaced even if the range key's start
// boundary is less than the limit, and if there are no point keys between the
// current iterator position and the limit. To provide this guarantee, reverse
// limited iteration ignores the limit as long as there is a range key
// overlapping the iteration position.

// IterValidityState captures the state of the Iterator.
type IterValidityState int8

const (
	// IterExhausted represents an Iterator that is exhausted.
	IterExhausted IterValidityState = iota
	// IterValid represents an Iterator that is valid.
	IterValid
	// IterAtLimit represents an Iterator that has a non-exhausted
	// internalIterator, but has reached a limit without any key for the
	// caller.
	IterAtLimit
)

// readSampling stores variables used to sample a read to trigger a read
// compaction
type readSampling struct {
	bytesUntilReadSampling uint64
	initialSamplePassed    bool
	pendingCompactions     readCompactionQueue
	// forceReadSampling is used for testing purposes to force a read sample on every
	// call to Iterator.maybeSampleRead()
	forceReadSampling bool
}

func (i *Iterator) findNextEntry(limit []byte) {
	i.iterValidityState = IterExhausted
	i.pos = iterPosCurForward
	if i.opts.rangeKeys() && i.rangeKey != nil {
		i.rangeKey.rangeKeyOnly = false
	}

	// Close the closer for the current value if one was open.
	if i.closeValueCloser() != nil {
		return
	}

	for i.iterKey != nil {
		key := *i.iterKey

		if i.hasPrefix {
			if n := i.split(key.UserKey); !i.equal(i.prefixOrFullSeekKey, key.UserKey[:n]) {
				return
			}
		}
		// Compare with limit every time we start at a different user key.
		// Note that given the best-effort contract of limit, we could avoid a
		// comparison in the common case by doing this only after
		// i.nextUserKey is called for the deletes below. However that makes
		// the behavior non-deterministic (since the behavior will vary based
		// on what has been compacted), which makes it hard to test with the
		// metamorphic test. So we forego that performance optimization.
		if limit != nil && i.cmp(limit, i.iterKey.UserKey) <= 0 {
			i.iterValidityState = IterAtLimit
			i.pos = iterPosCurForwardPaused
			return
		}

		switch key.Kind() {
		case InternalKeyKindRangeKeySet:
			// Save the current key.
			i.keyBuf = append(i.keyBuf[:0], key.UserKey...)
			i.key = i.keyBuf
			i.value = nil
			// There may also be a live point key at this userkey that we have
			// not yet read. We need to find the next entry with this user key
			// to find it. Save the range key so we don't lose it when we Next
			// the underlying iterator.
			i.saveRangeKey()
			pointKeyExists := i.nextPointCurrentUserKey()
			if i.err != nil {
				i.iterValidityState = IterExhausted
				return
			}
			i.rangeKey.rangeKeyOnly = !pointKeyExists
			i.iterValidityState = IterValid
			return

		case InternalKeyKindDelete, InternalKeyKindSingleDelete:
			i.nextUserKey()
			continue

		case InternalKeyKindSet, InternalKeyKindSetWithDelete:
			i.keyBuf = append(i.keyBuf[:0], key.UserKey...)
			i.key = i.keyBuf
			i.value = i.iterValue
			i.iterValidityState = IterValid
			i.saveRangeKey()
			return

		case InternalKeyKindMerge:
			// Resolving the merge may advance us to the next point key, which
			// may be covered by a different set of range keys. Save the range
			// key state so we don't lose it.
			i.saveRangeKey()
			if i.mergeForward(key) {
				i.iterValidityState = IterValid
				return
			}

			// The merge didn't yield a valid key, either because the value
			// merger indicated it should be deleted, or because an error was
			// encountered.
			i.iterValidityState = IterExhausted
			if i.err != nil {
				return
			}
			if i.pos != iterPosNext {
				i.nextUserKey()
			}
			if i.closeValueCloser() != nil {
				return
			}
			i.pos = iterPosCurForward

		default:
			i.err = base.CorruptionErrorf("pebble: invalid internal key kind: %d", errors.Safe(key.Kind()))
			i.iterValidityState = IterExhausted
			return
		}
	}
}

func (i *Iterator) nextPointCurrentUserKey() bool {
	i.pos = iterPosCurForward

	i.iterKey, i.iterValue = i.iter.Next()
	i.stats.ForwardStepCount[InternalIterCall]++
	if i.iterKey == nil || !i.equal(i.key, i.iterKey.UserKey) {
		i.pos = iterPosNext
		return false
	}

	key := *i.iterKey
	switch key.Kind() {
	case InternalKeyKindRangeKeySet:
		// RangeKeySets must always be interleaved as the first internal key
		// for a user key.
		i.err = base.CorruptionErrorf("pebble: unexpected range key set mid-user key")
		return false

	case InternalKeyKindDelete, InternalKeyKindSingleDelete:
		return false

	case InternalKeyKindSet, InternalKeyKindSetWithDelete:
		i.value = i.iterValue
		return true

	case InternalKeyKindMerge:
		return i.mergeForward(key)

	default:
		i.err = base.CorruptionErrorf("pebble: invalid internal key kind: %d", errors.Safe(key.Kind()))
		return false
	}
}

// mergeForward resolves a MERGE key, advancing the underlying iterator forward
// to merge with subsequent keys with the same userkey. mergeForward returns a
// boolean indicating whether or not the merge yielded a valid key. A merge may
// not yield a valid key if an error occurred, in which case i.err is non-nil,
// or the user's value merger specified the key to be deleted.
//
// mergeForward does not update iterValidityState.
func (i *Iterator) mergeForward(key base.InternalKey) (valid bool) {
	var valueMerger ValueMerger
	valueMerger, i.err = i.merge(key.UserKey, i.iterValue)
	if i.err != nil {
		return false
	}

	i.mergeNext(key, valueMerger)
	if i.err != nil {
		return false
	}

	var needDelete bool
	i.value, needDelete, i.valueCloser, i.err = finishValueMerger(
		valueMerger, true /* includesBase */)
	if i.err != nil {
		return false
	}
	if needDelete {
		_ = i.closeValueCloser()
		return false
	}
	return true
}

func (i *Iterator) closeValueCloser() error {
	if i.valueCloser != nil {
		i.err = i.valueCloser.Close()
		i.valueCloser = nil
	}
	return i.err
}

func (i *Iterator) nextUserKey() {
	if i.iterKey == nil {
		return
	}
	trailer := i.iterKey.Trailer
	done := i.iterKey.Trailer <= base.InternalKeyZeroSeqnumMaxTrailer
	if i.iterValidityState != IterValid {
		i.keyBuf = append(i.keyBuf[:0], i.iterKey.UserKey...)
		i.key = i.keyBuf
	}
	for {
		i.iterKey, i.iterValue = i.iter.Next()
		i.stats.ForwardStepCount[InternalIterCall]++
		// NB: We're guaranteed to be on the next user key if the previous key
		// had a zero sequence number (`done`), or the new key has a trailer
		// greater or equal to the previous key's trailer. This is true because
		// internal keys with the same user key are sorted by Trailer in
		// strictly monotonically descending order. We expect the trailer
		// optimization to trigger around 50% of the time with randomly
		// distributed writes. We expect it to trigger very frequently when
		// iterating through ingested sstables, which contain keys that all have
		// the same sequence number.
		if done || i.iterKey == nil || i.iterKey.Trailer >= trailer {
			break
		}
		if !i.equal(i.key, i.iterKey.UserKey) {
			break
		}
		done = i.iterKey.Trailer <= base.InternalKeyZeroSeqnumMaxTrailer
		trailer = i.iterKey.Trailer
	}
}

func (i *Iterator) maybeSampleRead() {
	// This method is only called when a public method of Iterator is
	// returning, and below we exclude the case were the iterator is paused at
	// a limit. The effect of these choices is that keys that are deleted, but
	// are encountered during iteration, are not accounted for in the read
	// sampling and will not cause read driven compactions, even though we are
	// incurring cost in iterating over them. And this issue is not limited to
	// Iterator, which does not see the effect of range deletes, which may be
	// causing iteration work in mergingIter. It is not clear at this time
	// whether this is a deficiency worth addressing.
	if i.iterValidityState != IterValid {
		return
	}
	if i.readState == nil {
		return
	}
	if i.readSampling.forceReadSampling {
		i.sampleRead()
		return
	}
	samplingPeriod := int32(int64(readBytesPeriod) * i.readState.db.opts.Experimental.ReadSamplingMultiplier)
	if samplingPeriod <= 0 {
		return
	}
	bytesRead := uint64(len(i.key) + len(i.value))
	for i.readSampling.bytesUntilReadSampling < bytesRead {
		i.readSampling.bytesUntilReadSampling += uint64(fastrand.Uint32n(2 * uint32(samplingPeriod)))
		// The block below tries to adjust for the case where this is the
		// first read in a newly-opened iterator. As bytesUntilReadSampling
		// starts off at zero, we don't want to sample the first read of
		// every newly-opened iterator, but we do want to sample some of them.
		if !i.readSampling.initialSamplePassed {
			i.readSampling.initialSamplePassed = true
			if fastrand.Uint32n(uint32(i.readSampling.bytesUntilReadSampling)) > uint32(bytesRead) {
				continue
			}
		}
		i.sampleRead()
	}
	i.readSampling.bytesUntilReadSampling -= bytesRead
}

func (i *Iterator) sampleRead() {
	var topFile *manifest.FileMetadata
	topLevel, numOverlappingLevels := numLevels, 0
	if mi, ok := i.iter.(*mergingIter); ok {
		if len(mi.levels) > 1 {
			mi.ForEachLevelIter(func(li *levelIter) bool {
				l := manifest.LevelToInt(li.level)
				if file := li.files.Current(); file != nil {
					var containsKey bool
					if i.pos == iterPosNext || i.pos == iterPosCurForward ||
						i.pos == iterPosCurForwardPaused {
						containsKey = i.cmp(file.SmallestPointKey.UserKey, i.key) <= 0
					} else if i.pos == iterPosPrev || i.pos == iterPosCurReverse ||
						i.pos == iterPosCurReversePaused {
						containsKey = i.cmp(file.LargestPointKey.UserKey, i.key) >= 0
					}
					// Do nothing if the current key is not contained in file's
					// bounds. We could seek the LevelIterator at this level
					// to find the right file, but the performance impacts of
					// doing that are significant enough to negate the benefits
					// of read sampling in the first place. See the discussion
					// at:
					// https://github.com/cockroachdb/pebble/pull/1041#issuecomment-763226492
					if containsKey {
						numOverlappingLevels++
						if numOverlappingLevels >= 2 {
							// Terminate the loop early if at least 2 overlapping levels are found.
							return true
						}
						topLevel = l
						topFile = file
					}
				}
				return false
			})
		}
	}
	if topFile == nil || topLevel >= numLevels {
		return
	}
	if numOverlappingLevels >= 2 {
		allowedSeeks := atomic.AddInt64(&topFile.Atomic.AllowedSeeks, -1)
		if allowedSeeks == 0 {

			// Since the compaction queue can handle duplicates, we can keep
			// adding to the queue even once allowedSeeks hits 0.
			// In fact, we NEED to keep adding to the queue, because the queue
			// is small and evicts older and possibly useful compactions.
			atomic.AddInt64(&topFile.Atomic.AllowedSeeks, topFile.InitAllowedSeeks)

			read := readCompaction{
				start:   topFile.SmallestPointKey.UserKey,
				end:     topFile.LargestPointKey.UserKey,
				level:   topLevel,
				fileNum: topFile.FileNum,
			}
			i.readSampling.pendingCompactions.add(&read, i.cmp)
		}
	}
}

func (i *Iterator) findPrevEntry(limit []byte) {
	i.iterValidityState = IterExhausted
	i.pos = iterPosCurReverse
	if i.opts.rangeKeys() && i.rangeKey != nil {
		i.rangeKey.rangeKeyOnly = false
	}

	// Close the closer for the current value if one was open.
	if i.valueCloser != nil {
		i.err = i.valueCloser.Close()
		i.valueCloser = nil
		if i.err != nil {
			i.iterValidityState = IterExhausted
			return
		}
	}

	var valueMerger ValueMerger
	firstLoopIter := true
	rangeKeyBoundary := false
	// The code below compares with limit in multiple places. As documented in
	// findNextEntry, this is being done to make the behavior of limit
	// deterministic to allow for metamorphic testing. It is not required by
	// the best-effort contract of limit.
	for i.iterKey != nil {
		key := *i.iterKey

		// NB: We cannot pause if the current key is covered by a range key.
		// Otherwise, the user might not ever learn of a range key that covers
		// the key space being iterated over in which there are no point keys.
		// Since limits are best effort, ignoring the limit in this case is
		// allowed by the contract of limit.
		if firstLoopIter && limit != nil && i.cmp(limit, i.iterKey.UserKey) > 0 && !i.rangeKeyWithinLimit(limit) {
			i.iterValidityState = IterAtLimit
			i.pos = iterPosCurReversePaused
			return
		}
		firstLoopIter = false

		if i.iterValidityState == IterValid {
			if !i.equal(key.UserKey, i.key) {
				// We've iterated to the previous user key.
				i.pos = iterPosPrev
				if valueMerger != nil {
					var needDelete bool
					i.value, needDelete, i.valueCloser, i.err = finishValueMerger(valueMerger, true /* includesBase */)
					if i.err == nil && needDelete {
						// The point key at this key is deleted. If we also have
						// a range key boundary at this key, we still want to
						// return. Otherwise, we need to continue looking for
						// a live key.
						i.value = nil
						if rangeKeyBoundary {
							i.rangeKey.rangeKeyOnly = true
						} else {
							i.iterValidityState = IterExhausted
							if i.closeValueCloser() == nil {
								continue
							}
						}
					}
				}
				if i.err != nil {
					i.iterValidityState = IterExhausted
				}
				return
			}
		}

		switch key.Kind() {
		case InternalKeyKindRangeKeySet:
			// Range key start boundary markers are interleaved with the maximum
			// sequence number, so if there's a point key also at this key, we
			// must've already iterated over it.
			// This is the final entry at this user key, so we may return
			i.rangeKey.rangeKeyOnly = i.iterValidityState != IterValid
			i.keyBuf = append(i.keyBuf[:0], key.UserKey...)
			i.key = i.keyBuf
			i.iterValidityState = IterValid
			i.saveRangeKey()
			// In all other cases, previous iteration requires advancing to
			// iterPosPrev in order to determine if the key is live and
			// unshadowed by another key at the same user key. In this case,
			// because range key start boundary markers are always interleaved
			// at the maximum sequence number, we know that there aren't any
			// additional keys with the same user key in the backward direction.
			//
			// We Prev the underlying iterator once anyways for consistency, so
			// that we can maintain the invariant during backward iteration that
			// i.iterPos = iterPosPrev.
			i.stats.ReverseStepCount[InternalIterCall]++
			i.iterKey, i.iterValue = i.iter.Prev()

			// Set rangeKeyBoundary so that on the next iteration, we know to
			// return the key even if the MERGE point key is deleted.
			rangeKeyBoundary = true

		case InternalKeyKindDelete, InternalKeyKindSingleDelete:
			i.value = nil
			i.iterValidityState = IterExhausted
			valueMerger = nil
			i.iterKey, i.iterValue = i.iter.Prev()
			i.stats.ReverseStepCount[InternalIterCall]++
			// Compare with the limit. We could optimize by only checking when
			// we step to the previous user key, but detecting that requires a
			// comparison too. Note that this position may already passed a
			// number of versions of this user key, but they are all deleted,
			// so the fact that a subsequent Prev*() call will not see them is
			// harmless. Also note that this is the only place in the loop,
			// other than the firstLoopIter case above, where we could step
			// to a different user key and start processing it for returning
			// to the caller.
			if limit != nil && i.iterKey != nil && i.cmp(limit, i.iterKey.UserKey) > 0 && !i.rangeKeyWithinLimit(limit) {
				i.iterValidityState = IterAtLimit
				i.pos = iterPosCurReversePaused
				return
			}
			continue

		case InternalKeyKindSet, InternalKeyKindSetWithDelete:
			i.keyBuf = append(i.keyBuf[:0], key.UserKey...)
			i.key = i.keyBuf
			// iterValue is owned by i.iter and could change after the Prev()
			// call, so use valueBuf instead. Note that valueBuf is only used
			// in this one instance; everywhere else (eg. in findNextEntry),
			// we just point i.value to the unsafe i.iter-owned value buffer.
			i.valueBuf = append(i.valueBuf[:0], i.iterValue...)
			i.value = i.valueBuf
			i.saveRangeKey()
			i.iterValidityState = IterValid
			i.iterKey, i.iterValue = i.iter.Prev()
			i.stats.ReverseStepCount[InternalIterCall]++
			valueMerger = nil
			continue

		case InternalKeyKindMerge:
			if i.iterValidityState == IterExhausted {
				i.keyBuf = append(i.keyBuf[:0], key.UserKey...)
				i.key = i.keyBuf
				i.saveRangeKey()
				valueMerger, i.err = i.merge(i.key, i.iterValue)
				if i.err != nil {
					return
				}
				i.iterValidityState = IterValid
			} else if valueMerger == nil {
				valueMerger, i.err = i.merge(i.key, i.value)
				if i.err == nil {
					i.err = valueMerger.MergeNewer(i.iterValue)
				}
				if i.err != nil {
					i.iterValidityState = IterExhausted
					return
				}
			} else {
				i.err = valueMerger.MergeNewer(i.iterValue)
				if i.err != nil {
					i.iterValidityState = IterExhausted
					return
				}
			}
			i.iterKey, i.iterValue = i.iter.Prev()
			i.stats.ReverseStepCount[InternalIterCall]++
			continue

		default:
			i.err = base.CorruptionErrorf("pebble: invalid internal key kind: %d", errors.Safe(key.Kind()))
			i.iterValidityState = IterExhausted
			return
		}
	}

	// i.iterKey == nil, so broke out of the preceding loop.
	if i.iterValidityState == IterValid {
		i.pos = iterPosPrev
		if valueMerger != nil {
			var needDelete bool
			i.value, needDelete, i.valueCloser, i.err = finishValueMerger(valueMerger, true /* includesBase */)
			if i.err == nil && needDelete {
				i.key = nil
				i.value = nil
				i.iterValidityState = IterExhausted
			}
		}
		if i.err != nil {
			i.iterValidityState = IterExhausted
		}
	}
}

func (i *Iterator) prevUserKey() {
	if i.iterKey == nil {
		return
	}
	if i.iterValidityState != IterValid {
		// If we're going to compare against the prev key, we need to save the
		// current key.
		i.keyBuf = append(i.keyBuf[:0], i.iterKey.UserKey...)
		i.key = i.keyBuf
	}
	for {
		i.iterKey, i.iterValue = i.iter.Prev()
		i.stats.ReverseStepCount[InternalIterCall]++
		if i.iterKey == nil {
			break
		}
		if !i.equal(i.key, i.iterKey.UserKey) {
			break
		}
	}
}

func (i *Iterator) mergeNext(key InternalKey, valueMerger ValueMerger) {
	// Save the current key.
	i.keyBuf = append(i.keyBuf[:0], key.UserKey...)
	i.key = i.keyBuf

	// Loop looking for older values for this key and merging them.
	for {
		i.iterKey, i.iterValue = i.iter.Next()
		i.stats.ForwardStepCount[InternalIterCall]++
		if i.iterKey == nil {
			i.pos = iterPosNext
			return
		}
		key = *i.iterKey
		if !i.equal(i.key, key.UserKey) {
			// We've advanced to the next key.
			i.pos = iterPosNext
			return
		}
		switch key.Kind() {
		case InternalKeyKindDelete, InternalKeyKindSingleDelete:
			// We've hit a deletion tombstone. Return everything up to this
			// point.
			return

		case InternalKeyKindSet, InternalKeyKindSetWithDelete:
			// We've hit a Set value. Merge with the existing value and return.
			i.err = valueMerger.MergeOlder(i.iterValue)
			return

		case InternalKeyKindMerge:
			// We've hit another Merge value. Merge with the existing value and
			// continue looping.
			i.err = valueMerger.MergeOlder(i.iterValue)
			if i.err != nil {
				return
			}
			continue

		case InternalKeyKindRangeKeySet:
			// The RANGEKEYSET marker must sort before a MERGE at the same user key.
			i.err = base.CorruptionErrorf("pebble: out of order range key marker")
			return

		default:
			i.err = base.CorruptionErrorf("pebble: invalid internal key kind: %d", errors.Safe(key.Kind()))
			return
		}
	}
}

// SeekGE moves the iterator to the first key/value pair whose key is greater
// than or equal to the given key. Returns true if the iterator is pointing at
// a valid entry and false otherwise.
func (i *Iterator) SeekGE(key []byte) bool {
	return i.SeekGEWithLimit(key, nil) == IterValid
}

// SeekGEWithLimit moves the iterator to the first key/value pair whose key is
// greater than or equal to the given key.
//
// If limit is provided, it serves as a best-effort exclusive limit. If the
// first key greater than or equal to the given search key is also greater than
// or equal to limit, the Iterator may pause and return IterAtLimit. Because
// limits are best-effort, SeekGEWithLimit may return a key beyond limit.
//
// If the Iterator is configured to iterate over range keys, SeekGEWithLimit
// guarantees it will surface any range keys with bounds overlapping the
// keyspace [key, limit).
func (i *Iterator) SeekGEWithLimit(key []byte, limit []byte) IterValidityState {
	lastPositioningOp := i.lastPositioningOp
	// Set it to unknown, since this operation may not succeed, in which case
	// the SeekGE following this should not make any assumption about iterator
	// position.
	i.lastPositioningOp = unknownLastPositionOp
	i.requiresReposition = false
	i.err = nil // clear cached iteration error
	i.hasPrefix = false
	i.stats.ForwardSeekCount[InterfaceCall]++
	if lowerBound := i.opts.GetLowerBound(); lowerBound != nil && i.cmp(key, lowerBound) < 0 {
		key = lowerBound
	} else if upperBound := i.opts.GetUpperBound(); upperBound != nil && i.cmp(key, upperBound) > 0 {
		key = upperBound
	}
	if i.rangeKey != nil {
		i.rangeKey.updated = false
		i.rangeKey.prevPosHadRangeKey = i.rangeKey.hasRangeKey && i.Valid()
	}
	seekInternalIter := true
	var flags base.SeekGEFlags
	// The following noop optimization only applies when i.batch == nil, since
	// an iterator over a batch is iterating over mutable data, that may have
	// changed since the last seek.
	if lastPositioningOp == seekGELastPositioningOp && i.batch == nil {
		cmp := i.cmp(i.prefixOrFullSeekKey, key)
		// If this seek is to the same or later key, and the iterator is
		// already positioned there, this is a noop. This can be helpful for
		// sparse key spaces that have many deleted keys, where one can avoid
		// the overhead of iterating past them again and again.
		if cmp <= 0 {
			if i.iterValidityState == IterExhausted ||
				(i.iterValidityState == IterValid && i.cmp(key, i.key) <= 0 &&
					(limit == nil || i.cmp(i.key, limit) < 0)) {
				// Noop
				if !invariants.Enabled || !disableSeekOpt(key, uintptr(unsafe.Pointer(i))) || i.forceEnableSeekOpt {
					i.lastPositioningOp = seekGELastPositioningOp
					return i.iterValidityState
				}
			}
			// cmp == 0 is not safe to optimize since
			// - i.pos could be at iterPosNext, due to a merge.
			// - Even if i.pos were at iterPosCurForward, we could have a DELETE,
			//   SET pair for a key, and the iterator would have moved past DELETE
			//   but stayed at iterPosCurForward. A similar situation occurs for a
			//   MERGE, SET pair where the MERGE is consumed and the iterator is
			//   at the SET.
			// We also leverage the IterAtLimit <=> i.pos invariant defined in the
			// comment on iterValidityState, to exclude any cases where i.pos
			// is iterPosCur{Forward,Reverse}Paused. This avoids the need to
			// special-case those iterator positions and their interactions with
			// TrySeekUsingNext, as the main uses for TrySeekUsingNext in CockroachDB
			// do not use limited Seeks in the first place.
			if cmp < 0 && i.iterValidityState != IterAtLimit && limit == nil {
				flags = flags.EnableTrySeekUsingNext()
			}
			if invariants.Enabled && flags.TrySeekUsingNext() && !i.forceEnableSeekOpt && disableSeekOpt(key, uintptr(unsafe.Pointer(i))) {
				flags = flags.DisableTrySeekUsingNext()
			}
			if i.pos == iterPosCurForwardPaused && i.cmp(key, i.iterKey.UserKey) <= 0 {
				// Have some work to do, but don't need to seek, and we can
				// start doing findNextEntry from i.iterKey.
				seekInternalIter = false
			}
		}
	}
	if seekInternalIter {
		i.iterKey, i.iterValue = i.iter.SeekGE(key, flags)
		i.stats.ForwardSeekCount[InternalIterCall]++
	}
	i.findNextEntry(limit)
	i.maybeSampleRead()
	if i.Error() == nil && i.batch == nil {
		// Prepare state for a future noop optimization.
		i.prefixOrFullSeekKey = append(i.prefixOrFullSeekKey[:0], key...)
		i.lastPositioningOp = seekGELastPositioningOp
	}
	return i.iterValidityState
}

// SeekPrefixGE moves the iterator to the first key/value pair whose key is
// greater than or equal to the given key and which has the same "prefix" as
// the given key. The prefix for a key is determined by the user-defined
// Comparer.Split function. The iterator will not observe keys not matching the
// "prefix" of the search key. Calling SeekPrefixGE puts the iterator in prefix
// iteration mode. The iterator remains in prefix iteration until a subsequent
// call to another absolute positioning method (SeekGE, SeekLT, First,
// Last). Reverse iteration (Prev) is not supported when an iterator is in
// prefix iteration mode. Returns true if the iterator is pointing at a valid
// entry and false otherwise.
//
// The semantics of SeekPrefixGE are slightly unusual and designed for
// iteration to be able to take advantage of bloom filters that have been
// created on the "prefix". If you're not using bloom filters, there is no
// reason to use SeekPrefixGE.
//
// An example Split function may separate a timestamp suffix from the prefix of
// the key.
//
//   Split(<key>@<timestamp>) -> <key>
//
// Consider the keys "a@1", "a@2", "aa@3", "aa@4". The prefixes for these keys
// are "a", and "aa". Note that despite "a" and "aa" sharing a prefix by the
// usual definition, those prefixes differ by the definition of the Split
// function. To see how this works, consider the following set of calls on this
// data set:
//
//   SeekPrefixGE("a@0") -> "a@1"
//   Next()              -> "a@2"
//   Next()              -> EOF
//
// If you're just looking to iterate over keys with a shared prefix, as
// defined by the configured comparer, set iterator bounds instead:
//
//  iter := db.NewIter(&pebble.IterOptions{
//    LowerBound: []byte("prefix"),
//    UpperBound: []byte("prefiy"),
//  })
//  for iter.First(); iter.Valid(); iter.Next() {
//    // Only keys beginning with "prefix" will be visited.
//  }
//
// See ExampleIterator_SeekPrefixGE for a working example.
//
// When iterating with range keys enabled, all range keys encountered are
// truncated to the seek key's prefix's bounds. The truncation of the upper
// bound requires that the database's Comparer is configured with a
// ImmediateSuccessor method. For example, a SeekPrefixGE("a@9") call with the
// prefix "a" will truncate range key bounds to [a,ImmediateSuccessor(a)].
func (i *Iterator) SeekPrefixGE(key []byte) bool {
	lastPositioningOp := i.lastPositioningOp
	// Set it to unknown, since this operation may not succeed, in which case
	// the SeekPrefixGE following this should not make any assumption about
	// iterator position.
	i.lastPositioningOp = unknownLastPositionOp
	i.requiresReposition = false
	i.err = nil // clear cached iteration error
	i.stats.ForwardSeekCount[InterfaceCall]++
	if i.rangeKey != nil {
		i.rangeKey.updated = false
		i.rangeKey.prevPosHadRangeKey = i.rangeKey.hasRangeKey && i.Valid()
	}
	if i.comparer.Split == nil {
		panic("pebble: split must be provided for SeekPrefixGE")
	}
	if i.comparer.ImmediateSuccessor == nil && i.opts.KeyTypes != IterKeyTypePointsOnly {
		panic("pebble: ImmediateSuccessor must be provided for SeekPrefixGE with range keys")
	}
	prefixLen := i.split(key)
	keyPrefix := key[:prefixLen]
	var flags base.SeekGEFlags
	if lastPositioningOp == seekPrefixGELastPositioningOp {
		if !i.hasPrefix {
			panic("lastPositioningOpsIsSeekPrefixGE is true, but hasPrefix is false")
		}
		// The iterator has not been repositioned after the last SeekPrefixGE.
		// See if we are seeking to a larger key, since then we can optimize
		// the seek by using next. Note that we could also optimize if Next
		// has been called, if the iterator is not exhausted and the current
		// position is <= the seek key. We are keeping this limited for now
		// since such optimizations require care for correctness, and to not
		// become de-optimizations (if one usually has to do all the next
		// calls and then the seek). This SeekPrefixGE optimization
		// specifically benefits CockroachDB.
		cmp := i.cmp(i.prefixOrFullSeekKey, keyPrefix)
		// cmp == 0 is not safe to optimize since
		// - i.pos could be at iterPosNext, due to a merge.
		// - Even if i.pos were at iterPosCurForward, we could have a DELETE,
		//   SET pair for a key, and the iterator would have moved past DELETE
		//   but stayed at iterPosCurForward. A similar situation occurs for a
		//   MERGE, SET pair where the MERGE is consumed and the iterator is
		//   at the SET.
		// In general some versions of i.prefix could have been consumed by
		// the iterator, so we only optimize for cmp < 0.
		if cmp < 0 {
			flags = flags.EnableTrySeekUsingNext()
		}
		if invariants.Enabled && flags.TrySeekUsingNext() && !i.forceEnableSeekOpt && disableSeekOpt(key, uintptr(unsafe.Pointer(i))) {
			flags = flags.DisableTrySeekUsingNext()
		}
	}
	// Make a copy of the prefix so that modifications to the key after
	// SeekPrefixGE returns does not affect the stored prefix.
	if cap(i.prefixOrFullSeekKey) < prefixLen {
		i.prefixOrFullSeekKey = make([]byte, prefixLen)
	} else {
		i.prefixOrFullSeekKey = i.prefixOrFullSeekKey[:prefixLen]
	}
	i.hasPrefix = true
	copy(i.prefixOrFullSeekKey, keyPrefix)

	if lowerBound := i.opts.GetLowerBound(); lowerBound != nil && i.cmp(key, lowerBound) < 0 {
		if n := i.split(lowerBound); !bytes.Equal(i.prefixOrFullSeekKey, lowerBound[:n]) {
			i.err = errors.New("pebble: SeekPrefixGE supplied with key outside of lower bound")
			i.iterValidityState = IterExhausted
			return false
		}
		key = lowerBound
	} else if upperBound := i.opts.GetUpperBound(); upperBound != nil && i.cmp(key, upperBound) > 0 {
		if n := i.split(upperBound); !bytes.Equal(i.prefixOrFullSeekKey, upperBound[:n]) {
			i.err = errors.New("pebble: SeekPrefixGE supplied with key outside of upper bound")
			i.iterValidityState = IterExhausted
			return false
		}
		key = upperBound
	}
	i.iterKey, i.iterValue = i.iter.SeekPrefixGE(i.prefixOrFullSeekKey, key, flags)
	i.stats.ForwardSeekCount[InternalIterCall]++
	i.findNextEntry(nil)
	i.maybeSampleRead()
	if i.Error() == nil {
		i.lastPositioningOp = seekPrefixGELastPositioningOp
	}
	return i.iterValidityState == IterValid
}

// Deterministic disabling of the seek optimization. It uses the iterator
// pointer, since we want diversity in iterator behavior for the same key.
// Used for tests.
func disableSeekOpt(key []byte, ptr uintptr) bool {
	// Fibonacci hash https://probablydance.com/2018/06/16/fibonacci-hashing-the-optimization-that-the-world-forgot-or-a-better-alternative-to-integer-modulo/
	simpleHash := (11400714819323198485 * uint64(ptr)) >> 63
	return key != nil && key[0]&byte(1) == 0 && simpleHash == 0
}

// SeekLT moves the iterator to the last key/value pair whose key is less than
// the given key. Returns true if the iterator is pointing at a valid entry and
// false otherwise.
func (i *Iterator) SeekLT(key []byte) bool {
	return i.SeekLTWithLimit(key, nil) == IterValid
}

// SeekLTWithLimit moves the iterator to the last key/value pair whose key is
// less than the given key.
//
// If limit is provided, it serves as a best-effort inclusive limit. If the last
// key less than the given search key is also less than limit, the Iterator may
// pause and return IterAtLimit. Because limits are best-effort, SeekLTWithLimit
// may return a key beyond limit.
//
// If the Iterator is configured to iterate over range keys, SeekLTWithLimit
// guarantees it will surface any range keys with bounds overlapping the
// keyspace up to limit.
func (i *Iterator) SeekLTWithLimit(key []byte, limit []byte) IterValidityState {
	lastPositioningOp := i.lastPositioningOp
	// Set it to unknown, since this operation may not succeed, in which case
	// the SeekLT following this should not make any assumption about iterator
	// position.
	i.lastPositioningOp = unknownLastPositionOp
	i.requiresReposition = false
	i.err = nil // clear cached iteration error
	i.stats.ReverseSeekCount[InterfaceCall]++
	if upperBound := i.opts.GetUpperBound(); upperBound != nil && i.cmp(key, upperBound) > 0 {
		key = upperBound
	} else if lowerBound := i.opts.GetLowerBound(); lowerBound != nil && i.cmp(key, lowerBound) < 0 {
		key = lowerBound
	}
	if i.rangeKey != nil {
		i.rangeKey.updated = false
		i.rangeKey.prevPosHadRangeKey = i.rangeKey.hasRangeKey && i.Valid()
	}
	i.hasPrefix = false
	seekInternalIter := true
	// The following noop optimization only applies when i.batch == nil, since
	// an iterator over a batch is iterating over mutable data, that may have
	// changed since the last seek.
	if lastPositioningOp == seekLTLastPositioningOp && i.batch == nil {
		cmp := i.cmp(key, i.prefixOrFullSeekKey)
		// If this seek is to the same or earlier key, and the iterator is
		// already positioned there, this is a noop. This can be helpful for
		// sparse key spaces that have many deleted keys, where one can avoid
		// the overhead of iterating past them again and again.
		if cmp <= 0 {
			// NB: when pos != iterPosCurReversePaused, the invariant
			// documented earlier implies that iterValidityState !=
			// IterAtLimit.
			if i.iterValidityState == IterExhausted ||
				(i.iterValidityState == IterValid && i.cmp(i.key, key) < 0 &&
					(limit == nil || i.cmp(limit, i.key) <= 0)) {
				if !invariants.Enabled || !disableSeekOpt(key, uintptr(unsafe.Pointer(i))) {
					i.lastPositioningOp = seekLTLastPositioningOp
					return i.iterValidityState
				}
			}
			if i.pos == iterPosCurReversePaused && i.cmp(i.iterKey.UserKey, key) < 0 {
				// Have some work to do, but don't need to seek, and we can
				// start doing findPrevEntry from i.iterKey.
				seekInternalIter = false
			}
		}
	}
	if seekInternalIter {
		i.iterKey, i.iterValue = i.iter.SeekLT(key, base.SeekLTFlagsNone)
		i.stats.ReverseSeekCount[InternalIterCall]++
	}
	i.findPrevEntry(limit)
	i.maybeSampleRead()
	if i.Error() == nil && i.batch == nil {
		// Prepare state for a future noop optimization.
		i.prefixOrFullSeekKey = append(i.prefixOrFullSeekKey[:0], key...)
		i.lastPositioningOp = seekLTLastPositioningOp
	}
	return i.iterValidityState
}

// First moves the iterator the the first key/value pair. Returns true if the
// iterator is pointing at a valid entry and false otherwise.
func (i *Iterator) First() bool {
	i.err = nil // clear cached iteration error
	i.hasPrefix = false
	i.lastPositioningOp = unknownLastPositionOp
	i.requiresReposition = false
	i.stats.ForwardSeekCount[InterfaceCall]++
	if i.rangeKey != nil {
		i.rangeKey.updated = false
		i.rangeKey.prevPosHadRangeKey = i.rangeKey.hasRangeKey && i.Valid()
	}

	if lowerBound := i.opts.GetLowerBound(); lowerBound != nil {
		i.iterKey, i.iterValue = i.iter.SeekGE(lowerBound, base.SeekGEFlagsNone)
		i.stats.ForwardSeekCount[InternalIterCall]++
	} else {
		i.iterKey, i.iterValue = i.iter.First()
		i.stats.ForwardSeekCount[InternalIterCall]++
	}
	i.findNextEntry(nil)
	i.maybeSampleRead()
	return i.iterValidityState == IterValid
}

// Last moves the iterator the the last key/value pair. Returns true if the
// iterator is pointing at a valid entry and false otherwise.
func (i *Iterator) Last() bool {
	i.err = nil // clear cached iteration error
	i.hasPrefix = false
	i.lastPositioningOp = unknownLastPositionOp
	i.requiresReposition = false
	i.stats.ReverseSeekCount[InterfaceCall]++
	if i.rangeKey != nil {
		i.rangeKey.updated = false
		i.rangeKey.prevPosHadRangeKey = i.rangeKey.hasRangeKey && i.Valid()
	}

	if upperBound := i.opts.GetUpperBound(); upperBound != nil {
		i.iterKey, i.iterValue = i.iter.SeekLT(upperBound, base.SeekLTFlagsNone)
		i.stats.ReverseSeekCount[InternalIterCall]++
	} else {
		i.iterKey, i.iterValue = i.iter.Last()
		i.stats.ReverseSeekCount[InternalIterCall]++
	}
	i.findPrevEntry(nil)
	i.maybeSampleRead()
	return i.iterValidityState == IterValid
}

// Next moves the iterator to the next key/value pair. Returns true if the
// iterator is pointing at a valid entry and false otherwise.
func (i *Iterator) Next() bool {
	return i.NextWithLimit(nil) == IterValid
}

// NextWithLimit moves the iterator to the next key/value pair.
//
// If limit is provided, it serves as a best-effort exclusive limit. If the next
// key  is greater than or equal to limit, the Iterator may pause and return
// IterAtLimit. Because limits are best-effort, NextWithLimit may return a key
// beyond limit.
//
// If the Iterator is configured to iterate over range keys, NextWithLimit
// guarantees it will surface any range keys with bounds overlapping the
// keyspace up to limit.
func (i *Iterator) NextWithLimit(limit []byte) IterValidityState {
	i.stats.ForwardStepCount[InterfaceCall]++
	if i.hasPrefix {
		if limit != nil {
			i.err = errors.New("cannot use limit with prefix iteration")
			i.iterValidityState = IterExhausted
			return i.iterValidityState
		} else if i.iterValidityState == IterExhausted {
			// No-op, already exhasuted. We avoid executing the Next because it
			// can break invariants: Specifically, a file that fails the bloom
			// filter test may result in its level being removed from the
			// merging iterator. The level's removal can cause a lazy combined
			// iterator to miss range keys and trigger a switch to combined
			// iteration at a larger key, breaking keyspan invariants.
			return i.iterValidityState
		}
	}
	if i.err != nil {
		return i.iterValidityState
	}
	i.lastPositioningOp = unknownLastPositionOp
	i.requiresReposition = false
	if i.rangeKey != nil {
		i.rangeKey.updated = false
		i.rangeKey.prevPosHadRangeKey = i.rangeKey.hasRangeKey && i.Valid()
	}
	switch i.pos {
	case iterPosCurForward:
		i.nextUserKey()
	case iterPosCurForwardPaused:
		// Already at the right place.
	case iterPosCurReverse:
		// Switching directions.
		// Unless the iterator was exhausted, reverse iteration needs to
		// position the iterator at iterPosPrev.
		if i.iterKey != nil {
			i.err = errors.New("switching from reverse to forward but iter is not at prev")
			i.iterValidityState = IterExhausted
			return i.iterValidityState
		}
		// We're positioned before the first key. Need to reposition to point to
		// the first key.
		if lowerBound := i.opts.GetLowerBound(); lowerBound != nil {
			i.iterKey, i.iterValue = i.iter.SeekGE(lowerBound, base.SeekGEFlagsNone)
			i.stats.ForwardSeekCount[InternalIterCall]++
		} else {
			i.iterKey, i.iterValue = i.iter.First()
			i.stats.ForwardSeekCount[InternalIterCall]++
		}
	case iterPosCurReversePaused:
		// Switching directions.
		// The iterator must not be exhausted since it paused.
		if i.iterKey == nil {
			i.err = errors.New("switching paused from reverse to forward but iter is exhausted")
			i.iterValidityState = IterExhausted
			return i.iterValidityState
		}
		i.nextUserKey()
	case iterPosPrev:
		// The underlying iterator is pointed to the previous key (this can
		// only happen when switching iteration directions). We set
		// i.iterValidityState to IterExhausted here to force the calls to
		// nextUserKey to save the current key i.iter is pointing at in order
		// to determine when the next user-key is reached.
		i.iterValidityState = IterExhausted
		if i.iterKey == nil {
			// We're positioned before the first key. Need to reposition to point to
			// the first key.
			if lowerBound := i.opts.GetLowerBound(); lowerBound != nil {
				i.iterKey, i.iterValue = i.iter.SeekGE(lowerBound, base.SeekGEFlagsNone)
				i.stats.ForwardSeekCount[InternalIterCall]++
			} else {
				i.iterKey, i.iterValue = i.iter.First()
				i.stats.ForwardSeekCount[InternalIterCall]++
			}
		} else {
			i.nextUserKey()
		}
		i.nextUserKey()
	case iterPosNext:
		// Already at the right place.
	}
	i.findNextEntry(limit)
	i.maybeSampleRead()
	return i.iterValidityState
}

// Prev moves the iterator to the previous key/value pair. Returns true if the
// iterator is pointing at a valid entry and false otherwise.
func (i *Iterator) Prev() bool {
	return i.PrevWithLimit(nil) == IterValid
}

// PrevWithLimit moves the iterator to the previous key/value pair.
//
// If limit is provided, it serves as a best-effort inclusive limit. If the
// previous key is less than limit, the Iterator may pause and return
// IterAtLimit. Because limits are best-effort, PrevWithLimit may return a key
// beyond limit.
//
// If the Iterator is configured to iterate over range keys, PrevWithLimit
// guarantees it will surface any range keys with bounds overlapping the
// keyspace up to limit.
func (i *Iterator) PrevWithLimit(limit []byte) IterValidityState {
	i.stats.ReverseStepCount[InterfaceCall]++
	if i.err != nil {
		return i.iterValidityState
	}
	i.lastPositioningOp = unknownLastPositionOp
	i.requiresReposition = false
	if i.rangeKey != nil {
		i.rangeKey.updated = false
		i.rangeKey.prevPosHadRangeKey = i.rangeKey.hasRangeKey && i.Valid()
	}
	if i.hasPrefix {
		i.err = errReversePrefixIteration
		i.iterValidityState = IterExhausted
		return i.iterValidityState
	}
	switch i.pos {
	case iterPosCurForward:
		// Switching directions, and will handle this below.
	case iterPosCurForwardPaused:
		// Switching directions, and will handle this below.
	case iterPosCurReverse:
		i.prevUserKey()
	case iterPosCurReversePaused:
		// Already at the right place.
	case iterPosNext:
		// The underlying iterator is pointed to the next key (this can only happen
		// when switching iteration directions). We will handle this below.
	case iterPosPrev:
		// Already at the right place.
	}
	if i.pos == iterPosCurForward || i.pos == iterPosNext || i.pos == iterPosCurForwardPaused {
		// Switching direction.
		stepAgain := i.pos == iterPosNext

		// Synthetic range key markers are a special case. Consider SeekGE(b)
		// which finds a range key [a, c). To ensure the user observes the range
		// key, the Iterator pauses at Key() = b. The iterator must advance the
		// internal iterator to see if there's also a coincident point key at
		// 'b', leaving the iterator at iterPosNext if there's not.
		//
		// This is a problem: Synthetic range key markers are only interleaved
		// during the original seek. A subsequent Prev() of i.iter will not move
		// back onto the synthetic range key marker. In this case where the
		// previous iterator position was a synthetic range key start boundary,
		// we must not step a second time.
		if i.isEphemeralPosition() {
			stepAgain = false
		}

		// We set i.iterValidityState to IterExhausted here to force the calls
		// to prevUserKey to save the current key i.iter is pointing at in
		// order to determine when the prev user-key is reached.
		i.iterValidityState = IterExhausted
		if i.iterKey == nil {
			// We're positioned after the last key. Need to reposition to point to
			// the last key.
			if upperBound := i.opts.GetUpperBound(); upperBound != nil {
				i.iterKey, i.iterValue = i.iter.SeekLT(upperBound, base.SeekLTFlagsNone)
				i.stats.ReverseSeekCount[InternalIterCall]++
			} else {
				i.iterKey, i.iterValue = i.iter.Last()
				i.stats.ReverseSeekCount[InternalIterCall]++
			}
		} else {
			i.prevUserKey()
		}
		if stepAgain {
			i.prevUserKey()
		}
	}
	i.findPrevEntry(limit)
	i.maybeSampleRead()
	return i.iterValidityState
}

// RangeKeyData describes a range key's data, set through RangeKeySet. The key
// boundaries of the range key is provided by Iterator.RangeBounds.
type RangeKeyData struct {
	Suffix []byte
	Value  []byte
}

// rangeKeyWithinLimit is called during limited reverse iteration when
// positioned over a key beyond the limit. If there exists a range key that lies
// within the limit, the iterator must not pause in order to ensure the user has
// an opportunity to observe the range key within limit.
//
// It would be valid to ignore the limit whenever there's a range key covering
// the key, but that would introduce nondeterminism. To preserve determinism for
// testing, the iterator ignores the limit only if the covering range key does
// cover the keyspace within the limit.
//
// This awkwardness exists because range keys are interleaved at their inclusive
// start positions. Note that limit is inclusive.
func (i *Iterator) rangeKeyWithinLimit(limit []byte) bool {
	if i.rangeKey == nil || !i.opts.rangeKeys() {
		return false
	}
	s := i.rangeKey.iiter.Span()
	// If the range key ends beyond the limit, then the range key does not cover
	// any portion of the keyspace within the limit and it is safe to pause.
	return s != nil && i.cmp(s.End, limit) > 0
}

// saveRangeKey saves the current range key to the underlying iterator's current
// range key state. If the range key has not changed, saveRangeKey is a no-op.
// If there is a new range key, saveRangeKey copies all of the key, value and
// suffixes into Iterator-managed buffers.
func (i *Iterator) saveRangeKey() {
	if i.rangeKey == nil || i.opts.KeyTypes == IterKeyTypePointsOnly {
		return
	}

	s := i.rangeKey.iiter.Span()
	if s == nil {
		i.rangeKey.hasRangeKey = false
		i.rangeKey.updated = i.rangeKey.prevPosHadRangeKey
		return
	} else if !i.rangeKey.stale {
		// The range key `s` is identical to the one currently saved. No-op.
		return
	}

	if s.KeysOrder != keyspan.BySuffixAsc {
		panic("pebble: range key span's keys unexpectedly not in ascending suffix order")
	}

	// Although `i.rangeKey.stale` is true, the span s may still be identical
	// to the currently saved span. This is possible when seeking the iterator,
	// which may land back on the same range key. If we previously had a range
	// key and the new one has an identical start key, then it must be the same
	// range key and we can avoid copying and keep `i.rangeKey.updated=false`.
	//
	// TODO(jackson): These key comparisons could be avoidable during relative
	// positioning operations continuing in the same direction, because these
	// ops will never encounter the previous position's range key while
	// stale=true. However, threading whether the current op is a seek or step
	// maybe isn't worth it. This key comparison is only necessary once when we
	// step onto a new range key, which should be relatively rare.
	if i.rangeKey.prevPosHadRangeKey && i.equal(i.rangeKey.start, s.Start) &&
		i.equal(i.rangeKey.end, s.End) {
		i.rangeKey.updated = false
		i.rangeKey.stale = false
		i.rangeKey.hasRangeKey = true
		return
	}

	i.rangeKey.hasRangeKey = true
	i.rangeKey.updated = true
	i.rangeKey.stale = false
	i.rangeKey.buf = append(i.rangeKey.buf[:0], s.Start...)
	i.rangeKey.start = i.rangeKey.buf
	i.rangeKey.buf = append(i.rangeKey.buf, s.End...)
	i.rangeKey.end = i.rangeKey.buf[len(i.rangeKey.buf)-len(s.End):]
	i.rangeKey.keys = i.rangeKey.keys[:0]
	for j := 0; j < len(s.Keys); j++ {
		if invariants.Enabled {
			if s.Keys[j].Kind() != base.InternalKeyKindRangeKeySet {
				panic("pebble: user iteration encountered non-RangeKeySet key kind")
			} else if j > 0 && i.cmp(s.Keys[j].Suffix, s.Keys[j-1].Suffix) < 0 {
				panic("pebble: user iteration encountered range keys not in suffix order")
			}
		}
		i.rangeKey.buf = append(i.rangeKey.buf, s.Keys[j].Suffix...)
		suffix := i.rangeKey.buf[len(i.rangeKey.buf)-len(s.Keys[j].Suffix):]
		i.rangeKey.buf = append(i.rangeKey.buf, s.Keys[j].Value...)
		value := i.rangeKey.buf[len(i.rangeKey.buf)-len(s.Keys[j].Value):]
		i.rangeKey.keys = append(i.rangeKey.keys, RangeKeyData{
			Suffix: suffix,
			Value:  value,
		})
	}
}

// RangeKeyChanged indicates whether the most recent iterator positioning
// operation resulted in the iterator stepping into or out of a new range key.
// If true, previously returned range key bounds and data has been invalidated.
// If false, previously obtained range key bounds, suffix and value slices are
// still valid and may continue to be read.
//
// Invalid iterator positions are considered to not hold range keys, meaning
// that if an iterator steps from an IterExhausted or IterAtLimit position onto
// a position with a range key, RangeKeyChanged will yield true.
func (i *Iterator) RangeKeyChanged() bool {
	return i.iterValidityState == IterValid && i.rangeKey != nil && i.rangeKey.updated
}

// HasPointAndRange indicates whether there exists a point key, a range key or
// both at the current iterator position.
func (i *Iterator) HasPointAndRange() (hasPoint, hasRange bool) {
	if i.iterValidityState != IterValid || i.requiresReposition {
		return false, false
	}
	if i.opts.KeyTypes == IterKeyTypePointsOnly {
		return true, false
	}
	return i.rangeKey == nil || !i.rangeKey.rangeKeyOnly, i.rangeKey != nil && i.rangeKey.hasRangeKey
}

// RangeBounds returns the start (inclusive) and end (exclusive) bounds of the
// range key covering the current iterator position. RangeBounds returns nil
// bounds if there is no range key covering the current iterator position, or
// the iterator is not configured to surface range keys.
func (i *Iterator) RangeBounds() (start, end []byte) {
	if i.rangeKey == nil || !i.opts.rangeKeys() || !i.rangeKey.hasRangeKey {
		return nil, nil
	}
	return i.rangeKey.start, i.rangeKey.end
}

// Key returns the key of the current key/value pair, or nil if done. The
// caller should not modify the contents of the returned slice, and its
// contents may change on the next call to Next.
func (i *Iterator) Key() []byte {
	return i.key
}

// Value returns the value of the current key/value pair, or nil if done. The
// caller should not modify the contents of the returned slice, and its
// contents may change on the next call to Next.
//
// Only valid if HasPointAndRange() returns true for hasPoint.
func (i *Iterator) Value() []byte {
	return i.value
}

// RangeKeys returns the range key values and their suffixes covering the
// current iterator position. The range bounds may be retrieved separately
// through Iterator.RangeBounds().
func (i *Iterator) RangeKeys() []RangeKeyData {
	if i.rangeKey == nil || !i.opts.rangeKeys() || !i.rangeKey.hasRangeKey {
		return nil
	}
	return i.rangeKey.keys
}

// Valid returns true if the iterator is positioned at a valid key/value pair
// and false otherwise.
func (i *Iterator) Valid() bool {
	return i.iterValidityState == IterValid && !i.requiresReposition
}

// Error returns any accumulated error.
func (i *Iterator) Error() error {
	if i.iter != nil {
		return firstError(i.err, i.iter.Error())
	}
	return i.err
}

// Close closes the iterator and returns any accumulated error. Exhausting
// all the key/value pairs in a table is not considered to be an error.
// It is not valid to call any method, including Close, after the iterator
// has been closed.
func (i *Iterator) Close() error {
	// Close the child iterator before releasing the readState because when the
	// readState is released sstables referenced by the readState may be deleted
	// which will fail on Windows if the sstables are still open by the child
	// iterator.
	if i.iter != nil {
		i.err = firstError(i.err, i.iter.Close())

		// Closing i.iter did not necessarily close the point and range key
		// iterators. Calls to SetOptions may have 'disconnected' either one
		// from i.iter if iteration key types were changed. Both point and range
		// key iterators are preserved in case the iterator needs to switch key
		// types again. We explicitly close both of these iterators here.
		//
		// NB: If the iterators were still connected to i.iter, they may be
		// closed, but calling Close on a closed internal iterator or fragment
		// iterator is allowed.
		if i.pointIter != nil && !i.closePointIterOnce {
			i.err = firstError(i.err, i.pointIter.Close())
		}
		if i.rangeKey != nil && i.rangeKey.rangeKeyIter != nil {
			i.err = firstError(i.err, i.rangeKey.rangeKeyIter.Close())
		}
	}
	err := i.err

	if i.readState != nil {
		if i.readSampling.pendingCompactions.size > 0 {
			// Copy pending read compactions using db.mu.Lock()
			i.readState.db.mu.Lock()
			i.readState.db.mu.compact.readCompactions.combine(&i.readSampling.pendingCompactions, i.cmp)
			reschedule := i.readState.db.mu.compact.rescheduleReadCompaction
			i.readState.db.mu.compact.rescheduleReadCompaction = false
			concurrentCompactions := i.readState.db.mu.compact.compactingCount
			i.readState.db.mu.Unlock()

			if reschedule && concurrentCompactions == 0 {
				// In a read heavy workload, flushes may not happen frequently enough to
				// schedule compactions.
				i.readState.db.compactionSchedulers.Add(1)
				go i.readState.db.maybeScheduleCompactionAsync()
			}
		}

		i.readState.unref()
		i.readState = nil
	}

	for _, readers := range i.externalReaders {
		for _, r := range readers {
			err = firstError(err, r.Close())
		}
	}

	// Close the closer for the current value if one was open.
	if i.valueCloser != nil {
		err = firstError(err, i.valueCloser.Close())
		i.valueCloser = nil
	}

	const maxKeyBufCacheSize = 4 << 10 // 4 KB

	if i.rangeKey != nil {
		// Avoid caching the key buf if it is overly large. The constant is
		// fairly arbitrary.
		if cap(i.rangeKey.buf) >= maxKeyBufCacheSize {
			i.rangeKey.buf = nil
		}
		*i.rangeKey = iteratorRangeKeyState{buf: i.rangeKey.buf}
		iterRangeKeyStateAllocPool.Put(i.rangeKey)
		i.rangeKey = nil
	}
	if alloc := i.alloc; alloc != nil {
		// Avoid caching the key buf if it is overly large. The constant is fairly
		// arbitrary.
		if cap(i.keyBuf) >= maxKeyBufCacheSize {
			alloc.keyBuf = nil
		} else {
			alloc.keyBuf = i.keyBuf
		}
		if cap(i.prefixOrFullSeekKey) >= maxKeyBufCacheSize {
			alloc.prefixOrFullSeekKey = nil
		} else {
			alloc.prefixOrFullSeekKey = i.prefixOrFullSeekKey
		}
		for j := range i.boundsBuf {
			if cap(i.boundsBuf[j]) >= maxKeyBufCacheSize {
				alloc.boundsBuf[j] = nil
			} else {
				alloc.boundsBuf[j] = i.boundsBuf[j]
			}
		}
		*alloc = iterAlloc{
			keyBuf:              alloc.keyBuf,
			boundsBuf:           alloc.boundsBuf,
			prefixOrFullSeekKey: alloc.prefixOrFullSeekKey,
		}
		iterAllocPool.Put(alloc)
	} else if alloc := i.getIterAlloc; alloc != nil {
		if cap(i.keyBuf) >= maxKeyBufCacheSize {
			alloc.keyBuf = nil
		} else {
			alloc.keyBuf = i.keyBuf
		}
		*alloc = getIterAlloc{
			keyBuf: alloc.keyBuf,
		}
		getIterAllocPool.Put(alloc)
	}
	return err
}

// SetBounds sets the lower and upper bounds for the iterator. Once SetBounds
// returns, the caller is free to mutate the provided slices.
//
// The iterator will always be invalidated and must be repositioned with a call
// to SeekGE, SeekPrefixGE, SeekLT, First, or Last.
func (i *Iterator) SetBounds(lower, upper []byte) {
	// Ensure that the Iterator appears exhausted, regardless of whether we
	// actually have to invalidate the internal iterator. Optimizations that
	// avoid exhaustion are an internal implementation detail that shouldn't
	// leak through the interface. The caller should still call an absolute
	// positioning method to reposition the iterator.
	i.requiresReposition = true

	if ((i.opts.LowerBound == nil) == (lower == nil)) &&
		((i.opts.UpperBound == nil) == (upper == nil)) &&
		i.equal(i.opts.LowerBound, lower) &&
		i.equal(i.opts.UpperBound, upper) {
		// Unchanged, noop.
		return
	}

	// Copy the user-provided bounds into an Iterator-owned buffer, and set them
	// on i.opts.{Lower,Upper}Bound.
	i.saveBounds(lower, upper)

	i.iter.SetBounds(i.opts.LowerBound, i.opts.UpperBound)
	// If the iterator has an open point iterator that's not currently being
	// used, propagate the new bounds to it.
	if i.pointIter != nil && !i.opts.pointKeys() {
		i.pointIter.SetBounds(i.opts.LowerBound, i.opts.UpperBound)
	}
	// If the iterator has a range key iterator, propagate bounds to it. The
	// top-level SetBounds on the interleaving iterator (i.iter) won't propagate
	// bounds to the range key iterator stack, because the FragmentIterator
	// interface doesn't define a SetBounds method. We need to directly inform
	// the iterConfig stack.
	if i.rangeKey != nil {
		i.rangeKey.iterConfig.SetBounds(i.opts.LowerBound, i.opts.UpperBound)
	}

	// Even though this is not a positioning operation, the alteration of the
	// bounds means we cannot optimize Seeks by using Next.
	i.invalidate()
}

func (i *Iterator) saveBounds(lower, upper []byte) {
	// Copy the user-provided bounds into an Iterator-owned buffer. We can't
	// overwrite the current bounds, because some internal iterators compare old
	// and new bounds for optimizations.

	buf := i.boundsBuf[i.boundsBufIdx][:0]
	if lower != nil {
		buf = append(buf, lower...)
		i.opts.LowerBound = buf
	} else {
		i.opts.LowerBound = nil
	}
	if upper != nil {
		buf = append(buf, upper...)
		i.opts.UpperBound = buf[len(buf)-len(upper):]
	} else {
		i.opts.UpperBound = nil
	}
	i.boundsBuf[i.boundsBufIdx] = buf
	i.boundsBufIdx = 1 - i.boundsBufIdx
}

// SetOptions sets new iterator options for the iterator. Note that the lower
// and upper bounds applied here will supersede any bounds set by previous calls
// to SetBounds.
//
// Note that the slices provided in this SetOptions must not be changed by the
// caller until the iterator is closed, or a subsequent SetBounds or SetOptions
// has returned. This is because comparisons between the existing and new bounds
// are sometimes used to optimize seeking. See the extended commentary on
// SetBounds.
//
// If the iterator was created over an indexed mutable batch, the iterator's
// view of the mutable batch is refreshed.
//
// The iterator will always be invalidated and must be repositioned with a call
// to SeekGE, SeekPrefixGE, SeekLT, First, or Last.
//
// If only lower and upper bounds need to be modified, prefer SetBounds.
func (i *Iterator) SetOptions(o *IterOptions) {
	if i.externalReaders != nil {
		if err := validateExternalIterOpts(o); err != nil {
			panic(err)
		}
	}

	// Ensure that the Iterator appears exhausted, regardless of whether we
	// actually have to invalidate the internal iterator. Optimizations that
	// avoid exhaustion are an internal implementation detail that shouldn't
	// leak through the interface. The caller should still call an absolute
	// positioning method to reposition the iterator.
	i.requiresReposition = true

	// Check if global state requires we close all internal iterators.
	//
	// If the Iterator is in an error state, invalidate the existing iterators
	// so that we reconstruct an iterator state from scratch.
	//
	// If OnlyReadGuaranteedDurable changed, the iterator stacks are incorrect,
	// improperly including or excluding memtables. Invalidate them so that
	// finishInitializingIter will reconstruct them.
	//
	// If either the original options or the new options specify a table filter,
	// we need to reconstruct the iterator stacks. If they both supply a table
	// filter, we can't be certain that it's the same filter since we have no
	// mechanism to compare the filter closures.
	closeBoth := i.err != nil ||
		o.OnlyReadGuaranteedDurable != i.opts.OnlyReadGuaranteedDurable ||
		o.TableFilter != nil || i.opts.TableFilter != nil

	// If either options specify block property filters for an iterator stack,
	// reconstruct it.
	if i.pointIter != nil && (closeBoth || len(o.PointKeyFilters) > 0 || len(i.opts.PointKeyFilters) > 0 ||
		o.RangeKeyMasking.Filter != nil || i.opts.RangeKeyMasking.Filter != nil) {
		i.err = firstError(i.err, i.pointIter.Close())
		i.pointIter = nil
	}
	if i.rangeKey != nil {
		if closeBoth || len(o.RangeKeyFilters) > 0 || len(i.opts.RangeKeyFilters) > 0 {
			i.err = firstError(i.err, i.rangeKey.rangeKeyIter.Close())
			i.rangeKey = nil
		} else {
			// If there's still a range key iterator stack, invalidate the
			// iterator. This ensures RangeKeyChanged() returns true if a
			// subsequent positioning operation discovers a range key. It also
			// prevents seek no-op optimizations.
			i.invalidate()
		}
	}

	// If the iterator is backed by a batch that's been mutated, refresh its
	// existing point and range-key iterators, and invalidate the iterator to
	// prevent seek-using-next optimizations. If we don't yet have a point-key
	// iterator or range-key iterator but we require one, it'll be created in
	// the slow path that reconstructs the iterator in finishInitializingIter.
	if i.batch != nil {
		nextBatchSeqNum := (uint64(len(i.batch.data)) | base.InternalKeySeqNumBatch)
		if nextBatchSeqNum != i.batchSeqNum {
			i.batchSeqNum = nextBatchSeqNum
			if i.pointIter != nil {
				if i.batch.countRangeDels == 0 {
					// No range deletions exist in the batch. We only need to
					// update the batchIter's snapshot.
					i.batchPointIter.snapshot = nextBatchSeqNum
					i.invalidate()
				} else if i.batchRangeDelIter.Count() == 0 {
					// When we constructed this iterator, there were no
					// rangedels in the batch. Iterator construction will have
					// excluded the batch rangedel iterator from the point
					// iterator stack. We need to reconstruct the point iterator
					// to add i.batchRangeDelIter into the iterator stack.
					i.err = firstError(i.err, i.pointIter.Close())
					i.pointIter = nil
				} else {
					// There are range deletions in the batch and we already
					// have a batch rangedel iterator. We can update the batch
					// rangedel iterator in place.
					//
					// NB: There may or may not be new range deletions. We can't
					// tell based on i.batchRangeDelIter.Count(), which is the
					// count of fragmented range deletions, NOT the number of
					// range deletions written to the batch
					// [i.batch.countRangeDels].
					i.batchPointIter.snapshot = nextBatchSeqNum
					i.batch.initRangeDelIter(&i.opts, &i.batchRangeDelIter, nextBatchSeqNum)
					i.invalidate()
				}
			}
			if i.rangeKey != nil && i.batch.countRangeKeys > 0 {
				if i.batchRangeKeyIter.Count() == 0 {
					// When we constructed this iterator, there were no range
					// keys in the batch. Iterator construction will have
					// excluded the batch rangekey iterator from the range key
					// iterator stack. We need to reconstruct the range key
					// iterator to add i.batchRangeKeyIter into the iterator
					// stack.
					i.err = firstError(i.err, i.rangeKey.rangeKeyIter.Close())
					i.rangeKey = nil
				} else {
					// There are range keys in the batch and we already
					// have a batch rangekey iterator. We can update the batch
					// rangekey iterator in place.
					//
					// NB: There may or may not be new range keys. We can't
					// tell based on i.batchRangeKeyIter.Count(), which is the
					// count of fragmented range keys, NOT the number of
					// range keys written to the batch [i.batch.countRangeKeys].
					i.batch.initRangeKeyIter(&i.opts, &i.batchRangeKeyIter, nextBatchSeqNum)
					i.invalidate()
				}
			}
		}
	}

	// Reset combinedIterState.initialized in case the iterator key types
	// changed. If there's already a range key iterator stack, the combined
	// iterator is already initialized.  Additionally, if the iterator is not
	// configured to include range keys, mark it as initialized to signal that
	// lower level iterators should not trigger a switch to combined iteration.
	i.lazyCombinedIter.combinedIterState = combinedIterState{
		initialized: i.rangeKey != nil || !i.opts.rangeKeys(),
	}

	boundsEqual := ((i.opts.LowerBound == nil) == (o.LowerBound == nil)) &&
		((i.opts.UpperBound == nil) == (o.UpperBound == nil)) &&
		i.equal(i.opts.LowerBound, o.LowerBound) &&
		i.equal(i.opts.UpperBound, o.UpperBound)

	if boundsEqual && o.KeyTypes == i.opts.KeyTypes &&
		(i.pointIter != nil || !i.opts.pointKeys()) &&
		(i.rangeKey != nil || !i.opts.rangeKeys() || i.opts.KeyTypes == IterKeyTypePointsAndRanges) &&
		i.equal(o.RangeKeyMasking.Suffix, i.opts.RangeKeyMasking.Suffix) &&
		o.UseL6Filters == i.opts.UseL6Filters {
		// The options are identical, so we can likely use the fast path. In
		// addition to all the above constraints, we cannot use the fast path if
		// configured to perform lazy combined iteration but an indexed batch
		// used by the iterator now contains range keys. Lazy combined iteration
		// is not compatible with batch range keys because we always need to
		// merge the batch's range keys into iteration.
		if i.rangeKey != nil || !i.opts.rangeKeys() || i.batch == nil || i.batch.countRangeKeys == 0 {
			// Fast path. This preserves the Seek-using-Next optimizations as
			// long as the iterator wasn't already invalidated up above.
			return
		}
	}
	// Slow path.

	// The options changed. Save the new ones to i.opts.
	if boundsEqual {
		// Copying the options into i.opts will overwrite LowerBound and
		// UpperBound fields with the user-provided slices. We need to hold on
		// to the Pebble-owned slices, so save them and re-set them after the
		// copy.
		lower, upper := i.opts.LowerBound, i.opts.UpperBound
		i.opts = *o
		i.opts.LowerBound, i.opts.UpperBound = lower, upper
	} else {
		i.opts = *o
		i.saveBounds(o.LowerBound, o.UpperBound)
		// Propagate the changed bounds to the existing point iterator.
		// NB: We propagate i.opts.{Lower,Upper}Bound, not o.{Lower,Upper}Bound
		// because i.opts now point to buffers owned by Pebble.
		if i.pointIter != nil {
			i.pointIter.SetBounds(i.opts.LowerBound, i.opts.UpperBound)
		}
		if i.rangeKey != nil {
			i.rangeKey.iterConfig.SetBounds(i.opts.LowerBound, i.opts.UpperBound)
		}
	}

	// Even though this is not a positioning operation, the invalidation of the
	// iterator stack means we cannot optimize Seeks by using Next.
	i.invalidate()

	// Iterators created through NewExternalIter have a different iterator
	// initialization process.
	if i.externalReaders != nil {
		finishInitializingExternal(i)
		return
	}
	finishInitializingIter(i.alloc)
}

func (i *Iterator) invalidate() {
	i.lastPositioningOp = unknownLastPositionOp
	i.hasPrefix = false
	i.iterKey = nil
	i.iterValue = nil
	i.err = nil
	// This switch statement isn't necessary for correctness since callers
	// should call a repositioning method. We could have arbitrarily set i.pos
	// to one of the values. But it results in more intuitive behavior in
	// tests, which do not always reposition.
	switch i.pos {
	case iterPosCurForward, iterPosNext, iterPosCurForwardPaused:
		i.pos = iterPosCurForward
	case iterPosCurReverse, iterPosPrev, iterPosCurReversePaused:
		i.pos = iterPosCurReverse
	}
	i.iterValidityState = IterExhausted
	if i.rangeKey != nil {
		i.rangeKey.iiter.Invalidate()
	}
}

// Metrics returns per-iterator metrics.
func (i *Iterator) Metrics() IteratorMetrics {
	m := IteratorMetrics{
		ReadAmp: 1,
	}
	if mi, ok := i.iter.(*mergingIter); ok {
		m.ReadAmp = len(mi.levels)
	}
	return m
}

// ResetStats resets the stats to 0.
func (i *Iterator) ResetStats() {
	i.stats = IteratorStats{}
}

// Stats returns the current stats.
func (i *Iterator) Stats() IteratorStats {
	return i.stats
}

// CloneOptions configures an iterator constructed through Iterator.Clone.
type CloneOptions struct {
	// IterOptions, if non-nil, define the iterator options to configure a
	// cloned iterator. If nil, the clone adopts the same IterOptions as the
	// iterator being cloned.
	IterOptions *IterOptions
	// RefreshBatchView may be set to true when cloning an Iterator over an
	// indexed batch. When false, the clone adopts the same (possibly stale)
	// view of the indexed batch as the cloned Iterator. When true, the clone is
	// constructed with a refreshed view of the batch, observing all of the
	// batch's mutations at the time of the Clone. If the cloned iterator was
	// not constructed to read over an indexed batch, RefreshVatchView has no
	// effect.
	RefreshBatchView bool
}

// Clone creates a new Iterator over the same underlying data, i.e., over the
// same {batch, memtables, sstables}). The resulting iterator is not positioned.
// It starts with the same IterOptions, unless opts.IterOptions is set.
//
// When called on an Iterator over an indexed batch, the clone's visibility of
// the indexed batch is determined by CloneOptions.RefreshBatchView. If false,
// the clone inherits the iterator's current (possibly stale) view of the batch,
// and callers may call SetOptions to subsequently refresh the clone's view to
// include all batch mutations. If true, the clone is constructed with a
// complete view of the indexed batch's mutations at the time of the Clone.
//
// Callers can use Clone if they need multiple iterators that need to see
// exactly the same underlying state of the DB. This should not be used to
// extend the lifetime of the data backing the original Iterator since that
// will cause an increase in memory and disk usage (use NewSnapshot for that
// purpose).
func (i *Iterator) Clone(opts CloneOptions) (*Iterator, error) {
	if opts.IterOptions == nil {
		opts.IterOptions = &i.opts
	}

	readState := i.readState
	if readState == nil {
		return nil, errors.Errorf("cannot Clone a closed Iterator")
	}
	// i is already holding a ref, so there is no race with unref here.
	readState.ref()
	// Bundle various structures under a single umbrella in order to allocate
	// them together.
	buf := iterAllocPool.Get().(*iterAlloc)
	dbi := &buf.dbi
	*dbi = Iterator{
		opts:                *opts.IterOptions,
		alloc:               buf,
		merge:               i.merge,
		comparer:            i.comparer,
		readState:           readState,
		keyBuf:              buf.keyBuf,
		prefixOrFullSeekKey: buf.prefixOrFullSeekKey,
		boundsBuf:           buf.boundsBuf,
		batch:               i.batch,
		batchSeqNum:         i.batchSeqNum,
		newIters:            i.newIters,
		newIterRangeKey:     i.newIterRangeKey,
		seqNum:              i.seqNum,
	}
	dbi.saveBounds(dbi.opts.LowerBound, dbi.opts.UpperBound)

	// If the caller requested the clone have a current view of the indexed
	// batch, set the clone's batch sequence number appropriately.
	if i.batch != nil && opts.RefreshBatchView {
		dbi.batchSeqNum = (uint64(len(i.batch.data)) | base.InternalKeySeqNumBatch)
	}

	return finishInitializingIter(buf), nil
}

func (stats *IteratorStats) String() string {
	return redact.StringWithoutMarkers(stats)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (stats *IteratorStats) SafeFormat(s redact.SafePrinter, verb rune) {
	for i := range stats.ForwardStepCount {
		switch IteratorStatsKind(i) {
		case InterfaceCall:
			s.SafeString("(interface (dir, seek, step): ")
		case InternalIterCall:
			s.SafeString(", (internal (dir, seek, step): ")
		}
		s.Printf("(fwd, %d, %d), (rev, %d, %d))",
			redact.Safe(stats.ForwardSeekCount[i]), redact.Safe(stats.ForwardStepCount[i]),
			redact.Safe(stats.ReverseSeekCount[i]), redact.Safe(stats.ReverseStepCount[i]))
	}
	if stats.InternalStats != (InternalIteratorStats{}) {
		s.SafeString(",\n(internal-stats: ")
		s.Printf("(block-bytes: (total %s, cached %s)), "+
			"(points: (count %s, key-bytes %s, value-bytes %s, tombstoned: %s))",
			humanize.IEC.Uint64(stats.InternalStats.BlockBytes),
			humanize.IEC.Uint64(stats.InternalStats.BlockBytesInCache),
			humanize.SI.Uint64(stats.InternalStats.PointCount),
			humanize.SI.Uint64(stats.InternalStats.KeyBytes),
			humanize.SI.Uint64(stats.InternalStats.ValueBytes),
			humanize.SI.Uint64(stats.InternalStats.PointsCoveredByRangeTombstones),
		)
	}
}
