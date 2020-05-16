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
	"math"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/pebble"
)

// pebbleIterator is a wrapper around a pebble.Iterator that implements the
// Iterator interface.
type pebbleIterator struct {
	// Underlying iterator for the DB.
	iter    *pebble.Iterator
	options pebble.IterOptions
	// Reusable buffer for MVCC key encoding.
	keyBuf []byte
	// Buffers for copying iterator bounds to. Note that the underlying memory
	// is not GCed upon Close(), to reduce the number of overall allocations.
	lowerBoundBuf []byte
	upperBoundBuf []byte
	// Set to true to govern whether to call SeekPrefixGE or SeekGE. Skips
	// SSTables based on MVCC key when true.
	prefix bool
	// If reusable is true, Close() does not actually close the underlying
	// iterator, but simply marks it as not inuse. Used by pebbleReadOnly.
	reusable bool
	inuse    bool
	// Stat tracking the number of sstables encountered during time-bound
	// iteration.
	timeBoundNumSSTables int
}

var _ Iterator = &pebbleIterator{}

var pebbleIterPool = sync.Pool{
	New: func() interface{} {
		return &pebbleIterator{}
	},
}

// Instantiates a new Pebble iterator, or gets one from the pool.
func newPebbleIterator(handle pebble.Reader, opts IterOptions) Iterator {
	iter := pebbleIterPool.Get().(*pebbleIterator)
	iter.init(handle, opts)
	return iter
}

// init resets this pebbleIterator for use with the specified arguments. The
// current instance could either be a cached iterator (eg. in pebbleBatch), or
// a newly-instantiated one through newPebbleIterator.
func (p *pebbleIterator) init(handle pebble.Reader, opts IterOptions) {
	*p = pebbleIterator{
		keyBuf:        p.keyBuf,
		lowerBoundBuf: p.lowerBoundBuf,
		upperBoundBuf: p.upperBoundBuf,
		prefix:        opts.Prefix,
		reusable:      p.reusable,
	}

	if !opts.Prefix && len(opts.UpperBound) == 0 && len(opts.LowerBound) == 0 {
		panic("iterator must set prefix or upper bound or lower bound")
	}

	if opts.LowerBound != nil {
		// This is the same as
		// p.options.LowerBound = EncodeKeyToBuf(p.lowerBoundBuf[:0], MVCCKey{Key: opts.LowerBound}) .
		// Since we are encoding zero-timestamp MVCC Keys anyway, we can just append
		// the NUL byte instead of calling EncodeKey which will do the same thing.
		p.lowerBoundBuf = append(p.lowerBoundBuf[:0], opts.LowerBound...)
		p.lowerBoundBuf = append(p.lowerBoundBuf, 0x00)
		p.options.LowerBound = p.lowerBoundBuf
	}
	if opts.UpperBound != nil {
		// Same as above.
		p.upperBoundBuf = append(p.upperBoundBuf[:0], opts.UpperBound...)
		p.upperBoundBuf = append(p.upperBoundBuf, 0x00)
		p.options.UpperBound = p.upperBoundBuf
	}

	if opts.MaxTimestampHint != (hlc.Timestamp{}) {
		encodedMinTS := string(encodeTimestamp(opts.MinTimestampHint))
		encodedMaxTS := string(encodeTimestamp(opts.MaxTimestampHint))
		p.options.TableFilter = func(userProps map[string]string) bool {
			tableMinTS := userProps["crdb.ts.min"]
			if len(tableMinTS) == 0 {
				if opts.WithStats {
					p.timeBoundNumSSTables++
				}
				return true
			}
			tableMaxTS := userProps["crdb.ts.max"]
			if len(tableMaxTS) == 0 {
				if opts.WithStats {
					p.timeBoundNumSSTables++
				}
				return true
			}
			used := encodedMaxTS >= tableMinTS && encodedMinTS <= tableMaxTS
			if used && opts.WithStats {
				p.timeBoundNumSSTables++
			}
			return used
		}
	} else if opts.MinTimestampHint != (hlc.Timestamp{}) {
		panic("min timestamp hint set without max timestamp hint")
	}

	p.iter = handle.NewIter(&p.options)
	if p.iter == nil {
		panic("unable to create iterator")
	}

	p.inuse = true
}

func (p *pebbleIterator) setOptions(opts IterOptions) {
	// Overwrite any stale options from last time.
	p.options = pebble.IterOptions{}

	if opts.MinTimestampHint != (hlc.Timestamp{}) || opts.MaxTimestampHint != (hlc.Timestamp{}) {
		panic("iterator with timestamp hints cannot be reused")
	}
	if !opts.Prefix && len(opts.UpperBound) == 0 && len(opts.LowerBound) == 0 {
		panic("iterator must set prefix or upper bound or lower bound")
	}

	p.prefix = opts.Prefix
	if opts.LowerBound != nil {
		// This is the same as
		// p.options.LowerBound = EncodeKeyToBuf(p.lowerBoundBuf[:0], MVCCKey{Key: opts.LowerBound}) .
		// Since we are encoding zero-timestamp MVCC Keys anyway, we can just append
		// the NUL byte instead of calling EncodeKey which will do the same thing.
		p.lowerBoundBuf = append(p.lowerBoundBuf[:0], opts.LowerBound...)
		p.lowerBoundBuf = append(p.lowerBoundBuf, 0x00)
		p.options.LowerBound = p.lowerBoundBuf
	}
	if opts.UpperBound != nil {
		// Same as above.
		p.upperBoundBuf = append(p.upperBoundBuf[:0], opts.UpperBound...)
		p.upperBoundBuf = append(p.upperBoundBuf, 0x00)
		p.options.UpperBound = p.upperBoundBuf
	}
	p.iter.SetBounds(p.options.LowerBound, p.options.UpperBound)
}

// Close implements the Iterator interface.
func (p *pebbleIterator) Close() {
	if !p.inuse {
		panic("closing idle iterator")
	}
	p.inuse = false

	if p.reusable {
		return
	}

	p.destroy()

	pebbleIterPool.Put(p)
}

// SeekGE implements the Iterator interface.
func (p *pebbleIterator) SeekGE(key MVCCKey) {
	p.keyBuf = EncodeKeyToBuf(p.keyBuf[:0], key)
	if p.prefix {
		p.iter.SeekPrefixGE(p.keyBuf)
	} else {
		p.iter.SeekGE(p.keyBuf)
	}
}

// Valid implements the Iterator interface.
func (p *pebbleIterator) Valid() (bool, error) {
	// NB: A Pebble Iterator always returns Valid()==false when an error is
	// present. If Valid() is true, there is no error.
	if ok := p.iter.Valid(); ok {
		return ok, nil
	}
	return false, p.iter.Error()
}

// Next implements the Iterator interface.
func (p *pebbleIterator) Next() {
	p.iter.Next()
}

// NextKey implements the Iterator interface.
func (p *pebbleIterator) NextKey() {
	if valid, err := p.Valid(); err != nil || !valid {
		return
	}
	p.keyBuf = append(p.keyBuf[:0], p.UnsafeKey().Key...)
	if !p.iter.Next() {
		return
	}
	if bytes.Equal(p.keyBuf, p.UnsafeKey().Key) {
		// This is equivalent to:
		// p.iter.SeekGE(EncodeKey(MVCCKey{p.UnsafeKey().Key.Next(), hlc.Timestamp{}}))
		p.iter.SeekGE(append(p.keyBuf, 0, 0))
	}
}

// UnsafeKey implements the Iterator interface.
func (p *pebbleIterator) UnsafeKey() MVCCKey {
	if valid, err := p.Valid(); err != nil || !valid {
		return MVCCKey{}
	}

	mvccKey, err := DecodeMVCCKey(p.iter.Key())
	if err != nil {
		return MVCCKey{}
	}

	return mvccKey
}

// unsafeRawKey returns the raw key from the underlying pebble.Iterator.
func (p *pebbleIterator) unsafeRawKey() []byte {
	return p.iter.Key()
}

// UnsafeValue implements the Iterator interface.
func (p *pebbleIterator) UnsafeValue() []byte {
	if valid, err := p.Valid(); err != nil || !valid {
		return nil
	}
	return p.iter.Value()
}

// SeekLT implements the Iterator interface.
func (p *pebbleIterator) SeekLT(key MVCCKey) {
	p.keyBuf = EncodeKeyToBuf(p.keyBuf[:0], key)
	p.iter.SeekLT(p.keyBuf)
}

// Prev implements the Iterator interface.
func (p *pebbleIterator) Prev() {
	p.iter.Prev()
}

// Key implements the Iterator interface.
func (p *pebbleIterator) Key() MVCCKey {
	key := p.UnsafeKey()
	keyCopy := make([]byte, len(key.Key))
	copy(keyCopy, key.Key)
	key.Key = keyCopy
	return key
}

// Value implements the Iterator interface.
func (p *pebbleIterator) Value() []byte {
	value := p.UnsafeValue()
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	return valueCopy
}

// ValueProto implements the Iterator interface.
func (p *pebbleIterator) ValueProto(msg protoutil.Message) error {
	value := p.UnsafeValue()

	return protoutil.Unmarshal(value, msg)
}

// ComputeStats implements the Iterator interface.
func (p *pebbleIterator) ComputeStats(
	start, end roachpb.Key, nowNanos int64,
) (enginepb.MVCCStats, error) {
	return ComputeStatsGo(p, start, end, nowNanos)
}

// Go-only version of IsValidSplitKey. Checks if the specified key is in
// NoSplitSpans.
func isValidSplitKey(key roachpb.Key, noSplitSpans []roachpb.Span) bool {
	for i := range noSplitSpans {
		if noSplitSpans[i].ContainsKey(key) {
			return false
		}
	}
	return true
}

// FindSplitKey implements the Iterator interface.
func (p *pebbleIterator) FindSplitKey(
	start, end, minSplitKey roachpb.Key, targetSize int64,
) (MVCCKey, error) {
	const timestampLen = 12

	sizeSoFar := int64(0)
	bestDiff := int64(math.MaxInt64)
	bestSplitKey := MVCCKey{}
	// found indicates that we have found a valid split key that is the best
	// known so far. If bestSplitKey is empty => that split key
	// is in prevKey, else it is in bestSplitKey.
	found := false
	prevKey := MVCCKey{}

	// We only have to consider no-split spans if our minimum split key possibly
	// lies before them. Note that the no-split spans are ordered by end-key.
	noSplitSpans := keys.NoSplitSpans
	for i := range noSplitSpans {
		if minSplitKey.Compare(noSplitSpans[i].EndKey) <= 0 {
			noSplitSpans = noSplitSpans[i:]
			break
		}
	}

	// Note that it is unnecessary to compare against "end" to decide to
	// terminate iteration because the iterator's upper bound has already been
	// set to end.
	mvccMinSplitKey := MakeMVCCMetadataKey(minSplitKey)
	p.SeekGE(MakeMVCCMetadataKey(start))
	for ; p.iter.Valid(); p.iter.Next() {
		mvccKey, err := DecodeMVCCKey(p.iter.Key())
		if err != nil {
			return MVCCKey{}, err
		}

		diff := targetSize - sizeSoFar
		if diff < 0 {
			diff = -diff
		}
		if diff > bestDiff {
			// diff will keep increasing past this point. And we must have had a valid
			// candidate in the past since we can't be worse than MaxInt64.
			break
		}

		if mvccMinSplitKey.Key != nil && !mvccKey.Less(mvccMinSplitKey) {
			// mvccKey is >= mvccMinSplitKey. Set the minSplitKey to nil so we do
			// not have to make any more checks going forward.
			mvccMinSplitKey.Key = nil
		}

		if mvccMinSplitKey.Key == nil && diff < bestDiff &&
			(len(noSplitSpans) == 0 || isValidSplitKey(mvccKey.Key, noSplitSpans)) {
			// This is a valid candidate for a split key.
			//
			// Instead of copying bestSplitKey just yet, flip the found flag. In the
			// most common case where the actual best split key is followed by a key
			// that has diff > bestDiff (see the if statement with that predicate
			// above), this lets us save a copy by reusing prevCandidateKey as the
			// best split key.
			bestDiff = diff
			found = true
			// Set length of bestSplitKey to 0, which the rest of this method relies
			// on to check if the last key encountered was the best split key.
			bestSplitKey.Key = bestSplitKey.Key[:0]
		} else if found && len(bestSplitKey.Key) == 0 {
			// We were just at a valid split key candidate, but then we came across
			// a key that cannot be a split key (i.e. is in noSplitSpans), or was not
			// an improvement over bestDiff. Copy the previous key as the
			// bestSplitKey.
			bestSplitKey.Timestamp = prevKey.Timestamp
			bestSplitKey.Key = append(bestSplitKey.Key[:0], prevKey.Key...)
		}

		sizeSoFar += int64(len(p.iter.Value()))
		if mvccKey.IsValue() && bytes.Equal(prevKey.Key, mvccKey.Key) {
			// We only advanced timestamps, but not new mvcc keys.
			sizeSoFar += timestampLen
		} else {
			sizeSoFar += int64(len(mvccKey.Key) + 1)
			if mvccKey.IsValue() {
				sizeSoFar += timestampLen
			}
		}

		prevKey.Key = append(prevKey.Key[:0], mvccKey.Key...)
		prevKey.Timestamp = mvccKey.Timestamp
	}

	// There are three distinct types of cases possible here:
	//
	// 1. No valid split key was found (found == false), in which case we return
	//    bestSplitKey (which should be MVCCKey{}).
	// 2. The best candidate seen for a split key so far was encountered in the
	//    last iteration of the above loop. We broke out of the loop either due
	//    to iterator exhaustion (!p.iter.Valid()), or an increasing diff. Return
	//    prevKey as the best split key.
	// 3. The best split key was seen multiple iterations ago, and was copied into
	//    bestSplitKey at some point (found == true, len(bestSplitKey.Key) > 0).
	//    Keys encountered after that point were invalid for being in noSplitSpans
	//    so return the bestSplitKey that had been copied.
	//
	// This if statement checks for case 2.
	if found && len(bestSplitKey.Key) == 0 {
		// Use the last key found as the best split key, since we broke out of the
		// loop (due to iterator exhaustion or increasing diff) right after we saw
		// the best split key. prevKey has to be a valid split key since the only
		// way we'd have both found && len(bestSplitKey.Key) == 0 is when we've
		// already checked prevKey for validity.
		return prevKey, nil
	}
	return bestSplitKey, nil
}

// SetUpperBound implements the Iterator interface.
func (p *pebbleIterator) SetUpperBound(upperBound roachpb.Key) {
	p.upperBoundBuf = append(p.upperBoundBuf[:0], upperBound...)
	p.upperBoundBuf = append(p.upperBoundBuf, 0x00)
	p.options.UpperBound = p.upperBoundBuf
	p.iter.SetBounds(p.options.LowerBound, p.options.UpperBound)
}

// Stats implements the Iterator interface.
func (p *pebbleIterator) Stats() IteratorStats {
	return IteratorStats{
		TimeBoundNumSSTs: p.timeBoundNumSSTables,
	}
}

// CheckForKeyCollisions indicates if the provided SST data collides with this
// iterator in the specified range.
func (p *pebbleIterator) CheckForKeyCollisions(
	sstData []byte, start, end roachpb.Key,
) (enginepb.MVCCStats, error) {
	return checkForKeyCollisionsGo(p, sstData, start, end)
}

func (p *pebbleIterator) destroy() {
	if p.inuse {
		panic("iterator still in use")
	}
	if p.iter != nil {
		err := p.iter.Close()
		if err != nil {
			panic(err)
		}
		p.iter = nil
	}
	// Reset all fields except for the key and lower/upper bound buffers. Holding
	// onto their underlying memory is more efficient to prevent extra
	// allocations down the line.
	*p = pebbleIterator{
		keyBuf:        p.keyBuf,
		lowerBoundBuf: p.lowerBoundBuf,
		upperBoundBuf: p.upperBoundBuf,
		reusable:      p.reusable,
	}
}
