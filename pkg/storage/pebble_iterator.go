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
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

// pebbleIterator is a wrapper around a pebble.Iterator that implements the
// MVCCIterator and EngineIterator interfaces. A single pebbleIterator
// should only be used in one of the two modes.
type pebbleIterator struct {
	// Underlying iterator for the DB.
	iter    *pebble.Iterator
	options pebble.IterOptions
	// Reusable buffer for MVCCKey or EngineKey encoding.
	keyBuf []byte
	// Buffers for copying iterator bounds to. Note that the underlying memory
	// is not GCed upon Close(), to reduce the number of overall allocations. We
	// use two slices for each of the bounds since this caller should not change
	// the slice holding the current bounds, that the callee (pebble.MVCCIterator)
	// is currently using, until after the caller has made the SetBounds call.
	lowerBoundBuf            [2][]byte
	upperBoundBuf            [2][]byte
	curBuf                   int
	testingSetBoundsListener testingSetBoundsListener

	// Set to true to govern whether to call SeekPrefixGE or SeekGE. Skips
	// SSTables based on MVCC/Engine key when true.
	prefix bool
	// If reusable is true, Close() does not actually close the underlying
	// iterator, but simply marks it as not inuse. Used by pebbleReadOnly.
	reusable bool
	inuse    bool
	// mvccDirIsReverse and mvccDone are used only for the methods implementing
	// MVCCIterator. They are used to prevent the iterator from iterating into
	// the lock table key space.
	//
	// The current direction. false for forward, true for reverse.
	mvccDirIsReverse bool
	// True iff the iterator is exhausted in the current direction. There is
	// no error to report when it is true.
	mvccDone bool
	// Stat tracking the number of sstables encountered during time-bound
	// iteration. Only used for MVCCIterator.
	timeBoundNumSSTables int
}

var _ MVCCIterator = &pebbleIterator{}
var _ EngineIterator = &pebbleIterator{}

var pebbleIterPool = sync.Pool{
	New: func() interface{} {
		return &pebbleIterator{}
	},
}

type cloneableIter interface {
	Clone() (*pebble.Iterator, error)
}

type testingSetBoundsListener interface {
	postSetBounds(lower, upper []byte)
}

// Instantiates a new Pebble iterator, or gets one from the pool.
func newPebbleIterator(
	handle pebble.Reader, iterToClone cloneableIter, opts IterOptions,
) *pebbleIterator {
	iter := pebbleIterPool.Get().(*pebbleIterator)
	iter.reusable = false // defensive
	iter.init(handle, iterToClone, opts)
	return iter
}

// init resets this pebbleIterator for use with the specified arguments. The
// current instance could either be a cached pebbleIterator (eg. in
// pebbleBatch), or a newly-instantiated one through newPebbleIterator. The
// underlying *pebble.Iterator is created using iterToClone, if non-nil and
// there are no timestamp hints, else it is created using handle.
func (p *pebbleIterator) init(handle pebble.Reader, iterToClone cloneableIter, opts IterOptions) {
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
		// p.options.LowerBound = EncodeKeyToBuf(p.lowerBoundBuf[0][:0], MVCCKey{Key: opts.LowerBound})
		// or EngineKey{Key: opts.LowerBound}.EncodeToBuf(...).
		// Since we are encoding keys with an empty version anyway, we can just
		// append the NUL byte instead of calling the above encode functions which
		// will do the same thing.
		p.lowerBoundBuf[0] = append(p.lowerBoundBuf[0][:0], opts.LowerBound...)
		p.lowerBoundBuf[0] = append(p.lowerBoundBuf[0], 0x00)
		p.options.LowerBound = p.lowerBoundBuf[0]
	}
	if opts.UpperBound != nil {
		// Same as above.
		p.upperBoundBuf[0] = append(p.upperBoundBuf[0][:0], opts.UpperBound...)
		p.upperBoundBuf[0] = append(p.upperBoundBuf[0], 0x00)
		p.options.UpperBound = p.upperBoundBuf[0]
	}

	doClone := iterToClone != nil
	if !opts.MaxTimestampHint.IsEmpty() {
		doClone = false
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
	} else if !opts.MinTimestampHint.IsEmpty() {
		panic("min timestamp hint set without max timestamp hint")
	}

	if doClone {
		var err error
		if p.iter, err = iterToClone.Clone(); err != nil {
			panic(err)
		}
		p.iter.SetBounds(p.options.LowerBound, p.options.UpperBound)
	} else {
		if handle == nil {
			panic("handle is nil for non-cloning path")
		}
		p.iter = handle.NewIter(&p.options)
	}
	if p.iter == nil {
		panic("unable to create iterator")
	}

	p.inuse = true
}

// setBounds is called to change the bounds on a pebbleIterator. Note that
// this is not the first time that bounds will be passed to the underlying
// pebble.Iterator. The existing bounds are in p.options.
func (p *pebbleIterator) setBounds(lowerBound, upperBound roachpb.Key) {
	// If the roachpb.Key bound is nil, the corresponding bound for the
	// pebble.Iterator will also be nil. p.options contains the current bounds
	// known to the pebble.Iterator.
	boundsChanged := ((lowerBound == nil) != (p.options.LowerBound == nil)) ||
		((upperBound == nil) != (p.options.UpperBound == nil))
	if !boundsChanged {
		// The nil-ness is the same but the values may be different.
		if lowerBound != nil {
			// Both must be non-nil. We know that we've appended 0x00 to
			// p.options.LowerBound, which must be ignored for this comparison.
			if !bytes.Equal(p.options.LowerBound[:len(p.options.LowerBound)-1], lowerBound) {
				boundsChanged = true
			}
		}
		// If the preceding if-block has not already set boundsChanged=true, see
		// if the upper bound has changed.
		if !boundsChanged && upperBound != nil {
			// Both must be non-nil. We know that we've appended 0x00 to
			// p.options.UpperBound, which must be ignored for this comparison.
			if !bytes.Equal(p.options.UpperBound[:len(p.options.UpperBound)-1], upperBound) {
				boundsChanged = true
			}
		}
	}
	if !boundsChanged {
		// This noop optimization helps the underlying pebble.Iterator to optimize
		// seeks.
		return
	}
	// Set the bounds to nil, before we selectively change them.
	p.options.LowerBound = nil
	p.options.UpperBound = nil
	p.curBuf = (p.curBuf + 1) % 2
	i := p.curBuf
	if lowerBound != nil {
		// This is the same as
		// p.options.LowerBound = EncodeKeyToBuf(p.lowerBoundBuf[i][:0], MVCCKey{Key: lowerBound}) .
		// or EngineKey{Key: lowerBound}.EncodeToBuf(...).
		// Since we are encoding keys with an empty version anyway, we can just
		// append the NUL byte instead of calling the above encode functions which
		// will do the same thing.
		p.lowerBoundBuf[i] = append(p.lowerBoundBuf[i][:0], lowerBound...)
		p.lowerBoundBuf[i] = append(p.lowerBoundBuf[i], 0x00)
		p.options.LowerBound = p.lowerBoundBuf[i]
	}
	if upperBound != nil {
		// Same as above.
		p.upperBoundBuf[i] = append(p.upperBoundBuf[i][:0], upperBound...)
		p.upperBoundBuf[i] = append(p.upperBoundBuf[i], 0x00)
		p.options.UpperBound = p.upperBoundBuf[i]
	}
	p.iter.SetBounds(p.options.LowerBound, p.options.UpperBound)
	if p.testingSetBoundsListener != nil {
		p.testingSetBoundsListener.postSetBounds(p.options.LowerBound, p.options.UpperBound)
	}
}

// Close implements the MVCCIterator interface.
func (p *pebbleIterator) Close() {
	if !p.inuse {
		panic("closing idle iterator")
	}
	p.inuse = false

	if p.reusable {
		p.iter.ResetStats()
		return
	}

	p.destroy()

	pebbleIterPool.Put(p)
}

// SeekGE implements the MVCCIterator interface.
func (p *pebbleIterator) SeekGE(key MVCCKey) {
	p.mvccDirIsReverse = false
	p.mvccDone = false
	p.keyBuf = EncodeKeyToBuf(p.keyBuf[:0], key)
	if p.prefix {
		p.iter.SeekPrefixGE(p.keyBuf)
	} else {
		p.iter.SeekGE(p.keyBuf)
	}
}

// SeekIntentGE implements the MVCCIterator interface.
func (p *pebbleIterator) SeekIntentGE(key roachpb.Key, _ uuid.UUID) {
	p.SeekGE(MVCCKey{Key: key})
}

// SeekEngineKeyGE implements the EngineIterator interface.
func (p *pebbleIterator) SeekEngineKeyGE(key EngineKey) (valid bool, err error) {
	p.keyBuf = key.EncodeToBuf(p.keyBuf[:0])
	var ok bool
	if p.prefix {
		ok = p.iter.SeekPrefixGE(p.keyBuf)
	} else {
		ok = p.iter.SeekGE(p.keyBuf)
	}
	// NB: A Pebble Iterator always returns ok==false when an error is
	// present.
	if ok {
		return true, nil
	}
	return false, p.iter.Error()
}

func (p *pebbleIterator) SeekEngineKeyGEWithLimit(
	key EngineKey, limit roachpb.Key,
) (state pebble.IterValidityState, err error) {
	p.keyBuf = key.EncodeToBuf(p.keyBuf[:0])
	if limit != nil {
		if p.prefix {
			panic("prefix iteration does not permit a limit")
		}
		// Append the sentinel byte to make an EngineKey that has an empty
		// version.
		limit = append(limit, '\x00')
	}
	if p.prefix {
		state = pebble.IterExhausted
		if p.iter.SeekPrefixGE(p.keyBuf) {
			state = pebble.IterValid
		}
	} else {
		state = p.iter.SeekGEWithLimit(p.keyBuf, limit)
	}
	if state == pebble.IterExhausted {
		return state, p.iter.Error()
	}
	return state, nil
}

// Valid implements the MVCCIterator interface. Must not be called from
// methods of EngineIterator.
func (p *pebbleIterator) Valid() (bool, error) {
	if p.mvccDone {
		return false, nil
	}
	// NB: A Pebble Iterator always returns Valid()==false when an error is
	// present. If Valid() is true, there is no error.
	if ok := p.iter.Valid(); ok {
		// The MVCCIterator interface is broken in that it silently discards
		// the error when UnsafeKey(), Key() are unable to parse the key as
		// an MVCCKey. This is especially problematic if the caller is
		// accidentally iterating into the lock table key space, since that
		// parsing will fail. We do a cheap check here to make sure we are
		// not in the lock table key space.
		//
		// TODO(sumeer): fix this properly by changing those method signatures.
		k := p.iter.Key()
		if len(k) == 0 {
			return false, errors.Errorf("iterator encountered 0 length key")
		}
		// Last byte is the version length + 1 or 0.
		versionLen := int(k[len(k)-1])
		if versionLen == engineKeyVersionLockTableLen+1 {
			p.mvccDone = true
			return false, nil
		}
		return ok, nil
	}
	return false, p.iter.Error()
}

// Next implements the MVCCIterator interface.
func (p *pebbleIterator) Next() {
	if p.mvccDirIsReverse {
		// Switching directions.
		p.mvccDirIsReverse = false
		p.mvccDone = false
	}
	if p.mvccDone {
		return
	}
	p.iter.Next()
}

// NextEngineKey implements the Engineterator interface.
func (p *pebbleIterator) NextEngineKey() (valid bool, err error) {
	ok := p.iter.Next()
	// NB: A Pebble Iterator always returns ok==false when an error is
	// present.
	if ok {
		return true, nil
	}
	return false, p.iter.Error()
}

func (p *pebbleIterator) NextEngineKeyWithLimit(
	limit roachpb.Key,
) (state pebble.IterValidityState, err error) {
	if limit != nil {
		// Append the sentinel byte to make an EngineKey that has an empty
		// version.
		limit = append(limit, '\x00')
	}
	state = p.iter.NextWithLimit(limit)
	if state == pebble.IterExhausted {
		return state, p.iter.Error()
	}
	return state, nil
}

// NextKey implements the MVCCIterator interface.
func (p *pebbleIterator) NextKey() {
	// Even though NextKey() is not allowed for switching direction by the
	// MVCCIterator interface, pebbleIterator works correctly even when
	// switching direction. So we set mvccDirIsReverse = false.
	if p.mvccDirIsReverse {
		// Switching directions.
		p.mvccDirIsReverse = false
		p.mvccDone = false
	}
	if p.mvccDone {
		return
	}
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

// UnsafeKey implements the MVCCIterator interface.
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

// UnsafeEngineKey implements the EngineIterator interface.
func (p *pebbleIterator) UnsafeEngineKey() (EngineKey, error) {
	engineKey, ok := DecodeEngineKey(p.iter.Key())
	if !ok {
		return engineKey, errors.Errorf("invalid encoded engine key: %x", p.iter.Key())
	}
	return engineKey, nil
}

// UnsafeRawKey returns the raw key from the underlying pebble.Iterator.
func (p *pebbleIterator) UnsafeRawKey() []byte {
	return p.iter.Key()
}

// UnsafeRawMVCCKey implements the MVCCIterator interface.
func (p *pebbleIterator) UnsafeRawMVCCKey() []byte {
	return p.iter.Key()
}

// UnsafeRawEngineKey implements the EngineIterator interface.
func (p *pebbleIterator) UnsafeRawEngineKey() []byte {
	return p.iter.Key()
}

// UnsafeValue implements the MVCCIterator and EngineIterator interfaces.
func (p *pebbleIterator) UnsafeValue() []byte {
	if ok := p.iter.Valid(); !ok {
		return nil
	}
	return p.iter.Value()
}

// SeekLT implements the MVCCIterator interface.
func (p *pebbleIterator) SeekLT(key MVCCKey) {
	p.mvccDirIsReverse = true
	p.mvccDone = false
	p.keyBuf = EncodeKeyToBuf(p.keyBuf[:0], key)
	p.iter.SeekLT(p.keyBuf)
}

// SeekEngineKeyLT implements the EngineIterator interface.
func (p *pebbleIterator) SeekEngineKeyLT(key EngineKey) (valid bool, err error) {
	p.keyBuf = key.EncodeToBuf(p.keyBuf[:0])
	ok := p.iter.SeekLT(p.keyBuf)
	// NB: A Pebble Iterator always returns ok==false when an error is
	// present.
	if ok {
		return true, nil
	}
	return false, p.iter.Error()
}

func (p *pebbleIterator) SeekEngineKeyLTWithLimit(
	key EngineKey, limit roachpb.Key,
) (state pebble.IterValidityState, err error) {
	p.keyBuf = key.EncodeToBuf(p.keyBuf[:0])
	if limit != nil {
		// Append the sentinel byte to make an EngineKey that has an empty
		// version.
		limit = append(limit, '\x00')
	}
	state = p.iter.SeekLTWithLimit(p.keyBuf, limit)
	if state == pebble.IterExhausted {
		return state, p.iter.Error()
	}
	return state, nil
}

// Prev implements the MVCCIterator interface.
func (p *pebbleIterator) Prev() {
	if !p.mvccDirIsReverse {
		// Switching directions.
		p.mvccDirIsReverse = true
		p.mvccDone = false
	}
	if p.mvccDone {
		return
	}
	p.iter.Prev()
}

// PrevEngineKey implements the EngineIterator interface.
func (p *pebbleIterator) PrevEngineKey() (valid bool, err error) {
	ok := p.iter.Prev()
	// NB: A Pebble Iterator always returns ok==false when an error is
	// present.
	if ok {
		return true, nil
	}
	return false, p.iter.Error()
}

func (p *pebbleIterator) PrevEngineKeyWithLimit(
	limit roachpb.Key,
) (state pebble.IterValidityState, err error) {
	if limit != nil {
		// Append the sentinel byte to make an EngineKey that has an empty
		// version.
		limit = append(limit, '\x00')
	}
	state = p.iter.PrevWithLimit(limit)
	if state == pebble.IterExhausted {
		return state, p.iter.Error()
	}
	return state, nil
}

// Key implements the MVCCIterator interface.
func (p *pebbleIterator) Key() MVCCKey {
	key := p.UnsafeKey()
	keyCopy := make([]byte, len(key.Key))
	copy(keyCopy, key.Key)
	key.Key = keyCopy
	return key
}

// EngineKey implements the EngineIterator interface.
func (p *pebbleIterator) EngineKey() (EngineKey, error) {
	key, err := p.UnsafeEngineKey()
	if err != nil {
		return key, err
	}
	return key.Copy(), nil
}

// Value implements the MVCCIterator and EngineIterator interfaces.
func (p *pebbleIterator) Value() []byte {
	value := p.UnsafeValue()
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	return valueCopy
}

// ValueProto implements the MVCCIterator interface.
func (p *pebbleIterator) ValueProto(msg protoutil.Message) error {
	value := p.UnsafeValue()

	return protoutil.Unmarshal(value, msg)
}

// IsCurIntentSeparated implements the MVCCIterator interface.
func (p *pebbleIterator) IsCurIntentSeparated() bool {
	return false
}

// ComputeStats implements the MVCCIterator interface.
func (p *pebbleIterator) ComputeStats(
	start, end roachpb.Key, nowNanos int64,
) (enginepb.MVCCStats, error) {
	return ComputeStatsForRange(p, start, end, nowNanos)
}

// Go-only version of IsValidSplitKey. Checks if the specified key is in
// NoSplitSpans.
func isValidSplitKey(key roachpb.Key, noSplitSpans []roachpb.Span) bool {
	if key.Equal(keys.Meta2KeyMax) {
		// We do not allow splits at Meta2KeyMax. The reason for this is that range
		// descriptors are stored at RangeMetaKey(range.EndKey), so the new range
		// that ends at Meta2KeyMax would naturally store its decriptor at
		// RangeMetaKey(Meta2KeyMax) = Meta1KeyMax. However, Meta1KeyMax already
		// serves a different role of holding a second copy of the descriptor for
		// the range that spans the meta2/userspace boundary (see case 3a in
		// rangeAddressing). If we allowed splits at Meta2KeyMax, the two roles
		// would overlap. See #1206.
		return false
	}
	for i := range noSplitSpans {
		if noSplitSpans[i].ProperlyContainsKey(key) {
			return false
		}
	}
	return true
}

// IsValidSplitKey returns whether the key is a valid split key. Adapter for
// the method above, for use from other packages.
func IsValidSplitKey(key roachpb.Key) bool {
	return isValidSplitKey(key, keys.NoSplitSpans)
}

// FindSplitKey implements the MVCCIterator interface.
func (p *pebbleIterator) FindSplitKey(
	start, end, minSplitKey roachpb.Key, targetSize int64,
) (MVCCKey, error) {
	return findSplitKeyUsingIterator(p, start, end, minSplitKey, targetSize)
}

func findSplitKeyUsingIterator(
	iter MVCCIterator, start, end, minSplitKey roachpb.Key, targetSize int64,
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
	iter.SeekGE(MakeMVCCMetadataKey(start))
	for ; ; iter.Next() {
		valid, err := iter.Valid()
		if err != nil {
			return MVCCKey{}, err
		}
		if !valid {
			break
		}
		mvccKey := iter.UnsafeKey()

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

		sizeSoFar += int64(len(iter.UnsafeValue()))
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

// SetUpperBound implements the MVCCIterator interface. Note that this is not
// the first time that bounds will be passed to the underlying
// pebble.Iterator. The existing bounds are in p.options.
func (p *pebbleIterator) SetUpperBound(upperBound roachpb.Key) {
	if upperBound == nil {
		panic("SetUpperBound must not use a nil key")
	}
	if p.options.UpperBound != nil {
		// We know that we've appended 0x00 to p.options.UpperBound, which must be
		// ignored for this comparison.
		if bytes.Equal(p.options.UpperBound[:len(p.options.UpperBound)-1], upperBound) {
			// Nothing to do. This noop optimization helps the underlying
			// pebble.Iterator to optimize seeks.
			return
		}
	}
	p.curBuf = (p.curBuf + 1) % 2
	i := p.curBuf
	if p.options.LowerBound != nil {
		p.lowerBoundBuf[i] = append(p.lowerBoundBuf[i][:0], p.options.LowerBound...)
		p.options.LowerBound = p.lowerBoundBuf[i]
	}
	p.upperBoundBuf[i] = append(p.upperBoundBuf[i][:0], upperBound...)
	p.upperBoundBuf[i] = append(p.upperBoundBuf[i], 0x00)
	p.options.UpperBound = p.upperBoundBuf[i]
	p.iter.SetBounds(p.options.LowerBound, p.options.UpperBound)
	if p.testingSetBoundsListener != nil {
		p.testingSetBoundsListener.postSetBounds(p.options.LowerBound, p.options.UpperBound)
	}
}

// Stats implements the {MVCCIterator,EngineIterator} interfaces.
func (p *pebbleIterator) Stats() IteratorStats {
	return IteratorStats{
		TimeBoundNumSSTs: p.timeBoundNumSSTables,
		Stats:            p.iter.Stats(),
	}
}

// SupportsPrev implements the MVCCIterator interface.
func (p *pebbleIterator) SupportsPrev() bool {
	return true
}

// CheckForKeyCollisions indicates if the provided SST data collides with this
// iterator in the specified range.
func (p *pebbleIterator) CheckForKeyCollisions(
	sstData []byte, start, end roachpb.Key,
) (enginepb.MVCCStats, error) {
	return checkForKeyCollisionsGo(p, sstData, start, end)
}

// GetRawIter is part of the EngineIterator interface.
func (p *pebbleIterator) GetRawIter() *pebble.Iterator {
	return p.iter
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
