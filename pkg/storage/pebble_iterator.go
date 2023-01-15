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
	"github.com/cockroachdb/cockroach/pkg/storage/pebbleiter"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
)

// pebbleIterator is a wrapper around a pebble.Iterator that implements the
// MVCCIterator and EngineIterator interfaces. A single pebbleIterator
// should only be used in one of the two modes.
type pebbleIterator struct {
	// Underlying iterator for the DB.
	iter    pebbleiter.Iterator
	options pebble.IterOptions
	// Reusable buffer for MVCCKey or EngineKey encoding.
	keyBuf []byte
	// Buffers for copying iterator options to. Note that the underlying memory
	// is not GCed upon Close(), to reduce the number of overall allocations.
	lowerBoundBuf      []byte
	upperBoundBuf      []byte
	rangeKeyMaskingBuf []byte
	// Filter to use if masking is enabled.
	maskFilter mvccWallTimeIntervalRangeKeyMask

	// Buffer used to store MVCCRangeKeyVersions returned by RangeKeys(). Lazily
	// initialized the first time an iterator's RangeKeys() method is called.
	mvccRangeKeyVersions []MVCCRangeKeyVersion

	// True if the iterator's underlying reader supports range keys.
	//
	// TODO(erikgrinaker): Remove after 22.2.
	supportsRangeKeys bool
	// Set to true to govern whether to call SeekPrefixGE or SeekGE. Skips
	// SSTables based on MVCC/Engine key when true.
	prefix bool
	// If reusable is true, Close() does not actually close the underlying
	// iterator, but simply marks it as not inuse. Used by pebbleReadOnly.
	reusable bool
	inuse    bool
	// Set to true if the underlying Pebble Iterator was created through
	// pebble.NewExternalIter, and so the iterator is iterating over files
	// external to the storage engine. This is used to avoid panicking on
	// corruption errors that should be non-fatal if encountered from external
	// sources of sstables.
	external bool
	// mvccDirIsReverse and mvccDone are used only for the methods implementing
	// MVCCIterator. They are used to prevent the iterator from iterating into
	// the lock table key space.
	//
	// The current direction. false for forward, true for reverse.
	mvccDirIsReverse bool
	// True iff the iterator is exhausted in the current direction. There is
	// no error to report when it is true.
	mvccDone bool
}

var _ MVCCIterator = &pebbleIterator{}
var _ EngineIterator = &pebbleIterator{}

var pebbleIterPool = sync.Pool{
	New: func() interface{} {
		return &pebbleIterator{}
	},
}

// newPebbleIterator creates a new Pebble iterator for the given Pebble reader.
func newPebbleIterator(
	handle pebble.Reader, opts IterOptions, durability DurabilityRequirement, supportsRangeKeys bool,
) *pebbleIterator {
	p := pebbleIterPool.Get().(*pebbleIterator)
	p.reusable = false // defensive
	p.init(nil, opts, durability, supportsRangeKeys)
	p.iter = pebbleiter.MaybeWrap(handle.NewIter(&p.options))
	return p
}

// newPebbleIteratorByCloning creates a new Pebble iterator by cloning the given
// iterator and reconfiguring it.
func newPebbleIteratorByCloning(
	iter pebbleiter.Iterator,
	opts IterOptions,
	durability DurabilityRequirement,
	supportsRangeKeys bool,
) *pebbleIterator {
	var err error
	p := pebbleIterPool.Get().(*pebbleIterator)
	p.reusable = false // defensive
	p.init(nil, opts, durability, supportsRangeKeys)
	p.iter, err = iter.Clone(pebble.CloneOptions{
		IterOptions:      &p.options,
		RefreshBatchView: true,
	})
	if err != nil {
		p.Close()
		panic(err)
	}
	return p
}

// newPebbleSSTIterator creates a new Pebble iterator for the given SSTs.
func newPebbleSSTIterator(
	files [][]sstable.ReadableFile, opts IterOptions, forwardOnly bool,
) (*pebbleIterator, error) {
	p := pebbleIterPool.Get().(*pebbleIterator)
	p.reusable = false // defensive
	p.init(nil, opts, StandardDurability, true /* supportsRangeKeys */)

	var externalIterOpts []pebble.ExternalIterOption
	if forwardOnly {
		externalIterOpts = append(externalIterOpts, pebble.ExternalIterForwardOnly{})
	}

	iter, err := pebble.NewExternalIter(DefaultPebbleOptions(), &p.options, files, externalIterOpts...)
	if err != nil {
		p.Close()
		return nil, err
	}
	p.iter = pebbleiter.MaybeWrap(iter)
	p.external = true
	return p, nil
}

// init resets this pebbleIterator for use with the specified arguments,
// reconfiguring the given iter. It is valid to pass a nil iter and then create
// p.iter using p.options, to avoid redundant reconfiguration via SetOptions().
func (p *pebbleIterator) init(
	iter pebbleiter.Iterator,
	opts IterOptions,
	durability DurabilityRequirement,
	supportsRangeKeys bool,
) {
	*p = pebbleIterator{
		iter:               iter,
		keyBuf:             p.keyBuf,
		lowerBoundBuf:      p.lowerBoundBuf,
		upperBoundBuf:      p.upperBoundBuf,
		rangeKeyMaskingBuf: p.rangeKeyMaskingBuf,
		reusable:           p.reusable,
		supportsRangeKeys:  supportsRangeKeys,
	}
	p.setOptions(opts, durability)
	p.inuse = true // after setOptions(), so panic won't cause reader to panic too
}

// initReuseOrCreate is a convenience method that (re-)initializes an existing
// pebbleIterator in one out of three ways:
//
// 1. iter != nil && !clone: use and reconfigure the given raw Pebble iterator.
// 2. iter != nil && clone: clone and reconfigure the given raw Pebble iterator.
// 3. iter == nil: create a new iterator from handle.
func (p *pebbleIterator) initReuseOrCreate(
	handle pebble.Reader,
	iter pebbleiter.Iterator,
	clone bool,
	opts IterOptions,
	durability DurabilityRequirement,
	supportsRangeKeys bool, // TODO(erikgrinaker): remove after 22.2
) {
	if iter != nil && !clone {
		p.init(iter, opts, durability, supportsRangeKeys)
		return
	}

	p.init(nil, opts, durability, supportsRangeKeys)
	if iter == nil {
		p.iter = pebbleiter.MaybeWrap(handle.NewIter(&p.options))
	} else if clone {
		var err error
		p.iter, err = iter.Clone(pebble.CloneOptions{
			IterOptions:      &p.options,
			RefreshBatchView: true,
		})
		if err != nil {
			p.Close()
			panic(err)
		}
	}
}

// setOptions updates the options for a pebbleIterator. If p.iter is non-nil, it
// updates the options on the existing iterator too.
func (p *pebbleIterator) setOptions(opts IterOptions, durability DurabilityRequirement) {
	if !opts.Prefix && len(opts.UpperBound) == 0 && len(opts.LowerBound) == 0 {
		panic("iterator must set prefix or upper bound or lower bound")
	}
	if opts.MinTimestampHint.IsSet() && opts.MaxTimestampHint.IsEmpty() {
		panic("min timestamp hint set without max timestamp hint")
	}
	if opts.Prefix && opts.RangeKeyMaskingBelow.IsSet() {
		panic("can't use range key masking with prefix iterators") // very high overhead
	}

	// If this Pebble database does not support range keys yet, fall back to
	// only iterating over point keys to avoid panics. This is effectively the
	// same, since a database without range key support contains no range keys,
	// except in the case of RangesOnly where the iterator must always be empty.
	if !p.supportsRangeKeys {
		if opts.KeyTypes == IterKeyTypeRangesOnly {
			opts.LowerBound = nil
			opts.UpperBound = []byte{0}
		}
		opts.KeyTypes = IterKeyTypePointsOnly
		opts.RangeKeyMaskingBelow = hlc.Timestamp{}
	}

	// Generate new Pebble iterator options.
	p.options = pebble.IterOptions{
		OnlyReadGuaranteedDurable: durability == GuaranteedDurability,
		KeyTypes:                  opts.KeyTypes,
		UseL6Filters:              opts.useL6Filters,
	}
	p.prefix = opts.Prefix

	if opts.LowerBound != nil {
		// This is the same as
		// p.options.LowerBound = EncodeKeyToBuf(p.lowerBoundBuf[0][:0], MVCCKey{Key: opts.LowerBound})
		// or EngineKey{Key: opts.LowerBound}.EncodeToBuf(...).
		// Since we are encoding keys with an empty version anyway, we can just
		// append the NUL byte instead of calling the above encode functions which
		// will do the same thing.
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
	if opts.RangeKeyMaskingBelow.IsSet() {
		p.rangeKeyMaskingBuf = encodeMVCCTimestampSuffixToBuf(
			p.rangeKeyMaskingBuf, opts.RangeKeyMaskingBelow)
		p.options.RangeKeyMasking.Suffix = p.rangeKeyMaskingBuf
		p.maskFilter.BlockIntervalFilter.Init(mvccWallTimeIntervalCollector, 0, math.MaxUint64)
		p.options.RangeKeyMasking.Filter = p.getBlockPropertyFilterMask
	}

	if opts.MaxTimestampHint.IsSet() {
		// TODO(erikgrinaker): For compatibility with SSTables written by 21.2 nodes
		// or earlier, we filter on table properties too. We still wrote these
		// properties in 22.1, but stop doing so in 22.2. We can remove this
		// filtering when nodes are guaranteed to no longer have SSTables written by
		// 21.2 or earlier (which can still happen e.g. when clusters are upgraded
		// through multiple major versions in rapid succession).
		encodedMinTS := string(encodeMVCCTimestamp(opts.MinTimestampHint))
		encodedMaxTS := string(encodeMVCCTimestamp(opts.MaxTimestampHint))
		p.options.TableFilter = func(userProps map[string]string) bool {
			tableMinTS := userProps["crdb.ts.min"]
			if len(tableMinTS) == 0 {
				return true
			}
			tableMaxTS := userProps["crdb.ts.max"]
			if len(tableMaxTS) == 0 {
				return true
			}
			return encodedMaxTS >= tableMinTS && encodedMinTS <= tableMaxTS
		}
		// We are given an inclusive [MinTimestampHint, MaxTimestampHint]. The
		// MVCCWAllTimeIntervalCollector has collected the WallTimes and we need
		// [min, max), i.e., exclusive on the upper bound.
		p.options.PointKeyFilters = []pebble.BlockPropertyFilter{
			sstable.NewBlockIntervalFilter(mvccWallTimeIntervalCollector,
				uint64(opts.MinTimestampHint.WallTime),
				uint64(opts.MaxTimestampHint.WallTime)+1),
		}
		// NB: We disable range key block filtering because of complications in
		// MVCCIncrementalIterator.maybeSkipKeys: the TBI may see different range
		// key fragmentation than the main iterator due to the filtering. This would
		// necessitate additional seeks/processing that likely negate the marginal
		// benefit of the range key filters. See:
		// https://github.com/cockroachdb/cockroach/issues/86260.
		//
		// However, we do collect block properties for range keys, in case we enable
		// this later.
		p.options.RangeKeyFilters = nil
	}

	// Set the new iterator options. We unconditionally do so, since Pebble will
	// optimize noop changes as needed, and it may affect batch write visibility.
	if p.iter != nil {
		p.iter.SetOptions(&p.options)
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
	p.keyBuf = EncodeMVCCKeyToBuf(p.keyBuf[:0], key)
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
	if !p.iter.Valid() {
		return false, p.iter.Error()
	}

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

	if util.RaceEnabled {
		if err := p.assertMVCCInvariants(); err != nil {
			return false, err
		}
	}
	return true, nil
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

	// NB: If p.prefix, iterators can't move onto a separate key by definition,
	// so the below call to NextPrefix will exhaust the iterator.
	p.iter.NextPrefix()
}

// UnsafeKey implements the MVCCIterator interface.
func (p *pebbleIterator) UnsafeKey() MVCCKey {
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
func (p *pebbleIterator) UnsafeValue() ([]byte, error) {
	if ok := p.iter.Valid(); !ok {
		return nil, nil
	}
	return p.iter.ValueAndErr()
}

// UnsafeLazyValue implements the MVCCIterator interface.
func (p *pebbleIterator) UnsafeLazyValue() pebble.LazyValue {
	if ok := p.iter.Valid(); !ok {
		panic(errors.AssertionFailedf("UnsafeLazyValue called on !Valid iterator"))
	}
	return p.iter.LazyValue()
}

// MVCCValueLenAndIsTombstone implements the MVCCIterator interface.
func (p *pebbleIterator) MVCCValueLenAndIsTombstone() (int, bool, error) {
	lv := p.iter.LazyValue()
	attr, ok := lv.TryGetShortAttribute()
	var isTombstone bool
	var valLen int
	if ok {
		isTombstone = attr != 0
		valLen = lv.Len()
	} else {
		// Must be an in-place value, since it did not have a short attribute.
		val := lv.InPlaceValue()
		var err error
		isTombstone, err = EncodedMVCCValueIsTombstone(val)
		if err != nil {
			return 0, false, err
		}
		valLen = len(val)
	}
	return valLen, isTombstone, nil
}

// ValueLen implements the MVCCIterator interface.
func (p *pebbleIterator) ValueLen() int {
	lv := p.iter.LazyValue()
	return lv.Len()
}

// SeekLT implements the MVCCIterator interface.
func (p *pebbleIterator) SeekLT(key MVCCKey) {
	p.mvccDirIsReverse = true
	p.mvccDone = false
	p.keyBuf = EncodeMVCCKeyToBuf(p.keyBuf[:0], key)
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
func (p *pebbleIterator) Value() ([]byte, error) {
	value, err := p.UnsafeValue()
	if err != nil {
		return nil, err
	}
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	return valueCopy, nil
}

// ValueProto implements the MVCCIterator interface.
func (p *pebbleIterator) ValueProto(msg protoutil.Message) error {
	value, err := p.UnsafeValue()
	if err != nil {
		return err
	}
	return protoutil.Unmarshal(value, msg)
}

// HasPointAndRange implements the MVCCIterator interface.
func (p *pebbleIterator) HasPointAndRange() (bool, bool) {
	return p.iter.HasPointAndRange()
}

// RangeBounds implements the MVCCIterator interface.
func (p *pebbleIterator) RangeBounds() roachpb.Span {
	start, end := p.iter.RangeBounds()

	// Avoid decoding empty keys: DecodeMVCCKey() will return errors for these,
	// which are expensive to construct.
	if len(start) == 0 && len(end) == 0 {
		return roachpb.Span{}
	}

	// TODO(erikgrinaker): We should surface these errors somehow, but for now we
	// follow UnsafeKey()'s example and silently return empty bounds.
	startKey, err := DecodeMVCCKey(start)
	if err != nil {
		return roachpb.Span{}
	}
	endKey, err := DecodeMVCCKey(end)
	if err != nil {
		return roachpb.Span{}
	}

	return roachpb.Span{Key: startKey.Key, EndKey: endKey.Key}
}

// EngineRangeBounds implements the EngineIterator interface.
func (p *pebbleIterator) EngineRangeBounds() (roachpb.Span, error) {
	start, end := p.iter.RangeBounds()
	if len(start) == 0 && len(end) == 0 {
		return roachpb.Span{}, nil
	}

	s, ok := DecodeEngineKey(start)
	if !ok || len(s.Version) > 0 {
		return roachpb.Span{}, errors.Errorf("invalid encoded engine key: %x", start)
	}
	e, ok := DecodeEngineKey(end)
	if !ok || len(e.Version) > 0 {
		return roachpb.Span{}, errors.Errorf("invalid encoded engine key: %x", end)
	}
	return roachpb.Span{Key: s.Key, EndKey: e.Key}, nil
}

// RangeKeys implements the MVCCIterator interface.
func (p *pebbleIterator) RangeKeys() MVCCRangeKeyStack {
	rangeKeys := p.iter.RangeKeys()
	stack := MVCCRangeKeyStack{
		Bounds:   p.RangeBounds(),
		Versions: p.mvccRangeKeyVersions[:0],
	}
	if cap(stack.Versions) < len(rangeKeys) {
		stack.Versions = make(MVCCRangeKeyVersions, 0, len(rangeKeys))
		p.mvccRangeKeyVersions = stack.Versions
	}

	for _, rangeKey := range rangeKeys {
		timestamp, err := DecodeMVCCTimestampSuffix(rangeKey.Suffix)
		if err != nil {
			// TODO(erikgrinaker): We should surface this error somehow, but for now
			// we follow UnsafeKey()'s example and silently skip them.
			continue
		}
		stack.Versions = append(stack.Versions, MVCCRangeKeyVersion{
			Timestamp: timestamp,
			Value:     rangeKey.Value,
		})
	}
	return stack
}

// RangeKeyChanged implements the MVCCIterator interface.
func (p *pebbleIterator) RangeKeyChanged() bool {
	return p.iter.RangeKeyChanged()
}

// EngineRangeKeys implements the EngineIterator interface.
func (p *pebbleIterator) EngineRangeKeys() []EngineRangeKeyValue {
	rangeKeys := p.iter.RangeKeys()
	rkvs := make([]EngineRangeKeyValue, 0, len(rangeKeys))
	for _, rk := range rangeKeys {
		rkvs = append(rkvs, EngineRangeKeyValue{Version: rk.Suffix, Value: rk.Value})
	}
	return rkvs
}

// Go-only version of IsValidSplitKey. Checks if the specified key is in
// NoSplitSpans.
func isValidSplitKey(key roachpb.Key, noSplitSpans []roachpb.Span) bool {
	if key.Equal(keys.Meta2KeyMax) {
		// We do not allow splits at Meta2KeyMax. The reason for this is that range
		// descriptors are stored at RangeMetaKey(range.EndKey), so the new range
		// that ends at Meta2KeyMax would naturally store its descriptor at
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
	var noSplitSpans []roachpb.Span
	for i := range keys.NoSplitSpans {
		if minSplitKey.Compare(keys.NoSplitSpans[i].EndKey) <= 0 {
			noSplitSpans = keys.NoSplitSpans[i:]
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

		sizeSoFar += int64(iter.ValueLen())
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

// Stats implements the {MVCCIterator,EngineIterator} interfaces.
func (p *pebbleIterator) Stats() IteratorStats {
	return IteratorStats{
		Stats: p.iter.Stats(),
	}
}

// IsPrefix implements the MVCCIterator interface.
func (p *pebbleIterator) IsPrefix() bool {
	return p.prefix
}

// GetRawIter is part of the EngineIterator interface.
func (p *pebbleIterator) GetRawIter() pebbleiter.Iterator {
	return p.iter
}

func (p *pebbleIterator) getBlockPropertyFilterMask() pebble.BlockPropertyFilterMask {
	return &p.maskFilter
}

func (p *pebbleIterator) destroy() {
	if p.inuse {
		panic("iterator still in use")
	}
	if p.iter != nil {
		// If an error is encountered during iteration, it'll already have been
		// surfaced by p.iter.Error() through Valid()'s error return value.
		// Closing a pebble iterator that's in an error state surfaces that same
		// error again. The client should've already handled the error when
		// surfaced through Valid(), but wants to close the iterator (eg,
		// potentially through a defer) and so we don't want to re-surface the
		// error.
		//
		// TODO(jackson): In addition to errors accumulated during iteration, Close
		// also returns errors encountered during the act of closing the iterator.
		// Currently, most of these errors are swallowed. The error returned by
		// iter.Close() may be an ephemeral error, or it may a misuse of the
		// Iterator or corruption. Only swallow ephemeral errors (eg,
		// DeadlineExceeded, etc), panic-ing on Close errors that are not known to
		// be ephemeral/retriable. While these ephemeral error types are enumerated,
		// we panic on the error types we know to be NOT ephemeral.
		//
		// See cockroachdb/pebble#1811.
		//
		// NB: The panic is omitted if the error is encountered on an external
		// iterator which is iterating over uncommitted sstables.
		if err := p.iter.Close(); !p.external && errors.Is(err, pebble.ErrCorruption) {
			panic(err)
		}
		p.iter = nil
	}
	// Reset all fields except for the key and option buffers. Holding onto their
	// underlying memory is more efficient to prevent extra allocations down the
	// line.
	*p = pebbleIterator{
		keyBuf:             p.keyBuf,
		lowerBoundBuf:      p.lowerBoundBuf,
		upperBoundBuf:      p.upperBoundBuf,
		rangeKeyMaskingBuf: p.rangeKeyMaskingBuf,
		reusable:           p.reusable,
	}
}

// assertMVCCInvariants asserts internal MVCC iterator invariants, returning an
// AssertionFailedf on any failures. It must be called on a valid iterator after
// a complete state transition.
func (p *pebbleIterator) assertMVCCInvariants() error {
	// Assert general MVCCIterator API invariants.
	if err := assertMVCCIteratorInvariants(p); err != nil {
		return err
	}

	// The underlying iterator must be valid, with !mvccDone.
	if !p.iter.Valid() {
		errMsg := p.iter.Error().Error()
		return errors.AssertionFailedf("underlying iter is invalid, with err=%s", errMsg)
	}
	if p.mvccDone {
		return errors.AssertionFailedf("valid iter with mvccDone set")
	}

	// The position must match the underlying iter.
	if key, iterKey := p.UnsafeKey(), p.iter.Key(); !bytes.Equal(EncodeMVCCKey(key), iterKey) {
		return errors.AssertionFailedf("UnsafeKey %s does not match iterator key %x", key, iterKey)
	}

	// The iterator must be marked as in use.
	if !p.inuse {
		return errors.AssertionFailedf("valid iter with inuse=false")
	}

	// Prefix must be exposed.
	if p.prefix != p.IsPrefix() {
		return errors.AssertionFailedf("IsPrefix() does not match prefix=%v", p.prefix)
	}

	// Ensure !supportsRangeKeys never exposes range keys.
	if !p.supportsRangeKeys {
		if _, hasRange := p.HasPointAndRange(); hasRange {
			return errors.AssertionFailedf("hasRange=true but supportsRangeKeys=false")
		}
	}

	return nil
}
