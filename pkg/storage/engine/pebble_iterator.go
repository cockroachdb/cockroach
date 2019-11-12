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
	"math"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
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
	return p.iter.Valid(), p.iter.Error()
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

	for p.iter.Next() {
		if !bytes.Equal(p.keyBuf, p.UnsafeKey().Key) {
			break
		}
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
	var bestSplitKey MVCCKey
	var prevKey []byte

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
		if diff > bestDiff && bestSplitKey.Key != nil {
			break
		}

		if diff < bestDiff &&
			(mvccMinSplitKey.Key == nil || !mvccKey.Less(mvccMinSplitKey)) &&
			(len(noSplitSpans) == 0 || isValidSplitKey(mvccKey.Key, noSplitSpans)) {
			// We are going to have to copy bestSplitKey, since by the time we find
			// out it's the actual best split key, the underlying slice would have
			// changed (due to the iter.Next() call).
			//
			// TODO(itsbilal): Instead of copying into bestSplitKey each time,
			// consider just calling iter.Prev() at the end to get to the same
			// key. This isn't quite as easy as it sounds due to the mvccKey and
			// noSplitSpans checks.
			bestDiff = diff
			bestSplitKey.Key = append(bestSplitKey.Key[:0], mvccKey.Key...)
			bestSplitKey.Timestamp = mvccKey.Timestamp
			// Avoid comparing against minSplitKey on future iterations.
			mvccMinSplitKey.Key = nil
		}

		sizeSoFar += int64(len(p.iter.Value()))
		if mvccKey.IsValue() && bytes.Equal(prevKey, mvccKey.Key) {
			// We only advanced timestamps, but not new mvcc keys.
			sizeSoFar += timestampLen
		} else {
			sizeSoFar += int64(len(mvccKey.Key) + 1)
			if mvccKey.IsValue() {
				sizeSoFar += timestampLen
			}
		}

		prevKey = append(prevKey[:0], mvccKey.Key...)
	}

	return bestSplitKey, nil
}

// MVCCGet implements the Iterator interface.
func (p *pebbleIterator) MVCCGet(
	key roachpb.Key, timestamp hlc.Timestamp, opts MVCCGetOptions,
) (value *roachpb.Value, intent *roachpb.Intent, err error) {
	if opts.Inconsistent && opts.Txn != nil {
		return nil, nil, errors.Errorf("cannot allow inconsistent reads within a transaction")
	}
	if len(key) == 0 {
		return nil, nil, emptyKeyError()
	}
	if p.iter == nil {
		panic("uninitialized iterator")
	}

	mvccScanner := pebbleMVCCScannerPool.Get().(*pebbleMVCCScanner)
	defer pebbleMVCCScannerPool.Put(mvccScanner)

	// MVCCGet is implemented as an MVCCScan where we retrieve a single key. We
	// specify an empty key for the end key which will ensure we don't retrieve a
	// key different than the start key. This is a bit of a hack.
	*mvccScanner = pebbleMVCCScanner{
		parent:       p.iter,
		start:        key,
		ts:           timestamp,
		maxKeys:      1,
		inconsistent: opts.Inconsistent,
		tombstones:   opts.Tombstones,
	}

	mvccScanner.init(opts.Txn)
	mvccScanner.get()

	if mvccScanner.err != nil {
		return nil, nil, mvccScanner.err
	}
	intents, err := buildScanIntents(mvccScanner.intents.Repr())
	if err != nil {
		return nil, nil, err
	}
	if !opts.Inconsistent && len(intents) > 0 {
		return nil, nil, &roachpb.WriteIntentError{Intents: intents}
	}

	if len(intents) > 1 {
		return nil, nil, errors.Errorf("expected 0 or 1 intents, got %d", len(intents))
	} else if len(intents) == 1 {
		intent = &intents[0]
	}

	if len(mvccScanner.results.repr) == 0 {
		return nil, intent, nil
	}

	mvccKey, rawValue, _, err := MVCCScanDecodeKeyValue(mvccScanner.results.repr)
	if err != nil {
		return nil, nil, err
	}

	value = &roachpb.Value{
		RawBytes:  rawValue,
		Timestamp: mvccKey.Timestamp,
	}
	return
}

// MVCCScan implements the Iterator interface.
func (p *pebbleIterator) MVCCScan(
	start, end roachpb.Key, max int64, timestamp hlc.Timestamp, opts MVCCScanOptions,
) (kvData [][]byte, numKVs int64, resumeSpan *roachpb.Span, intents []roachpb.Intent, err error) {
	if opts.Inconsistent && opts.Txn != nil {
		return nil, 0, nil, nil, errors.Errorf("cannot allow inconsistent reads within a transaction")
	}
	if len(end) == 0 {
		return nil, 0, nil, nil, emptyKeyError()
	}
	if max == 0 {
		resumeSpan = &roachpb.Span{Key: start, EndKey: end}
		return nil, 0, resumeSpan, nil, nil
	}
	if p.iter == nil {
		panic("uninitialized iterator")
	}

	mvccScanner := pebbleMVCCScannerPool.Get().(*pebbleMVCCScanner)
	defer pebbleMVCCScannerPool.Put(mvccScanner)

	*mvccScanner = pebbleMVCCScanner{
		parent:       p.iter,
		reverse:      opts.Reverse,
		start:        start,
		end:          end,
		ts:           timestamp,
		maxKeys:      max,
		inconsistent: opts.Inconsistent,
		tombstones:   opts.Tombstones,
	}

	mvccScanner.init(opts.Txn)
	resumeSpan, err = mvccScanner.scan()

	if err != nil {
		return nil, 0, nil, nil, err
	}

	kvData = mvccScanner.results.finish()
	numKVs = mvccScanner.results.count

	intents, err = buildScanIntents(mvccScanner.intents.Repr())
	if err != nil {
		return nil, 0, nil, nil, err
	}

	if !opts.Inconsistent && len(intents) > 0 {
		return nil, 0, resumeSpan, nil, &roachpb.WriteIntentError{Intents: intents}
	}
	return
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
	// Reset all fields except for the lower/upper bound buffers. Holding onto
	// their underlying memory is more efficient to prevent extra allocations
	// down the line.
	*p = pebbleIterator{
		lowerBoundBuf: p.lowerBoundBuf,
		upperBoundBuf: p.upperBoundBuf,
		reusable:      p.reusable,
	}
}
