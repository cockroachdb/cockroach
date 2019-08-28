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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/pebble"
	"golang.org/x/tools/container/intsets"
)

// pebbleIterator is a wrapper around a pebble.Iterator that implements the
// Iterator interface.
type pebbleIterator struct {
	// DB handle.
	parent        *Pebble
	// Underlying iterator for the DB.
	iter          *pebble.Iterator
	options       pebble.IterOptions
	// Reusable buffer for MVCC key encoding.
	keyBuf        []byte
	// Buffers for copying iterator bounds to. Note that the underlying memory
	// is not GCed upon Close(), to reduce the number of overall allocations.
	lowerBoundBuf []byte
	upperBoundBuf []byte
	// Set to true to govern whether to call SeekPrefixGE or SeekGE. Skips
	// SSTables based on MVCC key when true.
	prefix        bool
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
	}

	if !opts.Prefix && len(opts.UpperBound) == 0 && len(opts.LowerBound) == 0 {
		panic("iterator must set prefix or upper bound or lower bound")
	}

	if opts.LowerBound != nil {
		// This is the same as
		// p.options.LowerBound = EncodeKeyToBuf(p.lowerBoundBuf[:0], MVCCKey{Key: opts.LowerBound}) .
		// Since we are encoding zero-timestamp MVCC Keys anyway, we can just append
		// the null byte instead of calling EncodeKey which will do the same thing.
		p.lowerBoundBuf = append(p.lowerBoundBuf[:0], opts.LowerBound...)
		p.options.LowerBound = append(p.lowerBoundBuf, 0x00)
	}
	if opts.UpperBound != nil {
		// Same as above.
		p.upperBoundBuf = append(p.upperBoundBuf[:0], opts.UpperBound...)
		p.options.UpperBound = append(p.upperBoundBuf, 0x00)
	}

	p.iter = handle.NewIter(&p.options)
	if p.iter == nil {
		panic("unable to create iterator")
	}
}

// Close implements the Iterator interface.
func (p *pebbleIterator) Close() {
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
	}

	pebbleIterPool.Put(p)
}

// Seek implements the Iterator interface.
func (p *pebbleIterator) Seek(key MVCCKey) {
	p.keyBuf = EncodeKeyToBuf(p.keyBuf[:0], key)
	if (p.prefix) {
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

// UnsafeValue implements the Iterator interface.
func (p *pebbleIterator) UnsafeValue() []byte {
	if valid, err := p.Valid(); err != nil || !valid {
		return nil
	}
	return p.iter.Value()
}

// SeekReverse implements the Iterator interface.
func (p *pebbleIterator) SeekReverse(key MVCCKey) {
	// Do a SeekGE, not a SeekLT. This is because SeekReverse seeks to the
	// greatest key that's less than or equal to the specified key.
	p.Seek(key)
	p.keyBuf = EncodeKeyToBuf(p.keyBuf[:0], key)

	// The new key could either be greater or equal to the supplied key.
	// Backtrack one step if it is greater.
	comp := MVCCKeyCompare(p.keyBuf, p.iter.Key())
	if comp < 0 && p.iter.Valid() {
		p.Prev()
	}
}

// Prev implements the Iterator interface.
func (p *pebbleIterator) Prev() {
	p.iter.Prev()
}

// PrevKey implements the Iterator interface.
func (p *pebbleIterator) PrevKey() {
	if valid, err := p.Valid(); err != nil || !valid {
		return
	}
	curKey := p.Key()
	for p.iter.Prev() {
		if !bytes.Equal(curKey.Key, p.UnsafeKey().Key) {
			break
		}
	}
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

	return msg.Unmarshal(value)
}

// ComputeStats implements the Iterator interface.
func (p *pebbleIterator) ComputeStats(start, end MVCCKey, nowNanos int64) (enginepb.MVCCStats, error) {
	return ComputeStatsGo(p, start, end, nowNanos)
}

// Go-only version of IsValidSplitKey. Checks if the specified key is in
// NoSplitSpans.
func isValidSplitKey(key roachpb.Key) bool {
	for _, noSplitSpan := range keys.NoSplitSpans {
		if noSplitSpan.ContainsKey(key) {
			return false
		}
	}
	return true
}

// FindSplitKey implements the Iterator interface.
func (p *pebbleIterator) FindSplitKey(start, end, minSplitKey MVCCKey, targetSize int64) (MVCCKey, error) {
	const (
		timestampLen = int64(12)
	)
	p.Seek(start)
	p.keyBuf = EncodeKeyToBuf(p.keyBuf[:0], end)
	minSplitKeyBuf := EncodeKey(minSplitKey)
	prevKey := make([]byte, 0)

	sizeSoFar := int64(0)
	bestDiff := int64(intsets.MaxInt)
	bestSplitKey := MVCCKey{}

	for ;p.iter.Valid() && MVCCKeyCompare(p.iter.Key(), p.keyBuf) < 0; p.iter.Next() {
		mvccKey, err := DecodeMVCCKey(p.iter.Key())
		if err != nil {
			return MVCCKey{}, err
		}

		diff := targetSize - sizeSoFar
		if diff < 0 {
			diff = -diff
		}
		if diff < bestDiff && MVCCKeyCompare(p.iter.Key(), minSplitKeyBuf) >= 0 && isValidSplitKey(mvccKey.Key) {
			// We are going to have to copy bestSplitKey, since by the time we find
			// out it's the actual best split key, the underlying slice would have
			// changed (due to the iter.Next() call).
			bestDiff = diff
			bestSplitKey.Key = append(bestSplitKey.Key[:0], mvccKey.Key...)
			bestSplitKey.Timestamp = mvccKey.Timestamp
		}
		if diff > bestDiff && bestSplitKey.Key != nil {
			break
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
	// TODO(itsbilal): Implement in a separate PR. See #39674.
	panic("unimplemented for now, see #39674")
}

// MVCCScan implements the Iterator interface.
func (p *pebbleIterator) MVCCScan(
	start, end roachpb.Key, max int64, timestamp hlc.Timestamp, opts MVCCScanOptions,
) (kvData []byte, numKVs int64, resumeSpan *roachpb.Span, intents []roachpb.Intent, err error) {
	// TODO(itsbilal): Implement in a separate PR. See #39674.
	panic("unimplemented for now, see #39674")
}

// SetUpperBound implements the Iterator interface.
func (p *pebbleIterator) SetUpperBound(upperBound roachpb.Key) {
	p.upperBoundBuf = append(p.upperBoundBuf[:0], upperBound...)
	p.options.UpperBound = append(p.upperBoundBuf, 0x00)
	p.iter.SetBounds(p.options.LowerBound, p.options.UpperBound)
}

// Stats implements the Iterator interface.
func (p *pebbleIterator) Stats() IteratorStats {
	// TODO(itsbilal): Implement this.
	panic("implement me")
}
