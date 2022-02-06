// Copyright 2022 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// MVCCRangeKeyIterOptions are options for an MVCCRangeKeyIterator.
type MVCCRangeKeyIterOptions struct {
	// LowerBound sets the inclusive lower bound of the iterator. Range keys that
	// straddle the bound will have their start key truncated to it.
	//
	// NB: It may be tempting to use an MVCCKey here and include a timestamp, but
	// this would be pointless: giving e.g. a@4 would skip a range key starting at
	// a@5, but the range key would logically exist at the adjacent a.Next()@5 so
	// it would be emitted almost immediately anyway.
	LowerBound roachpb.Key
	// UpperBound sets the exclusive upper bound of the iterator. Range keys that
	// straddle the upper bound will have their end key truncated to it.
	UpperBound roachpb.Key
	// MinTimestamp sets the inclusive lower timestamp bound for the iterator.
	MinTimestamp hlc.Timestamp
	// MaxTimestamp sets the inclusive upper timestamp bound for the iterator.
	MaxTimestamp hlc.Timestamp
	// Fragmented disables defragmentation, emitting non-deterministic fragments
	// like SimpleMVCCIterator does. When enabled, this results in an iteration
	// order of StartKey,Timestamp rather than EndKey,Timestamp.
	Fragmented bool
}

// MVCCRangeKeyIterator is an iterator over range keys in an engine. Unlike
// SimpleMVCCIterator, range keys are defragmented into contiguous deterministic
// range keys. It does not support seeking or backtracking, see
// MVCCRangeKeyIterOptions for lower/upper bounds and other options.
//
// Iteration is in EndKey,Timestamp order rather than StartKey,Timestamp. For
// example: [c-e)@2, [a-z)@3, [x-z)@1. This is a memory optimization when
// defragmenting, which allows emitting completed range keys as soon as
// possible, only buffering incomplete ones in memory. To emit in
// StartKey,Timestamp order, we would additionally need to buffer all complete
// range keys that start after the current incomplete ones -- in the worst case,
// a range key across the entire key span would require all other range keys to
// be buffered in memory. But see the Fragmented option to emit
// non-deterministic range key fragments in StartKey,Timestamp order.
type MVCCRangeKeyIterator struct {
	iter        MVCCIterator
	opts        MVCCRangeKeyIterOptions
	incomplete  []*MVCCRangeKeyValue // defragmentation buffer
	complete    []MVCCRangeKeyValue  // queued for emission
	completeIdx int                  // current Key()
	err         error
}

// NewMVCCRangeKeyIterator sets up a new MVCCRangeKeyIterator and seeks to the
// first range key. The caller must call Close() when done.
func NewMVCCRangeKeyIterator(r Reader, opts MVCCRangeKeyIterOptions) *MVCCRangeKeyIterator {
	iter := &MVCCRangeKeyIterator{
		iter: r.NewMVCCIterator(MVCCKeyIterKind, IterOptions{
			KeyTypes:   IterKeyTypeRangesOnly,
			LowerBound: opts.LowerBound,
			UpperBound: opts.UpperBound,
			// TODO(erikgrinaker): We do not set Min/MaxTimestampHint here, because
			// both are required and it's apparently not always safe to use.
		}),
		opts:       opts,
		incomplete: make([]*MVCCRangeKeyValue, 0),
		complete:   make([]MVCCRangeKeyValue, 0),
	}

	// Seek the iterator to the lower bound and iterate until we've collected
	// the first complete range key (if any).
	iter.iter.SeekGE(MVCCKey{Key: opts.LowerBound})
	iter.findCompleteRangeKeys()

	return iter
}

// findCompleteRangeKeys defragments range keys at the current iterator position
// and any subsequent iterator positions until it completes one or more range
// keys, populating p.complete. Current p.complete is discarded.
func (p *MVCCRangeKeyIterator) findCompleteRangeKeys() {
	p.complete = p.complete[:0]
	p.completeIdx = 0
	p.updateRangeKeys()

	for len(p.complete) == 0 {
		if ok, err := p.iter.Valid(); err != nil {
			p.err = err
			return
		} else if !ok {
			break
		}
		p.iter.Next()
		// NB: We update range keys even if Next() invalidates the iterator, because
		// there may be incomplete range keys that become complete when the iterator
		// is exhausted.
		p.updateRangeKeys()
	}
}

// updateRangeKeys inspects the range keys at the current Pebble iterator
// position, defragments them in p.incomplete, and moves any completed
// range keys into p.complete.
func (p *MVCCRangeKeyIterator) updateRangeKeys() {
	var startKey, endKey roachpb.Key
	var rangeKeys []MVCCRangeKeyValue

	// If the iterator is exhausted, we still want to complete any remaining
	// incomplete range keys.
	if ok, err := p.iter.Valid(); err != nil {
		p.err = err
		return
	} else if ok {
		startKey, endKey = p.iter.RangeBounds()
		rangeKeys = p.iter.RangeKeys()
	}

	// Both rangeKeys and p.incomplete are sorted in descending timestamp order,
	// so we iterate over them in lockstep and insert/update/delete p.incomplete
	// as appropriate.
	var tsIdx, rkIdx int

	for rkIdx < len(rangeKeys) {
		rangeKey := rangeKeys[rkIdx]

		// Filter rangekeys by timestamp.
		//
		// TODO(erikgrinaker): This can be optimized to skip unnecessary comparisons
		// since rangeKeys is sorted by timestamp. Maybe later.
		if !p.opts.MinTimestamp.IsEmpty() && rangeKey.Key.Timestamp.Less(p.opts.MinTimestamp) {
			rkIdx++
			continue
		}
		if !p.opts.MaxTimestamp.IsEmpty() && p.opts.MaxTimestamp.Less(rangeKey.Key.Timestamp) {
			rkIdx++
			continue
		}

		// If we're at the end of p.incomplete, this range key must be new.
		if tsIdx >= len(p.incomplete) {
			p.incomplete = append(p.incomplete, &MVCCRangeKeyValue{
				Key: MVCCRangeKey{
					StartKey:  append(make([]byte, 0, len(startKey)), startKey...),
					EndKey:    append(make([]byte, 0, len(endKey)), endKey...),
					Timestamp: rangeKey.Key.Timestamp,
				},
				Value: append(make([]byte, 0, len(rangeKey.Value)), rangeKey.Value...),
			})
			rkIdx++
			tsIdx++
			continue
		}

		incomplete := p.incomplete[tsIdx]
		cmp := incomplete.Key.Timestamp.Compare(rangeKey.Key.Timestamp)
		switch {
		// If the timestamps match, the key spans are adjacent or overlapping, and
		// the values match then this range key extends the incomplete one.
		case cmp == 0 && bytes.Compare(startKey, incomplete.Key.EndKey) <= 0 &&
			bytes.Equal(rangeKey.Value, incomplete.Value):
			incomplete.Key.EndKey = append(incomplete.Key.EndKey[:0], endKey...)
			tsIdx++
			rkIdx++

		// This is a different range key at the same timestamp: complete the
		// existing one and start a new one.
		case cmp == 0:
			p.complete = append(p.complete, *incomplete)
			// NB: can't reuse slices, as they were placed in the completed range key.
			incomplete.Key.StartKey = append(make([]byte, 0, len(startKey)), startKey...)
			incomplete.Key.EndKey = append(make([]byte, 0, len(endKey)), endKey...)
			incomplete.Value = append(make([]byte, 0, len(rangeKey.Value)), rangeKey.Value...)

		// This incomplete range key is not present at this range key: complete it
		// and remove it from the list, then try again.
		case cmp == 1:
			p.complete = append(p.complete, *incomplete)
			p.incomplete = append(p.incomplete[:tsIdx], p.incomplete[tsIdx+1:]...)

		// This range key is a new incomplete range key: start defragmenting it.
		case cmp == -1:
			p.incomplete = append(p.incomplete[:tsIdx+1], p.incomplete[tsIdx:]...)
			p.incomplete[tsIdx] = &MVCCRangeKeyValue{
				Key: MVCCRangeKey{
					StartKey:  append(make(roachpb.Key, 0, len(startKey)), startKey...),
					EndKey:    append(make(roachpb.Key, 0, len(endKey)), endKey...),
					Timestamp: rangeKey.Key.Timestamp,
				},
				Value: append(make([]byte, 0, len(rangeKey.Value)), rangeKey.Value...),
			}
			tsIdx++
			rkIdx++

		default:
			p.err = errors.Errorf("unexpected comparison result %d", cmp)
			return
		}
	}

	// If the caller has requested fragments, we complete all range keys we found
	// this iteration by resetting tsIdx to 0. The loop below handles the rest.
	if p.opts.Fragmented {
		tsIdx = 0
	}

	// If there are any remaining incomplete range keys, they must be complete:
	// make them so.
	for _, ts := range p.incomplete[tsIdx:] {
		p.complete = append(p.complete, *ts)
	}
	p.incomplete = p.incomplete[:tsIdx]
}

// Close frees up resources held by the iterator.
func (p *MVCCRangeKeyIterator) Close() {
	p.iter.Close()
	p.complete = nil
	p.completeIdx = 0
}

// Next iterates to the next defragmented range key. Note the unusual iteration
// order, see struct comment for details.
func (p *MVCCRangeKeyIterator) Next() {
	p.completeIdx++
	if p.completeIdx >= len(p.complete) {
		p.iter.Next()
		// NB: Called even if Next() fails, because we may have incomplete
		// range keys that become complete when the iterator is exhausted.
		p.findCompleteRangeKeys()
	}
}

// Key returns the current range key. It will not be invalidated by the
// iterator, but it will be shared by all callers.
func (p *MVCCRangeKeyIterator) Key() MVCCRangeKey {
	return p.complete[p.completeIdx].Key
}

// Value returns the value of the current range key. It will not be invalidated
// by the iterator, but it will be shared by all callers.
func (p *MVCCRangeKeyIterator) Value() []byte {
	return p.complete[p.completeIdx].Value
}

// Valid returns (true, nil) if the iterator points to a valid key, (false, nil)
// if the iterator is exhausted, or (false, error) if an error occurred during
// iteration.
func (p *MVCCRangeKeyIterator) Valid() (bool, error) {
	if p.err != nil {
		return false, p.err
	}
	if _, err := p.iter.Valid(); err != nil {
		return false, err
	}
	return p.completeIdx < len(p.complete), nil
}
