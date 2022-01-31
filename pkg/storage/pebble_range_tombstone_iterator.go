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
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

type incompleteTombstone struct {
	startKey roachpb.Key
	endKey   roachpb.Key
	suffix   []byte
}

// complete builds a proper MVCCRangeKey range tombstone from an
// incompleteTombstone. It takes ownership of the byte slices, so the caller
// must not modify them afterwards.
func (i *incompleteTombstone) complete() (MVCCRangeKey, error) {
	timestamp, err := decodeMVCCTimestampSuffix(i.suffix)
	if err != nil {
		return MVCCRangeKey{}, err
	}
	tombstone := MVCCRangeKey{
		StartKey:  i.startKey,
		EndKey:    i.endKey,
		Timestamp: timestamp,
	}
	if err := tombstone.Validate(); err != nil {
		return MVCCRangeKey{}, err
	}
	return tombstone, nil
}

// pebbleRangeTombstoneIterator is an iterator over range tombstones. It uses a
// Pebble range key iterator, decodes range keys, and defragments tombstones
// into separate contiguous tombstones.
//
// The iteration order is EndKey,Timestamp rather than StartKey,Timestamp to
// conserve memory: otherwise, e.g. a tombstone across the entire keyspan would
// require all other tombstones to be buffered in memory before being emitted.
type pebbleRangeTombstoneIterator struct {
	iter                   *pebble.Iterator
	lowerBound, upperBound roachpb.Key
	minSuffix, maxSuffix   []byte
	incomplete             []*incompleteTombstone // defragmentation buffer
	complete               []MVCCRangeKey         // queued for emission
	completeIdx            int                    // current UnsafeKey()
	err                    error
}

// newPebbleRangeTombstoneIterator sets up a new pebbleRangeTombstoneIterator
// and seeks to the first range tombstone.
func newPebbleRangeTombstoneIterator(
	handle pebble.Reader, opts RangeTombstoneIterOptions,
) *pebbleRangeTombstoneIterator {
	pebbleOpts := &pebble.IterOptions{
		KeyTypes: pebble.IterKeyTypeRangesOnly,
	}
	if len(opts.LowerBound) > 0 {
		pebbleOpts.LowerBound = EncodeMVCCKey(MVCCKey{Key: opts.LowerBound})
	}
	if len(opts.UpperBound) > 0 {
		pebbleOpts.UpperBound = EncodeMVCCKey(MVCCKey{Key: opts.UpperBound})
	}
	iter := &pebbleRangeTombstoneIterator{
		iter:       handle.NewIter(pebbleOpts),
		lowerBound: opts.LowerBound,
		upperBound: opts.UpperBound,
		minSuffix:  encodeMVCCTimestampSuffix(opts.MinTimestamp),
		maxSuffix:  encodeMVCCTimestampSuffix(opts.MaxTimestamp),
		incomplete: make([]*incompleteTombstone, 0),
		complete:   make([]MVCCRangeKey, 0),
	}

	// Seek the iterator to the lower bound and iterate until we've collected
	// the first complete range tombstone (if any).
	iter.iter.SeekGE(pebbleOpts.LowerBound)
	iter.findCompleteTombstones()

	return iter
}

// findCompleteTombstones processes range keys at the current iterator position
// and any subsequent iterator positions until it completes one or more
// tombstones, populating completeTombstones. Current completeTombstones are
// discarded.
func (p *pebbleRangeTombstoneIterator) findCompleteTombstones() {
	p.complete = p.complete[:0]
	p.completeIdx = 0
	p.updateTombstones()

	for len(p.complete) == 0 && p.iter.Valid() {
		// NB: We update tombstones even if Next() fails or it invalidates the
		// iterator, because there may be incomplete tombstones that become complete
		// when the iterator is exhausted.
		p.iter.Next()
		p.updateTombstones()
	}
}

// updateTombstones inspects the range keys at the current Pebble iterator
// position, tracks tombstones in incompleteTombstones, and moves any
// completed tombstones into completeTombstones.
func (p *pebbleRangeTombstoneIterator) updateTombstones() {
	var startKey, endKey roachpb.Key
	var rangeKeys []pebble.RangeKeyData

	// If the iterator is exhausted, we still want to complete any remaining
	// incomplete tombstones.
	if p.iter.Valid() {
		startKey, endKey = p.iter.RangeBounds()
		rangeKeys = p.iter.RangeKeys()

		// TODO(erikgrinaker): Pebble does not yet truncate range keys to the
		// LowerBound or UpperBound of the range, so we truncate them here.
		if p.lowerBound != nil && bytes.Compare(startKey, p.lowerBound) < 0 {
			startKey = p.lowerBound
		}
		if p.upperBound != nil && bytes.Compare(endKey, p.upperBound) > 0 {
			endKey = p.upperBound
		}
	} else if err := p.iter.Error(); err != nil {
		p.err = err
		return
	}

	// TODO(erikgrinaker): Pebble does not yet respect UpperBound for range keys,
	// so we handle it here temporarily by exhausting the iterator. If Pebble
	// won't support this then we should mark the iterator as done instead.
	if len(p.upperBound) > 0 && bytes.Compare(startKey, p.upperBound) >= 0 {
		for p.iter.Next() {
		}
		startKey, endKey, rangeKeys = nil, nil, nil
	}

	// Both RangeKeys and incompleteTombstones are sorted in descending suffix
	// order, so we iterate over them in lockstep and insert/update/delete
	// incompleteTombstones as appropriate.
	var tsIdx, rkIdx int
	var rangeValue enginepb.MVCCRangeValue

	for rkIdx < len(rangeKeys) {
		rangeKey := rangeKeys[rkIdx]

		// Filter rangekeys by suffix.
		//
		// TODO(erikgrinaker): This can be optimized by skipping unnecessary
		// comparisons since rangeKeys is sorted by suffix. Maybe later.
		if p.minSuffix != nil && bytes.Compare(rangeKey.Suffix, p.minSuffix) < 0 {
			rkIdx++
			continue
		}
		if p.maxSuffix != nil && bytes.Compare(rangeKey.Suffix, p.maxSuffix) > 0 {
			rkIdx++
			continue
		}

		// Make sure this range key is in fact a range tombstone. This check can be
		// dropped if it hampers performance, since the only current range keys are
		// range tombstones.
		if err := protoutil.Unmarshal(rangeKey.Value, &rangeValue); err != nil {
			p.err = errors.Wrapf(err, "failed to decode range value for %s-%s", startKey, endKey)
			return
		}
		if rangeValue.Tombstone == nil {
			p.err = errors.Errorf("found unknown range value at %s-%s: %s", startKey, endKey, rangeValue)
			return
		}

		// If we're at the end of incompleteTombstones, this range tombstone must be new.
		if tsIdx >= len(p.incomplete) {
			p.incomplete = append(p.incomplete, &incompleteTombstone{
				startKey: append(make([]byte, 0, len(startKey)), startKey...),
				endKey:   append(make([]byte, 0, len(endKey)), endKey...),
				suffix:   append(make([]byte, 0, len(rangeKey.Suffix)), rangeKey.Suffix...),
			})
			rkIdx++
			tsIdx++
			continue
		}

		incomplete := p.incomplete[tsIdx]
		cmp := bytes.Compare(incomplete.suffix, rangeKey.Suffix)
		switch {
		// If the suffixes match and the key spans are adjacent or overlapping,
		// this range key extends the incomplete tombstone.
		case cmp == 0 && bytes.Compare(startKey, incomplete.endKey) <= 0:
			incomplete.endKey = append(incomplete.endKey[:0], endKey...)
			tsIdx++
			rkIdx++

		// This is a different tombstone at the same suffix: complete the existing
		// tombstone and start a new one.
		case cmp == 0:
			complete, err := incomplete.complete()
			if err != nil {
				p.err = err
				return
			}
			p.complete = append(p.complete, complete)

			// NB: can't reuse slices, as they were placed in the completed tombstone.
			incomplete.startKey = append(make([]byte, 0, len(startKey)), startKey...)
			incomplete.endKey = append(make([]byte, 0, len(endKey)), endKey...)

		// This incomplete tombstone is not present at this range key: complete it
		// and remove it from the list, then try again.
		case cmp == 1:
			complete, err := incomplete.complete()
			if err != nil {
				p.err = err
				return
			}
			p.complete = append(p.complete, complete)
			p.incomplete = append(p.incomplete[:tsIdx],
				p.incomplete[tsIdx+1:]...)

		// This range key is a new incomplete tombstone: start tracking it.
		case cmp == -1:
			p.incomplete = append(p.incomplete[:tsIdx+1],
				p.incomplete[tsIdx:]...)
			p.incomplete[tsIdx] = &incompleteTombstone{
				startKey: append(make(roachpb.Key, 0, len(startKey)), startKey...),
				endKey:   append(make(roachpb.Key, 0, len(endKey)), endKey...),
				suffix:   append(make([]byte, 0, len(rangeKey.Suffix)), rangeKey.Suffix...),
			}
			tsIdx++
			rkIdx++

		default:
			p.err = errors.Errorf("unexpected comparison result %d", cmp)
			return
		}
	}

	// If there are any remaining incomplete tombstones, they must be complete:
	// make them so.
	for _, incomplete := range p.incomplete[tsIdx:] {
		complete, err := incomplete.complete()
		if err != nil {
			p.err = err
			return
		}
		p.complete = append(p.complete, complete)
	}
	p.incomplete = p.incomplete[:tsIdx]
}

// Close implements MVCCRangeTombstoneIterator.
func (p *pebbleRangeTombstoneIterator) Close() {
	p.iter.Close()
	p.complete = nil
	p.completeIdx = 0
}

// Next implements MVCCRangeTombstoneIterator.
func (p *pebbleRangeTombstoneIterator) Next() {
	p.completeIdx++
	if p.completeIdx >= len(p.complete) {
		p.iter.Next()
		// NB: Called even if Next() fails, because we may have incomplete
		// tombstones that become complete when the iterator is exhausted.
		p.findCompleteTombstones()
	}
}

// Key implements MVCCRangeTombstoneIterator.
func (p *pebbleRangeTombstoneIterator) Key() MVCCRangeKey {
	return p.complete[p.completeIdx]
}

// Valid implements MVCCRangeTombstoneIterator.
func (p *pebbleRangeTombstoneIterator) Valid() (bool, error) {
	if p.err != nil {
		return false, p.err
	}
	if err := p.iter.Error(); err != nil {
		return false, err
	}
	return p.completeIdx < len(p.complete), nil
}
