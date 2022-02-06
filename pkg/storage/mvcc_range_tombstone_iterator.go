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

// MVCCRangeTombstoneIterOptions are options for an MVCCRangeTombstoneIterator.
type MVCCRangeTombstoneIterOptions struct {
	// LowerBound sets the inclusive lower bound of the iterator. Tombstones that
	// straddle the it will have their start key truncated to the lower bound.
	//
	// NB: It may be tempting to use an MVCCKey here and include a timestamp, but
	// this would be useless: giving e.g. a@4 would skip a tombstone starting at
	// a@5, but the tombstone would logically exist at the adjacent a.Next()@5 so
	// it would be emitted almost immediately anyway.
	LowerBound roachpb.Key
	// UpperBound sets the exclusive upper bound of the iterator. Tombstones that
	// straddle it will have their end key truncated to the upper bound.
	UpperBound roachpb.Key
	// MinTimestamp sets the inclusive lower timestamp bound for the iterator.
	MinTimestamp hlc.Timestamp
	// MaxTimestamp sets the inclusive upper timestamp bound for the iterator.
	MaxTimestamp hlc.Timestamp
	// Fragmented will emit tombstone fragments as they are stored in Pebble.
	// Fragments typically begin and end where a tombstone bound overlaps with
	// another tombstone, for all overlapping tombstones. However, fragmentation
	// is non-deterministic as it also depends on Pebble's internal SST structure
	// and mutation history.
	//
	// When enabled, this results in an iteration order of StartKey,Timestamp as
	// opposed to the normal EndKey,Timestamp order for range tombstones. This may
	// be useful for partial results and resumption, e.g. resume spans.
	Fragmented bool
}

// MVCCRangeTombstoneIterator iterates over range tombstones in an engine and
// defragments them into contiguous range tombstones. It does not support
// seeking or backtracking, see RangeTombstoneIterOptions for lower/upper bounds
// and other options.
//
// Iteration uses EndKey,Timestamp order rather than StartKey,Timestamp. For
// example, [a-z)@3 will be emitted after [c-e)@2, but before [x-z)@1. This is a
// memory optimization when defragmenting Pebble range keys, to allow emitting
// tombstones as soon as possible. Otherwise, a single tombstone across the the
// entire key span would require all other tombstones at other timestamps to be
// buffered in memory before they could be emitted. However, see the Fragmented
// option which emits non-deterministic fragments in StartKey,Timestamp order.
type MVCCRangeTombstoneIterator struct {
	iter        MVCCIterator
	opts        MVCCRangeTombstoneIterOptions
	incomplete  []*MVCCRangeKey // defragmentation buffer
	complete    []MVCCRangeKey  // queued for emission
	completeIdx int             // current Key()
	iterDone    bool            // TODO(erikgrinaker): remove this
	err         error
}

// NewMVCCRangeTombstoneIterator sets up a new MVCRangeTombstoneIterator and
// seeks to the first range tombstone. The caller must call Close() when done.
func NewMVCCRangeTombstoneIterator(
	r Reader, opts MVCCRangeTombstoneIterOptions,
) *MVCCRangeTombstoneIterator {
	iter := &MVCCRangeTombstoneIterator{
		iter: r.NewMVCCIterator(MVCCKeyIterKind, IterOptions{
			KeyTypes:   IterKeyTypeRangesOnly,
			LowerBound: EncodeMVCCKey(MVCCKey{Key: opts.LowerBound}),
			UpperBound: EncodeMVCCKey(MVCCKey{Key: opts.UpperBound}),
			// TODO(erikgrinaker): We do not set Min/MaxTimestampHint here, because
			// both are required and it's apparently not always safe to use.
		}),
		opts:       opts,
		incomplete: make([]*MVCCRangeKey, 0),
		complete:   make([]MVCCRangeKey, 0),
	}

	// Seek the iterator to the lower bound and iterate until we've collected
	// the first complete range tombstone (if any).
	iter.iter.SeekGE(MVCCKey{Key: opts.LowerBound})
	iter.findCompleteTombstones()

	return iter
}

// findCompleteTombstones processes range keys at the current iterator position
// and any subsequent iterator positions until it completes one or more
// tombstones, populating completeTombstones. Current completeTombstones are
// discarded.
func (p *MVCCRangeTombstoneIterator) findCompleteTombstones() {
	p.complete = p.complete[:0]
	p.completeIdx = 0
	p.updateTombstones()

	for len(p.complete) == 0 && !p.iterDone {
		if ok, err := p.iter.Valid(); err != nil {
			p.err = err
			return
		} else if !ok {
			break
		}
		p.iter.Next()
		// NB: We update tombstones even if Next() invalidates the iterator, because
		// there may be incomplete tombstones that become complete when the iterator
		// is exhausted.
		p.updateTombstones()
	}
}

// updateTombstones inspects the range keys at the current Pebble iterator
// position, tracks tombstones in incompleteTombstones, and moves any
// completed tombstones into completeTombstones.
func (p *MVCCRangeTombstoneIterator) updateTombstones() {
	var startKey, endKey roachpb.Key
	var rangeKeys []MVCCRangeKeyValue

	// If the iterator is exhausted, we still want to complete any remaining
	// incomplete tombstones.
	if ok, err := p.iter.Valid(); err != nil {
		p.err = err
		return
	} else if ok {
		startKey, endKey = p.iter.RangeBounds()
		rangeKeys = p.iter.RangeKeys()

		// TODO(erikgrinaker): Pebble does not yet truncate range keys to the
		// LowerBound or UpperBound of the range, so we truncate them here.
		if p.opts.LowerBound != nil && bytes.Compare(startKey, p.opts.LowerBound) < 0 {
			startKey = p.opts.LowerBound
		}
		if p.opts.UpperBound != nil && bytes.Compare(endKey, p.opts.UpperBound) > 0 {
			endKey = p.opts.UpperBound
		}
	}

	// TODO(erikgrinaker): Pebble does not yet respect UpperBound for range keys,
	// and seems to go into an infinite loop if we try to exhaust the iterator
	// here, so we use p.iterDone to mark it as done.
	if len(p.opts.UpperBound) > 0 && bytes.Compare(startKey, p.opts.UpperBound) >= 0 {
		p.iterDone = true
		startKey, endKey, rangeKeys = nil, nil, nil
	}

	// Both RangeKeys and incompleteTombstones are sorted in descending suffix
	// order, so we iterate over them in lockstep and insert/update/delete
	// incompleteTombstones as appropriate.
	var tsIdx, rkIdx int

	for rkIdx < len(rangeKeys) {
		rangeKey := rangeKeys[rkIdx]

		// Error on non-tombstone range keys. We expect all range keys to be range
		// tombstones currently.
		//
		// TODO(erikgrinaker): Pebble returns []byte{}, even though we wrote nil.
		if len(rangeKey.Value) != 0 {
			p.err = errors.Errorf("unexpected value for range key %s, expected nil: %x",
				rangeKey.Key, rangeKey.Value)
			return
		}

		// Filter rangekeys by suffix.
		//
		// TODO(erikgrinaker): This can be optimized by skipping unnecessary
		// comparisons since rangeKeys is sorted by suffix. Maybe later.
		if !p.opts.MinTimestamp.IsEmpty() && rangeKey.Key.Timestamp.Less(p.opts.MinTimestamp) {
			rkIdx++
			continue
		}
		if !p.opts.MaxTimestamp.IsEmpty() && p.opts.MaxTimestamp.Less(rangeKey.Key.Timestamp) {
			rkIdx++
			continue
		}

		// If we're at the end of incompleteTombstones, this range tombstone must be new.
		if tsIdx >= len(p.incomplete) {
			p.incomplete = append(p.incomplete, &MVCCRangeKey{
				StartKey:  append(make([]byte, 0, len(startKey)), startKey...),
				EndKey:    append(make([]byte, 0, len(endKey)), endKey...),
				Timestamp: rangeKey.Key.Timestamp,
			})
			rkIdx++
			tsIdx++
			continue
		}

		incomplete := p.incomplete[tsIdx]
		cmp := incomplete.Timestamp.Compare(rangeKey.Key.Timestamp)
		switch {
		// If the timestamps match and the key spans are adjacent or overlapping,
		// this range key extends the incomplete tombstone.
		case cmp == 0 && bytes.Compare(startKey, incomplete.EndKey) <= 0:
			incomplete.EndKey = append(incomplete.EndKey[:0], endKey...)
			tsIdx++
			rkIdx++

		// This is a different tombstone at the same suffix: complete the existing
		// tombstone and start a new one.
		case cmp == 0:
			p.complete = append(p.complete, *incomplete)
			// NB: can't reuse slices, as they were placed in the completed tombstone.
			incomplete.StartKey = append(make([]byte, 0, len(startKey)), startKey...)
			incomplete.EndKey = append(make([]byte, 0, len(endKey)), endKey...)

		// This incomplete tombstone is not present at this range key: complete it
		// and remove it from the list, then try again.
		case cmp == 1:
			p.complete = append(p.complete, *incomplete)
			p.incomplete = append(p.incomplete[:tsIdx], p.incomplete[tsIdx+1:]...)

		// This range key is a new incomplete tombstone: start tracking it.
		case cmp == -1:
			p.incomplete = append(p.incomplete[:tsIdx+1], p.incomplete[tsIdx:]...)
			p.incomplete[tsIdx] = &MVCCRangeKey{
				StartKey:  append(make(roachpb.Key, 0, len(startKey)), startKey...),
				EndKey:    append(make(roachpb.Key, 0, len(endKey)), endKey...),
				Timestamp: rangeKey.Key.Timestamp,
			}
			tsIdx++
			rkIdx++

		default:
			p.err = errors.Errorf("unexpected comparison result %d", cmp)
			return
		}
	}

	// If the caller has requested tombstone fragments, we complete all tombstones
	// we found during this iteration by resetting tsIdx to 0. The loop below will
	// handle the rest.
	if p.opts.Fragmented {
		tsIdx = 0
	}

	// If there are any remaining incomplete tombstones, they must be complete:
	// make them so.
	for _, ts := range p.incomplete[tsIdx:] {
		p.complete = append(p.complete, *ts)
	}
	p.incomplete = p.incomplete[:tsIdx]
}

// Close frees up resources held by the iterator.
func (p *MVCCRangeTombstoneIterator) Close() {
	p.iter.Close()
	p.complete = nil
	p.completeIdx = 0
}

// Next iterates to the next range tombstone. Note the unusual iteration
// order, see struct comment for details.
func (p *MVCCRangeTombstoneIterator) Next() {
	p.completeIdx++
	if p.completeIdx >= len(p.complete) {
		p.iter.Next()
		// NB: Called even if Next() fails, because we may have incomplete
		// tombstones that become complete when the iterator is exhausted.
		p.findCompleteTombstones()
	}
}

// Key returns the current range tombstone. It will not be invalidated by the
// iterator, but will be shared by all callers.
func (p *MVCCRangeTombstoneIterator) Key() MVCCRangeKey {
	return p.complete[p.completeIdx]
}

// Valid returns (true, nil) if the iterator points to a valid key, (false, nil)
// if the iterator is exhausted, or (false, error) if an error occurred during
// iteration.
func (p *MVCCRangeTombstoneIterator) Valid() (bool, error) {
	if p.err != nil {
		return false, p.err
	}
	if _, err := p.iter.Valid(); err != nil {
		return false, err
	}
	return p.completeIdx < len(p.complete), nil
}
