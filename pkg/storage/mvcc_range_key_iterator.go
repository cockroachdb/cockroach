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
	iter         MVCCIterator
	opts         MVCCRangeKeyIterOptions
	defragmenter *MVCCRangeKeyDefragmenter
	complete     []MVCCRangeKeyValue // queued for emission
	completeIdx  int                 // current Key()
	err          error
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
		opts:         opts,
		defragmenter: NewMVCCRangeKeyDefragmenter(),
		complete:     make([]MVCCRangeKeyValue, 0),
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

// updateRangeKeys defragments the range keys at the current Pebble iterator
// position, storing any completes range keys in p.complete.
func (p *MVCCRangeKeyIterator) updateRangeKeys() {
	p.completeIdx = 0

	var rangeKeys []MVCCRangeKeyValue

	// If the iterator is exhausted, we still want to complete any remaining
	// incomplete range keys.
	if ok, err := p.iter.Valid(); err != nil {
		p.err = err
		return
	} else if ok {
		rangeKeys = p.iter.RangeKeys()
	}

	// Filter range keys by timestamp
	//
	// TODO(erikgrinaker): This can be optimized to skip unnecessary comparisons
	// since rangeKeys is sorted by timestamp. Maybe later.
	if !p.opts.MinTimestamp.IsEmpty() || !p.opts.MaxTimestamp.IsEmpty() {
		filtered := make([]MVCCRangeKeyValue, 0, len(rangeKeys))
		for _, rkv := range rangeKeys {
			if !p.opts.MinTimestamp.IsEmpty() && rkv.Key.Timestamp.Less(p.opts.MinTimestamp) {
				continue
			}
			if !p.opts.MaxTimestamp.IsEmpty() && p.opts.MaxTimestamp.Less(rkv.Key.Timestamp) {
				continue
			}
			filtered = append(filtered, rkv)
		}
		rangeKeys = filtered
	}

	// Feed the fragments to the defragmenter.
	p.complete = p.defragmenter.AppendFragments(rangeKeys)

	// If the caller has requested fragments we complete the pending range keys by
	// feeding the defragmenter an empty fragment stack. The defragmenter will
	// then be empty on the next updateRangeKeys() call, so the above
	// AppendFragments() call will always return an empty list.
	if p.opts.Fragmented {
		p.complete = p.defragmenter.AppendFragments(nil)
	}
}

// Close frees up resources held by the iterator.
func (p *MVCCRangeKeyIterator) Close() {
	p.iter.Close()
	p.complete = nil
	p.completeIdx = 0
}

// Next iterates to the next defragmented range key. Note the unusual iteration
// order of EndKey,Timestamp -- see MVCCRangeKeyIterator comment for details.
func (p *MVCCRangeKeyIterator) Next() {
	p.completeIdx++
	if p.completeIdx >= len(p.complete) {
		p.iter.Next()
		// NB: Called even if iterator is now invalid, because we may have
		// incomplete range keys that become complete when the iterator is
		// exhausted.
		p.findCompleteRangeKeys()
	}
}

// Key returns the current range key. It will not be modified by the iterator,
// but it will be shared by all callers.
func (p *MVCCRangeKeyIterator) Key() MVCCRangeKey {
	return p.complete[p.completeIdx].Key
}

// Value returns the value of the current range key. It will not be modified
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

// MVCCRangeKeyDefragmenter defragments range keys into contiguous logical
// range keys. Abutting range keys at the same timestamp with the same
// value are considered the same logical range key.
type MVCCRangeKeyDefragmenter struct {
	incomplete []*MVCCRangeKeyValue
}

// NewMVCCRangeKeyDefragmenter creates a new range key defragmenter.
func NewMVCCRangeKeyDefragmenter() *MVCCRangeKeyDefragmenter {
	return &MVCCRangeKeyDefragmenter{}
}

// AppendFragments appends a new stack of range key fragments to the tracked
// range keys, extending or creating logical range keys as appropriate. The
// given fragments must be the next fragment stack when iterating across range
// keys in sequential order, ordered in descending timestamp order. Any tracked
// range keys that are not present in the given stack will be considered
// fully defragmented and returned.
func (d *MVCCRangeKeyDefragmenter) AppendFragments(
	fragments []MVCCRangeKeyValue,
) []MVCCRangeKeyValue {
	var complete []MVCCRangeKeyValue

	// Both fragments and d.incomplete are sorted in descending timestamp order,
	// so we iterate over them in lockstep and insert/update/delete d.incomplete
	// as appropriate.
	var incompleteIdx, fragmentsIdx int

	for fragmentsIdx < len(fragments) {
		fragment := fragments[fragmentsIdx]

		// If we're at the end of d.incomplete, this range key must be new.
		if incompleteIdx >= len(d.incomplete) {
			incomplete := fragment.Clone()
			d.incomplete = append(d.incomplete, &incomplete)
			fragmentsIdx++
			incompleteIdx++
			continue
		}

		incomplete := d.incomplete[incompleteIdx]
		cmp := incomplete.Key.Timestamp.Compare(fragment.Key.Timestamp)
		switch {
		// If the timestamps match, the key spans abut, and the values match then
		// this range key extends the incomplete one.
		case cmp == 0 && bytes.Equal(fragment.Key.StartKey, incomplete.Key.EndKey) &&
			bytes.Equal(fragment.Value, incomplete.Value):
			incomplete.Key.EndKey = append(incomplete.Key.EndKey[:0], fragment.Key.EndKey...)
			incompleteIdx++
			fragmentsIdx++

		// This is a different range key at the same timestamp: complete the
		// existing one and start a new one.
		case cmp == 0:
			complete = append(complete, *incomplete)
			*incomplete = fragment.Clone()
			incompleteIdx++
			fragmentsIdx++

		// This incomplete range key is not present at this range key: complete it
		// and remove it from the list, then try the current range key again.
		case cmp == 1:
			complete = append(complete, *incomplete)
			d.incomplete = append(d.incomplete[:incompleteIdx], d.incomplete[incompleteIdx+1:]...)

		// This range key is a new incomplete range key: start defragmenting it.
		case cmp == -1:
			incomplete := fragment.Clone()
			d.incomplete = append(d.incomplete[:incompleteIdx+1], d.incomplete[incompleteIdx:]...)
			d.incomplete[incompleteIdx] = &incomplete
			incompleteIdx++
			fragmentsIdx++

		default:
			panic(errors.AssertionFailedf("unexpected comparison result %d", cmp))
		}
	}

	// If there are any remaining incomplete range keys, they must be complete:
	// make them so.
	for _, incomplete := range d.incomplete[incompleteIdx:] {
		complete = append(complete, *incomplete)
	}
	d.incomplete = d.incomplete[:incompleteIdx]

	return complete
}
