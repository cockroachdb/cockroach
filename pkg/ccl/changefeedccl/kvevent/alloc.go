// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvevent

import "context"

// Alloc describes the resources allocated on behalf of an event.
// Allocations should eventually be released.
// However, it is allowed not to release an allocation due to an error.  In such cases,
// all active allocations will be released when changefeed shuts down.
type Alloc struct {
	bytes   int64 // memory allocated for this request.
	entries int64 // number of entries using those bytes, usually 1.
	ap      pool  // pool where those resources ought to be released.

	// otherPoolAllocs is a map from pool to Alloc that exists to deal with
	// cases where allocs from different pools might be merged into this pool.
	// This can happen, at the time of writing, when the backfill concludes.
	// By merging on a per-pool basis, we can accumulate exactly the number of
	// allocs as there are pools in use. Any entry in this map must have a
	// nil otherPoolAllocs field.
	otherPoolAllocs map[pool]*Alloc
}

// pool is an allocation pool responsible for freeing up previously acquired resources.
type pool interface {
	// Release releases resources to this pool.
	Release(ctx context.Context, bytes, entries int64)
}

// Release releases resources associated with this allocation.
func (a *Alloc) Release(ctx context.Context) {
	if a.isZero() {
		return
	}
	for _, oa := range a.otherPoolAllocs {
		oa.Release(ctx)
	}
	a.ap.Release(ctx, a.bytes, a.entries)
	a.clear()
}

// AdjustBytesToTarget adjust byte allocation to the specified target.
// Target bytes cannot be adjusted to the higher level than the current allocation.
func (a *Alloc) AdjustBytesToTarget(ctx context.Context, targetBytes int64) {
	if a.isZero() || targetBytes <= 0 || targetBytes >= a.bytes {
		return
	}
	toRelease := a.bytes - targetBytes
	a.bytes = targetBytes
	a.ap.Release(ctx, toRelease, 0)
}

// Bytes returns the size of this alloc in bytes.
func (a *Alloc) Bytes() int64 {
	return a.bytes
}

// Events returns the number of items associated with this alloc.
func (a *Alloc) Events() int64 {
	return a.entries
}

// Merge merges other resources into this allocation.
func (a *Alloc) Merge(other *Alloc) {
	defer other.clear()
	if a.isZero() { // a is a zero allocation -- just use the other.
		*a = *other
		return
	}

	// If other has any allocs from a pool other than its own, merge those
	// into this. Flattening first means that any alloc in otherPoolAllocs
	// will have a nil otherPoolAllocs.
	if other.otherPoolAllocs != nil {
		for _, oa := range other.otherPoolAllocs {
			a.Merge(oa)
		}
		other.otherPoolAllocs = nil
	}

	if samePool := a.ap == other.ap; samePool {
		a.bytes += other.bytes
		a.entries += other.entries
	} else {
		// If other is from another pool, either store it in the map or merge it
		// into an existing map entry.
		if a.otherPoolAllocs == nil {
			a.otherPoolAllocs = make(map[pool]*Alloc, 1)
		}
		if mergeAlloc, ok := a.otherPoolAllocs[other.ap]; ok {
			mergeAlloc.Merge(other)
		} else {
			otherCpy := *other
			a.otherPoolAllocs[other.ap] = &otherCpy
		}
	}
}

func (a *Alloc) clear()       { *a = Alloc{} }
func (a *Alloc) isZero() bool { return a.ap == nil }
func (a *Alloc) init(bytes int64, p pool) {
	if !a.isZero() {
		panic("cannot initialize already initialized alloc")
	}
	a.bytes = bytes
	a.entries = 1
	a.ap = p
}

// TestingMakeAlloc creates allocation for the specified number of bytes
// in a single message using allocation pool 'p'.
func TestingMakeAlloc(bytes int64, p pool) Alloc {
	return Alloc{bytes: bytes, entries: 1, ap: p}
}
