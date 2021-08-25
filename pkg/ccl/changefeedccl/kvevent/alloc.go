// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

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

	// Merged allocations that belong to a different pool.  Normally nil.
	otherPoolAllocs map[pool]*Alloc
}

// Release releases resources associated with this allocation.
func (a Alloc) Release(ctx context.Context) {
	if a.ap != nil {
		a.ap.Release(ctx, a.bytes, a.entries)
	}
	for _, oa := range a.otherPoolAllocs {
		oa.Release(ctx)
	}
}

// Merge merges other resources into this allocation.
func (a *Alloc) Merge(other *Alloc) {
	if a.ap == nil {
		// Okay to merge into nil allocation -- just use the other.
		*a = *other
		return
	}

	samePool := a.ap == other.ap
	mergeOtherAllocs := (a.otherPoolAllocs == nil && other.otherPoolAllocs != nil) ||
		(a.otherPoolAllocs != nil && other.otherPoolAllocs == nil)

	if !samePool || mergeOtherAllocs {
		// Slow case: this doesn't happen frequently (only right after backfill completes).
		a.mergeSlow(other)
	}

	if samePool {
		a.bytes += other.bytes
		a.entries += other.entries
	}

	other.clearAlloc()
}

func (a *Alloc) clearAlloc() {
	a.bytes = 0
	a.entries = 0
	a.ap = nil
	a.otherPoolAllocs = nil
}

// mergeSlow merges allocation that belongs to another alloc pool.
func (a *Alloc) mergeSlow(other *Alloc) {
	if a.otherPoolAllocs == nil {
		a.otherPoolAllocs = make(map[pool]*Alloc, 1)
	}

	mergeOther := func(toMerge *Alloc) {
		if mergeAlloc, ok := a.otherPoolAllocs[toMerge.ap]; ok {
			mergeAlloc.Merge(toMerge)
		} else {
			a.otherPoolAllocs[toMerge.ap] = mergeAlloc
		}
	}

	if a.ap != other.ap {
		// If we're merging across different pools, merge single allocation
		mergeOther(other)
	} else {
		// Merge other.otherPoolAllocs into a.otherPoolAllocs
		for _, opa := range other.otherPoolAllocs {
			mergeOther(opa)
		}
	}
}

// pool is an allocation pool responsible for freeing up previously acquired resources.
type pool interface {
	// Release releases resources to this pool.
	Release(ctx context.Context, bytes, entries int64)
}

// TestingMakeAlloc creates allocation for the specified number of bytes
// in a single message using allocation pool 'p'.
func TestingMakeAlloc(bytes int64, p pool) Alloc {
	return Alloc{bytes: bytes, entries: 1, ap: p}
}
