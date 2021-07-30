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
}

// Release releases resources associated with this allocation.
func (a Alloc) Release(ctx context.Context) {
	if a.ap != nil {
		a.ap.Release(ctx, a.bytes, a.entries)
	}
}

// Merge merges other resources into this allocation.
func (a *Alloc) Merge(other *Alloc) {
	if a.ap == nil {
		// Okay to merge into nil allocation -- just use the other.
		*a = *other
		return
	}

	if a.ap != other.ap {
		panic("cannot merge allocations from two different pools")
	}
	a.bytes += other.bytes
	a.entries += other.entries
	other.bytes = 0
	other.entries = 0
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
