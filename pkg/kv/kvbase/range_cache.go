// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package kvbase exports kv level interfaces to avoid dependency cycles.
package kvbase

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// RangeDescriptorCache is a simplified interface to the kv.RangeDescriptorCache
// for use at lower levels of the stack like storage.
type RangeDescriptorCache interface {
	// Lookup looks up range information for the range containing key.
	Lookup(ctx context.Context, key roachpb.RKey) (*RangeCacheEntry, error)
}

// RangeCacheEntry represents one cache entry.
//
// The cache stores *RangeCacheEntry. Entries are immutable: cache lookups
// returns the same *RangeCacheEntry to multiple queriers for efficiency, but
// nobody should modify the lookup result.
type RangeCacheEntry struct {
	// Desc is always populated.
	Desc roachpb.RangeDescriptor
	// Lease has info on the range's lease. It can be Empty() if no lease
	// information is known. When a lease is known, it is guaranteed that the
	// lease comes from Desc's range id (i.e. we'll never put a lease from another
	// range in here). This allows UpdateLease() to use Lease.Sequence to compare
	// leases. Moreover, the lease will correspond to one of the replicas in Desc.
	Lease roachpb.Lease
}

func (e RangeCacheEntry) String() string {
	return fmt.Sprintf("desc:%s, lease:%s", e.Desc, e.Lease)
}

// UpdateLease returns a new RangeCacheEntry with the receiver's descriptor and
// a new lease. The updated retval indicates whether the passed-in lease appears
// to be newer than the lease the entry had before. If updated is returned true,
// the caller should evict the existing entry (the receiver) and replace it with
// newEntry. (true, nil) can be returned meaning that the existing entry should
// be evicted, but there's no replacement that this function can provide; this
// happens when the passed-in lease indicates a leaseholder that's not part of
// the entry's descriptor. The descriptor must be really stale, and the caller
// should read a new version.
//
// If updated=false is returned, then newEntry will be the same as the receiver.
// This means that the passed-in lease is older than the lease already in the
// entry.
//
// If the new leaseholder is not a replica in the descriptor, we assume the
// lease information to be more recent than the entry's descriptor, and we
// return true, nil. The caller should evict the receiver from the cache, but
// it'll have to do extra work to figure out what to insert instead.
func (e *RangeCacheEntry) UpdateLease(l *roachpb.Lease) (updated bool, newEntry *RangeCacheEntry) {
	// If l is older than what the entry has (or the same), return early.
	// A new lease with a sequence of 0 is presumed to be newer than anything, and
	// an existing lease with a sequence of 0 is presumed to be older than
	// anything.
	//
	// We handle the case of a lease with the sequence equal to the existing
	// entry, but otherwise different. This results in the new lease updating the
	// entry, because the existing lease might correspond to a proposed lease that
	// a replica returned speculatively while a lease acquisition was in progress.
	if l.Sequence != 0 && e.Lease.Sequence != 0 && l.Sequence < e.Lease.Sequence {
		return false, e
	}

	if l.Equal(e.Lease) {
		return false, e
	}

	// Check whether the lease we were given is compatible with the replicas in
	// the descriptor. If it's not, the descriptor must be really stale, and the
	// RangeCacheEntry needs to be evicted.
	_, ok := e.Desc.GetReplicaDescriptorByID(l.Replica.ReplicaID)
	if !ok {
		return true, nil
	}

	// TODO(andrei): If the leaseholder is present, but the descriptor lists the
	// replica as a learner, this is a sign of a stale descriptor. I'm not sure
	// what to do about it, though.

	return true, &RangeCacheEntry{
		Desc:  e.Desc,
		Lease: *l,
	}
}

// NewerThan returns true if the receiver represents newer information about the
// range than o. The descriptors are assumed to be overlapping.
//
// When comparing two overlapping entries for deciding which one is stale, the
// descriptor's generation is checked first. For equal descriptor generations,
// the lease sequence number is checked second. For equal lease sequences,
// returns false. Note that this means that an Empty() e.Lease is considered
// older than any lease in o.
func (e *RangeCacheEntry) NewerThan(o *RangeCacheEntry) bool {
	if util.RaceEnabled {
		if _, err := e.Desc.RSpan().Intersect(&o.Desc); err != nil {
			panic(fmt.Sprintf("descriptors don't intersect: %s vs %s", e.Desc, o.Desc))
		}
	}
	if e.Desc.Generation == o.Desc.Generation {
		// If two RangeDescriptors overlap and have the same Generation, they must
		// be referencing the same range, in which case their lease sequences are
		// comparable.
		if e.Desc.RangeID != o.Desc.RangeID {
			panic(fmt.Sprintf("overlapping descriptors with same gen but different IDs: %s vs %s",
				e.Desc, o.Desc))
		}
		return e.Lease.Sequence > o.Lease.Sequence
	}
	return e.Desc.Generation > o.Desc.Generation
}
