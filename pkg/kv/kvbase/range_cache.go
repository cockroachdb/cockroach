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
)

// RangeDescriptorCache is a simplified interface to the kv.RangeDescriptorCache
// for use at lower levels of the stack like storage.
type RangeDescriptorCache interface {
	// Lookup looks up a range descriptor based on a key.
	Lookup(ctx context.Context, key roachpb.RKey) (*RangeCacheEntry, error)
}

// RangeCacheEntry represents one cache entry.
type RangeCacheEntry struct {
	Desc roachpb.RangeDescriptor
	// Lease can be Empty() if no lease information is known for the range.
	Lease roachpb.Lease
}

func (e *RangeCacheEntry) String() string {
	return fmt.Sprintf("desc:%s lease:%s", e.Desc, e.Lease)
}

// Equal returns true if the receiver represents an equivalent descriptor and
// lease to o. The descriptors are assumed to be overlapping.
func (e *RangeCacheEntry) Equal(o *RangeCacheEntry) bool {
	return e.Desc.Generation == o.Desc.Generation && e.Lease.Sequence == o.Lease.Sequence
}

// NewerThan returns true if the receiver represents newer information about the
// range than o. The descriptors are assumed to be overlapping.
//
// When comparing two overlapping entries for deciding which one is stale, the
// descriptor's generation is checked first. For equal descriptor generations,
// the lease sequence number is checked second.
func (e *RangeCacheEntry) NewerThan(o *RangeCacheEntry) bool {
	if e.Desc.Generation < o.Desc.Generation {
		return false
	}
	if e.Desc.Generation > o.Desc.Generation {
		return true
	}
	return e.Lease.Sequence > o.Lease.Sequence
}
