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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// RangeDescriptorCache is a simplified interface to the kv.RangeDescriptorCache
// for use at lower levels of the stack like storage.
type RangeDescriptorCache interface {
	// Lookup looks up range information for the range containing key.
	Lookup(ctx context.Context, key roachpb.RKey) (RangeCacheEntry, error)
}

// RangeCacheEntry represents the information returned by RangeDescriptorCache.Lookup().
type RangeCacheEntry interface {
	// Desc returns the cached descriptor. Note that, besides being possibly
	// stale, this descriptor also might not represent a descriptor that was ever
	// committed. See DescSpeculative().
	Desc() *roachpb.RangeDescriptor
	// DescSpeculative returns true if the descriptor in the entry is "speculative"
	// - i.e. it doesn't correspond to a committed value. Such descriptors have been
	// inserted in the cache with Generation=0.
	//
	// Speculative descriptors come from (not-yet-committed) intents.
	DescSpeculative() bool

	// Leaseholder returns the cached leaseholder replica, if known. Returns nil
	// if the leaseholder is not known.
	Leaseholder() *roachpb.ReplicaDescriptor

	// Lease returns the cached lease, if known. Returns nil if no lease is known.
	// It's possible for a leaseholder to be known, but not a full lease, in which
	// case Leaseholder() returns non-nil but Lease() returns nil.
	Lease() *roachpb.Lease
	// LeaseSpeculative returns true if the lease in the entry is "speculative"
	// - i.e. it doesn't correspond to a committed lease. Such leases have been
	// inserted in the cache with Sequence=0.
	LeaseSpeculative() bool
}
