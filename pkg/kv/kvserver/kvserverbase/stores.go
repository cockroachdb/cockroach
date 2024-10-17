// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserverbase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// StoresIterator is able to iterate over all stores on a given node.
type StoresIterator interface {
	ForEachStore(func(Store) error) error
}

// Store is an adapter to the underlying KV store.
type Store interface {
	// StoreID returns the store identifier.
	StoreID() roachpb.StoreID

	// Enqueue the replica with the given range ID into the named queue.
	Enqueue(
		ctx context.Context,
		queue string,
		rangeID roachpb.RangeID,
		skipShouldQueue bool,
	) error

	// SetQueueActive disables/enables the named queue.
	SetQueueActive(active bool, queue string) error

	// GetReplicaMutexForTesting returns the mutex of the replica with the given
	// range ID, or nil if no replica was found. This is used for testing.
	// Returns a syncutil.RWMutex rather than ReplicaMutex to avoid import cycles.
	GetReplicaMutexForTesting(rangeID roachpb.RangeID) *syncutil.RWMutex
}

// UnsupportedStoresIterator is a StoresIterator that only returns "unsupported"
// errors.
type UnsupportedStoresIterator struct{}

var _ StoresIterator = UnsupportedStoresIterator{}

// ForEachStore is part of the StoresIterator interface.
func (i UnsupportedStoresIterator) ForEachStore(f func(Store) error) error {
	return errorutil.UnsupportedUnderClusterVirtualization(errorutil.FeatureNotAvailableToNonSystemTenantsIssue)
}
