// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package state

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// CapacityChangeListener listens for notification of capacity change events.
type CapacityChangeListener interface {
	// CapacityChangeNotify notifies that a capacity change event has occurred
	// for the store with ID StoreID.
	CapacityChangeNotify(kvserver.CapacityChangeEvent, StoreID)
}

// NewCapacityListener listens for notification of new store capacity events.
type NewCapacityListener interface {
	// NewCapacityNotify notifies that a new capacity event has occurred for
	// the store with ID StoreID.
	NewCapacityNotify(roachpb.StoreCapacity, StoreID)
}

// ConfigChangeListener listens for notification of configuration changes such
// as stores being added.
type ConfigChangeListener interface {
	// StoreAddNotify notifies that a new store has been added with ID storeID.
	StoreAddNotify(StoreID, State)
}
