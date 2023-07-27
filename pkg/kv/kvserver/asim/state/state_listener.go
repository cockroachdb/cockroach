// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
