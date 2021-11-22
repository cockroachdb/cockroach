// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package singleversion provides abstractions to Lease descriptors in a
// way which prevents new versions from being introduced.
//
// This layer enables changefeeds to operate at low-latency.
package singleversion

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// Lease represents a lease which prevents a descriptor from being modified.
type Lease interface {
	Start() hlc.Timestamp
	Expiration() hlc.Timestamp
	Release(ctx context.Context, maxUsedTimestamp hlc.Timestamp)
	ID() descpb.ID
}

// Acquirer is used to acquire a lease.
type Acquirer interface {

	// Acquire acquires a new singleversion lease.
	Acquire(ctx context.Context, id descpb.ID) (Lease, error)

	// WatchForNotifications watches for all notifications to singleversion
	// leases destined for the specified IDs.
	WatchForNotifications(
		ctx context.Context,
		startTS hlc.Timestamp,
		ids catalog.DescriptorIDSet,
		ch chan<- Notification,
	) error
}

// Preemptor is used to verify that no outstanding leases exist before
// committing a transaction.
type Preemptor interface {

	// EnsureNoSingleVersionLeases will perform an operation on behalf of the
	// provided transaction to prevent any new leases from being acquired. In
	// parallel, it will check if there are any outstanding leases. If there are
	// outstanding leases, it will notify them to release, and then wait until
	// they are released.
	EnsureNoSingleVersionLeases(
		ctx context.Context, txn *kv.Txn, descriptors catalog.DescriptorIDSet,
	) error
}

// Notification objects are sent by the Preemptor to notify leaseholders to
// drop their singleversion leases via Acquirer.WatchForNotifications().
type Notification struct {
	ID        descpb.ID
	Timestamp hlc.Timestamp
}
