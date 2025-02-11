// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package lease

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// StorageTestingKnobs contains testing knobs.
type StorageTestingKnobs struct {
	// Called after a lease is removed from the store, with any operation error.
	// See LeaseRemovalTracker.
	LeaseReleasedEvent func(id descpb.ID, version descpb.DescriptorVersion, err error)
	// Called after a lease is acquired, with any operation error.
	LeaseAcquiredEvent func(desc catalog.Descriptor, err error)
	// Called before waiting on a results from a DoChan call of acquireNodeLease
	// in Acquire and AcquireFreshestFromStore.
	LeaseAcquireResultBlockEvent func(leaseBlockType AcquireType, id descpb.ID)
	// RemoveOnceDereferenced forces leases to be removed
	// as soon as they are dereferenced.
	RemoveOnceDereferenced bool
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*StorageTestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = &StorageTestingKnobs{}

// ManagerTestingKnobs contains test knobs.
type ManagerTestingKnobs struct {

	// A callback called after the leases are refreshed as a result of a gossip update.
	TestingDescriptorRefreshedEvent func(descriptor *descpb.Descriptor)

	// TestingDescriptorUpdateEvent is a callback when an update is received, before
	// the leases are refreshed. If a non-nil error is returned, the update is
	// ignored.
	TestingDescriptorUpdateEvent func(descriptor *descpb.Descriptor) error

	// TestingBeforeAcquireLeaseDuringRefresh is a callback right before
	// the lease manager attempts to acquire a lease for descriptor `id`.
	TestingBeforeAcquireLeaseDuringRefresh func(id descpb.ID) error

	// TestingOnNewVersion invoked when the range feed detects a new descriptor.
	TestingOnNewVersion func(id descpb.ID)

	// TestingOnLeaseGenerationBumpForNewVersion invoked when the lease generation,
	// after a new descriptor or initial descriptor version are observed via
	// the range feed.
	TestingOnLeaseGenerationBumpForNewVersion func(id descpb.ID)

	// To disable the deletion of orphaned leases at server startup.
	DisableDeleteOrphanedLeases bool

	// DisableRangeFeedCheckpoint is used to disable rangefeed checkpoints.
	DisableRangeFeedCheckpoint bool

	// RangeFeedReset channel is closed to indicate that the range feed
	// has been reset.
	RangeFeedResetChannel chan struct{}

	LeaseStoreTestingKnobs StorageTestingKnobs
}

var _ base.ModuleTestingKnobs = &ManagerTestingKnobs{}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*ManagerTestingKnobs) ModuleTestingKnobs() {}

// TestingAcquireAndAssertMinVersion acquires a read lease for the specified
// ID. The lease is grabbed on the latest version if >= specified version.
// It returns a descriptor and an expiration time valid for the timestamp.
// This method is useful for testing and is only intended to be used in that
// context.
func (m *Manager) TestingAcquireAndAssertMinVersion(
	ctx context.Context, timestamp hlc.Timestamp, id descpb.ID, minVersion descpb.DescriptorVersion,
) (LeasedDescriptor, error) {
	t := m.findDescriptorState(id, true)
	if err := ensureVersion(ctx, id, minVersion, m); err != nil {
		return nil, err
	}
	desc, _, err := t.findForTimestamp(ctx, timestamp)
	if err != nil {
		return nil, err
	}
	return desc, nil
}

// TestingOutstandingLeasesGauge returns the outstanding leases gauge that is
// used by this lease manager.
func (m *Manager) TestingOutstandingLeasesGauge() *metric.Gauge {
	return m.storage.outstandingLeases
}

// TestingSessionBasedLeasesExpiredGauge returns the session based leases
// expired gauge that is used by this lease manager.
func (m *Manager) TestingSessionBasedLeasesExpiredGauge() *metric.Gauge {
	return m.storage.sessionBasedLeasesExpired
}

// TestingSessionBasedLeasesWaitingToExpireGauge returns the session based leases
// waiting to expire gauge that is used by this lease manager.
func (m *Manager) TestingSessionBasedLeasesWaitingToExpireGauge() *metric.Gauge {
	return m.storage.sessionBasedLeasesWaitingToExpire
}
