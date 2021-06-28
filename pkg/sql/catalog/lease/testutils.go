// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package lease

import (
	"context"
	"time"

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
	// in descriptorState.acquire() and descriptorState.acquireFreshestFromStore().
	LeaseAcquireResultBlockEvent func(leaseBlockType AcquireBlockType)
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

	// To disable the deletion of orphaned leases at server startup.
	DisableDeleteOrphanedLeases bool

	// VersionPollIntervalForRangefeeds controls the polling interval for the
	// check whether the requisite version for rangefeed-based notifications has
	// been finalized.
	//
	// TODO(ajwerner): Remove this and replace it with a callback.
	VersionPollIntervalForRangefeeds time.Duration

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
