// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// A unique id for a particular descriptor version.
type descVersionID struct {
	id      descpb.ID
	version descpb.DescriptorVersion
}

// LeaseRemovalTracker can be used to wait for leases to be removed from the
// store (leases are removed from the store async w.r.t. Manager
// operations).
// To use it, its LeaseRemovedNotification method must be hooked up to
// StorageTestingKnobs.LeaseReleasedEvent. Then, every time you want to wait
// for a lease, get a tracker object through TrackRemoval() before calling
// Manager.Release(), and then call WaitForRemoval() on the tracker to
// block for the removal from the store.
//
// All methods are thread-safe.
type LeaseRemovalTracker struct {
	mu syncutil.Mutex
	// map from a lease whose release we're waiting for to a tracker for that
	// lease.
	tracking map[descVersionID]RemovalTracker
}

type RemovalTracker struct {
	removed chan struct{}
	// Pointer to a shared err. *err is written when removed is closed.
	err *error
}

// NewLeaseRemovalTracker creates a LeaseRemovalTracker.
func NewLeaseRemovalTracker() *LeaseRemovalTracker {
	return &LeaseRemovalTracker{
		tracking: make(map[descVersionID]RemovalTracker),
	}
}

// TrackRemoval starts monitoring lease removals for a particular lease.
// This should be called before triggering the operation that (asynchronously)
// removes the lease.
func (w *LeaseRemovalTracker) TrackRemoval(desc catalog.Descriptor) RemovalTracker {
	id := descVersionID{
		id:      desc.GetID(),
		version: desc.GetVersion(),
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if tracker, ok := w.tracking[id]; ok {
		return tracker
	}
	tracker := RemovalTracker{removed: make(chan struct{}), err: new(error)}
	w.tracking[id] = tracker
	return tracker
}

// WaitForRemoval blocks until the lease is removed from the store.
func (t RemovalTracker) WaitForRemoval() error {
	<-t.removed
	return *t.err
}

// LeaseRemovedNotification has to be called after a lease is removed from the
// store. This should be hooked up as a callback to
// StorageTestingKnobs.LeaseReleasedEvent.
func (w *LeaseRemovalTracker) LeaseRemovedNotification(
	id descpb.ID, version descpb.DescriptorVersion, err error,
) {
	w.mu.Lock()
	defer w.mu.Unlock()

	idx := descVersionID{
		id:      id,
		version: version,
	}

	if tracker, ok := w.tracking[idx]; ok {
		*tracker.err = err
		close(tracker.removed)
		delete(w.tracking, idx)
	}
}

// ExpireLeases ia a hack for testing that manually sets expirations to a past
// timestamp.
func (m *Manager) ExpireLeases(clock *hlc.Clock) {
	past := hlc.Timestamp{
		WallTime: clock.Now().GoTime().Add(-time.Millisecond).UnixNano(),
	}

	m.names.mu.Lock()
	defer m.names.mu.Unlock()
	_ = m.names.descriptors.IterateByID(func(entry catalog.NameEntry) error {
		desc := entry.(*descriptorVersionState)
		desc.mu.Lock()
		defer desc.mu.Unlock()
		desc.mu.expiration = past
		return nil
	})
}

// PublishMultiple updates multiple descriptors, maintaining the invariant
// that there are at most two versions of each descriptor out in the wild at any
// time by first waiting for all nodes to be on the current (pre-update) version
// of the descriptor.
//
// The update closure for all descriptors is called after the wait. The map argument
// is a map of the descriptors with the IDs given in the ids slice, and the
// closure mutates those descriptors. The txn argument closure is intended to be
// used for updating jobs. Note that it can't be used for anything except
// writing to system descriptors, since we set the system config trigger to write the
// schema changes.
//
// The closure may be called multiple times if retries occur; make sure it does
// not have side effects.
//
// Returns the updated versions of the descriptors.
//
// TODO (lucy): Providing the txn for the update closure just to update a job
// is not ideal. There must be a better API for this.
func (m *Manager) PublishMultiple(
	ctx context.Context,
	ids []descpb.ID,
	update func(*kv.Txn, map[descpb.ID]catalog.MutableDescriptor) error,
	logEvent func(*kv.Txn) error,
) (map[descpb.ID]catalog.Descriptor, error) {
	errLeaseVersionChanged := errors.New("lease version changed")
	// Retry while getting errLeaseVersionChanged.
	for r := retry.Start(base.DefaultRetryOptions()); r.Next(); {
		// Wait until there are no unexpired leases on the previous versions
		// of the descriptors.
		expectedVersions := make(map[descpb.ID]descpb.DescriptorVersion)
		for _, id := range ids {
			expected, err := m.WaitForOneVersion(ctx, id, base.DefaultRetryOptions())
			if err != nil {
				return nil, err
			}
			expectedVersions[id] = expected
		}

		descs := make(map[descpb.ID]catalog.MutableDescriptor)
		// There should be only one version of the descriptor, but it's
		// a race now to update to the next version.
		err := m.storage.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			versions := make(map[descpb.ID]descpb.DescriptorVersion)
			descsToUpdate := make(map[descpb.ID]catalog.MutableDescriptor)
			for _, id := range ids {
				// Re-read the current versions of the descriptor, this time
				// transactionally.
				desc, err := catalogkv.MustGetMutableDescriptorByID(ctx, txn, m.storage.codec, id)
				// Due to details in #51417, it is possible for a user to request a
				// descriptor which no longer exists. In that case, just return an error.
				if err != nil {
					return err
				}
				descsToUpdate[id] = desc
				if expectedVersions[id] != desc.GetVersion() {
					// The version changed out from under us. Someone else must be
					// performing a schema change operation.
					if log.V(3) {
						log.Infof(ctx, "publish %d (version changed): %d != %d", id, expectedVersions[id], desc.GetVersion())
					}
					return errLeaseVersionChanged
				}

				versions[id] = descsToUpdate[id].GetVersion()
			}

			// This is to write the updated descriptors if we're the system tenant.
			if err := txn.SetSystemConfigTrigger(m.storage.codec.ForSystemTenant()); err != nil {
				return err
			}

			// Run the update closure.
			if err := update(txn, descsToUpdate); err != nil {
				return err
			}
			for _, id := range ids {
				if versions[id] != descsToUpdate[id].GetVersion() {
					return errors.Errorf("updated version to: %d, expected: %d",
						descsToUpdate[id].GetVersion(), versions[id])
				}
				descsToUpdate[id].MaybeIncrementVersion()
				descs[id] = descsToUpdate[id]
			}

			b := txn.NewBatch()
			for id, desc := range descs {
				if err := catalogkv.WriteDescToBatch(ctx, false /* kvTrace */, m.storage.settings, b, m.storage.codec, id, desc); err != nil {
					return err
				}
			}
			if logEvent != nil {
				// If an event log is required for this update, ensure that the
				// descriptor change occurs first in the transaction. This is
				// necessary to ensure that the System configuration change is
				// gossiped. See the documentation for
				// transaction.SetSystemConfigTrigger() for more information.
				if err := txn.Run(ctx, b); err != nil {
					return err
				}
				if err := logEvent(txn); err != nil {
					return err
				}
				return txn.Commit(ctx)
			}
			// More efficient batching can be used if no event log message
			// is required.
			return txn.CommitInBatch(ctx, b)
		})

		switch {
		case err == nil:
			immutDescs := make(map[descpb.ID]catalog.Descriptor)
			for id, desc := range descs {
				immutDescs[id] = desc.ImmutableCopy()
			}
			return immutDescs, nil
		case errors.Is(err, errLeaseVersionChanged):
			// will loop around to retry
		default:
			return nil, err
		}
	}

	panic("not reached")
}

// Publish updates a descriptor. It also maintains the invariant that
// there are at most two versions of the descriptor out in the wild at any time
// by first waiting for all nodes to be on the current (pre-update) version of
// the descriptor.
//
// The update closure is called after the wait, and it provides the new version
// of the descriptor to be written. In a multi-step schema operation, this
// update should perform a single step.
//
// The closure may be called multiple times if retries occur; make sure it does
// not have side effects.
//
// Returns the updated version of the descriptor.
// TODO (lucy): Maybe have the closure take a *kv.Txn to match
// PublishMultiple.
func (m *Manager) Publish(
	ctx context.Context,
	id descpb.ID,
	update func(catalog.MutableDescriptor) error,
	logEvent func(*kv.Txn) error,
) (catalog.Descriptor, error) {
	ids := []descpb.ID{id}
	updates := func(_ *kv.Txn, descs map[descpb.ID]catalog.MutableDescriptor) error {
		desc, ok := descs[id]
		if !ok {
			return errors.AssertionFailedf(
				"required descriptor with ID %d not provided to update closure", id)
		}
		return update(desc)
	}

	results, err := m.PublishMultiple(ctx, ids, updates, logEvent)
	if err != nil {
		return nil, err
	}
	return results[id], nil
}
