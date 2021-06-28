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
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

type descriptorState struct {
	m       *Manager
	id      descpb.ID
	stopper *stop.Stopper

	// renewalInProgress is an atomic indicator for when a renewal for a
	// lease has begun. This is atomic to prevent multiple routines from
	// entering renewal initialization.
	renewalInProgress int32

	mu struct {
		syncutil.Mutex

		// descriptors sorted by increasing version. This set always
		// contains a descriptor version with a lease as the latest
		// entry. There may be more than one active lease when the system is
		// transitioning from one version of the descriptor to another or
		// when the node preemptively acquires a new lease for a version
		// when the old lease has not yet expired. In the latter case, a new
		// entry is created with the expiration time of the new lease and
		// the older entry is removed.
		active descriptorSet

		// Indicates that the descriptor has been, or is being, dropped or taken
		// offline.
		// If set, leases are released from the store as soon as their
		// refcount drops to 0, as opposed to waiting until they expire.
		// This flag will be unset by any subsequent lease acquisition, which can
		// happen after the table came back online again after having been taken
		// offline temporarily (as opposed to dropped).
		takenOffline bool

		// maxVersionSeen is used to prevent a race where a concurrent lease
		// acquisition might miss an event indicating that there is a new version
		// of a descriptor.
		maxVersionSeen descpb.DescriptorVersion

		// acquisitionsInProgress indicates that at least one caller is currently
		// in the process of performing an acquisition. This tracking is critical
		// to ensure that notifications of new versions which arrive before a lease
		// acquisition finishes but indicate that that new lease is expired are not
		// ignored.
		acquisitionsInProgress int
	}
}

// findForTimestamp finds a descriptor valid for the timestamp.
// In the most common case the timestamp passed to this method is close
// to the current time and in all likelihood the latest version of a
// descriptor if valid is returned.
//
// This returns errRenewLease when there is no descriptor cached
// or the latest descriptor version's ModificationTime satisfies the
// timestamp while it's expiration time doesn't satisfy the timestamp.
// This is an optimistic strategy betting that in all likelihood a
// higher layer renewing the lease on the descriptor and populating
// descriptorState will satisfy the timestamp on a subsequent call.
//
// In all other circumstances where a descriptor cannot be found for the
// timestamp errOlderReadTableVersion is returned requesting a higher layer
// to populate the descriptorState with a valid older version of the descriptor
// before calling.
//
// The refcount for the returned descriptorVersionState is incremented.
// It returns true if the descriptor returned is the known latest version
// of the descriptor.
func (t *descriptorState) findForTimestamp(
	ctx context.Context, timestamp hlc.Timestamp,
) (*descriptorVersionState, bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Acquire a lease if no descriptor exists in the cache.
	if len(t.mu.active.data) == 0 {
		return nil, false, errRenewLease
	}

	// Walk back the versions to find one that is valid for the timestamp.
	for i := len(t.mu.active.data) - 1; i >= 0; i-- {
		// Check to see if the ModificationTime is valid.
		if desc := t.mu.active.data[i]; desc.GetModificationTime().LessEq(timestamp) {
			latest := i+1 == len(t.mu.active.data)
			if !desc.hasExpired(timestamp) {
				// Existing valid descriptor version.
				desc.incRefCount(ctx)
				return desc, latest, nil
			}

			if latest {
				// Renew the lease if the lease has expired
				// The latest descriptor always has a lease.
				return nil, false, errRenewLease
			}
			break
		}
	}

	return nil, false, errReadOlderVersion
}

// upsertLeaseLocked inserts a lease for a particular descriptor version.
// If an existing lease exists for the descriptor version it replaces
// it and returns it.
func (t *descriptorState) upsertLeaseLocked(
	ctx context.Context, desc catalog.Descriptor, expiration hlc.Timestamp,
) (createdDescriptorVersionState *descriptorVersionState, toRelease *storedLease, _ error) {
	if t.mu.maxVersionSeen < desc.GetVersion() {
		t.mu.maxVersionSeen = desc.GetVersion()
	}
	s := t.mu.active.find(desc.GetVersion())
	if s == nil {
		if t.mu.active.findNewest() != nil {
			log.Infof(ctx, "new lease: %s", desc)
		}
		descState := newDescriptorVersionState(t, desc, expiration, true /* isLease */)
		t.mu.active.insert(descState)
		return descState, nil, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	// The desc is replacing an existing one at the same version.
	if !s.mu.expiration.Less(expiration) {
		// This is a violation of an invariant and can actually not
		// happen. We return an error here to aid in further investigations.
		return nil, nil, errors.AssertionFailedf("lease expiration monotonicity violation, (%s) vs (%s)", s, desc)
	}

	// subsume the refcount of the older lease. This is permitted because
	// the new lease has a greater expiration than the older lease and
	// any transaction using the older lease can safely use a deadline set
	// to the older lease's expiration even though the older lease is
	// released! This is because the new lease is valid at the same desc
	// version at a greater expiration.
	s.mu.expiration = expiration
	toRelease = s.mu.lease
	s.mu.lease = &storedLease{
		id:         desc.GetID(),
		version:    int(desc.GetVersion()),
		expiration: storedLeaseExpiration(expiration),
	}
	if log.ExpensiveLogEnabled(ctx, 2) {
		log.VEventf(ctx, 2, "replaced lease: %s with %s", toRelease, s.mu.lease)
	}
	return nil, toRelease, nil
}

var _ redact.SafeMessager = (*descriptorVersionState)(nil)

func newDescriptorVersionState(
	t *descriptorState, desc catalog.Descriptor, expiration hlc.Timestamp, isLease bool,
) *descriptorVersionState {
	descState := &descriptorVersionState{
		t:          t,
		Descriptor: desc,
	}
	descState.mu.expiration = expiration
	if isLease {
		descState.mu.lease = &storedLease{
			id:         desc.GetID(),
			version:    int(desc.GetVersion()),
			expiration: storedLeaseExpiration(expiration),
		}
	}
	return descState
}

// removeInactiveVersions removes inactive versions in t.mu.active.data with
// refcount 0. t.mu must be locked. It returns leases that need to be released.
func (t *descriptorState) removeInactiveVersions() []*storedLease {
	var leases []*storedLease
	// A copy of t.mu.active.data must be made since t.mu.active.data will be changed
	// within the loop.
	for _, desc := range append([]*descriptorVersionState(nil), t.mu.active.data...) {
		func() {
			desc.mu.Lock()
			defer desc.mu.Unlock()
			if desc.mu.refcount == 0 {
				t.mu.active.remove(desc)
				if l := desc.mu.lease; l != nil {
					desc.mu.lease = nil
					leases = append(leases, l)
				}
			}
		}()
	}
	return leases
}

// release returns a descriptorVersionState that needs to be released from
// the store.
func (t *descriptorState) release(ctx context.Context, s *descriptorVersionState) {

	// Decrements the refcount and returns true if the lease has to be removed
	// from the store.
	decRefcount := func(s *descriptorVersionState) *storedLease {
		// Figure out if we'd like to remove the lease from the store asap (i.e.
		// when the refcount drops to 0). If so, we'll need to mark the lease as
		// invalid.
		removeOnceDereferenced := t.m.removeOnceDereferenced() ||
			// Release from the store if the descriptor has been dropped or taken
			// offline.
			t.mu.takenOffline ||
			// Release from the store if the lease is not for the latest
			// version; only leases for the latest version can be acquired.
			s != t.mu.active.findNewest() ||
			s.GetVersion() < t.mu.maxVersionSeen

		s.mu.Lock()
		defer s.mu.Unlock()
		s.mu.refcount--
		if log.ExpensiveLogEnabled(ctx, 2) {
			log.Infof(ctx, "release: %s", s.stringLocked())
		}
		if s.mu.refcount < 0 {
			panic(errors.AssertionFailedf("negative ref count: %s", s))
		}

		if s.mu.refcount == 0 && s.mu.lease != nil && removeOnceDereferenced {
			l := s.mu.lease
			s.mu.lease = nil
			return l
		}
		return nil
	}
	maybeRemoveLease := func() *storedLease {
		t.mu.Lock()
		defer t.mu.Unlock()
		if l := decRefcount(s); l != nil {
			t.mu.active.remove(s)
			return l
		}
		return nil
	}
	if l := maybeRemoveLease(); l != nil {
		releaseLease(l, t.m)
	}
}

// maybeQueueLeaseRenewal queues a lease renewal if there is not already a lease
// renewal in progress.
func (t *descriptorState) maybeQueueLeaseRenewal(
	ctx context.Context, m *Manager, id descpb.ID, name string,
) error {
	if !atomic.CompareAndSwapInt32(&t.renewalInProgress, 0, 1) {
		return nil
	}

	// Start the renewal. When it finishes, it will reset t.renewalInProgress.
	return t.stopper.RunAsyncTask(context.Background(),
		"lease renewal", func(ctx context.Context) {
			t.startLeaseRenewal(ctx, m, id, name)
		})
}

// startLeaseRenewal starts a singleflight.Group to acquire a lease.
// This function blocks until lease acquisition completes.
// t.renewalInProgress must be set to 1 before calling.
func (t *descriptorState) startLeaseRenewal(
	ctx context.Context, m *Manager, id descpb.ID, name string,
) {
	log.VEventf(ctx, 1,
		"background lease renewal beginning for id=%d name=%q",
		id, name)
	if _, err := acquireNodeLease(ctx, m, id); err != nil {
		log.Errorf(ctx,
			"background lease renewal for id=%d name=%q failed: %s",
			id, name, err)
	} else {
		log.VEventf(ctx, 1,
			"background lease renewal finished for id=%d name=%q",
			id, name)
	}
	atomic.StoreInt32(&t.renewalInProgress, 0)
}

// markAcquisitionStart increments the acquisitionsInProgress counter.
func (t *descriptorState) markAcquisitionStart(ctx context.Context) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.acquisitionsInProgress++
}

// markAcquisitionDone decrements the acquisitionsInProgress counter.
func (t *descriptorState) markAcquisitionDone(ctx context.Context) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.acquisitionsInProgress--
}
