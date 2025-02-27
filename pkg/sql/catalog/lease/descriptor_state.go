// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package lease

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
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
	expensiveLogEnabled := log.ExpensiveLogEnabled(ctx, 2)
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
			if !desc.hasExpired(ctx, timestamp) {
				// Existing valid descriptor version.
				desc.incRefCount(ctx, expensiveLogEnabled)
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
// it and returns it. The regionEnumPrefix is used if the cluster is configured
// for multi-region system tables.
func (t *descriptorState) upsertLeaseLocked(
	ctx context.Context,
	desc catalog.Descriptor,
	session sqlliveness.Session,
	regionEnumPrefix []byte,
) error {
	if t.mu.maxVersionSeen < desc.GetVersion() {
		t.mu.maxVersionSeen = desc.GetVersion()
	}
	s := t.mu.active.find(desc.GetVersion())
	if s == nil {
		if t.mu.active.findNewest() != nil {
			log.Infof(ctx, "new lease: %s", desc)
		}
		descState := newDescriptorVersionState(t, desc, hlc.Timestamp{}, session, regionEnumPrefix, true /* isLease */)
		t.mu.active.insert(descState)
		t.m.names.insert(ctx, descState)
		return nil
	}
	// If the version already exists and the session ID matches nothing
	// needs to be done.
	if s.getSessionID() == session.ID() {
		return nil
	}

	// Otherwise, we need to update the existing lease to fix the session ID. The
	// previously stored lease also needs to be deleted.
	var existingLease storedLease
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		existingLease = *s.mu.lease
		s.mu.lease.sessionID = session.ID().UnsafeBytes()
		s.mu.session = session
	}()
	// Delete the existing lease on behalf of the caller.
	t.m.storage.release(ctx, t.m.stopper, &existingLease)
	return nil
}

var _ redact.SafeFormatter = (*descriptorVersionState)(nil)

func newDescriptorVersionState(
	t *descriptorState,
	desc catalog.Descriptor,
	expiration hlc.Timestamp,
	session sqlliveness.Session,
	prefix []byte,
	isLease bool,
) *descriptorVersionState {
	descState := &descriptorVersionState{
		t:          t,
		Descriptor: desc,
	}
	if isLease {
		descState.mu.lease = &storedLease{
			id:      desc.GetID(),
			prefix:  prefix,
			version: int(desc.GetVersion()),
		}
		descState.mu.lease.sessionID = session.ID().UnsafeBytes()
		if buildutil.CrdbTestBuild && !expiration.IsEmpty() {
			panic(errors.AssertionFailedf("expiration should always be empty for "+
				"session based leases (got: %s on Desc: %s(%d))", expiration.String(), desc.GetName(), desc.GetID()))
		}
	}
	descState.mu.session = session
	descState.mu.expiration = expiration

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
	expensiveLoggingEnabled := log.ExpensiveLogEnabled(ctx, 2)
	decRefCount := func(s *descriptorVersionState) (shouldRemove bool) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.mu.refcount--
		if expensiveLoggingEnabled {
			log.Infof(ctx, "release: %s", s.stringLocked())
		}
		return s.mu.refcount == 0
	}
	maybeMarkRemoveStoredLease := func(s *descriptorVersionState) *storedLease {
		// Figure out if we'd like to remove the lease from the store asap (i.e.
		// when the refcount drops to 0). If so, we'll need to mark the lease as
		// invalid.
		removeOnceDereferenced :=
			// Release from the store if the descriptor has been dropped or taken
			// offline.
			t.mu.takenOffline ||
				// Release from the store if the lease is not for the latest
				// version; only leases for the latest version can be acquired.
				s != t.mu.active.findNewest() ||
				s.GetVersion() < t.mu.maxVersionSeen ||
				t.m.removeOnceDereferenced()
		if !removeOnceDereferenced {
			return nil
		}
		s.mu.Lock()
		defer s.mu.Unlock()
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
		if shouldRemove := decRefCount(s); !shouldRemove {
			return nil
		}
		t.mu.Lock()
		defer t.mu.Unlock()
		if l := maybeMarkRemoveStoredLease(s); l != nil {
			leaseReleased := true
			// For testing, we will synchronously release leases, but that
			// exposes us to the danger of the context getting cancelled. To
			// eliminate this risk, we are going first remove the lease from
			// storage and then delete if from mqemory.
			if t.m.storage.testingKnobs.RemoveOnceDereferenced {
				leaseReleased = releaseLease(ctx, l, t.m)
				l = nil
			}
			if leaseReleased {
				t.mu.active.remove(s)
			}
			return l
		}
		return nil
	}
	if l := maybeRemoveLease(); l != nil {
		releaseLease(ctx, l, t.m)
	}
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
