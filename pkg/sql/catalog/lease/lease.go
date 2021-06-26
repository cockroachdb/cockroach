// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package lease provides functionality to create and manage sql schema leases.
package lease

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
)

var errRenewLease = errors.New("renew lease on id")
var errReadOlderVersion = errors.New("read older descriptor version from store")

// LeaseDuration controls the duration of sql descriptor leases.
var LeaseDuration = settings.RegisterDurationSetting(
	"sql.catalog.descriptor_lease_duration",
	"mean duration of sql descriptor leases, this actual duration is jitterred",
	base.DefaultDescriptorLeaseDuration)

func between0and1inclusive(f float64) error {
	if f < 0 || f > 1 {
		return errors.Errorf("value %f must be between 0 and 1", f)
	}
	return nil
}

// LeaseJitterFraction controls the percent jitter around sql lease durations
var LeaseJitterFraction = settings.RegisterFloatSetting(
	"sql.catalog.descriptor_lease_jitter_fraction",
	"mean duration of sql descriptor leases, this actual duration is jitterred",
	base.DefaultDescriptorLeaseJitterFraction,
	between0and1inclusive)

// WaitForNoVersion returns once there are no unexpired leases left
// for any version of the descriptor.
func (m *Manager) WaitForNoVersion(
	ctx context.Context, id descpb.ID, retryOpts retry.Options,
) error {
	for lastCount, r := 0, retry.Start(retryOpts); r.Next(); {
		// Check to see if there are any leases that still exist on the previous
		// version of the descriptor.
		now := m.storage.clock.Now()
		stmt := fmt.Sprintf(`SELECT count(1) FROM system.public.lease AS OF SYSTEM TIME '%s' WHERE ("descID" = %d AND expiration > $1)`,
			now.AsOfSystemTime(),
			id)
		values, err := m.storage.internalExecutor.QueryRowEx(
			ctx, "count-leases", nil, /* txn */
			sessiondata.InternalExecutorOverride{User: security.RootUserName()},
			stmt, now.GoTime(),
		)
		if err != nil {
			return err
		}
		if values == nil {
			return errors.New("failed to count leases")
		}
		count := int(tree.MustBeDInt(values[0]))
		if count == 0 {
			break
		}
		if count != lastCount {
			lastCount = count
			log.Infof(ctx, "waiting for %d leases to expire: desc=%d", count, id)
		}
	}
	return nil
}

// WaitForOneVersion returns once there are no unexpired leases on the
// previous version of the descriptor. It returns the current version.
// After returning there can only be versions of the descriptor >= to the
// returned version. Lease acquisition (see acquire()) maintains the
// invariant that no new leases for desc.Version-1 will be granted once
// desc.Version exists.
func (m *Manager) WaitForOneVersion(
	ctx context.Context, id descpb.ID, retryOpts retry.Options,
) (descpb.DescriptorVersion, error) {
	var version descpb.DescriptorVersion
	for lastCount, r := 0, retry.Start(retryOpts); r.Next(); {
		var desc catalog.Descriptor
		if err := m.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
			desc, err = catalogkv.MustGetDescriptorByID(ctx, txn, m.Codec(), id)
			return err
		}); err != nil {
			return 0, err
		}

		// Check to see if there are any leases that still exist on the previous
		// version of the descriptor.
		now := m.storage.clock.Now()
		descs := []IDVersion{NewIDVersionPrev(desc.GetName(), desc.GetID(), desc.GetVersion())}
		version = desc.GetVersion()
		count, err := CountLeases(ctx, m.storage.internalExecutor, descs, now)
		if err != nil {
			return 0, err
		}
		if count == 0 {
			break
		}
		if count != lastCount {
			lastCount = count
			log.Infof(ctx, "waiting for %d leases to expire: desc=%v", count, descs)
		}
	}
	return version, nil
}

// IDVersion represents a descriptor ID, version pair that are
// meant to map to a single immutable descriptor.
type IDVersion struct {
	// Name is only provided for pretty printing.
	Name    string
	ID      descpb.ID
	Version descpb.DescriptorVersion
}

// NewIDVersionPrev returns an initialized IDVersion with the
// previous version of the descriptor.
func NewIDVersionPrev(name string, id descpb.ID, currVersion descpb.DescriptorVersion) IDVersion {
	return IDVersion{Name: name, ID: id, Version: currVersion - 1}
}

// ensureVersion ensures that the latest version >= minVersion. It will
// check if the latest known version meets the criterion, or attempt to
// acquire a lease at the latest version with the hope that it meets
// the criterion.
func ensureVersion(
	ctx context.Context, id descpb.ID, minVersion descpb.DescriptorVersion, m *Manager,
) error {
	if s := m.findNewest(id); s != nil && minVersion <= s.GetVersion() {
		return nil
	}

	if err := m.AcquireFreshestFromStore(ctx, id); err != nil {
		return err
	}

	if s := m.findNewest(id); s != nil && s.GetVersion() < minVersion {
		return errors.Errorf("version %d for descriptor %s does not exist yet", minVersion, s.GetName())
	}
	return nil
}

type historicalDescriptor struct {
	desc       catalog.Descriptor
	expiration hlc.Timestamp // ModificationTime of the next descriptor
}

// Read an older descriptor version for the particular timestamp
// from the store. We unfortunately need to read more than one descriptor
// version just so that we can set the expiration time on the descriptor
// properly.
//
// TODO(vivek): Future work:
// 1. Read multiple versions of a descriptor through one kv call.
// 2. Translate multiple simultaneous calls to this method into a single call
//    as is done for acquireNodeLease().
// 3. Figure out a sane policy on when these descriptors should be purged.
//    They are currently purged in PurgeOldVersions.
func (m *Manager) readOlderVersionForTimestamp(
	ctx context.Context, id descpb.ID, timestamp hlc.Timestamp,
) ([]historicalDescriptor, error) {
	expiration, done := func() (hlc.Timestamp, bool) {
		t := m.findDescriptorState(id, false /* create */)
		t.mu.Lock()
		defer t.mu.Unlock()
		afterIdx := 0
		// Walk back the versions to find one that is valid for the timestamp.
		for i := len(t.mu.active.data) - 1; i >= 0; i-- {
			// Check to see if the ModificationTime is valid.
			if desc := t.mu.active.data[i]; desc.GetModificationTime().LessEq(timestamp) {
				if expiration := desc.getExpiration(); timestamp.Less(expiration) {
					// Existing valid descriptor version.
					return expiration, true
				}
				// We need a version after data[i], but before data[i+1].
				// We could very well use the timestamp to read the
				// descriptor, but unfortunately we will not be able to assign
				// it a proper expiration time. Therefore, we read
				// descriptor versions one by one from afterIdx back into the
				// past until we find a valid one.
				afterIdx = i + 1
				break
			}
		}

		if afterIdx == len(t.mu.active.data) {
			return hlc.Timestamp{}, true
		}

		// Read descriptor versions one by one into the past until we
		// find a valid one. Every version is assigned an expiration time that
		// is the ModificationTime of the previous one read.
		return t.mu.active.data[afterIdx].GetModificationTime(), false
	}()
	if done {
		return nil, nil
	}

	// Read descriptors from the store.
	var versions []historicalDescriptor
	for {
		desc, err := m.storage.getForExpiration(ctx, expiration, id)
		if err != nil {
			return nil, err
		}
		versions = append(versions, historicalDescriptor{
			desc:       desc,
			expiration: expiration,
		})
		if desc.GetModificationTime().LessEq(timestamp) {
			break
		}
		// Set the expiration time for the next descriptor.
		expiration = desc.GetModificationTime()
	}

	return versions, nil
}

// Insert descriptor versions. The versions provided are not in
// any particular order.
func (m *Manager) insertDescriptorVersions(id descpb.ID, versions []historicalDescriptor) {
	t := m.findDescriptorState(id, false /* create */)
	t.mu.Lock()
	defer t.mu.Unlock()
	for i := range versions {
		// Since we gave up the lock while reading the versions from
		// the store we have to ensure that no one else inserted the
		// same version.
		existingVersion := t.mu.active.findVersion(versions[i].desc.GetVersion())
		if existingVersion == nil {
			t.mu.active.insert(
				newDescriptorVersionState(t, versions[i].desc, versions[i].expiration, false))
		}
	}
}

// AcquireFreshestFromStore acquires a new lease from the store and
// inserts it into the active set. It guarantees that the lease returned is
// the one acquired after the call is made. Use this if the lease we want to
// get needs to see some descriptor updates that we know happened recently.
func (m *Manager) AcquireFreshestFromStore(ctx context.Context, id descpb.ID) error {
	// Create descriptorState if needed.
	_ = m.findDescriptorState(id, true /* create */)
	// We need to acquire a lease on a "fresh" descriptor, meaning that joining
	// a potential in-progress lease acquisition is generally not good enough.
	// If we are to join an in-progress acquisition, it needs to be an acquisition
	// initiated after this point.
	// So, we handle two cases:
	// 1. The first DoChan() call tells us that we didn't join an in-progress
	//     acquisition. Great, the lease that's being acquired is good.
	// 2. The first DoChan() call tells us that we did join an in-progress acq.
	//     We have to wait this acquisition out; it's not good for us. But any
	//     future acquisition is good, so the next time around the loop it doesn't
	//     matter if we initiate a request or join an in-progress one.
	// In both cases, we need to check if the lease we want is still valid because
	// lease acquisition is done without holding the descriptorState lock, so anything
	// can happen in between lease acquisition and us getting control again.
	attemptsMade := 0
	for {
		// Acquire a fresh lease.
		didAcquire, err := acquireNodeLease(ctx, m, id)
		if m.testingKnobs.LeaseStoreTestingKnobs.LeaseAcquireResultBlockEvent != nil {
			m.testingKnobs.LeaseStoreTestingKnobs.LeaseAcquireResultBlockEvent(AcquireFreshestBlock)
		}
		if err != nil {
			return err
		}

		if didAcquire {
			// Case 1: we didn't join an in-progress call and the lease is still
			// valid.
			break
		} else if attemptsMade > 1 {
			// Case 2: more than one acquisition has happened and the lease is still
			// valid.
			break
		}
		attemptsMade++
	}
	return nil
}

// If the lease cannot be obtained because the descriptor is in the process of
// being dropped or offline, the error will be of type inactiveTableError.
// The boolean returned is true if this call was actually responsible for the
// lease acquisition.
func acquireNodeLease(ctx context.Context, m *Manager, id descpb.ID) (bool, error) {
	var toRelease *storedLease
	resultChan, didAcquire := m.storage.group.DoChan(fmt.Sprintf("acquire%d", id), func() (interface{}, error) {
		// Note that we use a new `context` here to avoid a situation where a cancellation
		// of the first context cancels other callers to the `acquireNodeLease()` method,
		// because of its use of `singleflight.Group`. See issue #41780 for how this has
		// happened.
		newCtx, cancel := m.stopper.WithCancelOnQuiesce(logtags.WithTags(context.Background(), logtags.FromContext(ctx)))
		defer cancel()
		if m.isDraining() {
			return nil, errors.New("cannot acquire lease when draining")
		}
		newest := m.findNewest(id)
		var minExpiration hlc.Timestamp
		if newest != nil {
			minExpiration = newest.getExpiration()
		}
		desc, expiration, err := m.storage.acquire(newCtx, minExpiration, id)
		if err != nil {
			return nil, err
		}
		t := m.findDescriptorState(id, false /* create */)
		t.mu.Lock()
		t.mu.takenOffline = false
		defer t.mu.Unlock()
		var newDescVersionState *descriptorVersionState
		newDescVersionState, toRelease, err = t.upsertLeaseLocked(newCtx, desc, expiration)
		if err != nil {
			return nil, err
		}
		if newDescVersionState != nil {
			m.names.insert(newDescVersionState)
		}
		if toRelease != nil {
			releaseLease(toRelease, m)
		}
		return true, nil
	})
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case result := <-resultChan:
		if result.Err != nil {
			return false, result.Err
		}
	}
	return didAcquire, nil
}

// releaseLease from store.
func releaseLease(lease *storedLease, m *Manager) {
	ctx := context.TODO()
	if m.isDraining() {
		// Release synchronously to guarantee release before exiting.
		m.storage.release(ctx, m.stopper, lease)
		return
	}

	// Release to the store asynchronously, without the descriptorState lock.
	if err := m.stopper.RunAsyncTask(
		ctx, "sql.descriptorState: releasing descriptor lease",
		func(ctx context.Context) {
			m.storage.release(ctx, m.stopper, lease)
		}); err != nil {
		log.Warningf(ctx, "error: %s, not releasing lease: %q", err, lease)
	}
}

// purgeOldVersions removes old unused descriptor versions older than
// minVersion and releases any associated leases.
// If takenOffline is set, minVersion is ignored; no lease is acquired and all
// existing unused versions are removed. The descriptor is further marked dropped,
// which will cause existing in-use leases to be eagerly released once
// they're not in use any more.
// If t has no active leases, nothing is done.
func purgeOldVersions(
	ctx context.Context,
	db *kv.DB,
	id descpb.ID,
	dropped bool,
	minVersion descpb.DescriptorVersion,
	m *Manager,
) error {
	t := m.findDescriptorState(id, false /*create*/)
	if t == nil {
		return nil
	}
	t.mu.Lock()
	if t.mu.maxVersionSeen < minVersion {
		t.mu.maxVersionSeen = minVersion
	}
	empty := len(t.mu.active.data) == 0 && t.mu.acquisitionsInProgress == 0
	t.mu.Unlock()
	if empty && !dropped {
		// We don't currently have a version on this descriptor, so no need to refresh
		// anything.
		return nil
	}

	removeInactives := func(dropped bool) {
		t.mu.Lock()
		t.mu.takenOffline = dropped
		leases := t.removeInactiveVersions()
		t.mu.Unlock()
		for _, l := range leases {
			releaseLease(l, m)
		}
	}

	if dropped {
		removeInactives(true /* dropped */)
		return nil
	}

	if err := ensureVersion(ctx, id, minVersion, m); err != nil {
		return err
	}

	// Acquire a refcount on the descriptor on the latest version to maintain an
	// active lease, so that it doesn't get released when removeInactives()
	// is called below. Release this lease after calling removeInactives().
	desc, _, err := t.findForTimestamp(ctx, m.storage.clock.Now())
	if isInactive := catalog.HasInactiveDescriptorError(err); err == nil || isInactive {
		removeInactives(isInactive)
		if desc != nil {
			t.release(ctx, desc)
			return nil
		}
		return nil
	}
	return err
}

// AcquireBlockType is the type of blocking result event when
// calling LeaseAcquireResultBlockEvent.
type AcquireBlockType int

const (
	// AcquireBlock denotes the LeaseAcquireResultBlockEvent is
	// coming from descriptorState.acquire().
	AcquireBlock AcquireBlockType = iota
	// AcquireFreshestBlock denotes the LeaseAcquireResultBlockEvent is
	// from descriptorState.acquireFreshestFromStore().
	AcquireFreshestBlock
)

// Manager manages acquiring and releasing per-descriptor leases. It also
// handles resolving descriptor names to descriptor IDs. The leases are managed
// internally with a descriptor and expiration time exported by the
// API. The descriptor acquired needs to be released. A transaction
// can use a descriptor as long as its timestamp is within the
// validity window for the descriptor:
// descriptor.ModificationTime <= txn.ReadTimestamp < expirationTime
//
// Exported only for testing.
//
// The locking order is:
// Manager.mu > descriptorState.mu > nameCache.mu > descriptorVersionState.mu
type Manager struct {
	rangeFeedFactory *rangefeed.Factory
	storage          storage
	mu               struct {
		syncutil.Mutex
		descriptors map[descpb.ID]*descriptorState

		// updatesResolvedTimestamp keeps track of a timestamp before which all
		// descriptor updates have already been seen.
		updatesResolvedTimestamp hlc.Timestamp
	}

	draining atomic.Value

	// names is a cache for name -> id mappings. A mapping for the cache
	// should only be used if we currently have an active lease on the respective
	// id; otherwise, the mapping may well be stale.
	// Not protected by mu.
	names        nameCache
	testingKnobs ManagerTestingKnobs
	ambientCtx   log.AmbientContext
	stopper      *stop.Stopper
	sem          *quotapool.IntPool
}

const leaseConcurrencyLimit = 5

// NewLeaseManager creates a new Manager.
//
// internalExecutor can be nil to help bootstrapping, but then it needs to be set via
// SetInternalExecutor before the Manager is used.
//
// stopper is used to run async tasks. Can be nil in tests.
func NewLeaseManager(
	ambientCtx log.AmbientContext,
	nodeIDContainer *base.SQLIDContainer,
	db *kv.DB,
	clock *hlc.Clock,
	internalExecutor sqlutil.InternalExecutor,
	settings *cluster.Settings,
	codec keys.SQLCodec,
	testingKnobs ManagerTestingKnobs,
	stopper *stop.Stopper,
	rangeFeedFactory *rangefeed.Factory,
) *Manager {
	lm := &Manager{
		storage: storage{
			nodeIDContainer:  nodeIDContainer,
			db:               db,
			clock:            clock,
			internalExecutor: internalExecutor,
			settings:         settings,
			codec:            codec,
			group:            &singleflight.Group{},
			testingKnobs:     testingKnobs.LeaseStoreTestingKnobs,
			outstandingLeases: metric.NewGauge(metric.Metadata{
				Name:        "sql.leases.active",
				Help:        "The number of outstanding SQL schema leases.",
				Measurement: "Outstanding leases",
				Unit:        metric.Unit_COUNT,
			}),
		},
		rangeFeedFactory: rangeFeedFactory,
		testingKnobs:     testingKnobs,
		names:            makeNameCache(),
		ambientCtx:       ambientCtx,
		stopper:          stopper,
		sem:              quotapool.NewIntPool("lease manager", leaseConcurrencyLimit),
	}
	lm.stopper.AddCloser(lm.sem.Closer("stopper"))
	lm.mu.descriptors = make(map[descpb.ID]*descriptorState)
	lm.mu.updatesResolvedTimestamp = db.Clock().Now()

	lm.draining.Store(false)
	return lm
}

// NameMatchesDescriptor returns true if the provided name and IDs match this
// descriptor.
func NameMatchesDescriptor(
	desc catalog.Descriptor, parentID descpb.ID, parentSchemaID descpb.ID, name string,
) bool {
	return desc.GetParentID() == parentID &&
		desc.GetParentSchemaID() == parentSchemaID &&
		desc.GetName() == name
}

// findNewest returns the newest descriptor version state for the ID.
func (m *Manager) findNewest(id descpb.ID) *descriptorVersionState {
	t := m.findDescriptorState(id, false /* create */)
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.mu.active.findNewest()
}

// AcquireByName returns a version for the specified descriptor valid for
// the timestamp. It returns the descriptor and a expiration time.
// A transaction using this descriptor must ensure that its
// commit-timestamp < expiration-time. Care must be taken to not modify
// the returned descriptor. Renewal of a lease may begin in the
// background. Renewal is done in order to prevent blocking on future
// acquisitions.
//
// Known limitation: AcquireByName() calls Acquire() and therefore suffers
// from the same limitation as Acquire (See Acquire). AcquireByName() is
// unable to function correctly on a timestamp less than the timestamp
// of a transaction with a DROP/TRUNCATE on the descriptor. The limitation in
// the face of a DROP follows directly from the limitation on Acquire().
// A TRUNCATE is implemented by changing the name -> id mapping
// and by dropping the descriptor with the old id. While AcquireByName
// can use the timestamp and get the correct name->id  mapping at a
// timestamp, it uses Acquire() to get a descriptor with the corresponding
// id and fails because the id has been dropped by the TRUNCATE.
func (m *Manager) AcquireByName(
	ctx context.Context,
	timestamp hlc.Timestamp,
	parentID descpb.ID,
	parentSchemaID descpb.ID,
	name string,
) (LeasedDescriptor, error) {
	// When offline descriptor leases were not allowed to be cached,
	// attempt to acquire a lease on them would generate a descriptor
	// offline error. Recent changes allow offline descriptor leases
	// to be cached, but callers still need the offline error generated.
	// This logic will release the lease (the lease manager will still
	// cache it), and generate the offline descriptor error.
	validateDescriptorForReturn := func(desc LeasedDescriptor) (LeasedDescriptor, error) {
		if desc.Underlying().Offline() {
			if err := catalog.FilterDescriptorState(
				desc.Underlying(), tree.CommonLookupFlags{},
			); err != nil {
				desc.Release(ctx)
				return nil, err
			}
		}
		return desc, nil
	}
	// Check if we have cached an ID for this name.
	descVersion := m.names.get(ctx, parentID, parentSchemaID, name, timestamp)
	if descVersion != nil {
		if descVersion.GetModificationTime().LessEq(timestamp) {
			expiration := descVersion.getExpiration()
			// If this lease is nearly expired, ensure a renewal is queued.
			durationUntilExpiry := time.Duration(expiration.WallTime - timestamp.WallTime)
			if durationUntilExpiry < m.storage.leaseRenewalTimeout() {
				if t := m.findDescriptorState(descVersion.GetID(), false /* create */); t != nil {
					if err := t.maybeQueueLeaseRenewal(
						ctx, m, descVersion.GetID(), name); err != nil {
						return nil, err
					}
				}
			}
			return validateDescriptorForReturn(descVersion)
		}
		// m.names.get() incremented the refcount, we decrement it to get a new
		// version.
		descVersion.Release(ctx)
		// Return a valid descriptor for the timestamp.
		leasedDesc, err := m.Acquire(ctx, timestamp, descVersion.GetID())
		if err != nil {
			return nil, err
		}
		return validateDescriptorForReturn(leasedDesc)
	}

	// We failed to find something in the cache, or what we found is not
	// guaranteed to be valid by the time we use it because we don't have a
	// lease with at least a bit of lifetime left in it. So, we do it the hard
	// way: look in the database to resolve the name, then acquire a new lease.
	var err error
	id, err := m.resolveName(ctx, timestamp, parentID, parentSchemaID, name)
	if err != nil {
		return nil, err
	}
	desc, err := m.Acquire(ctx, timestamp, id)
	if err != nil {
		return nil, err
	}
	if !NameMatchesDescriptor(desc.Underlying(), parentID, parentSchemaID, name) {
		// We resolved name `name`, but the lease has a different name in it.
		// That can mean two things. Assume the descriptor is being renamed from A to B.
		// a) `name` is A. The transaction doing the RENAME committed (so the
		// descriptor has been updated to B), but its schema changer has not
		// finished yet. B is the new name of the descriptor, queries should use that. If
		// we already had a lease with name A, we would've allowed to use it (but we
		// don't, otherwise the cache lookup above would've given it to us).  Since
		// we don't, let's not allow A to be used, given that the lease now has name
		// B in it. It'd be sketchy to allow A to be used with an inconsistent name
		// in the descriptor.
		//
		// b) `name` is B. Like in a), the transaction doing the RENAME
		// committed (so the descriptor has been updated to B), but its schema
		// change has not finished yet. We still had a valid lease with name A in
		// it. What to do, what to do? We could allow name B to be used, but who
		// knows what consequences that would have, since its not consistent with
		// the descriptor. We could say "descriptor B not found", but that means that, until
		// the next gossip update, this node would not service queries for this
		// descriptor under the name B. That's no bueno, as B should be available to be
		// used immediately after the RENAME transaction has committed.
		// The problem is that we have a lease that we know is stale (the descriptor
		// in the DB doesn't necessarily have a new version yet, but it definitely
		// has a new name). So, lets force getting a fresh descriptor.
		// This case (modulo the "committed" part) also applies when the txn doing a
		// RENAME had a lease on the old name, and then tries to use the new name
		// after the RENAME statement.
		//
		// How do we disambiguate between the a) and b)? We get a fresh lease on
		// the descriptor, as required by b), and then we'll know if we're trying to
		// resolve the current or the old name.
		//
		// TODO(vivek): check if the entire above comment is indeed true. Review the
		// use of NameMatchesDescriptor() throughout this function.
		desc.Release(ctx)
		if err := m.AcquireFreshestFromStore(ctx, id); err != nil {
			return nil, err
		}
		desc, err = m.Acquire(ctx, timestamp, id)
		if err != nil {
			return nil, err
		}
		if !NameMatchesDescriptor(desc.Underlying(), parentID, parentSchemaID, name) {
			// If the name we had doesn't match the newest descriptor in the DB, then
			// we're trying to use an old name.
			desc.Release(ctx)
			return nil, catalog.ErrDescriptorNotFound
		}
	}
	return validateDescriptorForReturn(desc)
}

// resolveName resolves a descriptor name to a descriptor ID at a particular
// timestamp by looking in the database. If the mapping is not found,
// catalog.ErrDescriptorNotFound is returned.
func (m *Manager) resolveName(
	ctx context.Context,
	timestamp hlc.Timestamp,
	parentID descpb.ID,
	parentSchemaID descpb.ID,
	name string,
) (descpb.ID, error) {
	id := descpb.InvalidID
	if err := m.storage.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Run the name lookup as high-priority, thereby pushing any intents out of
		// its way. We don't want schema changes to prevent name resolution/lease
		// acquisitions; we'd rather force them to refresh. Also this prevents
		// deadlocks in cases where the name resolution is triggered by the
		// transaction doing the schema change itself.
		if err := txn.SetUserPriority(roachpb.MaxUserPriority); err != nil {
			return err
		}
		txn.SetFixedTimestamp(ctx, timestamp)
		var found bool
		var err error
		found, id, err = catalogkv.LookupObjectID(ctx, txn, m.storage.codec, parentID, parentSchemaID, name)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		return nil
	}); err != nil {
		return id, err
	}
	if id == descpb.InvalidID {
		return id, catalog.ErrDescriptorNotFound
	}
	return id, nil
}

// LeasedDescriptor tracks and manages leasing related
// information for a descriptor.
type LeasedDescriptor interface {
	catalog.NameEntry

	// Underlying returns the underlying descriptor which has been leased.
	// The implementation of the methods on this object delegate to
	// that object.
	Underlying() catalog.Descriptor

	// Expiration returns the current expiration. Subsequent calls may return a
	// later timestamp but will never return an earlier one.
	Expiration() hlc.Timestamp

	// Release releases the reference to this leased descriptor. The descriptor
	// should not be used after the lease has been released.
	Release(ctx context.Context)
}

// Acquire acquires a read lease for the specified descriptor ID valid for
// the timestamp. It returns the descriptor and a expiration time.
// A transaction using this descriptor must ensure that its
// commit-timestamp < expiration-time. Care must be taken to not modify
// the returned descriptor.
//
// Known limitation: Acquire() can return an error after the descriptor with
// the ID has been dropped. This is true even when using a timestamp
// less than the timestamp of the DROP command. This is because Acquire
// can only return an older version of a descriptor if the latest version
// can be leased; as it stands a dropped descriptor cannot be leased.
func (m *Manager) Acquire(
	ctx context.Context, timestamp hlc.Timestamp, id descpb.ID,
) (LeasedDescriptor, error) {
	for {
		t := m.findDescriptorState(id, true /*create*/)
		desc, latest, err := t.findForTimestamp(ctx, timestamp)
		if err == nil {
			// If the latest lease is nearly expired, ensure a renewal is queued.
			if latest {
				durationUntilExpiry := time.Duration(desc.getExpiration().WallTime - timestamp.WallTime)
				if durationUntilExpiry < m.storage.leaseRenewalTimeout() {
					if err := t.maybeQueueLeaseRenewal(ctx, m, id, desc.GetName()); err != nil {
						return nil, err
					}
				}
			}
			return desc, nil
		}
		switch {
		case errors.Is(err, errRenewLease):
			if err := func() error {
				t.markAcquisitionStart(ctx)
				defer t.markAcquisitionDone(ctx)
				// Renew lease and retry. This will block until the lease is acquired.
				_, errLease := acquireNodeLease(ctx, m, id)
				return errLease
			}(); err != nil {
				return nil, err
			}

			if m.testingKnobs.LeaseStoreTestingKnobs.LeaseAcquireResultBlockEvent != nil {
				m.testingKnobs.LeaseStoreTestingKnobs.LeaseAcquireResultBlockEvent(AcquireBlock)
			}

		case errors.Is(err, errReadOlderVersion):
			// Read old versions from the store. This can block while reading.
			versions, errRead := m.readOlderVersionForTimestamp(ctx, id, timestamp)
			if errRead != nil {
				return nil, errRead
			}
			m.insertDescriptorVersions(id, versions)

		default:
			return nil, err
		}
	}
}

// removeOnceDereferenced returns true if the Manager thinks
// a descriptorVersionState can be removed after its refcount goes to 0.
func (m *Manager) removeOnceDereferenced() bool {
	return m.storage.testingKnobs.RemoveOnceDereferenced ||
		// Release from the store if the Manager is draining.
		m.isDraining()
}

func (m *Manager) isDraining() bool {
	return m.draining.Load().(bool)
}

// SetDraining (when called with 'true') removes all inactive leases. Any leases
// that are active will be removed once the lease's reference count drops to 0.
//
// The reporter callback, if non-nil, is called on a best effort basis
// to report work that needed to be done and which may or may not have
// been done by the time this call returns. See the explanation in
// pkg/server/drain.go for details.
func (m *Manager) SetDraining(drain bool, reporter func(int, redact.SafeString)) {
	m.draining.Store(drain)
	if !drain {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range m.mu.descriptors {
		t.mu.Lock()
		leases := t.removeInactiveVersions()
		t.mu.Unlock()
		for _, l := range leases {
			releaseLease(l, m)
		}
		if reporter != nil {
			// Report progress through the Drain RPC.
			reporter(len(leases), "descriptor leases")
		}
	}
}

// If create is set, cache and stopper need to be set as well.
func (m *Manager) findDescriptorState(id descpb.ID, create bool) *descriptorState {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.mu.descriptors[id]
	if t == nil && create {
		t = &descriptorState{m: m, id: id, stopper: m.stopper}
		m.mu.descriptors[id] = t
	}
	return t
}

// RefreshLeases starts a goroutine that refreshes the lease manager
// leases for descriptors received in the latest system configuration via gossip or
// rangefeeds. This function must be passed a non-nil gossip if
// RangefeedLeases is not active.
func (m *Manager) RefreshLeases(ctx context.Context, s *stop.Stopper, db *kv.DB) {
	descUpdateCh := make(chan *descpb.Descriptor)
	m.watchForUpdates(ctx, descUpdateCh)
	_ = s.RunAsyncTask(ctx, "refresh-leases", func(ctx context.Context) {
		for {
			select {
			case desc := <-descUpdateCh:
				// NB: We allow nil descriptors to be sent to synchronize the updating of
				// descriptors.
				if desc == nil {
					continue
				}

				if evFunc := m.testingKnobs.TestingDescriptorUpdateEvent; evFunc != nil {
					if err := evFunc(desc); err != nil {
						log.Infof(ctx, "skipping update of %v due to knob: %v",
							desc, err)
						continue
					}
				}

				id, version, name, state, _, err := descpb.GetDescriptorMetadata(desc)
				if err != nil {
					log.Fatalf(ctx, "invalid descriptor %v: %v", desc, err)
				}
				dropped := state == descpb.DescriptorState_DROP
				// Try to refresh the lease to one >= this version.
				log.VEventf(ctx, 2, "purging old version of descriptor %d@%d (dropped %v)",
					id, version, dropped)
				if err := purgeOldVersions(ctx, db, id, dropped, version, m); err != nil {
					log.Warningf(ctx, "error purging leases for descriptor %d(%s): %s",
						id, name, err)
				}

				if evFunc := m.testingKnobs.TestingDescriptorRefreshedEvent; evFunc != nil {
					evFunc(desc)
				}

			case <-s.ShouldQuiesce():
				return
			}
		}
	})
}

// watchForUpdates will watch a rangefeed on the system.descriptor table for
// updates.
func (m *Manager) watchForUpdates(ctx context.Context, descUpdateCh chan<- *descpb.Descriptor) {
	if log.V(1) {
		log.Infof(ctx, "using rangefeeds for lease manager updates")
	}
	descriptorTableStart := m.Codec().TablePrefix(keys.DescriptorTableID)
	descriptorTableSpan := roachpb.Span{
		Key:    descriptorTableStart,
		EndKey: descriptorTableStart.PrefixEnd(),
	}
	handleEvent := func(
		ctx context.Context, ev *roachpb.RangeFeedValue,
	) {
		if len(ev.Value.RawBytes) == 0 {
			return
		}
		var descriptor descpb.Descriptor
		if err := ev.Value.GetProto(&descriptor); err != nil {
			logcrash.ReportOrPanic(ctx, &m.storage.settings.SV,
				"%s: unable to unmarshal descriptor %v", ev.Key, ev.Value)
			return
		}
		if descriptor.Union == nil {
			return
		}
		descpb.MaybeSetDescriptorModificationTimeFromMVCCTimestamp(&descriptor, ev.Value.Timestamp)
		id, version, name, _, _, err := descpb.GetDescriptorMetadata(&descriptor)
		if err != nil {
			panic(err)
		}
		if log.V(2) {
			log.Infof(ctx, "%s: refreshing lease on descriptor: %d (%s), version: %d",
				ev.Key, id, name, version)
		}
		select {
		case <-ctx.Done():
		case descUpdateCh <- &descriptor:
		}
	}
	// Ignore errors here because they indicate that the server is shutting down.
	// Also note that the range feed automatically shuts down when the server
	// shuts down, so we don't need to call Close() ourselves.
	_, _ = m.rangeFeedFactory.RangeFeed(
		ctx, "lease", descriptorTableSpan, hlc.Timestamp{}, handleEvent,
	)
}

// leaseRefreshLimit is the upper-limit on the number of descriptor leases
// that will continuously have their lease refreshed.
var leaseRefreshLimit = settings.RegisterIntSetting(
	"sql.tablecache.lease.refresh_limit",
	"maximum number of descriptors to periodically refresh leases for",
	500,
)

// PeriodicallyRefreshSomeLeases so that leases are fresh and can serve
// traffic immediately.
// TODO(vivek): Remove once epoch based table leases are implemented.
func (m *Manager) PeriodicallyRefreshSomeLeases(ctx context.Context) {
	_ = m.stopper.RunAsyncTask(ctx, "lease-refresher", func(ctx context.Context) {
		leaseDuration := LeaseDuration.Get(&m.storage.settings.SV)
		if leaseDuration <= 0 {
			return
		}
		refreshTimer := timeutil.NewTimer()
		defer refreshTimer.Stop()
		refreshTimer.Reset(m.storage.jitteredLeaseDuration() / 2)
		for {
			select {
			case <-m.stopper.ShouldQuiesce():
				return

			case <-refreshTimer.C:
				refreshTimer.Read = true
				refreshTimer.Reset(m.storage.jitteredLeaseDuration() / 2)

				m.refreshSomeLeases(ctx)
			}
		}
	})
}

// Refresh some of the current leases.
func (m *Manager) refreshSomeLeases(ctx context.Context) {
	limit := leaseRefreshLimit.Get(&m.storage.settings.SV)
	if limit <= 0 {
		return
	}
	// Construct a list of descriptors needing their leases to be reacquired.
	m.mu.Lock()
	ids := make([]descpb.ID, 0, len(m.mu.descriptors))
	var i int64
	for k, desc := range m.mu.descriptors {
		if i++; i > limit {
			break
		}
		desc.mu.Lock()
		takenOffline := desc.mu.takenOffline
		desc.mu.Unlock()
		if !takenOffline {
			ids = append(ids, k)
		}
	}
	m.mu.Unlock()
	// Limit the number of concurrent lease refreshes.
	var wg sync.WaitGroup
	for i := range ids {
		id := ids[i]
		wg.Add(1)
		if err := m.stopper.RunLimitedAsyncTask(
			ctx, fmt.Sprintf("refresh descriptor: %d lease", id), m.sem, true /*wait*/, func(ctx context.Context) {
				defer wg.Done()
				if _, err := acquireNodeLease(ctx, m, id); err != nil {
					log.Infof(ctx, "refreshing descriptor: %d lease failed: %s", id, err)
				}
			}); err != nil {
			log.Infof(ctx, "didnt refresh descriptor: %d lease: %s", id, err)
			wg.Done()
		}
	}
	wg.Wait()
}

// DeleteOrphanedLeases releases all orphaned leases created by a prior
// instance of this node. timeThreshold is a walltime lower than the
// lowest hlc timestamp that the current instance of the node can use.
func (m *Manager) DeleteOrphanedLeases(timeThreshold int64) {
	if m.testingKnobs.DisableDeleteOrphanedLeases {
		return
	}
	// TODO(asubiotto): clear up the nodeID naming here and in the table below,
	// tracked as https://github.com/cockroachdb/cockroach/issues/48271.
	nodeID := m.storage.nodeIDContainer.SQLInstanceID()
	if nodeID == 0 {
		panic("zero nodeID")
	}

	// Run as async worker to prevent blocking the main server Start method.
	// Exit after releasing all the orphaned leases.
	_ = m.stopper.RunAsyncTask(context.Background(), "del-orphaned-leases", func(ctx context.Context) {
		// This could have been implemented using DELETE WHERE, but DELETE WHERE
		// doesn't implement AS OF SYSTEM TIME.

		// Read orphaned leases.
		sqlQuery := fmt.Sprintf(`
SELECT "descID", version, expiration FROM system.public.lease AS OF SYSTEM TIME %d WHERE "nodeID" = %d
`, timeThreshold, nodeID)
		var rows []tree.Datums
		retryOptions := base.DefaultRetryOptions()
		retryOptions.Closer = m.stopper.ShouldQuiesce()
		// The retry is required because of errors caused by node restarts. Retry 30 times.
		if err := retry.WithMaxAttempts(ctx, retryOptions, 30, func() error {
			var err error
			rows, err = m.storage.internalExecutor.QueryBuffered(
				ctx, "read orphaned leases", nil /*txn*/, sqlQuery,
			)
			return err
		}); err != nil {
			log.Warningf(ctx, "unable to read orphaned leases: %+v", err)
			return
		}

		var wg sync.WaitGroup
		defer wg.Wait()
		for i := range rows {
			// Early exit?
			row := rows[i]
			wg.Add(1)
			lease := storedLease{
				id:         descpb.ID(tree.MustBeDInt(row[0])),
				version:    int(tree.MustBeDInt(row[1])),
				expiration: tree.MustBeDTimestamp(row[2]),
			}
			if err := m.stopper.RunLimitedAsyncTask(
				ctx, fmt.Sprintf("release lease %+v", lease), m.sem, true /*wait*/, func(ctx context.Context) {
					m.storage.release(ctx, m.stopper, &lease)
					log.Infof(ctx, "released orphaned lease: %+v", lease)
					wg.Done()
				}); err != nil {
				wg.Done()
			}
		}
	})
}

// DB returns the Manager's handle to a kv.DB.
func (m *Manager) DB() *kv.DB {
	return m.storage.db
}

// Codec return the Manager's SQLCodec.
func (m *Manager) Codec() keys.SQLCodec {
	return m.storage.codec
}

// Metrics contains a pointer to all relevant lease.Manager metrics, for
// registration.
type Metrics struct {
	OutstandingLeases *metric.Gauge
}

// MetricsStruct returns a struct containing all of this Manager's metrics.
func (m *Manager) MetricsStruct() Metrics {
	return Metrics{
		OutstandingLeases: m.storage.outstandingLeases,
	}
}

// VisitLeases introspects the state of leases managed by the Manager.
//
// TODO(ajwerner): consider refactoring the function to take a struct, maybe
// called LeaseInfo.
func (m *Manager) VisitLeases(
	f func(desc catalog.Descriptor, takenOffline bool, refCount int, expiration tree.DTimestamp) (wantMore bool),
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ts := range m.mu.descriptors {
		visitor := func() (wantMore bool) {
			ts.mu.Lock()
			defer ts.mu.Unlock()

			takenOffline := ts.mu.takenOffline

			for _, state := range ts.mu.active.data {
				state.mu.Lock()
				lease := state.mu.lease
				refCount := state.mu.refcount
				state.mu.Unlock()

				if lease == nil {
					continue
				}

				if !f(state.Descriptor, takenOffline, refCount, lease.expiration) {
					return false
				}
			}
			return true
		}
		if !visitor() {
			return
		}
	}
}
