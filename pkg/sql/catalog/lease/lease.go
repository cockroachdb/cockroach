// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package lease provides functionality to create and manage sql schema leases.
package lease

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/catkv"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	kvstorage "github.com/cockroachdb/cockroach/pkg/storage"
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
	settings.ApplicationLevel,
	"sql.catalog.descriptor_lease_duration",
	"mean duration of sql descriptor leases, this actual duration is jitterred",
	base.DefaultDescriptorLeaseDuration)

// LeaseJitterFraction controls the percent jitter around sql lease durations
var LeaseJitterFraction = settings.RegisterFloatSetting(
	settings.ApplicationLevel,
	"sql.catalog.descriptor_lease_jitter_fraction",
	"mean duration of sql descriptor leases, this actual duration is jitterred",
	base.DefaultDescriptorLeaseJitterFraction,
	settings.Fraction)

// isSystemDatabaseMultiRegion returns if the system database is set-up for
// multi-region.
func (m *Manager) isSystemDatabaseMultiRegion(ctx context.Context) (bool, error) {
	sysDBDesc, err := m.Acquire(ctx, m.storage.clock.Now(), keys.SystemDatabaseID)
	if err != nil {
		return false, err
	}
	defer sysDBDesc.Release(ctx)
	if desc, ok := sysDBDesc.Underlying().(catalog.DatabaseDescriptor); ok && desc.IsMultiRegion() {
		return true, nil
	}
	return false, nil
}

// WaitForNoVersion returns once there are no unexpired leases left
// for any version of the descriptor.
func (m *Manager) WaitForNoVersion(
	ctx context.Context, id descpb.ID, retryOpts retry.Options,
) error {

	for lastCount, r := 0, retry.Start(retryOpts); r.Next(); {
		// Detect if the system database is multi-region.
		isMultiRegion, err := m.isSystemDatabaseMultiRegion(ctx)
		if err != nil {
			return err
		}
		// Check to see if there are any leases that still exist on the previous
		// version of the descriptor.
		now := m.storage.clock.Now()
		singleRegionQuery, singleRegionQueryParam := getSingleRegionClause(ctx, isMultiRegion, m.settings)
		stmt := fmt.Sprintf(`SELECT count(1) FROM system.public.lease AS OF SYSTEM TIME '%s' WHERE ("descID" = %d AND expiration > $1) %s`,
			now.AsOfSystemTime(),
			id,
			singleRegionQuery)
		values, err := m.storage.db.Executor().QueryRowEx(
			ctx, "count-leases", nil, /* txn */
			sessiondata.RootUserSessionDataOverride,
			stmt, now.GoTime(), singleRegionQueryParam,
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
// previous version of the descriptor. It returns the descriptor with the
// current version, though note that it will not be validated or hydrated.
//
// After returning there can only be versions of the descriptor >= to the
// returned version. Lease acquisition (see acquire()) maintains the
// invariant that no new leases for desc.Version-1 will be granted once
// desc.Version exists.
//
// If the descriptor is not found, an error will be returned. The error
// can be detected by using errors.Is(err, catalog.ErrDescriptorNotFound).
func (m *Manager) WaitForOneVersion(
	ctx context.Context, id descpb.ID, retryOpts retry.Options,
) (desc catalog.Descriptor, _ error) {
	for lastCount, r := 0, retry.Start(retryOpts); r.Next(); {
		if err := m.storage.db.KV().Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
			// Use the lower-level MaybeGetDescriptorByIDUnvalidated to avoid
			// performing validation while waiting for leases to drain.
			// Validation is somewhat expensive but more importantly, is not
			// particularly desirable in this context: there are valid cases where
			// descriptors can be removed or made invalid. For instance, the
			// descriptor could be a type or a schema which is dropped by a subsequent
			// concurrent schema change.
			const isDescriptorRequired = false
			cr := m.storage.newCatalogReader(ctx)
			c, err := cr.GetByIDs(ctx, txn, []descpb.ID{id}, isDescriptorRequired, catalog.Any)
			if err != nil {
				return err
			}
			desc = c.LookupDescriptor(id)
			if desc == nil {
				return errors.Wrapf(catalog.ErrDescriptorNotFound, "waiting for leases to drain on descriptor %d", id)
			}
			return nil
		}); err != nil {
			return nil, err
		}
		// Detect if the system database is multi-region.
		isMultiRegion, err := m.isSystemDatabaseMultiRegion(ctx)
		if err != nil {
			return nil, err
		}
		// Check to see if there are any leases that still exist on the previous
		// version of the descriptor.
		descs := []IDVersion{NewIDVersionPrev(desc.GetName(), desc.GetID(), desc.GetVersion())}
		count, err := CountLeases(ctx, m.storage.db.Executor(), isMultiRegion, m.settings, descs, m.storage.clock.Now())
		if err != nil {
			return nil, err
		}
		if count == 0 {
			break
		}
		if count != lastCount {
			lastCount = count
			log.Infof(ctx, "waiting for %d leases to expire: desc=%v", count, descs)
		}
	}
	return desc, nil
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

// Retrieves historical descriptors of given id within the lower and upper bound
// timestamp from the MVCC key range in decreasing modification time order. Any
// descriptor versions that were modified in the range [lower, upper) will be
// retrieved through an export request. A lower bound of an empty timestamp
// hlc.Timestamp{} will result in an error.
//
// In the following scenario v4 is our oldest active lease
// [v1@t1			][v2@t3			][v3@t5				][v4@t7
//
//	[start												end]
//
// getDescriptorsFromStoreForInterval(..., start, end) will get back:
// [v3, v2] (reverse order)
//
// Note that this does not necessarily retrieve a descriptor version that was
// alive at the lower bound timestamp.
func getDescriptorsFromStoreForInterval(
	ctx context.Context,
	db *kv.DB,
	codec keys.SQLCodec,
	id descpb.ID,
	lowerBound, upperBound hlc.Timestamp,
) ([]historicalDescriptor, error) {
	// Ensure lower bound is not an empty timestamp (now).
	if lowerBound.IsEmpty() {
		return nil, errors.AssertionFailedf(
			"getDescriptorsFromStoreForInterval: lower bound cannot be empty")
	}
	// TODO(ajwerner): We'll want to lift this limitation in order to allow this
	// function to find descriptors which could not be found by leasing. This
	// will also require some careful managing of expiration timestamps for the
	// final descriptor.
	if upperBound.IsEmpty() {
		return nil, errors.AssertionFailedf(
			"getDescriptorsFromStoreForInterval: upper bound cannot be empty")
	}

	// Create an export request (1 kv call) for all descriptors for given
	// descriptor ID written during the interval [timestamp, endTimestamp).
	batchRequestHeader := kvpb.Header{
		Timestamp:                   upperBound.Prev(),
		ReturnElasticCPUResumeSpans: true,
	}
	descriptorKey := catalogkeys.MakeDescMetadataKey(codec, id)
	// Unmarshal key span retrieved from export request to construct historical descs.
	var descriptorsRead []historicalDescriptor
	for {
		requestHeader := kvpb.RequestHeader{
			Key:    descriptorKey,
			EndKey: descriptorKey.PrefixEnd(),
		}
		req := &kvpb.ExportRequest{
			RequestHeader: requestHeader,
			StartTime:     lowerBound.Prev(),
			MVCCFilter:    kvpb.MVCCFilter_All,
		}

		// Export request returns descriptors in decreasing modification time.
		res, pErr := kv.SendWrappedWith(ctx, db.NonTransactionalSender(), batchRequestHeader, req)
		if pErr != nil {
			return nil, errors.Wrapf(pErr.GoError(), "error in retrieving descs between %s, %s",
				lowerBound, upperBound)
		}

		// Keep track of the most recently processed descriptor's modification time to
		// set as the expiration for the next descriptor to process. Recall we process
		// descriptors in decreasing modification time.
		subsequentModificationTime := upperBound
		exportResp := res.(*kvpb.ExportResponse)
		for _, file := range exportResp.Files {
			if err := func() error {
				it, err := kvstorage.NewMemSSTIterator(file.SST, false, /* verify */
					kvstorage.IterOptions{
						// NB: We assume there will be no MVCC range tombstones here.
						KeyTypes:   kvstorage.IterKeyTypePointsOnly,
						LowerBound: keys.MinKey,
						UpperBound: keys.MaxKey,
					})
				if err != nil {
					return err
				}
				defer func() {
					if it != nil {
						it.Close()
					}
				}()

				// Convert each MVCC key value pair corresponding to the specified
				// descriptor ID.
				for it.SeekGE(kvstorage.NilKey); ; it.Next() {
					if ok, err := it.Valid(); err != nil {
						return err
					} else if !ok {
						// Close and nil out the iter to release the underlying resources.
						it.Close()
						it = nil
						return nil
					}

					// Decode key and value of descriptor.
					k := it.UnsafeKey()
					descContent, err := it.UnsafeValue()
					if err != nil {
						return err
					}
					if descContent == nil {
						return errors.Wrapf(errors.New("unsafe value error"), "error "+
							"extracting raw bytes of descriptor with key %s modified between "+
							"%s, %s", k.String(), k.Timestamp, subsequentModificationTime)
					}

					// Construct a plain descriptor.
					value := roachpb.Value{RawBytes: descContent, Timestamp: k.Timestamp}
					descBuilder, err := descbuilder.FromSerializedValue(&value)
					if err != nil {
						return err
					}

					// Construct a historical descriptor with expiration.
					histDesc := historicalDescriptor{
						desc:       descBuilder.BuildImmutable(),
						expiration: subsequentModificationTime,
					}
					descriptorsRead = append(descriptorsRead, histDesc)

					// Update the expiration time for next descriptor.
					subsequentModificationTime = k.Timestamp
				}
			}(); err != nil {
				return nil, err
			}
		}

		// Check if the ExportRequest paginated with a resume span.
		if exportResp.ResumeSpan == nil {
			break
		}
		descriptorKey = exportResp.ResumeSpan.Key
	}

	return descriptorsRead, nil
}

// Read older descriptor versions for the particular timestamp from store
// through an ExportRequest. The ExportRequest queries the key span for versions
// in range [timestamp, earliest modification time in memory) or [timestamp:] if
// there are no active leases in memory. This is followed by a call to
// getForExpiration in case the ExportRequest doesn't grab the earliest
// descriptor version we are interested in, resulting at most 2 KV calls.
//
// TODO(vivek, james): Future work:
//  1. Translate multiple simultaneous calls to this method into a single call
//     as is done for acquireNodeLease().
//  2. Figure out a sane policy on when these descriptors should be purged.
//     They are currently purged in PurgeOldVersions.
func (m *Manager) readOlderVersionForTimestamp(
	ctx context.Context, id descpb.ID, timestamp hlc.Timestamp,
) ([]historicalDescriptor, error) {
	// Retrieve the endTimestamp for our query, which will be the first
	// modification timestamp above our query timestamp.
	t := m.findDescriptorState(id, false /*create*/)
	// A missing descriptor state indicates that this descriptor has been
	// purged in the meantime. We should go back around in the acquisition
	// loop to make the appropriate error appear.
	if t == nil {
		return nil, nil
	}
	endTimestamp, done := func() (hlc.Timestamp, bool) {
		t.mu.Lock()
		defer t.mu.Unlock()

		// If there are no descriptors, then we won't have a valid end timestamp.
		if len(t.mu.active.data) == 0 {
			return hlc.Timestamp{}, true
		}
		// We permit gaps in historical versions. We want to find the timestamp
		// that represents the start of the validity interval for the known version
		// which immediately follows the timestamps we're searching for.
		i := sort.Search(len(t.mu.active.data), func(i int) bool {
			return timestamp.Less(t.mu.active.data[i].GetModificationTime())
		})

		// If the timestamp we're searching for is somehow after the last descriptor
		// we have in play, then either we have the right descriptor, or some other
		// shenanigans where we've evicted the descriptor has occurred.
		//
		// TODO(ajwerner): When we come to modify this code to allow us to find
		// historical descriptors which have been dropped, we'll need to rework
		// this case and support providing no upperBound to
		// getDescriptorFromStoreForInterval.
		if i == len(t.mu.active.data) ||
			// If we found a descriptor that isn't the first descriptor, go and check
			// whether the descriptor for which we're searching actually exists. This
			// will deal with cases where a concurrent fetch filled it in for us.
			i > 0 && timestamp.Less(t.mu.active.data[i-1].getExpiration()) {
			return hlc.Timestamp{}, true
		}
		return t.mu.active.data[i].GetModificationTime(), false
	}()
	if done {
		return nil, nil
	}

	// Retrieve descriptors in range [timestamp, endTimestamp) in decreasing
	// modification time order.
	descs, err := getDescriptorsFromStoreForInterval(
		ctx, m.storage.db.KV(), m.Codec(), id, timestamp, endTimestamp,
	)
	if err != nil {
		return nil, err
	}

	// In the case where the descriptor we're looking for is modified before the
	// input timestamp, we get the descriptor before the earliest descriptor we
	// have from either in memory or from the call to
	// getDescriptorsFromStoreForInterval.
	var earliestModificationTime hlc.Timestamp
	if len(descs) == 0 {
		earliestModificationTime = endTimestamp
	} else {
		earliestModificationTime = descs[len(descs)-1].desc.GetModificationTime()
	}

	// Unless the timestamp is exactly at the earliest modification time from
	// ExportRequest, we'll invoke another call to retrieve the descriptor with
	// modification time prior to the timestamp.
	if timestamp.Less(earliestModificationTime) {
		desc, err := m.storage.getForExpiration(ctx, earliestModificationTime, id)
		if err != nil {
			return nil, err
		}
		descs = append(descs, historicalDescriptor{
			desc:       desc,
			expiration: earliestModificationTime,
		})
	}

	return descs, nil
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
				newDescriptorVersionState(t, versions[i].desc, versions[i].expiration, nil, false))
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
	// 1. The first acquireNodeLease(..) call tells us that we didn't join an
	//    in-progress acquisition but rather initiated one. Great, the lease
	//    that's being acquired is good.
	// 2. The first acquireNodeLease(..) call tells us that we did join an
	//    in-progress acquisition;
	//    We have to wait this acquisition out; it's not good for us. But any
	//    future acquisition is good, so the next time around the loop it doesn't
	//    matter if we initiate a request or join an in-progress one (because
	//    either way, it's an acquisition performed after this call).
	attemptsMade := 0
	for {
		// Acquire a fresh lease.
		didAcquire, err := acquireNodeLease(ctx, m, id, AcquireFreshestBlock)
		if err != nil {
			return err
		}

		if didAcquire {
			// Case 1: we initiated a lease acquisition call.
			break
		} else if attemptsMade > 0 {
			// Case 2: we joined an in-progress lease acquisition call but that call
			//         was initiated after we entered this function.
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
func acquireNodeLease(
	ctx context.Context, m *Manager, id descpb.ID, typ AcquireType,
) (bool, error) {
	start := timeutil.Now()
	log.VEventf(ctx, 2, "acquiring lease for descriptor %d...", id)
	var toRelease *storedLease
	future, didAcquire := m.storage.group.DoChan(ctx,
		strconv.Itoa(int(id)),
		singleflight.DoOpts{
			Stop:               m.stopper,
			InheritCancelation: false,
		},
		func(ctx context.Context) (interface{}, error) {
			if m.IsDraining() {
				return nil, errors.New("cannot acquire lease when draining")
			}
			newest := m.findNewest(id)
			var minExpiration hlc.Timestamp
			if newest != nil {
				minExpiration = newest.getExpiration()
			}
			desc, expiration, regionPrefix, err := m.storage.acquire(ctx, minExpiration, id)
			if err != nil {
				return nil, err
			}
			t := m.findDescriptorState(id, false /* create */)
			if t == nil {
				return nil, errors.AssertionFailedf("could not find descriptor state for id %d", id)
			}
			t.mu.Lock()
			t.mu.takenOffline = false
			defer t.mu.Unlock()
			var newDescVersionState *descriptorVersionState
			newDescVersionState, toRelease, err = t.upsertLeaseLocked(ctx, desc, expiration, regionPrefix)
			if err != nil {
				return nil, err
			}
			if newDescVersionState != nil {
				m.names.insert(newDescVersionState)
			}
			if toRelease != nil {
				releaseLease(ctx, toRelease, m)
			}
			return true, nil
		})
	if m.testingKnobs.LeaseStoreTestingKnobs.LeaseAcquireResultBlockEvent != nil {
		m.testingKnobs.LeaseStoreTestingKnobs.LeaseAcquireResultBlockEvent(typ, id)
	}
	result := future.WaitForResult(ctx)
	if result.Err != nil {
		return false, result.Err
	}
	log.VEventf(ctx, 2, "acquired lease for descriptor %d, took %v", id, timeutil.Since(start))
	return didAcquire, nil
}

// releaseLease from store.
func releaseLease(ctx context.Context, lease *storedLease, m *Manager) {
	if m.IsDraining() {
		// Release synchronously to guarantee release before exiting.
		m.storage.release(ctx, m.stopper, lease)
		return
	}

	// Release to the store asynchronously, without the descriptorState lock.
	newCtx := m.ambientCtx.AnnotateCtx(context.Background())
	// AddTags and not WithTags, so that we combine the tags with those
	// filled by AnnotateCtx.
	newCtx = logtags.AddTags(newCtx, logtags.FromContext(ctx))
	if err := m.stopper.RunAsyncTask(
		newCtx, "sql.descriptorState: releasing descriptor lease",
		func(ctx context.Context) {
			m.storage.release(ctx, m.stopper, lease)
		}); err != nil {
		log.Warningf(ctx, "error: %s, not releasing lease: %q", err, lease)
	}
}

// purgeOldVersions removes old unused descriptor versions older than
// minVersion and releases any associated leases.
// If dropped is set, minVersion is ignored; no lease is acquired and all
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
	empty := func() bool {
		t.mu.Lock()
		defer t.mu.Unlock()
		if t.mu.maxVersionSeen < minVersion {
			t.mu.maxVersionSeen = minVersion
		}
		return len(t.mu.active.data) == 0 && t.mu.acquisitionsInProgress == 0
	}()
	if empty && !dropped {
		// We don't currently have a version on this descriptor, so no need to refresh
		// anything.
		return nil
	}

	removeInactives := func(dropped bool) {
		leases := func() []*storedLease {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.mu.takenOffline = dropped
			return t.removeInactiveVersions()
		}()
		for _, l := range leases {
			releaseLease(ctx, l, m)
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

// AcquireType is the type of blocking result event when
// calling LeaseAcquireResultBlockEvent.
type AcquireType int

const (
	// AcquireBlock denotes the LeaseAcquireResultBlockEvent is
	// coming from descriptorState.acquire().
	AcquireBlock AcquireType = iota
	// AcquireFreshestBlock denotes the LeaseAcquireResultBlockEvent is
	// from descriptorState.acquireFreshestFromStore().
	AcquireFreshestBlock
	// AcquireBackground happens due to periodic background refreshes.
	AcquireBackground
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
	settings         *cluster.Settings
	mu               struct {
		syncutil.Mutex
		// TODO(james): Track size of leased descriptors in memory.
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
	db isql.DB,
	clock *hlc.Clock,
	settings *cluster.Settings,
	settingsWatcher *settingswatcher.SettingsWatcher,
	codec keys.SQLCodec,
	testingKnobs ManagerTestingKnobs,
	stopper *stop.Stopper,
	rangeFeedFactory *rangefeed.Factory,
) *Manager {
	lm := &Manager{
		storage: storage{
			nodeIDContainer: nodeIDContainer,
			writer:          newKVWriter(codec, db.KV(), keys.LeaseTableID, settingsWatcher),
			db:              db,
			clock:           clock,
			settings:        settings,
			codec:           codec,
			sysDBCache:      catkv.NewSystemDatabaseCache(codec, settings),
			group:           singleflight.NewGroup("acquire-lease", "descriptor ID"),
			testingKnobs:    testingKnobs.LeaseStoreTestingKnobs,
			outstandingLeases: metric.NewGauge(metric.Metadata{
				Name:        "sql.leases.active",
				Help:        "The number of outstanding SQL schema leases.",
				Measurement: "Outstanding leases",
				Unit:        metric.Unit_COUNT,
			}),
		},
		settings:         settings,
		rangeFeedFactory: rangeFeedFactory,
		testingKnobs:     testingKnobs,
		names:            makeNameCache(),
		ambientCtx:       ambientCtx,
		stopper:          stopper,
		sem:              quotapool.NewIntPool("lease manager", leaseConcurrencyLimit),
	}
	lm.storage.regionPrefix = &atomic.Value{}
	lm.storage.regionPrefix.Store(enum.One)
	lm.stopper.AddCloser(lm.sem.Closer("stopper"))
	lm.mu.descriptors = make(map[descpb.ID]*descriptorState)
	lm.mu.updatesResolvedTimestamp = clock.Now()

	lm.draining.Store(false)
	return lm
}

// NameMatchesDescriptor returns true if the provided name and IDs match this
// descriptor.
func NameMatchesDescriptor(
	desc catalog.Descriptor, parentID descpb.ID, parentSchemaID descpb.ID, name string,
) bool {
	return desc.GetName() == name &&
		desc.GetParentID() == parentID &&
		desc.GetParentSchemaID() == parentSchemaID
}

// findNewest returns the newest descriptor version state for the ID.
func (m *Manager) findNewest(id descpb.ID) *descriptorVersionState {
	t := m.findDescriptorState(id, false /* create */)
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.mu.active.findNewest()
}

// SetRegionPrefix sets the prefix this Manager uses to write leases. If val
// is empty, this call is a no-op. Note that the default value is enum.One.
// This means that leases acquired before initial startup may write their
// entry to a remote region.
//
// TODO(ajwerner): We ought to reject attempts to lease descriptors before
// we've set the prefix if the table is partitioned. In principle, this should
// just mean returning ErrDescriptorNotFound. The challenge will be to sort
// out all the conditions in which we expect, or don't expect to get a prefix
// in a timely manner.
func (m *Manager) SetRegionPrefix(val []byte) {
	if len(val) > 0 {
		m.storage.regionPrefix.Store(val)
	}
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
		if err := catalog.FilterOfflineDescriptor(desc.Underlying()); err != nil {
			desc.Release(ctx)
			return nil, err
		}
		return desc, nil
	}
	// Check if we have cached an ID for this name.
	descVersion, expiration := m.names.get(ctx, parentID, parentSchemaID, name, timestamp)
	if descVersion != nil {
		if descVersion.GetModificationTime().LessEq(timestamp) {
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
			return nil, catalog.NewDescriptorNotFoundError(id)
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
) (id descpb.ID, _ error) {
	req := []descpb.NameInfo{{ParentID: parentID, ParentSchemaID: parentSchemaID, Name: name}}
	if err := m.storage.db.KV().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Run the name lookup as high-priority, thereby pushing any intents out of
		// its way. We don't want schema changes to prevent name resolution/lease
		// acquisitions; we'd rather force them to refresh. Also this prevents
		// deadlocks in cases where the name resolution is triggered by the
		// transaction doing the schema change itself.
		if err := txn.SetUserPriority(roachpb.MaxUserPriority); err != nil {
			return err
		}
		if err := txn.SetFixedTimestamp(ctx, timestamp); err != nil {
			return err
		}
		c, err := m.storage.newCatalogReader(ctx).GetByNames(ctx, txn, req)
		if err != nil {
			return err
		}
		if e := c.LookupNamespaceEntry(&req[0]); e != nil {
			id = e.GetID()
		}
		return nil
	}); err != nil {
		return id, err
	}
	if id == descpb.InvalidID {
		return id, errors.Wrapf(catalog.ErrDescriptorNotFound,
			"resolving name %s with parentID %d and parentSchemaID %d",
			name, parentID, parentSchemaID,
		)
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
// the timestamp. It returns the descriptor and an expiration time.
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
				_, errLease := acquireNodeLease(ctx, m, id, AcquireBlock)
				return errLease
			}(); err != nil {
				return nil, err
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
		m.IsDraining()
}

// IsDraining returns true if this node's lease manager is draining.
func (m *Manager) IsDraining() bool {
	return m.draining.Load().(bool)
}

// SetDraining (when called with 'true') removes all inactive leases. Any leases
// that are active will be removed once the lease's reference count drops to 0.
//
// The reporter callback, if non-nil, is called on a best effort basis
// to report work that needed to be done and which may or may not have
// been done by the time this call returns. See the explanation in
// pkg/server/drain.go for details.
func (m *Manager) SetDraining(
	ctx context.Context, drain bool, reporter func(int, redact.SafeString),
) {
	m.draining.Store(drain)
	if !drain {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range m.mu.descriptors {
		leases := func() []*storedLease {
			t.mu.Lock()
			defer t.mu.Unlock()
			return t.removeInactiveVersions()
		}()
		for _, l := range leases {
			releaseLease(ctx, l, m)
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
	descUpdateCh := make(chan catalog.Descriptor)
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
					if err := evFunc(desc.DescriptorProto()); err != nil {
						log.Infof(ctx, "skipping update of %v due to knob: %v",
							desc, err)
						continue
					}
				}

				dropped := desc.Dropped()
				// Try to refresh the lease to one >= this version.
				log.VEventf(ctx, 2, "purging old version of descriptor %d@%d (dropped %v)",
					desc.GetID(), desc.GetVersion(), dropped)
				purge := func(ctx context.Context) {
					if err := purgeOldVersions(ctx, db, desc.GetID(), dropped, desc.GetVersion(), m); err != nil {
						log.Warningf(ctx, "error purging leases for descriptor %d(%s): %s",
							desc.GetID(), desc.GetName(), err)
					}
				}
				// New descriptors may appear in the future if the descriptor table is
				// global or if the transaction which performed the schema change wrote
				// to a global table. Attempts to acquire a lease after that new
				// version has appeared won't acquire the latest version, they'll
				// acquire the previous version because they'll occur at "now".
				//
				// That, in and of itself, is not a problem. The problem is that we
				// may release the lease on the current version before we can start
				// acquiring the lease on the new version. This could lead to periods
				// of increased latency right as the descriptor has been committed.
				if now := db.Clock().Now(); now.Less(desc.GetModificationTime()) {
					_ = s.RunAsyncTask(ctx, "wait to purge", func(ctx context.Context) {
						toWait := time.Duration(desc.GetModificationTime().WallTime - now.WallTime)
						select {
						case <-time.After(toWait):
							purge(ctx)
						case <-ctx.Done():
						case <-s.ShouldQuiesce():
						}
					})
				} else {
					purge(ctx)
				}

				if evFunc := m.testingKnobs.TestingDescriptorRefreshedEvent; evFunc != nil {
					evFunc(desc.DescriptorProto())
				}

			case <-s.ShouldQuiesce():
				return
			}
		}
	})
}

// watchForUpdates will watch a rangefeed on the system.descriptor table for
// updates.
func (m *Manager) watchForUpdates(ctx context.Context, descUpdateCh chan<- catalog.Descriptor) {
	if log.V(1) {
		log.Infof(ctx, "using rangefeeds for lease manager updates")
	}
	descriptorTableStart := m.Codec().TablePrefix(keys.DescriptorTableID)
	descriptorTableSpan := roachpb.Span{
		Key:    descriptorTableStart,
		EndKey: descriptorTableStart.PrefixEnd(),
	}
	handleEvent := func(
		ctx context.Context, ev *kvpb.RangeFeedValue,
	) {
		if len(ev.Value.RawBytes) == 0 {
			return
		}
		b, err := descbuilder.FromSerializedValue(&ev.Value)
		if err != nil {
			logcrash.ReportOrPanic(ctx, &m.storage.settings.SV,
				"%s: unable to unmarshal descriptor %v", ev.Key, ev.Value)
		}
		if b == nil {
			return
		}
		mut := b.BuildCreatedMutable()
		if log.V(2) {
			log.Infof(ctx, "%s: refreshing lease on descriptor: %d (%s), version: %d",
				ev.Key, mut.GetID(), mut.GetName(), mut.GetVersion())
		}
		select {
		case <-ctx.Done():
		case descUpdateCh <- mut:
		}
	}
	// Ignore errors here because they indicate that the server is shutting down.
	// Also note that the range feed automatically shuts down when the server
	// shuts down, so we don't need to call Close() ourselves.
	_, _ = m.rangeFeedFactory.RangeFeed(
		ctx, "lease", []roachpb.Span{descriptorTableSpan}, hlc.Timestamp{}, handleEvent,
		rangefeed.WithSystemTablePriority(),
	)
}

// leaseRefreshLimit is the upper-limit on the number of descriptor leases
// that will continuously have their lease refreshed.
var leaseRefreshLimit = settings.RegisterIntSetting(
	settings.ApplicationLevel,
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
	ids := func() []descpb.ID {
		m.mu.Lock()
		defer m.mu.Unlock()

		ids := make([]descpb.ID, 0, len(m.mu.descriptors))
		var i int64
		for k, desc := range m.mu.descriptors {
			if i++; i > limit {
				break
			}
			takenOffline := func() bool {
				desc.mu.Lock()
				defer desc.mu.Unlock()
				return desc.mu.takenOffline
			}()
			if !takenOffline {
				ids = append(ids, k)
			}
		}
		return ids
	}()
	// Limit the number of concurrent lease refreshes.
	var wg sync.WaitGroup
	for i := range ids {
		id := ids[i]
		wg.Add(1)
		if err := m.stopper.RunAsyncTaskEx(
			ctx,
			stop.TaskOpts{
				TaskName:   fmt.Sprintf("refresh descriptor: %d lease", id),
				Sem:        m.sem,
				WaitForSem: true,
			},
			func(ctx context.Context) {
				defer wg.Done()

				if evFunc := m.testingKnobs.TestingBeforeAcquireLeaseDuringRefresh; evFunc != nil {
					if err := evFunc(id); err != nil {
						log.Infof(ctx, "knob failed for desc (%v): %v", id, err)
						return
					}
				}

				if _, err := acquireNodeLease(ctx, m, id, AcquireBackground); err != nil {
					log.Errorf(ctx, "refreshing descriptor: %d lease failed: %s", id, err)

					if errors.Is(err, catalog.ErrDescriptorNotFound) {
						// Lease renewal failed due to removed descriptor; Remove this descriptor from cache.
						if err := purgeOldVersions(
							ctx, m.storage.db.KV(), id, true /* dropped */, 0 /* minVersion */, m,
						); err != nil {
							log.Warningf(ctx, "error purging leases for descriptor %d: %s",
								id, err)
						}
						func() {
							m.mu.Lock()
							defer m.mu.Unlock()
							delete(m.mu.descriptors, id)
						}()
					}
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
func (m *Manager) DeleteOrphanedLeases(ctx context.Context, timeThreshold int64) {
	if m.testingKnobs.DisableDeleteOrphanedLeases {
		return
	}
	instanceID := m.storage.nodeIDContainer.SQLInstanceID()
	if instanceID == 0 {
		panic("SQL instance ID not set")
	}

	// Run as async worker to prevent blocking the main server Start method.
	// Exit after releasing all the orphaned leases.
	newCtx := m.ambientCtx.AnnotateCtx(context.Background())
	// AddTags and not WithTags, so that we combine the tags with those
	// filled by AnnotateCtx.
	newCtx = logtags.AddTags(newCtx, logtags.FromContext(ctx))
	_ = m.stopper.RunAsyncTask(newCtx, "del-orphaned-leases", func(ctx context.Context) {
		// This could have been implemented using DELETE WHERE, but DELETE WHERE
		// doesn't implement AS OF SYSTEM TIME.

		// Read orphaned leases.
		const (
			queryWithRegion = `
SELECT "descID", version, expiration, crdb_region FROM system.public.lease AS OF SYSTEM TIME %d WHERE "nodeID" = %d
`
			queryWithoutRegion = `
SELECT "descID", version, expiration FROM system.public.lease AS OF SYSTEM TIME %d WHERE "nodeID" = %d
`
		)
		query := queryWithRegion
		if !m.settings.Version.IsActive(ctx, clusterversion.V23_1_SystemRbrReadNew) {
			query = queryWithoutRegion
		}
		sqlQuery := fmt.Sprintf(query, timeThreshold, instanceID)
		var rows []tree.Datums
		retryOptions := base.DefaultRetryOptions()
		retryOptions.Closer = m.stopper.ShouldQuiesce()
		// The retry is required because of errors caused by node restarts. Retry 30 times.
		if err := retry.WithMaxAttempts(ctx, retryOptions, 30, func() error {
			var err error
			rows, err = m.storage.db.Executor().QueryBuffered(
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
			if len(row) == 4 {
				if ed, ok := row[3].(*tree.DEnum); ok {
					lease.prefix = ed.PhysicalRep
				} else if bd, ok := row[3].(*tree.DBytes); ok {
					lease.prefix = []byte((*bd))
				}
			}
			if err := m.stopper.RunAsyncTaskEx(
				ctx,
				stop.TaskOpts{
					TaskName:   fmt.Sprintf("release lease %+v", lease),
					Sem:        m.sem,
					WaitForSem: true,
				},
				func(ctx context.Context) {
					m.storage.release(ctx, m.stopper, &lease)
					log.Infof(ctx, "released orphaned lease: %+v", lease)
					wg.Done()
				}); err != nil {
				wg.Done()
			}
		}
	})
}

// Codec returns the Manager's SQLCodec.
func (m *Manager) Codec() keys.SQLCodec {
	return m.storage.codec
}

// SystemDatabaseCache returns the Manager's catkv.SystemDatabaseCache.
func (m *Manager) SystemDatabaseCache() *catkv.SystemDatabaseCache {
	return m.storage.sysDBCache
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
				lease, refCount := func() (*storedLease, int) {
					state.mu.Lock()
					defer state.mu.Unlock()
					return state.mu.lease, state.mu.refcount
				}()

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
