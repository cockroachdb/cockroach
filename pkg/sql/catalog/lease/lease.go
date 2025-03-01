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
	"strings"
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/regionliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
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
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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

var LeaseMonitorRangeFeedCheckInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"sql.catalog.descriptor_lease_monitor_range_feed.check_interval",
	"the leasing subsystem will check for checkpoints for the range feed within "+
		"this interval",
	time.Minute*5,
)

var LeaseMonitorRangeFeedResetTime = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"sql.catalog.descriptor_lease_monitor_range_feed.reset_time",
	"once the range feed has stopped receiving checkpoints for this "+
		"period of time the range feed will be restarted",
	time.Minute*25,
)

var WaitForInitialVersion = settings.RegisterBoolSetting(settings.ApplicationLevel,
	"sql.catalog.descriptor_wait_for_initial_version.enabled",
	"enables waiting for the initial version of a descriptor",
	true)

// WaitForNoVersion returns once there are no unexpired leases left
// for any version of the descriptor.
func (m *Manager) WaitForNoVersion(
	ctx context.Context,
	id descpb.ID,
	cachedDatabaseRegions regionliveness.CachedDatabaseRegions,
	retryOpts retry.Options,
) error {
	versions := []IDVersion{
		{
			Name:    fmt.Sprintf("[%d]", id),
			ID:      id,
			Version: 0, // Unused any version flag used below.
		},
	}
	// Increment the long wait gauge for wait for no version, if this function
	// takes longer than the lease duration.
	decAfterWait := m.IncGaugeAfterLeaseDuration(GaugeWaitForNoVersion)
	defer decAfterWait()
	wsTracker := startWaitStatsTracker(ctx)
	defer wsTracker.end()
	for lastCount, r := 0, retry.Start(retryOpts); r.Next(); {
		now := m.storage.clock.Now()
		detail, err := countLeasesWithDetail(ctx, m.storage.db, m.Codec(), cachedDatabaseRegions, m.settings, versions, now, true /*forAnyVersion*/)
		if err != nil {
			return err
		}
		if detail.count == 0 {
			break
		}
		if detail.count != lastCount {
			lastCount = detail.count
			wsTracker.updateProgress(detail)
			log.Infof(ctx, "waiting for %d leases to expire: desc=%d", detail.count, id)
		}
		if lastCount == 0 {
			break
		}
	}
	return nil
}

// maybeGetDescriptorsWithoutValidation gets a descriptor without validating
// from the KV layer.
func (m *Manager) maybeGetDescriptorsWithoutValidation(
	ctx context.Context, ids descpb.IDs, existenceExpected bool,
) (descs catalog.Descriptors, err error) {
	err = m.storage.db.KV().Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		const isDescriptorRequired = false
		cr := m.storage.newCatalogReader(ctx)
		c, err := cr.GetByIDs(ctx, txn, ids, isDescriptorRequired, catalog.Any)
		if err != nil {
			return err
		}
		descs = make(catalog.Descriptors, 0, len(ids))
		for _, id := range ids {
			desc := c.LookupDescriptor(id)
			if desc == nil {
				// Descriptor was dropped on us, so return a structured error.
				if existenceExpected {
					return errors.Wrapf(catalog.ErrDescriptorNotFound, "descriptor %d could not be fetched to count leases", id)
				}
			} else {
				descs = append(descs, desc)
			}
		}
		return nil
	})
	return descs, err
}

// countDescriptorsHeldBySessionIDs can be used to make sure certain nodes
// (sessions) observe the existence of a given set of descriptors. Assuming the given
// sessions are still alive.
func countDescriptorsHeldBySessionIDs(
	ctx context.Context,
	txn isql.Txn,
	descIDs descpb.IDs,
	region string,
	sessionIDs []sqlliveness.SessionID,
) (int, error) {
	regionClause := ""
	if region != "" {
		regionClause = fmt.Sprintf("AND crdb_region='%s'", region)
	}
	b := strings.Builder{}
	for _, sessionID := range sessionIDs {
		if b.Len() > 0 {
			b.WriteString(",")
		}
		b.WriteString(fmt.Sprintf("x'%s'", sessionID.String()))
	}
	d := strings.Builder{}
	for _, descID := range descIDs {
		if d.Len() > 0 {
			d.WriteString(",")
		}
		d.WriteString(fmt.Sprintf("%d", descID))
	}
	// Query the count from the region.
	row, err := txn.QueryRow(ctx, "wait-for-new-descriptor", txn.KV(),
		fmt.Sprintf(
			`
SELECT count(*)
  FROM system.lease
 WHERE desc_id IN (%s)
       AND session_id IN (%s)
       AND crdb_internal.sql_liveness_is_alive(session_id)
       %s;`,
			d.String(),
			b.String(),
			regionClause,
		),
	)
	if err != nil {
		return 0, err
	}
	return int(tree.MustBeDInt(row[0])), nil
}

// getSessionsHoldingDescriptor can be used to fetch on a per-region basis the
// sessionIDs that are currently holding a lease on descID. If region is empty,
// then all regions will be queried.
func getSessionsHoldingDescriptor(
	ctx context.Context, txn isql.Txn, descID descpb.ID, region string,
) ([]sqlliveness.SessionID, error) {
	queryStr := `
SELECT DISTINCT session_id FROM system.lease WHERE desc_id=%d AND crdb_internal.sql_liveness_is_alive(session_id) 
`
	if region != "" {
		queryStr += fmt.Sprintf(" AND crdb_region='%s'", region)
	}
	rows, err := txn.QueryBuffered(ctx, "active-schema-leases-by-region", txn.KV(),
		fmt.Sprintf(queryStr,
			descID))
	if err != nil {
		return nil, err
	}
	sessionIDs := make([]sqlliveness.SessionID, 0, len(rows))
	for _, row := range rows {
		sessionIDs = append(sessionIDs, sqlliveness.SessionID(tree.MustBeDBytes(row[0])))
	}
	return sessionIDs, nil
}

// WaitForInitialVersion waits for a lease to be acquired on a newly created
// object on any nodes that have already leased the schema out. This ensures
// that their leaseGeneration is incremented before the user commit completes,
// which will ensure that any existing cached queries will detect the new object
// (i.e. the optimizer memo will use the generation value as short circuit).
func (m *Manager) WaitForInitialVersion(
	ctx context.Context,
	descriptorsIds descpb.IDs,
	retryOpts retry.Options,
	regions regionliveness.CachedDatabaseRegions,
) error {
	if !WaitForInitialVersion.Get(&m.settings.SV) ||
		!m.storage.settings.Version.IsActive(ctx, clusterversion.V25_1) {
		return nil
	}
	wsTracker := startWaitStatsTracker(ctx)
	defer wsTracker.end()
	decrAfterWait := m.IncGaugeAfterLeaseDuration(GaugeWaitForInitialVersion)
	defer decrAfterWait()
	// Track the set of descriptors, this will have descriptors removed on
	// partial fulfillment (i.e. some subset having the initial version set).
	var ids catalog.DescriptorIDSet
	for _, id := range descriptorsIds {
		ids.Add(id)
	}
	for lastCount, r := 0, retry.Start(retryOpts); r.Next(); {
		descs, err := m.maybeGetDescriptorsWithoutValidation(ctx, ids.Ordered(), false /* existenceExpected */)
		if err != nil {
			return err
		}
		// If the object no longer exists or isn't fully created then skip this
		// check, since there is no initial version. We only need to wait for
		// initial versions on tables or types, since their resolution is impacted
		// by the search_path variable. So, remote lease manager need to be aware
		// to invalidate cached metadata (like optimizer memos).
		// We don't need to worry about functions because their signature is stored
		// inside the schema descriptor, which will cause an implicit bump invalidating
		// cached metadata.
		descsToProcess := make([]catalog.Descriptor, 0, len(descs))
		idsPerSchema := make(map[descpb.ID]descpb.IDs)

		for _, desc := range descs {
			if (desc.DescriptorType() != catalog.Table && desc.DescriptorType() != catalog.Type) ||
				desc.Dropped() ||
				desc.Adding() {
				continue
			}
			descsToProcess = append(descsToProcess, desc)
			idsPerSchema[desc.GetParentSchemaID()] = append(idsPerSchema[desc.GetParentSchemaID()], desc.GetID())
		}
		// No schemas to wait for.
		if len(idsPerSchema) == 0 {
			return nil
		}
		// Go over each schema and set of descriptor IDs
		totalCount := 0
		totalExpectedCount := 0
		for schemaID, descIDsForSchema := range idsPerSchema {
			// Check to see if there are any leases that still exist on the previous
			// version of the descriptor.
			now := m.storage.clock.Now()
			var count int
			db := m.storage.db
			// Get a list of sessions that had the schema leased out when this descriptor
			// was created / modified.
			var sessionsPerRegion map[string][]sqlliveness.SessionID
			expectedSessions := 0
			if err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				txn.KV().SetDebugName("wait-for-initial-lease-count-schema-leases")
				// Look at what was leasing the schema at them modification time, we expect
				// this be before the GC TTL because:
				// 1) The defaults settings on the system database are not aggressive
				// 2) We expect this to be a short wait in the recent past.
				// If for some reason this timestamp is outside the GC TTL the transaction
				// will get an error, which is a potential caveat here.
				if err := txn.KV().SetFixedTimestamp(ctx, descsToProcess[0].GetModificationTime()); err != nil {
					return err
				}
				expectedSessions = 0
				sessionsPerRegion = make(map[string][]sqlliveness.SessionID)

				prober := regionliveness.NewLivenessProber(db.KV(), m.storage.codec, regions, m.settings)
				regionMap, err := prober.QueryLiveness(ctx, txn.KV())
				if err != nil {
					return err
				}
				// On single region clusters we can query everything at once.
				if regionMap == nil {
					sessionIDs, err := getSessionsHoldingDescriptor(ctx, txn, schemaID, "")
					if err != nil {
						return err
					}
					sessionsPerRegion[""] = sessionIDs
					expectedSessions += len(sessionIDs)
				}
				// Otherwise, process active schema leases by region, and use the
				// region liveness subsystem to detect offline regions.
				return regionMap.ForEach(func(region string) error {
					var sessionIDs []sqlliveness.SessionID
					var err error
					if hasTimeout, timeout := prober.GetProbeTimeout(); hasTimeout {
						err = timeutil.RunWithTimeout(ctx, "active-schema-leases-by-region", timeout, func(ctx context.Context) error {
							var err error
							sessionIDs, err = getSessionsHoldingDescriptor(ctx, txn, schemaID, region)
							return err
						})
					} else {
						sessionIDs, err = getSessionsHoldingDescriptor(ctx, txn, schemaID, region)
					}
					if err != nil {
						return handleRegionLivenessErrors(ctx, prober, region, err)
					}
					sessionsPerRegion[region] = sessionIDs
					expectedSessions += len(sessionIDs)
					return nil
				})
			}); err != nil {
				return err
			}
			// Next ensure the initial version exists on all nodes that have the schema
			// leased out.
			if err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				count = 0
				txn.KV().SetDebugName("wait-for-initial-lease")
				if err := txn.KV().SetFixedTimestamp(ctx, now); err != nil {
					return err
				}
				prober := regionliveness.NewLivenessProber(db.KV(), m.storage.codec, regions, m.settings)
				regionMap, err := prober.QueryLiveness(ctx, txn.KV())
				if err != nil {
					return err
				}
				// On multi-region we are going to process initial version on a per-region
				// basis, which will allow us to query / inform region liveness of offline
				// regions.
				if regions != nil && regions.IsMultiRegion() {
					return regionMap.ForEach(func(region string) error {
						sessionIDs := sessionsPerRegion[region]
						// Skip any regions without sessions.
						if len(sessionIDs) == 0 {
							return nil
						}

						var regionCount int
						var err error
						if hasTimeout, timeout := prober.GetProbeTimeout(); hasTimeout {
							err = timeutil.RunWithTimeout(ctx, "wait-for-new-descriptor-by-region", timeout, func(ctx context.Context) error {
								var err error
								regionCount, err = countDescriptorsHeldBySessionIDs(ctx, txn, descIDsForSchema, region, sessionIDs)
								return err
							})
						} else {
							regionCount, err = countDescriptorsHeldBySessionIDs(ctx, txn, descIDsForSchema, region, sessionIDs)
						}
						if err := handleRegionLivenessErrors(ctx, prober, region, err); err != nil {
							return err
						}
						count += regionCount
						return nil
					})
				} else {
					// Otherwise, we can query the initial versions directly.
					count, err = countDescriptorsHeldBySessionIDs(ctx, txn, descIDsForSchema, "", sessionsPerRegion[""])
					return err
				}
			}); err != nil {
				return err
			}
			if count == expectedSessions*len(descIDsForSchema) {
				// Remove descriptors that have had their wait completed.
				for _, id := range descIDsForSchema {
					ids.Remove(id)
				}
			}
			totalCount += count
			totalExpectedCount += expectedSessions * len(descIDsForSchema)
		}
		// All the expected sessions are there now.
		if totalCount == totalExpectedCount {
			break
		}
		if totalCount != lastCount {
			log.Infof(ctx, "waiting for descriptors %v to appear on %d nodes. Last count was %d", ids.Ordered(), totalExpectedCount, totalCount)
			wsTracker.updateProgress(countDetail{
				count:       totalCount,
				targetCount: totalExpectedCount,
			})
		}
		lastCount = totalCount
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
	ctx context.Context,
	id descpb.ID,
	regions regionliveness.CachedDatabaseRegions,
	retryOpts retry.Options,
) (desc catalog.Descriptor, _ error) {
	// Increment the long wait gauge for wait for one version, if this function
	// takes longer than the lease duration.
	decAfterWait := m.IncGaugeAfterLeaseDuration(GaugeWaitForOneVersion)
	defer decAfterWait()
	wsTracker := startWaitStatsTracker(ctx)
	defer wsTracker.end()
	for lastCount, r := 0, retry.Start(retryOpts); r.Next(); {
		var err error
		var descArr catalog.Descriptors
		if descArr, err = m.maybeGetDescriptorsWithoutValidation(ctx, descpb.IDs{id}, true); err != nil {
			return nil, err
		}
		desc = descArr[0]
		// Check to see if there are any leases that still exist on the previous
		// version of the descriptor.
		now := m.storage.clock.Now()
		descs := []IDVersion{NewIDVersionPrev(desc.GetName(), desc.GetID(), desc.GetVersion())}
		detail, err := countLeasesWithDetail(ctx, m.storage.db, m.Codec(), regions, m.settings, descs, now, false /*forAnyVersion*/)
		if err != nil {
			return nil, err
		}
		if detail.count == 0 {
			break
		}
		if detail.count != lastCount {
			lastCount = detail.count
			wsTracker.updateProgress(detail)
			log.Infof(ctx, "waiting for %d leases to expire: desc=%v", detail.count, descs)
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
					// For offline tables the modification time is never set by the builder,
					// which can break historical queries. So, manually set this value here.
					desc := descBuilder.BuildImmutable()
					if desc.Offline() && desc.DescriptorType() == catalog.Table {
						descMut := descBuilder.BuildExistingMutable()
						descMut.(*tabledesc.Mutable).ForceModificationTime(k.Timestamp)
						desc = descMut
					}

					// Construct a historical descriptor with expiration.
					histDesc := historicalDescriptor{
						desc:       desc,
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
			i > 0 && timestamp.Less(t.mu.active.data[i-1].getExpiration(ctx)) {
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
func (m *Manager) insertDescriptorVersions(
	ctx context.Context, id descpb.ID, versions []historicalDescriptor,
) error {
	t := m.findDescriptorState(id, false /* create */)
	session, err := m.storage.livenessProvider.Session(ctx)
	if err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for i := range versions {
		// Since we gave up the lock while reading the versions from
		// the store we have to ensure that no one else inserted the
		// same version.
		existingVersion := t.mu.active.findVersion(versions[i].desc.GetVersion())
		if existingVersion == nil {
			t.mu.active.insert(
				newDescriptorVersionState(t, versions[i].desc, versions[i].expiration, session, nil, false))
		}
	}
	return nil
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
			var currentVersion descpb.DescriptorVersion
			var currentSessionID sqlliveness.SessionID
			if newest != nil {
				currentVersion = newest.GetVersion()
				currentSessionID = newest.getSessionID()
			}
			// A session will always be populated, since we use session based leasing.
			session, err := m.storage.livenessProvider.Session(ctx)
			if err != nil {
				return false, errors.Wrapf(err, "lease acquisition was unable to resolve liveness session")
			}
			desc, regionPrefix, err := m.storage.acquire(ctx, session, id, currentVersion, currentSessionID)
			if err != nil {
				return nil, err
			}
			// If a nil descriptor is returned, then the latest version has already
			// been leased. So, nothing needs to be done here.
			if desc == nil {
				return true, nil
			}
			t := m.findDescriptorState(id, false /* create */)
			if t == nil {
				return nil, errors.AssertionFailedf("could not find descriptor state for id %d", id)
			}
			t.mu.Lock()
			t.mu.takenOffline = false
			defer t.mu.Unlock()
			err = t.upsertLeaseLocked(ctx, desc, session, regionPrefix)
			if err != nil {
				return nil, err
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

// releaseLease deletes an entry from system.lease.
func releaseLease(ctx context.Context, lease *storedLease, m *Manager) (released bool) {
	// Force the release to happen synchronously, if we are draining or, when we
	// force removals for unit tests. This didn't matter with expiration based leases
	// since each renewal would have a different expiry (but the same version in
	// synthetic scenarios). In the session based model renewals will come in with
	// the same session ID, and potentially we can end up racing with inserts and
	// deletes on the storage side. For real world scenario, this never happens
	// because we release only if a new version exists.
	if m.IsDraining() || m.removeOnceDereferenced() {
		// Release synchronously to guarantee release before exiting.
		// It's possible for the context to get cancelled, so return if
		// it was released.
		return m.storage.release(ctx, m.stopper, lease)
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
	// Asynchronous job is releasing it.
	return true
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
		leases, leaseToExpire := func() (leasesToRemove []*storedLease, leasesToExpire *descriptorVersionState) {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.mu.takenOffline = dropped
			return t.removeInactiveVersions(), t.mu.active.findPreviousToExpire(dropped)
		}()
		for _, l := range leases {
			releaseLease(ctx, l, m)
		}
		// If there are old versions with an active refcount, we cannot allow
		// these to stay forever. So, setup an expiry on them, which is required
		// for session based leases.
		if leaseToExpire != nil {
			func() {
				m.mu.Lock()
				leaseToExpire.mu.Lock()
				defer leaseToExpire.mu.Unlock()
				defer m.mu.Unlock()
				// Expire any active old versions into the future based on the lease
				// duration. If the session lifetime had been longer then use
				// that. We will only expire later into the future, then what
				// was previously observed, since transactions may have already
				// picked this time. If the lease duration is zero, then we are
				// looking at instant expiration for testing.
				leaseDuration := LeaseDuration.Get(&m.storage.settings.SV)
				leaseToExpire.mu.expiration = m.storage.db.KV().Clock().Now().AddDuration(leaseDuration)
				if sessionExpiry := leaseToExpire.mu.session.Expiration(); leaseDuration > 0 && leaseToExpire.mu.expiration.Less(sessionExpiry) {
					leaseToExpire.mu.expiration = sessionExpiry
				}
				if leaseToExpire.mu.lease != nil {
					m.storage.sessionBasedLeasesWaitingToExpire.Inc(1)
					m.mu.leasesToExpire = append(m.mu.leasesToExpire, leaseToExpire)
				}
			}()
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

		// Session based leases that will be removed with expiry, since
		// a new version has arrived.
		leasesToExpire []*descriptorVersionState

		// rangeFeedCheckpoints tracks the health of the range by tracking
		// the number of observed checkpoints.
		rangeFeedCheckpoints int

		// rangeFeedIsUnavailableAt tracks when the range feed first became unavailable
		// or when it was last restarted after unavailability.
		rangeFeedIsUnavailableAt time.Time

		// rangeFeed current range feed on system.descriptors.
		rangeFeed *rangefeed.RangeFeed
	}

	// closeTimeStamp for the range feed, which is the timestamp
	// that we have all the updates for.
	closeTimestamp atomic.Value

	draining atomic.Value

	// names is a cache for name -> id mappings. A mapping for the cache
	// should only be used if we currently have an active lease on the respective
	// id; otherwise, the mapping may well be stale.
	// Not protected by mu.
	names            nameCache
	testingKnobs     ManagerTestingKnobs
	ambientCtx       log.AmbientContext
	stopper          *stop.Stopper
	sem              *quotapool.IntPool
	refreshAllLeases chan struct{}

	// descUpdateCh receives updated descriptors from the range feed.
	descUpdateCh chan catalog.Descriptor
	// descDelCh receives deleted descriptors from the range feed.
	descDelCh chan descpb.ID
	// rangefeedErrCh receives any terminal errors from the rangefeed.
	rangefeedErrCh chan error
	// leaseGeneration increments any time a new or existing descriptor is
	// detected by the lease manager. Once this count is incremented new data
	// is available.
	leaseGeneration atomic.Int64

	// waitForInit used when the lease manager is starting up prevent leases from
	// being acquired before the range feed.
	waitForInit chan struct{}
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
	livenessProvider sqlliveness.Provider,
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
			settings:         settings,
			codec:            codec,
			livenessProvider: livenessProvider,
			sysDBCache:       catkv.NewSystemDatabaseCache(codec, settings),
			group:            singleflight.NewGroup("acquire-lease", "descriptor ID"),
			testingKnobs:     testingKnobs.LeaseStoreTestingKnobs,
			leasingMetrics: leasingMetrics{
				outstandingLeases: metric.NewGauge(metric.Metadata{
					Name:        "sql.leases.active",
					Help:        "The number of outstanding SQL schema leases.",
					Measurement: "Outstanding leases",
					Unit:        metric.Unit_COUNT,
				}),
				sessionBasedLeasesWaitingToExpire: metric.NewGauge(metric.Metadata{
					Name:        "sql.leases.waiting_to_expire",
					Help:        "The number of outstanding session based SQL schema leases with expiry.",
					Measurement: "Outstanding Leases Waiting to Expire",
					Unit:        metric.Unit_COUNT,
				}),
				sessionBasedLeasesExpired: metric.NewGauge(metric.Metadata{
					Name:        "sql.leases.expired",
					Help:        "The number of outstanding session based SQL schema leases expired.",
					Measurement: "Leases expired because of a new version",
					Unit:        metric.Unit_COUNT,
				}),
				longWaitForNoVersionsActive: metric.NewGauge(metric.Metadata{
					Name:        "sql.leases.long_wait_for_no_version",
					Help:        "The number of wait for no versions that are taking more than the lease duration.",
					Measurement: "Number of wait for long wait for no version routines executing",
					Unit:        metric.Unit_COUNT,
				}),
				longWaitForOneVersionsActive: metric.NewGauge(metric.Metadata{
					Name:        "sql.leases.long_wait_for_one_version",
					Help:        "The number of wait for one versions that are taking more than the lease duration.",
					Measurement: "Number of wait for long wait for one version routines executing",
					Unit:        metric.Unit_COUNT,
				}),
				longTwoVersionInvariantViolationWaitActive: metric.NewGauge(metric.Metadata{
					Name:        "sql.leases.long_wait_for_two_version_invariant",
					Help:        "The number of two version invariant waits that are taking more than the lease duration.",
					Measurement: "Number of two version invariant wait routines executing",
					Unit:        metric.Unit_COUNT,
				}),
				longWaitForInitialVersionActive: metric.NewGauge(metric.Metadata{
					Name:        "sql.leases.long_wait_for_initial_version",
					Help:        "The number of wait for initial version routines taking more than the lease duration.",
					Measurement: "Number of wait for initial version routines executing",
					Unit:        metric.Unit_COUNT,
				}),
			},
		},
		settings:         settings,
		rangeFeedFactory: rangeFeedFactory,
		testingKnobs:     testingKnobs,
		names:            makeNameCache(),
		ambientCtx:       ambientCtx,
		stopper:          stopper,
		sem:              quotapool.NewIntPool("lease manager", leaseConcurrencyLimit),
		refreshAllLeases: make(chan struct{}),
	}
	lm.leaseGeneration.Swap(1) // Start off with 1 as the initial value.
	lm.storage.regionPrefix = &atomic.Value{}
	lm.storage.regionPrefix.Store(enum.One)
	lm.storage.writer = newKVWriter(codec, db.KV(), keys.LeaseTableID, settingsWatcher)
	lm.stopper.AddCloser(lm.sem.Closer("stopper"))
	lm.mu.descriptors = make(map[descpb.ID]*descriptorState)
	lm.waitForInit = make(chan struct{})
	// We are going to start the range feed later when StartRefreshLeasesTask
	// is invoked inside pre-start. So, that guarantees all range feed events
	// that will be generated will be after the current time. So, historical
	// queries with in this tenant (i.e. PCR catalog reader) before this point are
	// guaranteed to be up to date.
	lm.closeTimestamp.Store(db.KV().Clock().Now())
	lm.draining.Store(false)
	lm.descUpdateCh = make(chan catalog.Descriptor)
	lm.descDelCh = make(chan descpb.ID)
	lm.rangefeedErrCh = make(chan error)
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
	descVersion, _ := m.names.get(ctx, parentID, parentSchemaID, name, timestamp)
	if descVersion != nil {
		if descVersion.GetModificationTime().LessEq(timestamp) {
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
		if e := c.LookupNamespaceEntry(req[0]); e != nil {
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
	Expiration(ctx context.Context) hlc.Timestamp

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
		desc, _, err := t.findForTimestamp(ctx, timestamp)
		if err == nil {
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
			errRead = m.insertDescriptorVersions(ctx, id, versions)
			if errRead != nil {
				return nil, errRead
			}
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

// isDescriptorStateEmpty determines if a descriptor state exists and
// has any active versions inside it.
func (m *Manager) isDescriptorStateEmpty(id descpb.ID) bool {
	st := m.findDescriptorState(id, false /* create */)
	if st == nil {
		return true
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return len(st.mu.active.data) == 0
}

// maybeWaitForInit waits for the lease manager to startup.
func (m *Manager) maybeWaitForInit() {
	select {
	case <-m.waitForInit:
	case <-m.stopper.ShouldQuiesce():
	}
}

// If create is set, cache and stopper need to be set as well.
func (m *Manager) findDescriptorState(id descpb.ID, create bool) *descriptorState {
	m.maybeWaitForInit()
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.mu.descriptors[id]
	if t == nil && create {
		t = &descriptorState{m: m, id: id, stopper: m.stopper}
		m.mu.descriptors[id] = t
	}
	return t
}

// StartRefreshLeasesTask starts a goroutine that refreshes the lease manager
// leases for descriptors received in the latest system configuration via gossip or
// rangefeeds. This function must be passed a non-nil gossip if
// RangefeedLeases is not active.
func (m *Manager) StartRefreshLeasesTask(ctx context.Context, s *stop.Stopper, db *kv.DB) {
	m.mu.Lock()
	defer m.mu.Unlock()
	defer close(m.waitForInit)
	m.watchForUpdates(ctx)
	_ = s.RunAsyncTask(ctx, "refresh-leases", func(ctx context.Context) {
		for {
			select {
			case id := <-m.descDelCh:
				// Descriptor is marked as deleted, so mark it for deletion or
				// remove it if it's no longer in use.
				_ = s.RunAsyncTask(ctx, "purgeOldVersionsOrAcquireInitialVersion deleted descriptor", func(ctx context.Context) {
					// Once the descriptor is purged notify that some change has occurred.
					defer m.leaseGeneration.Add(1)
					state := m.findNewest(id)
					if state != nil {
						if err := purgeOldVersions(ctx, db, id, true /* dropped */, state.GetVersion(), m); err != nil {
							log.Warningf(ctx, "error purging leases for deleted descriptor %d",
								id)
						}
					}
				})
			case desc := <-m.descUpdateCh:
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
				// purgeOldVersionsOrAcquireInitialVersion will purge older versions of
				// descriptors. Or if a new descriptor shows up then the initial version
				// will be acquired if the schema is already leased to invalidate metadata
				// caches (like optimizer memos).
				purgeOldVersionsOrAcquireInitialVersion := func(ctx context.Context) {
					if m.testingKnobs.TestingOnNewVersion != nil {
						m.testingKnobs.TestingOnNewVersion(desc.GetID())
					}
					// Notify of any new / modified descriptors below once a new lease is
					// acquired.
					defer m.leaseGeneration.Add(1)
					if m.testingKnobs.TestingOnLeaseGenerationBumpForNewVersion != nil {
						defer m.testingKnobs.TestingOnLeaseGenerationBumpForNewVersion(desc.GetID())
					}

					// Whenever a new relation / type is created under an already leased
					// schema we are going to lease the object out immediately. This allows
					// us to use the generation count to cache information like Memo's in
					// the optimizer. The creating object will wait for us to acquire the
					// lease and acknowledge the existence before that txn can return to
					// the user.
					if WaitForInitialVersion.Get(&m.settings.SV) &&
						(!desc.Adding() && !desc.Dropped() && !desc.Offline()) &&
						desc.GetParentSchemaID() != descpb.InvalidID &&
						(m.isDescriptorStateEmpty(desc.GetID())) &&
						m.findDescriptorState(desc.GetParentSchemaID(), false) != nil {
						err := ensureVersion(ctx, desc.GetID(), desc.GetVersion(), m)
						if err != nil {
							log.Warningf(ctx, "error fetching lease for descriptor %s", err)
						}
					}
					// Even if an initial acquisition happens above, we need to purge old
					// descriptor versions, which could have been acquired concurrently.
					// For example the range feed sees version 2 and a query concurrently
					// acquires version 1.
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
					_ = s.RunAsyncTask(ctx, "wait to purgeOldVersionsOrAcquireInitialVersion", func(ctx context.Context) {
						toWait := time.Duration(desc.GetModificationTime().WallTime - now.WallTime)
						select {
						case <-time.After(toWait):
							purgeOldVersionsOrAcquireInitialVersion(ctx)
						case <-ctx.Done():
						case <-s.ShouldQuiesce():
						}
					})
				} else {
					purgeOldVersionsOrAcquireInitialVersion(ctx)
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

// GetLeaseGeneration provides an integer which will change whenever new
// descriptor versions are available. This can be used for fast comparisons
// to make sure previously looked up information is still valid.
func (m *Manager) GetLeaseGeneration() int64 {
	return m.leaseGeneration.Load()
}

// GetSafeReplicationTS gets the timestamp till which the leased descriptors
// have been synced.
func (m *Manager) GetSafeReplicationTS() hlc.Timestamp {
	return m.closeTimestamp.Load().(hlc.Timestamp)
}

// watchForUpdates will watch a rangefeed on the system.descriptor table for
// updates.
func (m *Manager) watchForUpdates(ctx context.Context) {
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
		// Skip updates if rangefeeds are disabled.
		if m.testingKnobs.DisableRangeFeedCheckpoint {
			return
		}
		if len(ev.Value.RawBytes) == 0 {
			id, err := m.Codec().DecodeDescMetadataID(ev.Key)
			if err != nil {
				log.Infof(ctx, "unable to decode metadata key %v", ev.Key)
				return
			}
			select {
			case <-ctx.Done():
			case m.descDelCh <- descpb.ID(id):
			}
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
		case m.descUpdateCh <- mut:
		}
	}

	handleCheckpoint := func(ctx context.Context, checkpoint *kvpb.RangeFeedCheckpoint) {
		// Track checkpoints that occur from the rangefeed to make sure progress
		// is always made.
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.testingKnobs.DisableRangeFeedCheckpoint {
			return
		}
		m.mu.rangeFeedCheckpoints += 1
		m.closeTimestamp.Store(checkpoint.ResolvedTS)
	}

	// If we already started a range feed terminate it first
	if m.mu.rangeFeed != nil {
		m.mu.rangeFeed.Close()
		m.mu.rangeFeed = nil
		if m.testingKnobs.RangeFeedResetChannel != nil {
			close(m.testingKnobs.RangeFeedResetChannel)
			m.testingKnobs.RangeFeedResetChannel = nil
		}
	}
	// Ignore errors here because they indicate that the server is shutting down.
	// Also note that the range feed automatically shuts down when the server
	// shuts down, so we don't need to call Close() ourselves.
	m.mu.rangeFeed, _ = m.rangeFeedFactory.RangeFeed(
		ctx, "lease", []roachpb.Span{descriptorTableSpan}, hlc.Timestamp{}, handleEvent,
		rangefeed.WithSystemTablePriority(),
		rangefeed.WithOnCheckpoint(handleCheckpoint),
		rangefeed.WithOnInternalError(func(ctx context.Context, err error) {
			select {
			case m.rangefeedErrCh <- err:
			case <-ctx.Done():
			case <-m.stopper.ShouldQuiesce():
			}
		}),
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

// getRangeFeedMonitorSettings determines how long the range feed becomes silent
// before we started treating it as an availability issue on the cluster.
func (m *Manager) getRangeFeedMonitorSettings() (timeout time.Duration, monitoringEnabled bool) {
	timeout = LeaseMonitorRangeFeedCheckInterval.Get(&m.settings.SV)
	// Even if the monitoring disabled, the timer will be
	// used to refresh this setting.
	checkFrequency := timeout
	if timeout == 0 {
		timeout = time.Minute
	}
	return timeout, checkFrequency > 0
}

// checkRangeFeedStatus ensures that the range feed is always checkpointing and
// receiving data. On recovery, we will always refresh all descriptors with the
// assumption we have lost updates (especially if restarts have ocurred).
func (m *Manager) checkRangeFeedStatus(ctx context.Context) (forceRefresh bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	lastCheckpoints := m.mu.rangeFeedCheckpoints
	m.mu.rangeFeedCheckpoints = 0
	// No checkpoints have occurred on the range feed, so we are no longer
	// getting any updates. At this point there is some type of availability
	// issue.
	if lastCheckpoints == 0 &&
		m.mu.rangeFeedIsUnavailableAt.IsZero() {
		// Track the first unavailability event.
		m.mu.rangeFeedIsUnavailableAt = timeutil.Now()
		log.Warningf(ctx, "lease manager range feed has stopped making progress.")
	} else if !m.mu.rangeFeedIsUnavailableAt.IsZero() &&
		lastCheckpoints > 0 {
		m.mu.rangeFeedIsUnavailableAt = time.Time{}
		log.Warningf(ctx, "lease manager range feed has recovered.")
		// Force all descriptors to refresh.
		forceRefresh = true
	}
	return forceRefresh
}

// RunBackgroundLeasingTask runs background leasing tasks which are
// responsible for expiring old descriptor versions, monitoring
// range feed progress / recovery, and supporting legacy expiry
// based leases.
func (m *Manager) RunBackgroundLeasingTask(ctx context.Context) {
	// The refresh loop is used to clean up leases that have expired (because of
	// a new version), and track range feed availability. This will run based on
	// the lease duration, but will still periodically run if the duration is zero.
	getRefreshTimerDuration := func() time.Duration {
		if LeaseDuration.Get(&m.storage.settings.SV) <= 0 {
			return 200 * time.Millisecond
		} else {
			return m.storage.jitteredLeaseDuration()
		}
	}
	_ = m.stopper.RunAsyncTask(ctx, "lease-refresher", func(ctx context.Context) {
		refreshTimerDuration := getRefreshTimerDuration()
		var refreshTimer timeutil.Timer
		defer refreshTimer.Stop()
		refreshTimer.Reset(refreshTimerDuration / 2)
		// Used to make sure that the system.descriptor lease is active.
		var rangeFeedProgressWatchDog timeutil.Timer
		rangeFeedProgressWatchDogTimeout,
			rangeFeedProgressWatchDogEnabled := m.getRangeFeedMonitorSettings()
		rangeFeedProgressWatchDog.Reset(rangeFeedProgressWatchDogTimeout)
		for {
			select {
			case <-m.stopper.ShouldQuiesce():
				return

			case <-m.refreshAllLeases:
				m.refreshSomeLeases(ctx, true /*refreshAll*/)
			case <-rangeFeedProgressWatchDog.C:
				rangeFeedProgressWatchDog.Read = true
				// Detect if the range feed has stopped making
				// progress.
				if rangeFeedProgressWatchDogEnabled {
					refreshAllDescriptors := m.checkRangeFeedStatus(ctx)
					// If the range feed recovers after a failure, re-read all
					// descriptors.
					if refreshAllDescriptors {
						m.refreshSomeLeases(ctx, true /*refreshAll*/)
					}
				}
				rangeFeedProgressWatchDogTimeout,
					rangeFeedProgressWatchDogEnabled = m.getRangeFeedMonitorSettings()
				rangeFeedProgressWatchDog.Reset(rangeFeedProgressWatchDogTimeout)
			case err := <-m.rangefeedErrCh:
				log.Warningf(ctx, "lease rangefeed failed with error: %s", err.Error())
				m.handleRangeFeedError(ctx)
				m.refreshSomeLeases(ctx, true /*refreshAll*/)
			case <-refreshTimer.C:
				refreshTimer.Read = true
				refreshTimer.Reset(getRefreshTimerDuration() / 2)

				// Check for any react to any range feed availability problems, and
				// if needed refresh the full set of descriptors.
				m.handleRangeFeedAvailability(ctx)

				// Clean up session based leases that have expired.
				m.cleanupExpiredSessionLeases(ctx)
			}
		}
	})
}

// handleRangeFeedAvailability detects if there is any availability issue
// with the range feed and attempts restarts.
func (m *Manager) handleRangeFeedAvailability(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check to see if the range feed is currently functional, and
	// did not previously go down.
	if m.mu.rangeFeedIsUnavailableAt.IsZero() {
		return
	}

	// If the range feed is down for too long, force a restart of
	// the range feed.
	if timeutil.Since(m.mu.rangeFeedIsUnavailableAt) >=
		LeaseMonitorRangeFeedResetTime.Get(&m.settings.SV) {
		m.restartLeasingRangeFeedLocked(ctx)
	}
}

func (m *Manager) handleRangeFeedError(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.restartLeasingRangeFeedLocked(ctx)
}

func (m *Manager) restartLeasingRangeFeedLocked(ctx context.Context) {
	log.Warning(ctx, "attempting restart of leasing range feed")
	// Attempt a range feed restart if it has been down too long.
	m.watchForUpdates(ctx)
	// Track when the last restart occurred.
	m.mu.rangeFeedIsUnavailableAt = timeutil.Now()
	m.mu.rangeFeedCheckpoints = 0
}

// cleanupExpiredSessionLeases expires session based leases marked for removal,
// which will happen when a new version is published.
func (m *Manager) cleanupExpiredSessionLeases(ctx context.Context) {
	now := m.storage.db.KV().Clock().Now()
	latest := -1
	// Leases which have hit their expiry time, which will be finally discarded
	// from the storage layer.
	var leasesToDiscard []*descriptorVersionState
	func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		// First go through the leases which have an expiry set and find out which
		// ones have expired. The slice is sorted by the expiry time, so once we
		// find the first non-expired lease we are done.
		for i, desc := range m.mu.leasesToExpire {
			if desc.hasExpired(ctx, now) {
				latest = i
			} else {
				break
			}
		}
		// Next extract the slice of the leases that needed to be removed from
		// storage.
		if latest >= 0 {
			leasesToDiscard = m.mu.leasesToExpire[0 : latest+1]
			m.storage.sessionBasedLeasesWaitingToExpire.Dec(int64(len(leasesToDiscard)))
			m.mu.leasesToExpire = m.mu.leasesToExpire[latest+1:]
		}
	}()

	// Finally for each lease that has expired we can discard it from the storage
	// layer.
	if len(leasesToDiscard) > 0 {
		if err := m.stopper.RunAsyncTask(ctx, "clearing expired session based leases from storage", func(ctx context.Context) {
			for _, l := range leasesToDiscard {
				l.mu.Lock()
				leaseToDelete := l.mu.lease
				l.mu.lease = nil
				l.mu.Unlock()
				// Its possible the reference count has concurrently hit
				// zero and been cleaned up, so check if there is a lease
				// to delete first.
				if leaseToDelete != nil {
					m.storage.release(ctx, m.stopper, leaseToDelete)
					m.storage.sessionBasedLeasesExpired.Inc(1)
				}

			}
		}); err != nil {
			log.Infof(ctx, "unable to delete leases from storage %s", err)
		}
	}
}

// Refresh some of the current leases.
func (m *Manager) refreshSomeLeases(ctx context.Context, includeAll bool) {
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
			if i++; i > limit && !includeAll {
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
	// Indicate some descriptor has changed at the end of the manual
	// refresh.
	m.leaseGeneration.Add(1)
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

		// Read orphaned leases from the system.lease table.
		query := `SELECT s."desc_id",  s.version, s."session_id", s.crdb_region FROM system.lease as s 
		WHERE s."sql_instance_id"=%d
`
		sqlQuery := fmt.Sprintf(query, instanceID)

		var rows []tree.Datums
		retryOptions := base.DefaultRetryOptions()
		retryOptions.Closer = m.stopper.ShouldQuiesce()
		// The retry is required because of errors caused by node restarts. Retry 30 times.
		if err := retry.WithMaxAttempts(ctx, retryOptions, 30, func() error {
			return m.storage.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				if err := txn.KV().SetFixedTimestamp(ctx, hlc.Timestamp{WallTime: timeThreshold}); err != nil {
					return err
				}
				var err error
				rows, err = txn.QueryBuffered(
					ctx, "read orphaned leases", txn.KV(), sqlQuery,
				)
				return err
			})
		}); err != nil {
			log.Warningf(ctx, "unable to read orphaned leases: %v", err)
			return
		}
		var wg sync.WaitGroup
		defer wg.Wait()
		for i := range rows {
			// Early exit?
			row := rows[i]
			wg.Add(1)
			lease := &storedLease{
				id:      descpb.ID(tree.MustBeDInt(row[0])),
				version: int(tree.MustBeDInt(row[1])),
			}
			if row[2] != tree.DNull {
				lease.sessionID = []byte(tree.MustBeDBytes(row[2]))
			}
			if ed, ok := row[3].(*tree.DEnum); ok {
				lease.prefix = ed.PhysicalRep
			} else if bd, ok := row[3].(*tree.DBytes); ok {
				lease.prefix = []byte((*bd))
			}
			if err := m.stopper.RunAsyncTaskEx(
				ctx,
				stop.TaskOpts{
					TaskName:   fmt.Sprintf("release lease %+v", lease),
					Sem:        m.sem,
					WaitForSem: true,
				},
				func(ctx context.Context) {
					m.storage.release(ctx, m.stopper, lease)
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
	OutstandingLeases                    *metric.Gauge
	SessionBasedLeasesWaitingToExpire    *metric.Gauge
	SessionBasedLeasesExpired            *metric.Gauge
	LongWaitForOneVersionsActive         *metric.Gauge
	LongWaitForNoVersionsActive          *metric.Gauge
	LongWaitForTwoVersionInvariantActive *metric.Gauge
	LongWaitForInitialVersionActive      *metric.Gauge
}

// MetricsStruct returns a struct containing all of this Manager's metrics.
func (m *Manager) MetricsStruct() Metrics {
	return Metrics{
		OutstandingLeases:                    m.storage.outstandingLeases,
		SessionBasedLeasesExpired:            m.storage.sessionBasedLeasesExpired,
		SessionBasedLeasesWaitingToExpire:    m.storage.sessionBasedLeasesWaitingToExpire,
		LongWaitForNoVersionsActive:          m.storage.longWaitForNoVersionsActive,
		LongWaitForOneVersionsActive:         m.storage.longWaitForOneVersionsActive,
		LongWaitForTwoVersionInvariantActive: m.storage.longTwoVersionInvariantViolationWaitActive,
		LongWaitForInitialVersionActive:      m.storage.longWaitForInitialVersionActive,
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

// AfterLeaseDurationGauge metric to increment after a long wait.
type AfterLeaseDurationGauge int

const (
	_ AfterLeaseDurationGauge = iota
	// GaugeWaitForOneVersion gauge for WaitForOneVersion.
	GaugeWaitForOneVersion
	// GaugeWaitForNoVersion gauge for WaitForNoVersion.
	GaugeWaitForNoVersion
	// GaugeWaitForTwoVersionViolation gauge for CheckTwoVersionInvariant.
	GaugeWaitForTwoVersionViolation
	// GaugeWaitForInitialVersion gauge for WaitForInitialVersion.
	GaugeWaitForInitialVersion
)

// IncGaugeAfterLeaseDuration increments a wait metric after the lease duration
// has passed. A function is returned to decrement the same metric after.
func (m *Manager) IncGaugeAfterLeaseDuration(
	gaugeType AfterLeaseDurationGauge,
) (decrAfterWait func()) {
	var gauge *metric.Gauge
	switch gaugeType {
	case GaugeWaitForOneVersion:
		gauge = m.storage.longWaitForOneVersionsActive
	case GaugeWaitForNoVersion:
		gauge = m.storage.longWaitForNoVersionsActive
	case GaugeWaitForTwoVersionViolation:
		gauge = m.storage.longTwoVersionInvariantViolationWaitActive
	case GaugeWaitForInitialVersion:
		gauge = m.storage.longWaitForInitialVersionActive
	default:
		panic(errors.Newf("unknown gauge type %d", gaugeType))
	}
	leaseDuration := LeaseDuration.Get(&m.settings.SV)
	timer := time.AfterFunc(leaseDuration, func() {
		gauge.Inc(1)
	})
	return func() {
		if !timer.Stop() {
			gauge.Dec(1)
		}
	}
}

// waitStatsTracker is used to maintain the stats in descpb.WaitStats
type waitStatsTracker struct {
	ws        descpb.WaitStats
	startTime time.Time
	recSpan   *tracing.Span // Set only if recording events in the span
}

// startWaitStatsTracker will initialize the waitStatsTracker. If the span is
// set up for recording, then it will save state so that we can call
// RecordStructured as we collect stats. If it isn't setup for recording, an
// empty struct is returned. Subsequent calls to updateProgress/end will still
// work but behave as no-ops.
func startWaitStatsTracker(ctx context.Context) waitStatsTracker {
	if sp := tracing.SpanFromContext(ctx); sp.RecordingType() != tracingpb.RecordingOff {
		return waitStatsTracker{
			startTime: timeutil.Now(),
			recSpan:   sp,
			ws: descpb.WaitStats{
				Uuid: uuid.NewV4(),
			},
		}
	}
	return waitStatsTracker{}
}

// updateProgress will refresh stats while we are in the middle of waiting.
func (w *waitStatsTracker) updateProgress(detail countDetail) {
	if w.recSpan != nil {
		w.ws.NumRetries++
		w.ws.LastCount = int32(detail.count)
		w.ws.SampleSqlInstanceId = int32(detail.sampleSQLInstanceID)
		w.ws.NumSqlInstances = int32(detail.numSQLInstances)
		w.ws.TargetCount = int32(detail.targetCount)
		w.ws.ElapsedTimeInMs = timeutil.Since(w.startTime).Milliseconds()
		w.recSpan.RecordStructured(&w.ws)
	}
}

// end is called when the wait is over.
func (w *waitStatsTracker) end() {
	if w.recSpan != nil {
		w.ws.ElapsedTimeInMs = timeutil.Since(w.startTime).Milliseconds()
		w.ws.LastCount = 0
		w.recSpan.RecordStructured(&w.ws)
	}
}

// TestingSetDisableRangeFeedCheckpointFn sets the testing knob used to
// disable rangefeed checkpoints.
func (m *Manager) TestingSetDisableRangeFeedCheckpointFn(disable bool) chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.rangeFeedCheckpoints = 0
	m.testingKnobs.DisableRangeFeedCheckpoint = disable
	if disable {
		m.testingKnobs.RangeFeedResetChannel = make(chan struct{})
	} else {
		m.testingKnobs.RangeFeedResetChannel = nil
	}
	return m.testingKnobs.RangeFeedResetChannel
}

// TestingMarkInit marks the lease manager as initialized without a range feed being started.
// This is only used for testing.
func (m *Manager) TestingMarkInit() {
	m.mu.Lock()
	defer m.mu.Unlock()
	close(m.waitForInit)
}
