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
	"github.com/cockroachdb/cockroach/pkg/multitenant"
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
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	kvstorage "github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/startup"
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
	"github.com/google/btree"
)

var errRenewLease = errors.New("renew lease on id")
var errReadOlderVersion = errors.New("read older descriptor version from store")
var errLeaseManagerIsDraining = errors.New("cannot acquire lease when draining")

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

var LockedLeaseTimestamp = settings.RegisterBoolSetting(settings.ApplicationLevel,
	"sql.catalog.descriptor_lease.use_locked_timestamps.enabled",
	"guarantees transactional version consistency for descriptors used by the lease manager,"+
		"descriptors used can be intentionally older to support this",
	true)

var RetainOldVersionsForLocked = settings.RegisterBoolSetting(settings.ApplicationLevel,
	"sql.catalog.descriptor_lease.lock_old_versions.enabled",
	"enables retaining old versions to avoid retries when locked lease timestamps are enabled, at the expense "+
		"of delaying schema changes",
	false)

var MaxBatchLeaseCount = settings.RegisterIntSetting(settings.ApplicationLevel,
	"sql.catalog.descriptor_lease.max_batch_lease_count",
	"the maximum number of descriptors to lease in a single batch",
	1000)

// GetLockedLeaseTimestampEnabled returns if locked leasing timestamps are enabled.
func GetLockedLeaseTimestampEnabled(ctx context.Context, settings *cluster.Settings) bool {
	return LockedLeaseTimestamp.Get(&settings.SV) &&
		settings.Version.IsActive(ctx, clusterversion.V26_1)
}

// WaitForNoVersion returns once there are no unexpired leases left
// for any version of the descriptor.
func (m *Manager) WaitForNoVersion(
	ctx context.Context,
	id descpb.ID,
	regions regionliveness.CachedDatabaseRegions,
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
		detail, err := countLeasesWithDetail(ctx, m.storage.db, m.Codec(), regions, m.settings, versions, now, true /*forAnyVersion*/)
		if err != nil {
			return err
		}
		if detail.count == 0 {
			break
		}
		if detail.count != lastCount {
			lastCount = detail.count
			wsTracker.updateProgress(detail)
			log.Dev.Infof(ctx, "waiting for %d leases to expire: desc=%d", detail.count, id)
		}
		if lastCount == 0 {
			break
		}
	}
	return nil
}

// maybeGetDescriptorsWithoutValidation gets descriptors without validating from
// the KV layer.
func (m *Manager) maybeGetDescriptorsWithoutValidation(
	ctx context.Context, ids descpb.IDs, existenceExpected bool,
) (catalog.Descriptors, error) {
	descs := make(catalog.Descriptors, 0, len(ids))

	if err := m.storage.db.KV().Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		const isDescriptorRequired = false
		cr := m.storage.newCatalogReader(ctx)
		c, err := cr.GetByIDs(ctx, txn, ids, isDescriptorRequired, catalog.Any)
		if err != nil {
			return err
		}

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
	}); err != nil {
		return nil, err
	}

	return descs, nil
}

// maybeGetDescriptorWithoutValidation gets a descriptor without validating
// from the KV layer.
func (m *Manager) maybeGetDescriptorWithoutValidation(
	ctx context.Context, id descpb.ID, existenceExpected bool,
) (catalog.Descriptor, error) {
	descArr, err := m.maybeGetDescriptorsWithoutValidation(ctx, descpb.IDs{id}, existenceExpected)
	if err != nil {
		return nil, err
	}

	if len(descArr) == 0 {
		return nil, nil
	}

	return descArr[0], nil
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

// countSessionsHoldingStaleDescriptor finds sessionIDs that are holding a lease
// on the previous version of desc but not the current version.
func countSessionsHoldingStaleDescriptor(
	ctx context.Context, txn isql.Txn, desc catalog.Descriptor, region string,
) (int, error) {
	b := strings.Builder{}

	// Counts sessions that have previous version of the descriptor but not the current version
	b.WriteString(fmt.Sprintf(`
		SELECT count(DISTINCT l1.session_id)
		FROM system.lease l1 
		WHERE l1.desc_id = %d 
		AND l1.version < %d 
		AND crdb_internal.sql_liveness_is_alive(l1.session_id)
		AND NOT EXISTS (
			SELECT 1 FROM system.lease l2 
			WHERE l2.desc_id = l1.desc_id 
			AND l2.session_id = l1.session_id 
			AND l2.version = %d
		`, desc.GetID(), desc.GetVersion(), desc.GetVersion()))
	if region != "" {
		b.WriteString(fmt.Sprintf(" AND l2.crdb_region='%s'", region))
	}
	b.WriteString(")")
	if region != "" {
		b.WriteString(fmt.Sprintf(" AND l1.crdb_region='%s'", region))
	}

	rows, err := txn.QueryBuffered(ctx, "count-sessions-holding-stale-descriptor", txn.KV(), b.String())
	if err != nil {
		return 0, err
	}

	if len(rows) == 0 {
		return 0, nil
	}

	return int(tree.MustBeDInt(rows[0][0])), nil
}

// WaitForInitialVersion waits for a lease to be acquired on a newly created
// object on any nodes that have already leased the schema out. This ensures
// that their leaseGeneration is incremented before the user commit completes,
// which will ensure that any existing cached queries will detect the new object
// (i.e. the optimizer memo will use the generation value as short circuit).
func (m *Manager) WaitForInitialVersion(
	ctx context.Context,
	descriptorsIds descpb.IDs,
	regions regionliveness.CachedDatabaseRegions,
	retryOpts retry.Options,
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
						skipRegion, handledErr := handleRegionLivenessErrors(ctx, prober, region, err)
						if skipRegion {
							return nil
						}
						return handledErr
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
						skipRegion, err := handleRegionLivenessErrors(ctx, prober, region, err)
						if err != nil {
							return err
						}
						if skipRegion {
							return nil
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
			log.Dev.Infof(ctx, "waiting for descriptors %v to appear on %d nodes. Last count was %d", ids.Ordered(), totalExpectedCount, totalCount)
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
// If the descriptor is not found, an error will be returned.
func (m *Manager) WaitForOneVersion(
	ctx context.Context,
	id descpb.ID,
	regions regionliveness.CachedDatabaseRegions,
	retryOpts retry.Options,
) (catalog.Descriptor, error) {
	// Increment the long wait gauge for wait for one version, if this function
	// takes longer than the lease duration.
	decAfterWait := m.IncGaugeAfterLeaseDuration(GaugeWaitForOneVersion)
	defer decAfterWait()
	wsTracker := startWaitStatsTracker(ctx)
	defer wsTracker.end()

	var desc catalog.Descriptor
	for lastCount, r := 0, retry.Start(retryOpts); r.Next(); {
		var err error
		desc, err = m.maybeGetDescriptorWithoutValidation(ctx, id, true)
		if err != nil {
			return nil, err
		}

		// Check to see if there are any leases that still exist on the previous
		// version of the descriptor.
		now := m.storage.clock.Now()
		descs := []IDVersion{NewIDVersionPrev(desc.GetName(), desc.GetID(), desc.GetVersion())}
		detail, err := countLeasesWithDetail(ctx, m.storage.db, m.Codec(), regions, m.settings, descs, now, false /*forAnyVersion*/)
		if err != nil {
			return nil, err
		}
		if detail.count == 0 {
			log.Dev.Infof(ctx, "all leases have expired at %v: desc=%v", now, descs)
			break
		}
		if detail.count != lastCount {
			lastCount = detail.count
			wsTracker.updateProgress(detail)
			log.Dev.Infof(ctx, "waiting for %d leases to expire: desc=%v", detail.count, descs)
		}
	}

	return desc, nil
}

// WaitForNewVersion returns once all leaseholders of any version of the
// descriptor hold a lease of the current version.
//
// The MaxRetries and MaxDuration in retryOpts should not be set.
func (m *Manager) WaitForNewVersion(
	ctx context.Context,
	id descpb.ID,
	regions regionliveness.CachedDatabaseRegions,
	retryOpts retry.Options,
) (catalog.Descriptor, error) {
	if retryOpts.MaxRetries != 0 {
		return nil, errors.New("The MaxRetries option shouldn't be set in WaitForNewVersion")
	}
	if retryOpts.MaxDuration != 0 {
		return nil, errors.New("The MaxDuration option shouldn't be set in WaitForNewVersion")
	}

	var desc catalog.Descriptor

	var success bool
	// Block until each leaseholder on the previous version of the descriptor
	// also holds a lease on the current version of the descriptor (`for all
	// session: (session in Prev => session in Curr)` for the set theory
	// enjoyers).
	for r := retry.Start(retryOpts); r.Next(); {
		var err error
		desc, err = m.maybeGetDescriptorWithoutValidation(ctx, id, true)
		if err != nil {
			return nil, err
		}

		db := m.storage.db

		// Get the sessions with leases in each region.
		if err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			prober := regionliveness.NewLivenessProber(db.KV(), m.storage.codec, regions, m.settings)
			regionMap, err := prober.QueryLiveness(ctx, txn.KV())
			if err != nil {
				return err
			}

			// On single region clusters we can query everything at once.
			if regionMap == nil {
				regionMap = regionliveness.LiveRegions{"": struct{}{}}
			}

			var staleSessionCount int
			for region := range regionMap {
				var regionStaleSessionCount int
				var err error
				if hasTimeout, timeout := prober.GetProbeTimeout(); hasTimeout {
					err = timeutil.RunWithTimeout(ctx, "count-sessions-holding-stale-descriptor-by-region", timeout, func(ctx context.Context) (countErr error) {
						regionStaleSessionCount, countErr = countSessionsHoldingStaleDescriptor(ctx, txn, desc, region)
						return countErr
					})
				} else {
					regionStaleSessionCount, err = countSessionsHoldingStaleDescriptor(ctx, txn, desc, region)
				}
				if err != nil {
					skipRegion, handledErr := handleRegionLivenessErrors(ctx, prober, region, err)
					if handledErr != nil {
						return handledErr
					}
					if skipRegion {
						continue
					}
				}

				staleSessionCount += regionStaleSessionCount

				if regionStaleSessionCount != 0 { // quit early
					if region == "" {
						log.Dev.Infof(ctx, "%d sessions holding stale descriptor", regionStaleSessionCount)
					} else {
						log.Dev.Infof(ctx, "Region '%s' has %d sessions holding stale descriptor", region, regionStaleSessionCount)
					}
					break
				}
			}

			if staleSessionCount == 0 {
				success = true
			}

			return nil
		}); err != nil {
			return nil, err
		}

		if success {
			break
		}
	}
	if !success {
		return nil, errors.New("Exited lease acquisition loop before success")
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

	s := m.findNewest(id)
	if s != nil && s.GetVersion() < minVersion {
		return errors.Errorf("version %d for descriptor %s does not exist yet", minVersion, s.GetName())
	} else if s != nil {
		// Process any descriptor updates that will allow us to move the close timestamp
		// forward.
		m.processDescriptorUpdate(ctx, id, s.GetVersion(), s.GetModificationTime())
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
	isOffline bool,
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
					if len(descContent) == 0 {
						// Skip any deletions of the descriptor.
						if isOffline {
							subsequentModificationTime = k.Timestamp
							continue
						}
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
	endTimestamp, isOffline, done := func() (hlc.Timestamp, bool, bool) {
		t.mu.Lock()
		defer t.mu.Unlock()

		// If there are no descriptors, then we won't have a valid end timestamp.
		if len(t.mu.active.data) == 0 {
			// If the descriptor is offline, then we can return when it was taken
			// offline, a valid descriptor exists within this interval.
			// Note: This allows us to populate dropped descriptors, which will
			// be cleaned up via purgeOldVersions when these descriptors are actually
			// deleted from the descriptor table (via the range feed).
			if t.mu.takenOffline {
				return t.mu.takenOfflineAt, true, false
			}
			return hlc.Timestamp{}, false, true
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
			return hlc.Timestamp{}, false, true
		}
		return t.mu.active.data[i].GetModificationTime(), false, false
	}()
	if done {
		return nil, nil
	}

	// Retrieve descriptors in range [timestamp, endTimestamp) in decreasing
	// modification time order.
	descs, err := getDescriptorsFromStoreForInterval(
		ctx, m.storage.db.KV(), m.Codec(), id, timestamp, endTimestamp, isOffline,
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
	// modification time prior to the timestamp. For offline descriptors, we will
	// try reading again if nothing was read above as a last resort.
	if timestamp.Less(earliestModificationTime) || (isOffline && len(descs) == 0) {
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

// wrapMemoryError adds a hint on memory errors to indicate
// which setting should be bumped.
func wrapMemoryError(err error) error {
	return errors.WithHint(err, "Consider increasing --max-sql-memory startup parameter.")
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
	newVersionsToInsert := make([]*descriptorVersionState, 0, len(versions))
	defer func() {
		// Release memory for anything not inserted, entries are
		// cleared after each insert.
		for _, state := range newVersionsToInsert {
			if state == nil {
				continue
			}
			t.m.boundAccount.Shrink(ctx, state.getByteSize())
		}
	}()
	for i := range versions {
		// Since we gave up the lock while reading the versions from
		// the store we have to ensure that no one else inserted the
		// same version.
		existingVersion := t.mu.active.findVersion(versions[i].desc.GetVersion())
		if existingVersion == nil {
			descState := newDescriptorVersionState(t, versions[i].desc, versions[i].expiration, session, nil, false)
			if err := t.m.boundAccount.Grow(ctx, descState.getByteSize()); err != nil {
				return wrapMemoryError(err)
			}
			newVersionsToInsert = append(newVersionsToInsert, descState)
		}
	}
	// Only insert if all versions were allocated.
	for idx, descState := range newVersionsToInsert {
		t.mu.active.insert(descState)
		newVersionsToInsert[idx] = nil
	}

	return nil
}

// AcquireFreshestFromStore acquires a new lease from the store and
// inserts it into the active set. It guarantees that the lease returned is
// the one acquired after the call is made. Use this if the lease we want to
// get needs to see some descriptor updates that we know happened recently.
func (m *Manager) AcquireFreshestFromStore(ctx context.Context, id descpb.ID) error {
	// Create descriptorState if needed.
	state := m.findDescriptorState(id, true /* create */)
	state.markAcquisitionStart(ctx)
	defer state.markAcquisitionDone(ctx)
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
		didAcquire, err := m.acquireNodeLease(ctx, id, AcquireFreshestBlock)
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

// upsertDescriptorIntoState inserts a descriptor into the descriptor state.
func (m *Manager) upsertDescriptorIntoState(
	ctx context.Context, id descpb.ID, session sqlliveness.Session, desc catalog.Descriptor,
) error {
	t := m.findDescriptorState(id, false /* create */)
	if t == nil {
		return errors.AssertionFailedf("could not find descriptor state for id %d", id)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.setTakenOfflineLocked(false)
	err := t.upsertLeaseLocked(ctx, desc, session, m.storage.getRegionPrefix())
	if err != nil {
		return err
	}
	return nil
}

// maybePrepareDescriptorForBulkAcquisition sets up the descriptor state for a bulk
// acquisition. Returns true if the descriptor has been setup for bulk acquistion,
// and false if an acqusition is already fetching it.
func (m *Manager) maybePrepareDescriptorForBulkAcquisition(
	id descpb.ID, acquisitionCh chan struct{},
) bool {
	state := m.findDescriptorState(id, true)
	state.mu.Lock()
	defer state.mu.Unlock()
	// Only work on descriptors that haven't been acquired or have no
	// acquisition in progress.
	if (state.mu.takenOffline || len(state.mu.active.data) > 0) ||
		state.mu.acquisitionsInProgress > 0 {
		return false
	}
	// Mark the descriptor as having an acquisition in progress, any normal
	// acquisition will wait for the bulk acquisition to finish.
	state.mu.acquisitionsInProgress++
	state.mu.acquisitionChannel = acquisitionCh
	return true
}

// completeBulkAcquisition clears up the descriptor state after a bulk acquisition.
func (m *Manager) completeBulkAcquisition(id descpb.ID) {
	state := m.findDescriptorState(id, false)
	if state == nil {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	state.mu.acquisitionChannel = nil
	state.mu.acquisitionsInProgress -= 1
}

// EnsureBatch ensures that a set of IDs are already cached by the lease manager
// by doing a bulk acquisition. The acquisition will be done on a best effort
// basis, where missing descriptors, dropped, adding, or validation errors will
// be ignored.
func (m *Manager) EnsureBatch(ctx context.Context, ids []descpb.ID) error {
	maxBatchSize := MaxBatchLeaseCount.Get(&m.storage.settings.SV)
	idsToFetch := make([]descpb.ID, 0, min(len(ids), int(maxBatchSize)))
	lastVersion := make([]descpb.DescriptorVersion, cap(idsToFetch))
	lastSession := make([]sqlliveness.SessionID, cap(idsToFetch))
	for len(ids) > 0 {
		processNextBatch := func() error {
			// Reset for the next batch.
			idsToFetch = idsToFetch[:0]
			acquisitionCh := make(chan struct{})
			defer close(acquisitionCh)

			// Figure out which IDs have no state object allocated.
			batchCompleted := true
			for idx, id := range ids {
				if !m.maybePrepareDescriptorForBulkAcquisition(id, acquisitionCh) {
					continue
				}
				idsToFetch = append(idsToFetch, id)
				if len(idsToFetch) == int(maxBatchSize) {
					ids = ids[idx+1:]
					batchCompleted = false
					break
				}
			}
			// All of the entries in the slice have been processed.
			if batchCompleted {
				ids = nil
			}
			// Nothing needs to be fetched everything is cached in memory.
			if len(idsToFetch) == 0 {
				return nil
			}
			// Clean up the acquisition state after-wards.
			defer func() {
				for _, id := range idsToFetch {
					m.completeBulkAcquisition(id)
				}
			}()
			// Initial acquisition so version and session ID are blank for any
			// previous version.
			lastVersion = lastVersion[:len(idsToFetch)]
			lastSession = lastSession[:len(idsToFetch)]
			// Execute a batch acquisition at the storage layer.
			session, err := m.storage.livenessProvider.Session(ctx)
			if err != nil {
				return errors.Wrapf(err, "lease acquisition was unable to resolve liveness session")
			}
			batch, _, err := m.storage.acquireBatch(ctx, session, idsToFetch, lastVersion, lastSession)
			if err != nil {
				return err
			}
			// Upsert the descriptors into the descriptor state.
			for idx, id := range idsToFetch {
				// If any descriptors in the batch have failed to acquire, we are going
				// to skip them. This can be due to validation error, being dropped, or
				// being added, or missing. The caller can surface these when it attempts
				// to access these leases, and we will attempt acquisition again.
				if batch.errs[idx] != nil {
					continue
				}
				if err := m.upsertDescriptorIntoState(ctx, id, session, batch.descs[idx]); err != nil {
					return err
				}
			}
			return nil
		}
		if err := processNextBatch(); err != nil {
			return err
		}
	}
	return nil
}

// If the lease cannot be obtained because the descriptor is in the process of
// being dropped or offline, the error will be of type inactiveTableError.
// The boolean returned is true if this call was actually responsible for the
// lease acquisition. Any callers must allocate the descriptor state and mark
// that an acquisition is in progress with descriptorState.markAcquisitionStart,
// before invoking this method.
func (m *Manager) acquireNodeLease(
	ctx context.Context, id descpb.ID, typ AcquireType,
) (bool, error) {
	// Precondition: Ensure that the descriptor state is setup and ready for any
	// acquisition.
	if buildutil.CrdbTestBuild {
		state := m.findDescriptorState(id, false)
		if state == nil {
			return false, errors.AssertionFailedf("descriptor state must be allocated before acquistion of %d", id)
		}
		state.mu.Lock()
		acquisition := state.mu.acquisitionsInProgress
		state.mu.Unlock()
		if acquisition <= 0 {
			return false, errors.AssertionFailedf("descriptor acquistion must be marked before invoking acquire node lease on %d.", id)
		}
	}
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
				return false, errLeaseManagerIsDraining
			}
			waitedForBulkAcquire, err := func() (bool, error) {
				state := m.findDescriptorState(id, false)
				state.mu.Lock()
				acquisitionChannel := state.mu.acquisitionChannel
				state.mu.Unlock()
				if acquisitionChannel == nil {
					return false, nil
				}
				select {
				case <-acquisitionChannel:
					return true, nil
				case <-ctx.Done():
					return false, ctx.Err()
				}
			}()
			if waitedForBulkAcquire || err != nil {
				// We didn't do any acquiring and just waited for a bulk operation.
				return false, err
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

			doAcquisition := func() (catalog.Descriptor, error) {
				desc, _, err := m.storage.acquire(ctx, session, id, currentVersion, currentSessionID)
				if err != nil {
					return nil, err
				}

				return desc, nil
			}

			// These tables are special and can have their versions bumped
			// without blocking on other nodes converging to that version.
			if newest != nil && (id == keys.UsersTableID ||
				id == keys.RoleMembersTableID ||
				id == keys.RoleOptionsTableID ||
				m.isMaybeSystemPrivilegesTable(ctx, id)) {

				// The two-version invariant allows an update in lease manager
				// without (immediately) acquiring a new lease. This prevents
				// a race on where the lease is acquired but the manager isn't
				// yet updated.
				desc, err := m.maybeGetDescriptorWithoutValidation(ctx, id, true /* existenceRequired */)
				if err != nil {
					return false, err
				}
				if err := m.upsertDescriptorIntoState(ctx, id, session, desc); err != nil {
					return false, err
				}
				// Descriptor versions for these tables participate directly in memo
				// staleness checks. Bump the lease generation now so queries that
				// rely on the new privileges/options are forced to replan immediately
				// instead of waiting for the asynchronous lease update.
				m.leaseGeneration.Add(1)

				desc, err = doAcquisition()
				if err != nil {
					return false, err
				}
				if desc == nil {
					return true, nil
				}
			} else {
				desc, err := doAcquisition()
				if err != nil {
					return false, err
				}
				// If a nil descriptor is returned, then the latest version has already
				// been leased. So, nothing needs to be done here.
				if desc == nil {
					return true, nil
				}
				if err := m.upsertDescriptorIntoState(ctx, id, session, desc); err != nil {
					return false, err
				}
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

var systemPrivilegesTableDescID atomic.Uint32

// isMaybeSystemPrivilegesTable tries to determine if the given descriptor is
// for the system privileges table. It depends on the namecache to resolve the
// table descriptor after which it's memoized.
func (m *Manager) isMaybeSystemPrivilegesTable(ctx context.Context, id descpb.ID) bool {
	if systemPrivilegesTableDescID.Load() == uint32(descpb.InvalidID) {
		if privilegesDesc, _ := m.names.get(ctx, keys.SystemDatabaseID, keys.SystemPublicSchemaID, "privileges", hlc.Timestamp{}); privilegesDesc != nil {
			systemPrivilegesTableDescID.CompareAndSwap(uint32(descpb.InvalidID), uint32(privilegesDesc.GetID()))
			privilegesDesc.Release(ctx)
		}
	}

	return uint32(id) == systemPrivilegesTableDescID.Load()
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
		log.Dev.Warningf(ctx, "error: %s, not releasing lease: %q", err, lease)
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
func (m *Manager) purgeOldVersions(
	ctx context.Context, db *kv.DB, id descpb.ID, dropped bool, minVersion descpb.DescriptorVersion,
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
			t.setTakenOfflineLocked(dropped)
			return t.removeInactiveVersions(ctx), t.mu.active.findPreviousToExpire(dropped)
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
				defer m.mu.Unlock()
				leaseToExpire.mu.Lock()
				defer leaseToExpire.mu.Unlock()
				// Expire any active old versions into the future based on the lease
				// duration. If the session lifetime had been longer then use
				// that. We will only expire later into the future, then what
				// was previously observed, since transactions may have already
				// picked this time. If the lease duration is zero, then we are
				// looking at instant expiration for testing.
				leaseDuration := LeaseDuration.Get(&m.storage.settings.SV)
				expiration := m.storage.db.KV().Clock().Now().AddDuration(leaseDuration)
				if sessionExpiry := (*leaseToExpire.session.Load()).Expiration(); leaseDuration > 0 && expiration.Less(sessionExpiry) {
					expiration = sessionExpiry
				}
				leaseToExpire.expiration.Store(&expiration)
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

	var err error
	var desc *descriptorVersionState
	for r := retry.StartWithCtx(ctx,
		retry.Options{
			MaxDuration: time.Second * 30}); r.Next(); {
		// Acquire a refcount on the descriptor on the latest version to maintain an
		// active lease so that it doesn't get released when removeInactives()
		// is called below. Release this lease after calling removeInactives().
		desc, _, err = t.findForTimestamp(ctx, TimestampToReadTimestamp(m.storage.clock.Now()))
		if err == nil || !errors.Is(err, errRenewLease) {
			break
		}
		// We encountered an error telling us to renew the lease.
		newest := m.findNewest(id)
		// It is possible that a concurrent drop / removal of this descriptor is
		// occurring. If the newest version just doesn't exist, bail out.
		if newest == nil {
			break
		}
		// Assert this should never happen due to a fixed expiration, since the range
		// feed is responsible for purging old versions and acquiring new versions.
		if newest.hasFixedExpiration() {
			return errors.AssertionFailedf("the latest version of the descriptor has " +
				"a fixed expiration, this should never happen")
		}
		// Otherwise, we ran into some type of transient issue, where the sqllivness
		// session was expired. This could happen if the sqlliveness range is slow
		// for some reason.
		log.Dev.Infof(ctx, "unable to acquire lease on latest descriptor "+
			"version of ID: %d, retrying...", id)
	}
	// As a last resort, we will release all versions of the descriptor. This is
	// suboptimal, but the safest option.
	if errors.Is(err, errRenewLease) {
		log.Dev.Warningf(ctx, "unable to acquire lease on latest descriptor "+
			"version of ID: %d, cleaning up all versions from storage.", id)
		err = nil
	}

	// Optionally, acquire the refcount on the previous version for the locked
	// leasing mode.
	acquireLeaseOnPrevious := func() error {
		if !GetLockedLeaseTimestampEnabled(ctx, m.storage.settings) ||
			!RetainOldVersionsForLocked.Get(&m.storage.settings.SV) {
			return nil
		}
		var handles []*closeTimeStampHandle
		// Release the timestamp handles, which will allow this lease to be cleaned
		// up if the timestamps are destroyed. Note: The descriptor state cannot
		// be locked for the release mechanism.
		defer func() {
			for _, handle := range handles {
				handle.release(ctx)
			}
		}()
		prev, newest := func() (*descriptorVersionState, *descriptorVersionState) {
			state := m.findDescriptorState(id, false)
			state.mu.Lock()
			defer state.mu.Unlock()
			// Find the previous version and determine the timestamp handle that
			// it belongs to.
			return state.mu.active.findPrevious(), state.mu.active.findNewest()
		}()
		// If there is no previous or the previous version is a historical descriptor,
		// nothing needs to be done (i.e. no active lease or already setup for
		// expiration).
		if prev == nil || prev.session.Load() == nil || prev.expiration.Load() != nil {
			return nil
		}
		// Get all the close timestamp handles that require this descriptor version.
		handles = m.shouldRetainPriorVersions(ctx, prev.GetModificationTime())
		if handles == nil {
			return nil
		}
		var gatheredErrors error
		for _, handle := range handles {
			if err := func() error {
				handle.mu.Lock()
				defer handle.mu.Unlock()
				if handle.mu.leasesToRelease == nil {
					return nil
				}
				// If there is already an old version of the descriptor, then we have
				// a huge problem. We are violating the two versions invariant, since at
				// least 3 versions exist.
				if _, ok := handle.mu.leasesToRelease[id]; ok {
					return errors.AssertionFailedf("two version invariant was violated, since "+
						"we are retaining more than two versions for %s (%d) (versions: %d (%s), %d (%s), %d (%s))",
						prev.Underlying().GetName(),
						prev.GetID(),
						handle.mu.leasesToRelease[id].Underlying().GetVersion(),
						handle.mu.leasesToRelease[id].(*descriptorVersionState).getExpiration(ctx),
						prev.GetVersion(),
						prev.getExpiration(ctx),
						newest.GetVersion(),
						newest.getExpiration(ctx))
				}
				// Bump the refcount on the previous version of the descriptor and attach
				// it to the relevant timestamp.
				prev.incRefCount(ctx, false)
				handle.mu.leasesToRelease[id] = prev
				return nil
			}(); err != nil {
				// Gather errors and keep acquiring for other timestamps.
				gatheredErrors = errors.Join(gatheredErrors, err)
			}
		}
		return gatheredErrors
	}

	if isInactive := catalog.HasInactiveDescriptorError(err); err == nil || isInactive {
		// If previous versions are released, then acquire a lease on the previous
		// version for the locked leasing mode.
		if acquirePreviousErr := acquireLeaseOnPrevious(); acquirePreviousErr != nil {
			log.Dev.Errorf(ctx, "unable to acquire lease on previous version of descriptor: %s", acquirePreviousErr)
		}
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

// descriptorTxnUpdate tracks updates to a set of descriptors in the
// system.descriptors by a transaction, that should be made available
// atomically.
type descriptorTxnUpdate struct {
	mu struct {
		syncutil.Mutex
		// DescriptorUpdates contains any descriptor versions that we are
		// presently waiting for.
		descpb.DescriptorUpdates
	}
	timestamp hlc.Timestamp
	key       roachpb.Key
}

// closeTimeStampHandle represents a close timestamp tracked by the lease
// manager.
type closeTimeStampHandle struct {
	timestamp hlc.Timestamp
	// refCount is the reference count for this handle.
	refCount atomic.Int64
	// freeOnRelease is set to true if the handle should be released when the
	// refCount hits zero.
	freeOnRelease atomic.Bool
	mu            struct {
		syncutil.Mutex
		// leasesToRelease tracks old leases that should be released when the
		// the reference count hits zero.
		leasesToRelease map[descpb.ID]LeasedDescriptor
	}
}

// newCloseTimeStampHandle creates a new close timestamp handle for the lease
// manager.
func newCloseTimeStampHandle(timestamp hlc.Timestamp) *closeTimeStampHandle {
	ch := &closeTimeStampHandle{
		timestamp: timestamp,
	}
	ch.mu.leasesToRelease = make(map[descpb.ID]LeasedDescriptor)
	return ch
}

// acquire acquires a reference on the close timestamp.
func (c *closeTimeStampHandle) acquire(_ context.Context) {
	c.refCount.Add(1)
}

// release releases a close timestamp handle.
func (c *closeTimeStampHandle) release(ctx context.Context) {
	// If freeOnRelease is set when the reference count hits zero, then
	// clean up instantly.
	newCount := c.refCount.Add(-1)
	if newCount == 0 && c.freeOnRelease.Load() {
		c.cleanupHandle(ctx)
	}
	if buildutil.CrdbTestBuild && newCount < 0 {
		panic(errors.AssertionFailedf("double free of a close timestamp handle"))
	}
}

// cleanupHandle releases all old leases held by this older timestamp.
func (c *closeTimeStampHandle) cleanupHandle(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, v := range c.mu.leasesToRelease {
		v.Release(ctx)
	}
	c.mu.leasesToRelease = nil
}

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

		// rangeFeedRestartInProgress tracks if a range feed restart is in progress.
		rangeFeedRestartInProgress bool

		// descriptorTxnUpdatesToProcess is a list of descriptor txn updates that
		// are waiting for new descriptor versions to be available.
		descriptorTxnUpdatesToProcess *btree.BTreeG[*descriptorTxnUpdate]

		// hasUpdatesToDelete tracks if there is data in the key space that
		// should be cleaned up.
		hasUpdatesToDelete bool

		// activeCloseTimestamps contains the most recent timestamp handles currently
		// in use by transactions.
		activeCloseTimestamps []*closeTimeStampHandle
	}

	// closeTimeStamp is the most recent closeTimeStamp handle, for
	// which the range feed will have all the updates for.
	closeTimestamp atomic.Value

	draining atomic.Bool

	// names is a cache for name -> id mappings. A mapping for the cache
	// should only be used if we currently have an active lease on the respective
	// id; otherwise, the mapping may well be stale.
	// Not protected by mu.
	names        nameCache
	testingKnobs ManagerTestingKnobs
	ambientCtx   log.AmbientContext
	stopper      *stop.Stopper
	sem          *quotapool.IntPool

	// descUpdateCh receives updated descriptors from the range feed.
	descUpdateCh chan catalog.Descriptor
	// descDelCh receives deleted descriptors from the range feed.
	descDelCh chan descpb.ID
	// descMetaDataUpdateCh receives updated transaction metadata from the
	// range feed.
	descMetaDataUpdateCh chan *descriptorTxnUpdate
	// rangefeedErrCh receives any terminal errors from the rangefeed.
	rangefeedErrCh chan error
	// leaseGeneration increments any time a new or existing descriptor is
	// detected by the lease manager. Once this count is incremented new data
	// is available.
	leaseGeneration atomic.Int64

	// waitForInit used when the lease manager is starting up prevent leases from
	// being acquired before the range feed.
	waitForInit chan struct{}

	// initComplete is a fast check to confirm that initialization is complete, since
	// performance testing showed select on the waitForInit channel can be expensive.
	initComplete atomic.Bool

	// bytesMonitor tracks the memory usage from leased descriptors.
	bytesMonitor *mon.BytesMonitor
	// boundAccount tracks the memory usage from leased descriptors.
	boundAccount *mon.ConcurrentBoundAccount

	// TestingDisableRangeFeedCheckpoint is used to disable rangefeed checkpoints.
	// Ideally, this would be inside StorageTestingKnobs, but those get copied into
	// the lease manager.
	TestingDisableRangeFeedCheckpoint atomic.Bool
}

const leaseConcurrencyLimit = 5

// NewLeaseManager creates a new Manager.
//
// internalExecutor can be nil to help bootstrapping, but then it needs to be set via
// SetInternalExecutor before the Manager is used.
//
// stopper is used to run async tasks. Can be nil in tests.
func NewLeaseManager(
	ctx context.Context,
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
	rootBytesMonitor *mon.BytesMonitor,
) *Manager {
	// See pkg/sql/mem_metrics.go
	// log10int64times1000 = log10(math.MaxInt64) * 1000, rounded up somewhat
	const log10int64times1000 = 19 * 1000
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
				leaseCurBytesCount: metric.NewGauge(metric.Metadata{
					Name:        "sql.leases.lease_cur_bytes_count",
					Help:        "The current number of bytes used by the lease manager.",
					Measurement: "Number of bytes used by the lease manager.",
					Unit:        metric.Unit_BYTES,
				}),
				leaseMaxBytesHist: metric.NewHistogram(metric.HistogramOptions{
					Metadata: metric.Metadata{
						Name:        "sql.leases.lease_max_bytes_hist",
						Help:        "Memory used by the lease manager.",
						Measurement: "Number of bytes used by the lease manager.",
						Unit:        metric.Unit_BYTES,
					},
					Duration:     base.DefaultHistogramWindowInterval(),
					MaxVal:       log10int64times1000,
					SigFigs:      3,
					BucketConfig: metric.MemoryUsage64MBBuckets,
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
	}
	lm.leaseGeneration.Swap(1) // Start off with 1 as the initial value.
	lm.storage.regionPrefix = &atomic.Value{}
	lm.storage.regionPrefix.Store(enum.One)
	lm.storage.writer = newKVWriter(codec, db.KV(), keys.LeaseTableID, settingsWatcher)
	lm.stopper.AddCloser(lm.sem.Closer("stopper"))
	lm.stopper.AddCloser(stop.CloserFn(lm.AssertAllLeasesAreReleasedAfterDrain))
	lm.mu.descriptors = make(map[descpb.ID]*descriptorState)
	lm.mu.descriptorTxnUpdatesToProcess = btree.NewG(2, func(a, b *descriptorTxnUpdate) bool {
		return a.timestamp.Less(b.timestamp)
	})
	lm.mu.hasUpdatesToDelete = true
	lm.waitForInit = make(chan struct{})
	// We are going to start the range feed later when StartRefreshLeasesTask
	// is invoked inside pre-start. So, that guarantees all range feed events
	// that will be generated will be after the current time. So, historical
	// queries with in this tenant (i.e. PCR catalog reader) before this point are
	// guaranteed to be up to date.
	lm.closeTimestamp.Store(newCloseTimeStampHandle(clock.Now()))
	lm.draining.Store(false)
	lm.descUpdateCh = make(chan catalog.Descriptor)
	lm.descDelCh = make(chan descpb.ID)
	lm.descMetaDataUpdateCh = make(chan *descriptorTxnUpdate)
	lm.rangefeedErrCh = make(chan error)
	lm.bytesMonitor = mon.NewMonitor(mon.Options{
		Name:       mon.MakeName("leased-descriptors"),
		CurCount:   lm.storage.leasingMetrics.leaseCurBytesCount,
		MaxHist:    lm.storage.leasingMetrics.leaseMaxBytesHist,
		Res:        mon.MemoryResource,
		Settings:   settings,
		LongLiving: true,
	})
	lm.bytesMonitor.StartNoReserved(context.Background(), rootBytesMonitor)
	lm.boundAccount = lm.bytesMonitor.MakeConcurrentBoundAccount()
	// Add a stopper for the bound account that we are using to
	// track memory usage.
	lm.stopper.AddCloser(stop.CloserFn(func() {
		lm.boundAccount.Close(ctx)
		lm.bytesMonitor.Stop(ctx)
	}))
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
	timestamp ReadTimestamp,
	parentID descpb.ID,
	parentSchemaID descpb.ID,
	name string,
) (LeasedDescriptor, error) {
	if m.IsDraining() {
		return nil, errLeaseManagerIsDraining
	}
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
	descVersion, _ := m.names.get(ctx, parentID, parentSchemaID, name, timestamp.GetTimestamp())
	if descVersion != nil {
		if descVersion.GetModificationTime().LessEq(timestamp.GetTimestamp()) {
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
	id, err := m.resolveName(ctx, timestamp.GetTimestamp(), parentID, parentSchemaID, name)
	if err != nil &&
		(timestamp.GetTimestamp() == timestamp.GetBaseTimestamp() ||
			!errors.Is(err, catalog.ErrDescriptorNotFound)) {
		return nil, err
	} else if errors.Is(err, catalog.ErrDescriptorNotFound) {
		// The descriptor was not found at the lease timestamp, so attempt the
		// real timestamp in use by this txn. This implies the object may have
		// just been created.
		id, err = m.resolveName(ctx, timestamp.GetBaseTimestamp(), parentID, parentSchemaID, name)
		if err != nil {
			return nil, err
		}
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
	log.VEventf(ctx, 2, "resolving name %q with parentID=%d parentSchemaID=%d",
		name, parentID, parentSchemaID)
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
		log.VEventf(ctx, 2, "name %q not found with parentID=%d parentSchemaID=%d",
			name, parentID, parentSchemaID)
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
	ctx context.Context, timestamp ReadTimestamp, id descpb.ID,
) (LeasedDescriptor, error) {
	for {
		if m.IsDraining() {
			return nil, errLeaseManagerIsDraining
		}
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
				_, errLease := m.acquireNodeLease(ctx, id, AcquireBlock)
				return errLease
			}(); err != nil {
				return nil, err
			}

		case errors.Is(err, errReadOlderVersion):
			// Read old versions from the store. This can block while reading.
			versions, errRead := m.readOlderVersionForTimestamp(ctx, id, timestamp.GetTimestamp())
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
	return m.draining.Load()
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
			leasesToRelease := t.removeInactiveVersions(ctx)
			return leasesToRelease
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

// AssertAllLeasesAreReleasedAfterDrain asserts that all leases are released after
// draining.
func (m *Manager) AssertAllLeasesAreReleasedAfterDrain() {
	if !buildutil.CrdbTestBuild || !m.draining.Load() {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range m.mu.descriptors {
		func() {
			descriptorStr := strings.Builder{}
			panicWithErr := false
			t.mu.Lock()
			defer t.mu.Unlock()
			// Ensure that all leases are released at this time.
			if len(t.mu.active.data) > 0 {
				// Check if any of these have a non-zero ref count indicating some type of
				// leak. It may be possible for the entry to exist if we were interrupted
				// mid-acquisition by the draining process. But the reference count should
				// *never* be non-zero.
				for _, l := range t.mu.active.data {
					if l.refcount.Load() == 0 {
						continue
					}
					panicWithErr = true
					if descriptorStr.Len() > 0 {
						descriptorStr.WriteString(",")
					}
					descriptorStr.WriteString(fmt.Sprintf("{%s}", l.String()))
				}
				if panicWithErr {
					panic(errors.AssertionFailedf("descriptor leak was detected for ID: %d, "+
						"with versions [%s]", t.id, descriptorStr.String()))
				}
			}
		}()
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
	if m.initComplete.Load() {
		return
	}
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

// hasDescUpdateBeenApplied checks if all versions of a descriptor update are
// present. If not, it returns a new descpb.DescriptorUpdates containing the
// versions that are still missing.
func (m *Manager) hasDescUpdateBeenApplied(
	updates descpb.DescriptorUpdates,
) (allApplied bool, remaining descpb.DescriptorUpdates) {
	allApplied = true
	for idx, descID := range updates.DescriptorIDs {
		descVersionState := m.findNewest(descID)
		requiredVersion := updates.DescriptorVersions[idx]
		// If a descriptor is missing, we still consider it as applied, since
		// we will lease a newer version.
		if descVersionState != nil && descVersionState.GetVersion() < requiredVersion {
			allApplied = allApplied && descVersionState == nil
			remaining.DescriptorIDs = append(remaining.DescriptorIDs, descID)
			remaining.DescriptorVersions = append(remaining.DescriptorVersions, requiredVersion)
		}
	}
	return allApplied, remaining
}

// getDescriptorTxnUpdateEntry returns the descriptorTxnUpdate entry for the given timestamp,
// if there are any pending updates.
func (m *Manager) getDescriptorTxnUpdateEntry(timestamp hlc.Timestamp) *descriptorTxnUpdate {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := &descriptorTxnUpdate{timestamp: timestamp}
	entry, hasTS := m.mu.descriptorTxnUpdatesToProcess.Get(key)
	if !hasTS {
		return nil
	}
	return entry
}

// processDescriptorUpdate will process this descriptor update for txn
// consistency.
func (m *Manager) processDescriptorUpdate(
	ctx context.Context, id descpb.ID, version descpb.DescriptorVersion, timestamp hlc.Timestamp,
) {
	entry := m.getDescriptorTxnUpdateEntry(timestamp)
	// This timestamp has no pending update.
	if entry == nil {
		return
	}
	noEntriesLeft := func() bool {
		// Lock this specific entry
		entry.mu.Lock()
		defer entry.mu.Unlock()
		// Process this descriptor version inside the entry.
		for idx, descID := range entry.mu.DescriptorIDs {
			if descID != id {
				continue
			}
			// The target version has not arrived yet for this update.
			if entry.mu.DescriptorVersions[idx] > version {
				return false
			}
			// Otherwise, let's move this version to the end first.
			endIdx := len(entry.mu.DescriptorIDs) - 1
			if idx != endIdx {
				entry.mu.DescriptorIDs[idx], entry.mu.DescriptorIDs[endIdx] =
					entry.mu.DescriptorIDs[endIdx], entry.mu.DescriptorIDs[idx]
				entry.mu.DescriptorVersions[idx], entry.mu.DescriptorVersions[endIdx] =
					entry.mu.DescriptorVersions[endIdx], entry.mu.DescriptorVersions[idx]
			}
			// Remove one element from the end after.
			entry.mu.DescriptorIDs = entry.mu.DescriptorIDs[:len(entry.mu.DescriptorIDs)-1]
			entry.mu.DescriptorVersions = entry.mu.DescriptorVersions[:len(entry.mu.DescriptorVersions)-1]
			break
		}
		return len(entry.mu.DescriptorIDs) == 0
	}()
	// Everything is processed, so we can move the timestamp forward.
	if noEntriesLeft {
		// If the length is zero by this point, then we can move the timestamp forward
		// and remove this entry.
		targetTimestamp := m.markDescriptorUpdatesAsComplete()
		if !targetTimestamp.IsEmpty() {
			m.advanceCloseTimestamp(ctx, targetTimestamp)
		}
	}
}

// markDescriptorUpdatesAsComplete will move the close timestamp forward if
// all descriptor updates have been applied.
func (m *Manager) markDescriptorUpdatesAsComplete() hlc.Timestamp {
	m.mu.Lock()
	defer m.mu.Unlock()
	timestamp := hlc.Timestamp{}
	for {
		minUpdate, hasMin := m.mu.descriptorTxnUpdatesToProcess.Min()
		if !hasMin {
			break
		}
		shouldRemoveEntry := func() bool {
			minUpdate.mu.Lock()
			defer minUpdate.mu.Unlock()
			return len(minUpdate.mu.DescriptorIDs) == 0
		}()
		if !shouldRemoveEntry {
			break
		}
		timestamp = minUpdate.timestamp
		m.mu.descriptorTxnUpdatesToProcess.Delete(minUpdate)
	}
	return timestamp
}

// advanceCloseTimestamp advances the close timestamp atomically, if the current
// close timestamp is smaller.
func (m *Manager) advanceCloseTimestamp(ctx context.Context, timestamp hlc.Timestamp) {
	timestampHandle := newCloseTimeStampHandle(timestamp)
	for {
		oldTimestamp := m.closeTimestamp.Load().(*closeTimeStampHandle)
		// Timestamp has already been advanced.
		if !oldTimestamp.timestamp.Less(timestamp) {
			return
		}
		// Otherwise, attempt to swap the timestamp, if successful, we
		// will return.
		if m.closeTimestamp.CompareAndSwap(oldTimestamp, timestampHandle) {
			// Insert the new timestamp handle.
			m.insertNewTimestampHandle(timestampHandle)
			// Now that the new timestamp is set, we can mark the old one as ready
			// for removal.
			oldTimestamp.freeOnRelease.Swap(true)
			// Purge any old timestamp handles.
			m.maybeFreeOldTimestampHandles(ctx)
		}
	}
}

// insertNewTimestampHandle adds a new close timestamp handle.
func (m *Manager) insertNewTimestampHandle(handle *closeTimeStampHandle) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Insert at a lower position
	idx := sort.Search(len(m.mu.activeCloseTimestamps), func(i int) bool {
		return !m.mu.activeCloseTimestamps[i].timestamp.Less(handle.timestamp)
	})
	if idx == len(m.mu.activeCloseTimestamps) {
		m.mu.activeCloseTimestamps = append(m.mu.activeCloseTimestamps, handle)
		return
	}
	// Otherwise insert in the middle.
	m.mu.activeCloseTimestamps = append(m.mu.activeCloseTimestamps, nil)
	copy(m.mu.activeCloseTimestamps[idx+1:], m.mu.activeCloseTimestamps[idx:])
	m.mu.activeCloseTimestamps[idx] = handle
}

// shouldRetainPriorVersions acquires the close timestamp handles that can make
// use of leases at the modification timestamp.
func (m *Manager) shouldRetainPriorVersions(
	ctx context.Context, modificationTime hlc.Timestamp,
) []*closeTimeStampHandle {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*closeTimeStampHandle
	// We will retain this lease with all active prior close timestamp handles.
	for i := 0; i < len(m.mu.activeCloseTimestamps); i++ {
		t := m.mu.activeCloseTimestamps[i]
		if modificationTime.LessEq(t.timestamp) {
			t.acquire(ctx)
			result = append(result, t)
		}
	}
	return result
}

// maybeFreeOldTimestampHandles cleans up any old close timestamps that
// have hit a refcount of zero.
func (m *Manager) maybeFreeOldTimestampHandles(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	numTimestamps := len(m.mu.activeCloseTimestamps)
	for i := 0; i < numTimestamps; i++ {
		t := m.mu.activeCloseTimestamps[i]
		if !t.freeOnRelease.Load() || t.refCount.Load() != 0 {
			continue
		}
		t.cleanupHandle(ctx)
		// Move complete elements to the end.
		if numTimestamps > 1 {
			m.mu.activeCloseTimestamps[numTimestamps-1], m.mu.activeCloseTimestamps[i] =
				m.mu.activeCloseTimestamps[i], m.mu.activeCloseTimestamps[numTimestamps-1]
		}
		numTimestamps--
		i--
	}
	m.mu.activeCloseTimestamps = m.mu.activeCloseTimestamps[:numTimestamps]
}

// StartRefreshLeasesTask starts a goroutine that refreshes the lease manager
// leases for descriptors received in the latest system configuration via gossip or
// rangefeeds. This function must be passed a non-nil gossip if
// RangefeedLeases is not active.
func (m *Manager) StartRefreshLeasesTask(ctx context.Context, s *stop.Stopper, db *kv.DB) {
	ctx = multitenant.WithTenantCostControlExemption(ctx)
	m.mu.Lock()
	defer m.mu.Unlock()
	defer close(m.waitForInit)
	defer m.initComplete.Swap(true)
	m.watchForUpdates(ctx)
	_ = s.RunAsyncTask(ctx, "refresh-leases", func(ctx context.Context) {
		defer func() {
			if err := recover(); err != nil {
				log.Dev.Warningf(ctx, "panic in refresh-leases: %v", err)
				panic(err)
			}
		}()

		for {
			select {
			case id := <-m.descDelCh:
				// Descriptor is marked as deleted, so mark it for deletion or
				// remove it if it's no longer in use.
				_ = s.RunAsyncTask(ctx, "purgeOldVersionsOrAcquireInitialVersion deleted descriptor", func(ctx context.Context) {
					defer func() {
						if err := recover(); err != nil {
							log.Dev.Warningf(ctx, "panic in purgeOldVersionsOrAcquireInitialVersion deleted descriptor: %v", err)
							panic(err)
						}
					}()
					// Once the descriptor is purged notify that some change has occurred.
					defer m.leaseGeneration.Add(1)
					state := m.findNewest(id)
					if state != nil {
						if err := m.purgeOldVersions(ctx, db, id, true /* dropped */, state.GetVersion()); err != nil {
							log.Dev.Warningf(ctx, "error purging leases for deleted descriptor %d",
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
						log.Dev.Infof(ctx, "skipping update of %v due to knob: %v",
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
							log.Dev.Warningf(ctx, "error fetching lease for descriptor %s", err)
						}
					}
					// Even if an initial acquisition happens above, we need to purge old
					// descriptor versions, which could have been acquired concurrently.
					// For example the range feed sees version 2 and a query concurrently
					// acquires version 1.
					if err := m.purgeOldVersions(ctx, db, desc.GetID(), dropped, desc.GetVersion()); err != nil {
						log.Dev.Warningf(ctx, "error purging leases for descriptor %d(%s): %s",
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
						defer func() {
							if err := recover(); err != nil {
								log.Dev.Warningf(ctx, "panic in wait to purgeOldVersionsOrAcquireInitialVersion: %v", err)
								panic(err)
							}
						}()
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
			case descUpdate := <-m.descMetaDataUpdateCh:
				// Check if the update has been applied.
				allApplied, remaining := m.hasDescUpdateBeenApplied(descUpdate.mu.DescriptorUpdates)
				descUpdate.mu.DescriptorUpdates = remaining
				advanceTimestamp := func() bool {
					m.mu.Lock()
					defer m.mu.Unlock()
					m.mu.hasUpdatesToDelete = true
					// If there are no other earlier pending updates, advance the timestamp.
					minUpdate, hasMin := m.mu.descriptorTxnUpdatesToProcess.Min()
					if allApplied && (!hasMin ||
						!minUpdate.timestamp.Less(descUpdate.timestamp)) {
						return true
					}
					// Otherwise, insert this into the queue of pending updates
					m.mu.descriptorTxnUpdatesToProcess.ReplaceOrInsert(descUpdate)
					return false
				}()
				if advanceTimestamp {
					m.advanceCloseTimestamp(ctx, descUpdate.timestamp)
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
	return m.closeTimestamp.Load().(*closeTimeStampHandle).timestamp
}

// getSafeReplicationTSHandle returns a handle for the current close time
// stamp and acquires a ref count. The caller is responsible for releasing
// this ref count.
func (m *Manager) getSafeReplicationTSHandle(ctx context.Context) *closeTimeStampHandle {
	for {
		handle := m.closeTimestamp.Load().(*closeTimeStampHandle)
		handle.acquire(ctx)
		// Ensure that the timestamp is not marked as released. Otherwise, backing
		// descriptors may get freed from under us.
		if !handle.freeOnRelease.Load() {
			return handle
		}
		// Handle is being freed, acquire a new one.
		handle.release(ctx)
	}
}

// closeRangeFeed closes the currently open range feed, which will involve
// temporarily releasing the lease manager mutex.
func (m *Manager) closeRangeFeedLocked() {
	// We cannot terminate the range feed while holding the lease manager
	// lock, since there may be event handlers that need the lock that need to
	// drain.
	oldRangeFeed := m.mu.rangeFeed
	m.mu.rangeFeed = nil
	m.mu.Unlock() // nolint:deferunlockcheck
	oldRangeFeed.Close()
	m.mu.Lock() // nolint:deferunlockcheck
}

// watchForUpdates will watch a rangefeed on the system.descriptor table for
// updates.
func (m *Manager) watchForUpdates(ctx context.Context) {
	if log.V(1) {
		log.Dev.Infof(ctx, "using rangefeeds for lease manager updates")
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
		if m.TestingDisableRangeFeedCheckpoint.Load() {
			return
		}
		// Check first if it is the special descriptor metadata key used to track
		// descriptors modified by transactions.
		if isUpdateKey, err := m.Codec().DecodeDescUpdateKey(ev.Key); err != nil || isUpdateKey {
			if err != nil {
				log.Dev.Warningf(ctx, "unable to decode update metadata key %v", ev.Key)
				return
			}
			// Ignore deletes on this key space.
			if len(ev.Value.RawBytes) == 0 {
				return
			}
			// Otherwise, this is a descriptor update key.
			var descUpdates descpb.DescriptorUpdates
			err := ev.Value.GetProto(&descUpdates)
			if err != nil {
				log.Dev.Warningf(ctx, "unable to decode descriptor update value %v", err)
				return
			}
			update := &descriptorTxnUpdate{
				key:       ev.Key,
				timestamp: ev.Timestamp(),
			}
			update.mu.DescriptorUpdates = descUpdates
			select {
			case <-m.stopper.ShouldQuiesce():
			case <-ctx.Done():
			case m.descMetaDataUpdateCh <- update:
			}
			return
		} else if len(ev.Value.RawBytes) == 0 {
			id, err := m.Codec().DecodeDescMetadataID(ev.Key)
			if err != nil {
				log.Dev.Infof(ctx, "unable to decode metadata key %v", ev.Key)
				return
			}
			select {
			case <-m.stopper.ShouldQuiesce():
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
			log.Dev.Infof(ctx, "%s: refreshing lease on descriptor: %d (%s), version: %d",
				ev.Key, mut.GetID(), mut.GetName(), mut.GetVersion())
		}
		select {
		case <-m.stopper.ShouldQuiesce():
		case <-ctx.Done():
		case m.descUpdateCh <- mut:
		}
	}

	handleCheckpoint := func(ctx context.Context, checkpoint *kvpb.RangeFeedCheckpoint) {
		if m.TestingDisableRangeFeedCheckpoint.Load() {
			return
		}
		// Track checkpoints that occur from the rangefeed to make sure progress
		// is always made.
		m.advanceCloseTimestamp(ctx, checkpoint.ResolvedTS)
		m.mu.Lock()
		defer m.mu.Unlock()
		m.mu.rangeFeedCheckpoints += 1
		// Clean up all entries before the resolve timestamp.
		for {
			minTS, hasMin := m.mu.descriptorTxnUpdatesToProcess.Min()
			if !hasMin || !minTS.timestamp.Less(checkpoint.ResolvedTS) {
				break
			}
			m.mu.descriptorTxnUpdatesToProcess.Delete(minTS)
		}
	}

	// Assert that the range feed is already terminated.
	if m.mu.rangeFeed != nil {
		if buildutil.CrdbTestBuild {
			panic(errors.AssertionFailedf("range feed was not closed before a restart attempt"))
		}
		log.Dev.Warningf(ctx, "range feed was not closed before a restart attempt")
	}
	// Always populate a timestamp on the range feed, otherwise we run the risk
	// of a race condition between when the range feed selects a timestamp versus
	// when we start handing out leases (i.e. close waitForInit) The startup below
	// is asynchronous in nature, so the timestamp could be after close the waitForInit
	// channel.
	now := m.storage.db.KV().Clock().Now()
	log.Dev.Infof(ctx, "starting lease manager rangefeed at timestamp %s", now)
	// Ignore errors here because they indicate that the server is shutting down.
	// Also note that the range feed automatically shuts down when the server
	// shuts down, so we don't need to call Close() ourselves.
	m.mu.rangeFeed, _ = m.rangeFeedFactory.RangeFeed(
		ctx, "lease", []roachpb.Span{descriptorTableSpan}, now, handleEvent,
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
		log.Dev.Warningf(ctx, "lease manager range feed has stopped making progress.")
	} else if !m.mu.rangeFeedIsUnavailableAt.IsZero() &&
		lastCheckpoints > 0 {
		m.mu.rangeFeedIsUnavailableAt = time.Time{}
		log.Dev.Warningf(ctx, "lease manager range feed has recovered.")
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
	ctx = multitenant.WithTenantCostControlExemption(ctx)
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
		// Descriptor update clean-up timer
		var descriptorUpdateCleanupTimer timeutil.Timer
		descriptorUpdateCleanupTimerDuration := 30 * time.Minute
		descriptorUpdateCleanupTimer.Reset(descriptorUpdateCleanupTimerDuration)
		for {
			select {
			case <-m.stopper.ShouldQuiesce():
				return

			case <-rangeFeedProgressWatchDog.C:
				// Detect if the range feed has stopped making
				// progress.
				if rangeFeedProgressWatchDogEnabled {
					refreshAllDescriptors := m.checkRangeFeedStatus(ctx)
					// If the range feed recovers after a failure, re-read all
					// descriptors.
					if refreshAllDescriptors {
						m.refreshSomeLeases(ctx, true /*refreshAndPurgeAllDescriptors*/)
					}
				}
				rangeFeedProgressWatchDogTimeout,
					rangeFeedProgressWatchDogEnabled = m.getRangeFeedMonitorSettings()
				rangeFeedProgressWatchDog.Reset(rangeFeedProgressWatchDogTimeout)
			case err := <-m.rangefeedErrCh:
				log.Dev.Warningf(ctx, "lease rangefeed failed with error: %s", err.Error())
				m.handleRangeFeedError(ctx)
				m.refreshSomeLeases(ctx, true /*refreshAndPurgeAllDescriptors*/)
			case <-refreshTimer.C:
				refreshTimer.Reset(getRefreshTimerDuration() / 2)

				// Check for any react to any range feed availability problems, and
				// if needed refresh the full set of descriptors.
				m.handleRangeFeedAvailability(ctx)

				// Clean up session based leases that have expired.
				m.cleanupExpiredSessionLeases(ctx)
			case <-descriptorUpdateCleanupTimer.C:
				// Clean up the update key space.
				m.cleanupUpdateKeys(ctx)
				descriptorUpdateCleanupTimer.Reset(descriptorUpdateCleanupTimerDuration)
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
	// If someone else is already starting a range feed then exit early.
	if m.mu.rangeFeedRestartInProgress {
		return
	}
	log.Dev.Warning(ctx, "attempting restart of leasing range feed")
	// We will temporarily release the lock closing the range feed,
	// in case things need to drain before termination. It is possible for
	// another restart to enter once we release the lock.
	m.mu.rangeFeedRestartInProgress = true
	if m.mu.rangeFeed != nil {
		m.closeRangeFeedLocked()
		if m.testingKnobs.RangeFeedResetChannel != nil {
			close(m.testingKnobs.RangeFeedResetChannel)
			m.testingKnobs.RangeFeedResetChannel = nil
		}
	}
	// Attempt a range feed restart if it has been down too long.
	m.watchForUpdates(ctx)
	// Track when the last restart occurred.
	m.mu.rangeFeedIsUnavailableAt = timeutil.Now()
	m.mu.rangeFeedCheckpoints = 0
	m.mu.rangeFeedRestartInProgress = false
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
			log.Dev.Infof(ctx, "unable to delete leases from storage %s", err)
		}
	}
}

// cleanupUpdateKeys updates special keys that exist for tracking descriptor
// updates within transactions.
func (m *Manager) cleanupUpdateKeys(ctx context.Context) {
	// Only clean update keys if we received any range feed updates.
	m.mu.Lock()
	hasUpdates := m.mu.hasUpdatesToDelete
	m.mu.Unlock()
	if !hasUpdates {
		return
	}
	// Issue a DelRange on this key space.
	err := m.storage.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		prefix := m.storage.codec.DescUpdatePrefix()
		_, err := txn.KV().DelRange(ctx, prefix, prefix.PrefixEnd(), false)
		return err
	})
	if err != nil {
		log.Dev.Infof(ctx, "unable to delete descriptor update keys from storage: %v", err)
	}
}

// Refresh some of the current leases. If refreshAndPurgeAllDescriptors is set,
// then all descriptors are refreshed, and old versions are purged.
func (m *Manager) refreshSomeLeases(ctx context.Context, refreshAndPurgeAllDescriptors bool) {
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
			if i++; i > limit && !refreshAndPurgeAllDescriptors {
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
						log.Dev.Infof(ctx, "knob failed for desc (%v): %v", id, err)
						return
					}
				}
				// Mark that an acquisition is in progress.
				state := m.findDescriptorState(id, false)
				state.markAcquisitionStart(ctx)
				defer state.markAcquisitionDone(ctx)
				if _, err := m.acquireNodeLease(ctx, id, AcquireBackground); err != nil {
					log.Dev.Errorf(ctx, "refreshing descriptor: %d lease failed: %s", id, err)

					if errors.Is(err, catalog.ErrDescriptorNotFound) || errors.Is(err, catalog.ErrDescriptorDropped) {
						// Lease renewal failed due to removed descriptor; Remove this descriptor from cache.
						if err := m.purgeOldVersions(
							ctx, m.storage.db.KV(), id, true /* dropped */, 0, /* minVersion */
						); err != nil {
							log.Dev.Warningf(ctx, "error purging leases for descriptor %d: %v",
								id, err)
						}
						func() {
							m.mu.Lock()
							defer m.mu.Unlock()
							delete(m.mu.descriptors, id)
						}()
					}
				}
				if refreshAndPurgeAllDescriptors {
					// If we are refreshing all descriptors, then we want to purge older versions as
					// we are doing this operation.
					err := m.purgeOldVersions(ctx, m.storage.db.KV(), id, false /* dropped */, 0 /* minVersion */)
					if err != nil {
						log.Dev.Warningf(ctx, "error purging leases for descriptor %d: %v",
							id, err)
					}
				}
			}); err != nil {
			log.Dev.Infof(ctx, "didnt refresh descriptor: %d lease: %s", id, err)
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
func (m *Manager) DeleteOrphanedLeases(
	ctx context.Context, timeThreshold int64, locality roachpb.Locality,
) {
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
	newCtx = multitenant.WithTenantCostControlExemption(newCtx)

	// AddTags and not WithTags, so that we combine the tags with those
	// filled by AnnotateCtx.
	newCtx = logtags.AddTags(newCtx, logtags.FromContext(ctx))
	_ = m.stopper.RunAsyncTask(newCtx, "del-orphaned-leases", func(ctx context.Context) {
		retryOpts := base.DefaultRetryOptions()
		retryOpts.MaxRetries = 10
		m.deleteOrphanedLeasesFromStaleSession(ctx, retryOpts, timeThreshold, locality)
		m.deleteOrphanedLeasesWithSameInstanceID(ctx, retryOpts, timeThreshold, instanceID)
		m.cleanupUpdateKeys(ctx)
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
					return state.mu.lease, int(state.refcount.Load())
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
	m.TestingDisableRangeFeedCheckpoint.Store(disable)
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
	m.initComplete.Swap(true)
}

// deleteOrphanedLeasesFromStaleSession deletes leases from sessions that are
// no longer alive.
func (m *Manager) deleteOrphanedLeasesFromStaleSession(
	ctx context.Context, retryOpts retry.Options, initialTimestamp int64, locality roachpb.Locality,
) {
	ex := m.storage.db.Executor()
	row, err := ex.QueryRowEx(ctx, "check-system-db-multi-region", nil,
		sessiondata.NodeUserSessionDataOverride,
		"SELECT EXISTS (SELECT * FROM [SHOW REGIONS FROM DATABASE system])")
	if err != nil {
		log.Dev.Warningf(ctx, "unable to query if system database is multi-region: %v", err)
		return
	}
	// For multi-region system databases, only focus on our own region; there is
	// no need to incur cross-region hops.
	region := string(enum.One)
	multiRegionSystemDb := tree.MustBeDBool(row[0])
	if !locality.Empty() && bool(multiRegionSystemDb) {
		region = locality.Tiers[0].Value
	}

	log.Dev.Infof(ctx, "starting orphaned lease cleanup from stale sessions in region %s", region)

	var distinctSessions []tree.Datums
	aostTime := hlc.Timestamp{WallTime: initialTimestamp}
	const limit = 50

	// Build the query based on whether the system database is multi-region.
	// For multi-region, we need to cast the region parameter to the enum type.
	// For single-region, we use the bytes representation.
	var distinctSessionQuery string
	var regionParam interface{}
	isMultiRegion := bool(multiRegionSystemDb)
	if isMultiRegion {
		distinctSessionQuery = `SELECT DISTINCT(session_id) FROM system.lease AS OF SYSTEM TIME %s WHERE crdb_region=$1::system.crdb_internal_region AND NOT crdb_internal.sql_liveness_is_alive(session_id, true) LIMIT $2`
		regionParam = region
	} else {
		distinctSessionQuery = `SELECT DISTINCT(session_id) FROM system.lease AS OF SYSTEM TIME %s WHERE crdb_region=$1 AND NOT crdb_internal.sql_liveness_is_alive(session_id, true) LIMIT $2`
		regionParam = enum.One
	}

	totalSessionsProcessed := 0
	totalLeasesDeleted := 0

	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		// Get a list of distinct, dead session IDs that exist in the system.lease
		// table.
		distinctSessions, err = ex.QueryBufferedEx(ctx,
			"query-lease-table-dead-sessions",
			nil,
			sessiondata.NodeUserSessionDataOverride,
			fmt.Sprintf(distinctSessionQuery, aostTime.AsOfSystemTime()),
			regionParam,
			limit,
		)
		if err != nil {
			if !startup.IsRetryableReplicaError(err) {
				log.Dev.Warningf(ctx, "unable to read session IDs for orphaned leases: %v", err)
				return
			}
		}

		if len(distinctSessions) > 0 {
			log.Dev.Infof(ctx, "found %d dead sessions from which to clean up orphaned leases", len(distinctSessions))
		}

		// Delete rows in our lease table with orphaned sessions.
		for _, sessionRow := range distinctSessions {
			sessionID := sqlliveness.SessionID(tree.MustBeDBytes(sessionRow[0]))
			sessionLeasesDeleted, err := deleteLeaseWithSessionIDWithBatch(ctx, ex, retryOpts, isMultiRegion, sessionID, regionParam, limit)
			if err != nil {
				log.Dev.Warningf(ctx, "unable to delete orphaned leases for session %s: %v", sessionID, err)
				break
			}
			totalLeasesDeleted += sessionLeasesDeleted
			log.Dev.Infof(ctx, "deleted %d orphaned leases for dead session %s", sessionLeasesDeleted, sessionID)
		}

		totalSessionsProcessed += len(distinctSessions)

		// No more dead sessions to clean up.
		if len(distinctSessions) < limit {
			log.Dev.Infof(ctx, "completed orphaned lease cleanup for region %s: %d sessions processed, %d leases deleted",
				region, totalSessionsProcessed, totalLeasesDeleted)
			return
		}

		// Log progress for large cleanup operations.
		log.Dev.Infof(ctx, "orphaned lease cleanup progress for region %s: %d sessions processed, %d leases deleted so far",
			region, totalSessionsProcessed, totalLeasesDeleted)

		// Advance our aostTime timstamp so that our query to detect leases with
		// dead sessions is aware of new deletes and does not keep selecting the
		// same leases.
		aostTime = hlc.Timestamp{WallTime: m.storage.clock.Now().WallTime}
	}
}

// deleteLeaseWithSessionIDWithBatch uses batchSize to batch deletes for leases
// with the given sessionID in system.lease. Returns the total number of leases deleted.
func deleteLeaseWithSessionIDWithBatch(
	ctx context.Context,
	ex isql.Executor,
	retryOpts retry.Options,
	multiRegionSystemDb bool,
	sessionID sqlliveness.SessionID,
	regionParam interface{},
	batchSize int,
) (int, error) {
	// Build the query based on whether the system database is multi-region.
	// For multi-region, we need to cast the region parameter to the enum type.
	// For single-region, we use the bytes representation.
	var deleteOrphanedQuery string
	if multiRegionSystemDb {
		deleteOrphanedQuery = `DELETE FROM system.lease WHERE session_id=$1 AND crdb_region=$2::system.crdb_internal_region LIMIT $3`
	} else {
		deleteOrphanedQuery = `DELETE FROM system.lease WHERE session_id=$1 AND crdb_region=$2 LIMIT $3`
	}

	totalDeleted := 0
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		var rowsDeleted int
		var err error
		rowsDeleted, err = ex.ExecEx(ctx, "delete-orphaned-leases-by-session", nil,
			sessiondata.NodeUserSessionDataOverride,
			deleteOrphanedQuery,
			sessionID.UnsafeBytes(),
			regionParam,
			batchSize)
		if err != nil {
			if !startup.IsRetryableReplicaError(err) {
				return totalDeleted, err
			}
		}
		totalDeleted += rowsDeleted

		// No more rows to clean up.
		if rowsDeleted < batchSize {
			break
		}
	}
	return totalDeleted, nil
}

func (m *Manager) deleteOrphanedLeasesWithSameInstanceID(
	ctx context.Context,
	retryOptions retry.Options,
	timeThreshold int64,
	instanceID base.SQLInstanceID,
) {
	// This could have been implemented using DELETE WHERE, but DELETE WHERE
	// doesn't implement AS OF SYSTEM TIME.

	// Read orphaned leases from the system.lease table.
	query := `SELECT s."desc_id",  s.version, s."session_id", s.crdb_region FROM system.lease as s 
		WHERE s."sql_instance_id"=%d
`
	sqlQuery := fmt.Sprintf(query, instanceID)

	var rows []tree.Datums
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
		log.Dev.Warningf(ctx, "unable to read orphaned leases: %v", err)
		return
	}

	totalLeases := len(rows)
	log.Dev.Infof(ctx, "found %d orphaned leases to clean up for instance ID %d", totalLeases, instanceID)
	if totalLeases == 0 {
		return
	}

	var wg sync.WaitGroup
	var releasedCount atomic.Int64
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
				defer wg.Done()
				m.storage.release(ctx, m.stopper, lease)
				released := releasedCount.Add(1)
				log.Dev.Infof(ctx, "released orphaned lease: %+v", lease)

				// Log progress every 100 leases for large cleanup operations.
				if released%100 == 0 || released == int64(totalLeases) {
					log.Dev.Infof(ctx, "orphaned lease cleanup progress for instance ID %d: %d/%d leases released",
						instanceID, released, totalLeases)
				}
			}); err != nil {
			log.Dev.Warningf(ctx, "could not start async task for releasing orphaned lease %+v: %v", lease, err)
			wg.Done()
		}
	}

	wg.Wait()
	log.Dev.Infof(ctx, "completed orphaned lease cleanup for instance ID %d: %d/%d leases released",
		instanceID, releasedCount.Load(), totalLeases)
}

// GetReadTimestamp returns a locked timestamp to use for lease management.
func (m *Manager) GetReadTimestamp(ctx context.Context, timestamp hlc.Timestamp) ReadTimestamp {
	if GetLockedLeaseTimestampEnabled(ctx, m.settings) {
		replicationTS := m.getSafeReplicationTSHandle(ctx)
		if replicationTS.timestamp.Less(timestamp) {
			return LeaseTimestamp{
				ReadTimestamp:  timestamp,
				LeaseTimestamp: replicationTS.timestamp,
				handle:         replicationTS,
			}
		}
		replicationTS.release(ctx)
	}
	// Fallback to existing behavior with timestamps.
	return LeaseTimestamp{
		ReadTimestamp: timestamp,
	}
}

// TestingGetBoundAccount returns the bound account used by the lease manager.
func (m *Manager) TestingGetBoundAccount() *mon.ConcurrentBoundAccount {
	return m.boundAccount
}
