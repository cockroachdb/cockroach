// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gcjob

import (
	"context"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var maxDeadline = timeutil.Unix(0, math.MaxInt64)

// refreshTables updates the status of tables/indexes that are waiting to be
// GC'd.
// It returns whether or not any index/table has expired and the duration until
// the next index/table expires.
func refreshTables(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	tableIDs []descpb.ID,
	tableDropTimes map[descpb.ID]int64,
	indexDropTimes map[descpb.IndexID]int64,
	job *jobs.Job,
	progress *jobspb.SchemaChangeGCProgress,
) (expired bool, earliestDeadline time.Time) {
	earliestDeadline = maxDeadline
	var haveAnyMissing bool
	for _, tableID := range tableIDs {
		tableHasExpiredElem, tableIsMissing, deadline := updateStatusForGCElements(
			ctx,
			execCfg,
			job.ID(),
			tableID,
			tableDropTimes, indexDropTimes,
			progress,
		)
		expired = expired || tableHasExpiredElem
		haveAnyMissing = haveAnyMissing || tableIsMissing
		if deadline.Before(earliestDeadline) {
			earliestDeadline = deadline
		}
	}

	if expired || haveAnyMissing {
		persistProgress(ctx, execCfg, job, progress, sql.StatusWaitingGC)
	}

	return expired, earliestDeadline
}

// updateStatusForGCElements updates the status for indexes on this table if any
// are waiting for GC. If the table is waiting for GC then the status of the table
// will be updated.
// It returns whether any indexes or the table have expired as well as the time
// until the next index expires if there are any more to drop. It also returns
// whether the table descriptor is missing indicating that it was gc'd by
// another job, in which case the progress will have been updated.
func updateStatusForGCElements(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	jobID jobspb.JobID,
	tableID descpb.ID,
	tableDropTimes map[descpb.ID]int64,
	indexDropTimes map[descpb.IndexID]int64,
	progress *jobspb.SchemaChangeGCProgress,
) (expired, missing bool, timeToNextTrigger time.Time) {
	defTTL := execCfg.DefaultZoneConfig.GC.TTLSeconds
	cfg := execCfg.SystemConfig.GetSystemConfig()
	// If the system config is nil, it means we have not seen an initial system
	// config. Because we register for notifications when the system config
	// changes before we get here, we'll get notified to update statuses as soon
	// a new configuration is available. If we were to proceed, we'd hit a nil
	// pointer panic.
	if cfg == nil {
		return false, false, maxDeadline
	}
	protectedtsCache := execCfg.ProtectedTimestampProvider
	earliestDeadline := timeutil.Unix(0, int64(math.MaxInt64))

	if err := sql.DescsTxn(ctx, execCfg, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
		table, err := col.ByIDWithoutLeased(txn.KV()).Get().Table(ctx, tableID)
		if err != nil {
			return err
		}
		zoneCfg, err := cfg.GetZoneConfigForObject(execCfg.Codec, config.ObjectID(tableID))
		if err != nil {
			log.Errorf(ctx, "zone config for desc: %d, err = %+v", tableID, err)
			return nil
		}
		tableTTL := getTableTTL(defTTL, zoneCfg)

		// Update the status of the table if the table was dropped.
		if table.Dropped() {
			deadline := updateTableStatus(ctx, execCfg, jobID, int64(tableTTL), table, tableDropTimes, progress)
			if timeutil.Until(deadline) < 0 {
				expired = true
			} else if deadline.Before(earliestDeadline) {
				earliestDeadline = deadline
			}
		}

		// Update the status of any indexes waiting for GC.
		indexesExpired, deadline := updateIndexesStatus(
			ctx, execCfg, jobID, tableTTL, table, protectedtsCache, zoneCfg, indexDropTimes, progress,
		)
		if indexesExpired {
			expired = true
		}
		if deadline.Before(earliestDeadline) {
			earliestDeadline = deadline
		}

		return nil
	}); err != nil {
		if isMissingDescriptorError(err) {
			log.Warningf(ctx, "table %d not found, marking as GC'd", tableID)
			markTableGCed(ctx, tableID, progress, jobspb.SchemaChangeGCProgress_CLEARED)
			for indexID := range indexDropTimes {
				markIndexGCed(ctx, indexID, progress, jobspb.SchemaChangeGCProgress_CLEARED)
			}
			return false, true, maxDeadline
		}
		log.Warningf(ctx, "error while calculating GC time for table %d, err: %+v", tableID, err)
		return false, false, maxDeadline
	}

	return expired, false, earliestDeadline
}

// updateTableStatus sets the status the table to DELETING if the GC TTL has
// expired.
func updateTableStatus(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	jobID jobspb.JobID,
	ttlSeconds int64,
	table catalog.TableDescriptor,
	tableDropTimes map[descpb.ID]int64,
	progress *jobspb.SchemaChangeGCProgress,
) time.Time {
	deadline := timeutil.Unix(0, int64(math.MaxInt64))
	sp := table.TableSpan(execCfg.Codec)

	for i, t := range progress.Tables {
		droppedTable := &progress.Tables[i]
		if droppedTable.ID != table.GetID() || droppedTable.Status == jobspb.SchemaChangeGCProgress_CLEARED {
			continue
		}

		deadlineNanos := tableDropTimes[t.ID] + ttlSeconds*time.Second.Nanoseconds()
		deadline = timeutil.Unix(0, deadlineNanos)
		isProtected, err := isProtected(
			ctx,
			jobID,
			tableDropTimes[t.ID],
			execCfg,
			execCfg.SpanConfigKVAccessor,
			execCfg.ProtectedTimestampProvider,
			sp,
		)
		if err != nil {
			log.Errorf(ctx, "error checking protection status %v", err)
			// We don't want to make GC decisions if we can't validate the protection
			// status of a table. We don't change the status of the table to DELETING
			// and simply return a high deadline value; The GC job will be retried
			// automatically up the stack.
			return maxDeadline
		}
		if isProtected {
			log.Infof(ctx, "a timestamp protection delayed GC of table %d", t.ID)
			return maxDeadline
		}

		lifetime := timeutil.Until(deadline)
		if lifetime < 0 {
			if log.V(2) {
				log.Infof(ctx, "detected expired table %d", t.ID)
			}
			droppedTable.Status = jobspb.SchemaChangeGCProgress_CLEARING
		} else {
			if log.V(2) {
				log.Infof(ctx, "table %d still has %+v until GC", t.ID, lifetime)
			}
		}
		break
	}

	return deadline
}

// updateIndexesStatus updates the status on every index that is waiting for GC
// TTL in this table.
// It returns whether any indexes have expired and the timestamp of when another
// index should be GC'd, if any, otherwise MaxInt.
func updateIndexesStatus(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	jobID jobspb.JobID,
	tableTTL int32,
	table catalog.TableDescriptor,
	protectedtsCache protectedts.Cache,
	zoneCfg *zonepb.ZoneConfig,
	indexDropTimes map[descpb.IndexID]int64,
	progress *jobspb.SchemaChangeGCProgress,
) (expired bool, soonestDeadline time.Time) {
	// Update the deadline for indexes that are being dropped, if any.
	soonestDeadline = timeutil.Unix(0, int64(math.MaxInt64))
	for i := 0; i < len(progress.Indexes); i++ {
		idxProgress := &progress.Indexes[i]
		if idxProgress.Status == jobspb.SchemaChangeGCProgress_CLEARED {
			continue
		}

		sp := table.IndexSpan(execCfg.Codec, idxProgress.IndexID)

		ttlSeconds := getIndexTTL(tableTTL, zoneCfg, idxProgress.IndexID)

		deadlineNanos := indexDropTimes[idxProgress.IndexID] + int64(ttlSeconds)*time.Second.Nanoseconds()
		deadline := timeutil.Unix(0, deadlineNanos)
		isProtected, err := isProtected(
			ctx,
			jobID,
			indexDropTimes[idxProgress.IndexID],
			execCfg,
			execCfg.SpanConfigKVAccessor,
			protectedtsCache,
			sp,
		)
		if err != nil {
			log.Errorf(ctx, "error checking protection status %v", err)
			continue
		}
		if isProtected {
			log.Infof(ctx, "a timestamp protection delayed GC of index %d from table %d", idxProgress.IndexID, table.GetID())
			continue
		}
		lifetime := time.Until(deadline)
		if lifetime > 0 {
			if log.V(2) {
				log.Infof(ctx, "index %d from table %d still has %+v until GC", idxProgress.IndexID, table.GetID(), lifetime)
			}
		}
		if lifetime < 0 {
			expired = true
			if log.V(2) {
				log.Infof(ctx, "detected expired index %d from table %d", idxProgress.IndexID, table.GetID())
			}
			idxProgress.Status = jobspb.SchemaChangeGCProgress_CLEARING
		} else if deadline.Before(soonestDeadline) {
			soonestDeadline = deadline
		}
	}
	return expired, soonestDeadline
}

// Helpers.

func getIndexTTL(tableTTL int32, placeholder *zonepb.ZoneConfig, indexID descpb.IndexID) int32 {
	ttlSeconds := tableTTL
	if placeholder != nil {
		if subzone := placeholder.GetSubzone(
			uint32(indexID), ""); subzone != nil && subzone.Config.GC != nil {
			ttlSeconds = subzone.Config.GC.TTLSeconds
		}
	}
	return ttlSeconds
}

func getTableTTL(defTTL int32, zoneCfg *zonepb.ZoneConfig) int32 {
	ttlSeconds := defTTL
	if zoneCfg != nil {
		ttlSeconds = zoneCfg.GC.TTLSeconds
	}
	return ttlSeconds
}

// isProtected returns true if the supplied span is considered protected, and
// thus exempt from GC-ing, given the wall time at which it was dropped.
//
// This function is intended for table/index spans -- for spans that cover a
// secondary tenant's keyspace, checkout `isTenantProtected` instead.
func isProtected(
	ctx context.Context,
	jobID jobspb.JobID,
	droppedAtTime int64,
	execCfg *sql.ExecutorConfig,
	kvAccessor spanconfig.KVAccessor,
	ptsCache protectedts.Cache,
	sp roachpb.Span,
) (bool, error) {
	// Wrap this in a closure sp we can pass the protection status to the testing
	// knob.
	isProtected, err := func() (bool, error) {
		// We check the old protected timestamp subsystem for protected timestamps
		// if this is the GC job of the system tenant.
		if execCfg.Codec.ForSystemTenant() &&
			deprecatedIsProtected(ctx, ptsCache, droppedAtTime, sp) {
			return true, nil
		}

		spanConfigRecords, err := kvAccessor.GetSpanConfigRecords(ctx, spanconfig.Targets{
			spanconfig.MakeTargetFromSpan(sp),
		})
		if err != nil {
			return false, err
		}

		_, tenID, err := keys.DecodeTenantPrefix(execCfg.Codec.TenantPrefix())
		if err != nil {
			return false, err
		}
		systemSpanConfigs, err := kvAccessor.GetAllSystemSpanConfigsThatApply(ctx, tenID)
		if err != nil {
			return false, err
		}

		// Collect all protected timestamps that apply to the given span; both by
		// virtue of span configs and system span configs.
		var protectedTimestamps []hlc.Timestamp
		collectProtectedTimestamps := func(configs ...roachpb.SpanConfig) {
			for _, config := range configs {
				for _, protectionPolicy := range config.GCPolicy.ProtectionPolicies {
					// We don't consider protected timestamps written by backups if the span
					// is indicated as "excluded from backup". Checkout the field
					// descriptions for more details about this coupling.
					if config.ExcludeDataFromBackup && protectionPolicy.IgnoreIfExcludedFromBackup {
						continue
					}
					protectedTimestamps = append(protectedTimestamps, protectionPolicy.ProtectedTimestamp)
				}
			}
		}
		for _, record := range spanConfigRecords {
			collectProtectedTimestamps(record.GetConfig())
		}
		collectProtectedTimestamps(systemSpanConfigs...)

		for _, protectedTimestamp := range protectedTimestamps {
			if protectedTimestamp.WallTime < droppedAtTime {
				return true, nil
			}
		}

		return false, nil
	}()
	if err != nil {
		return false, err
	}

	if fn := execCfg.GCJobTestingKnobs.RunAfterIsProtectedCheck; fn != nil {
		fn(jobID, isProtected)
	}

	return isProtected, nil
}

// Returns whether or not a key in the given spans is protected.
// TODO(pbardea): If the TTL for this index/table expired and we're only blocked
// on a protected timestamp, this may be useful information to surface to the
// user.
func deprecatedIsProtected(
	ctx context.Context, protectedtsCache protectedts.Cache, atTime int64, sp roachpb.Span,
) bool {
	protected := false
	protectedtsCache.Iterate(ctx,
		sp.Key, sp.EndKey,
		func(r *ptpb.Record) (wantMore bool) {
			// If we encounter any protected timestamp records in this span, we
			// can't GC.
			if r.Timestamp.WallTime < atTime {
				protected = true
				return false
			}
			return true
		})
	return protected
}

// isTenantProtected returns true if there exist any protected timestamp records
// written by the system tenant, that targets the tenant with tenantID.
func isTenantProtected(
	ctx context.Context, atTime hlc.Timestamp, tenantID roachpb.TenantID, execCfg *sql.ExecutorConfig,
) (bool, error) {
	if !execCfg.Codec.ForSystemTenant() {
		return false, errors.AssertionFailedf("isTenantProtected incorrectly invoked by secondary tenant")
	}

	isProtected := false
	ptsProvider := execCfg.ProtectedTimestampProvider
	if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		ptsState, err := ptsProvider.WithTxn(txn).GetState(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to get protectedts State")
		}
		ptsStateReader := spanconfig.NewProtectedTimestampStateReader(ctx, ptsState)

		// First check if the system tenant has any cluster level protections that protect
		// all secondary tenants.
		clusterProtections := ptsStateReader.GetProtectionPoliciesForCluster()
		for _, p := range clusterProtections {
			if p.ProtectedTimestamp.Less(atTime) {
				isProtected = true
				return nil
			}
		}

		// Now check if the system tenant has any protections that target the
		// tenantID's keyspace.
		protectionsOnTenant := ptsStateReader.GetProtectionsForTenant(tenantID)
		for _, p := range protectionsOnTenant {
			if p.ProtectedTimestamp.Less(atTime) {
				isProtected = true
				return nil
			}
		}

		return nil
	}); err != nil {
		return false, err
	}
	return isProtected, nil
}

// refreshTenant updates the status of tenant that is waiting to be GC'd. It
// returns whether or the tenant has expired or the duration until it expires.
func refreshTenant(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	dropTime int64,
	details *jobspb.SchemaChangeGCDetails,
	progress *jobspb.SchemaChangeGCProgress,
) (expired bool, _ time.Time, _ error) {
	if progress.Tenant.Status != jobspb.SchemaChangeGCProgress_WAITING_FOR_CLEAR {
		return true, time.Time{}, nil
	}

	tenID := details.Tenant.ID
	// Read the tenant's GC TTL to check if the tenant's data has expired.
	cfg := execCfg.SystemConfig.GetSystemConfig()
	tenantTTLSeconds := execCfg.DefaultZoneConfig.GC.TTLSeconds
	zoneCfg, err := cfg.GetZoneConfigForObject(keys.SystemSQLCodec, keys.TenantsRangesID)
	if err == nil {
		tenantTTLSeconds = zoneCfg.GC.TTLSeconds
	} else {
		log.Errorf(ctx, "zone config for tenants range: err = %+v", err)
	}

	deadlineNanos := dropTime + int64(tenantTTLSeconds)*time.Second.Nanoseconds()
	deadlineUnix := timeutil.Unix(0, deadlineNanos)
	if timeutil.Now().UnixNano() >= deadlineNanos {
		// If the tenant's GC TTL has elapsed, check if there are any protected timestamp records
		// that apply to the tenant keyspace.
		atTime := hlc.Timestamp{WallTime: dropTime}
		isProtected, err := isTenantProtected(ctx, atTime, roachpb.MustMakeTenantID(tenID), execCfg)
		if err != nil {
			return false, time.Time{}, err
		}

		if isProtected {
			log.Infof(ctx, "GC TTL for dropped tenant %d has expired, but protected timestamp "+
				"record(s) on the tenant keyspace are preventing GC", tenID)
			return false, deadlineUnix, nil
		}

		// At this point, the tenant's keyspace is ready for GC.
		progress.Tenant.Status = jobspb.SchemaChangeGCProgress_CLEARING
		return true, deadlineUnix, nil
	}
	return false, deadlineUnix, nil
}
