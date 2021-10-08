// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gcjob

import (
	"context"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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
	jobID jobspb.JobID,
	progress *jobspb.SchemaChangeGCProgress,
) (expired bool, earliestDeadline time.Time) {
	earliestDeadline = maxDeadline
	var haveAnyMissing bool
	for _, tableID := range tableIDs {
		tableHasExpiredElem, tableIsMissing, deadline := updateStatusForGCElements(
			ctx,
			execCfg,
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
		persistProgress(ctx, execCfg, jobID, progress, sql.RunningStatusWaitingGC)
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
	tableID descpb.ID,
	tableDropTimes map[descpb.ID]int64,
	indexDropTimes map[descpb.IndexID]int64,
	progress *jobspb.SchemaChangeGCProgress,
) (expired, missing bool, timeToNextTrigger time.Time) {
	defTTL := execCfg.DefaultZoneConfig.GC.TTLSeconds
	cfg := execCfg.SystemConfig.GetSystemConfig()
	protectedtsCache := execCfg.ProtectedTimestampProvider

	earliestDeadline := timeutil.Unix(0, int64(math.MaxInt64))

	if err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		table, err := catalogkv.MustGetTableDescByID(ctx, txn, execCfg.Codec, tableID)
		if err != nil {
			return err
		}

		zoneCfg, err := cfg.GetZoneConfigForObject(execCfg.Codec, uint32(tableID))
		if err != nil {
			log.Errorf(ctx, "zone config for desc: %d, err = %+v", tableID, err)
			return nil
		}
		tableTTL := getTableTTL(defTTL, zoneCfg)

		// Update the status of the table if the table was dropped.
		if table.Dropped() {
			deadline := updateTableStatus(ctx, execCfg, int64(tableTTL), protectedtsCache, table, tableDropTimes, progress)
			if timeutil.Until(deadline) < 0 {
				expired = true
			} else if deadline.Before(earliestDeadline) {
				earliestDeadline = deadline
			}
		}

		// Update the status of any indexes waiting for GC.
		indexesExpired, deadline := updateIndexesStatus(ctx, execCfg, tableTTL, table, protectedtsCache, zoneCfg, indexDropTimes, progress)
		if indexesExpired {
			expired = true
		}
		if deadline.Before(earliestDeadline) {
			earliestDeadline = deadline
		}

		return nil
	}); err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			log.Warningf(ctx, "table %d not found, marking as GC'd", tableID)
			markTableGCed(ctx, tableID, progress)
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
	ttlSeconds int64,
	protectedtsCache protectedts.Cache,
	table catalog.TableDescriptor,
	tableDropTimes map[descpb.ID]int64,
	progress *jobspb.SchemaChangeGCProgress,
) time.Time {
	deadline := timeutil.Unix(0, int64(math.MaxInt64))
	sp := table.TableSpan(execCfg.Codec)

	for i, t := range progress.Tables {
		droppedTable := &progress.Tables[i]
		if droppedTable.ID != table.GetID() || droppedTable.Status == jobspb.SchemaChangeGCProgress_DELETED {
			continue
		}

		deadlineNanos := tableDropTimes[t.ID] + ttlSeconds*time.Second.Nanoseconds()
		deadline = timeutil.Unix(0, deadlineNanos)
		if isProtected(ctx, protectedtsCache, tableDropTimes[t.ID], sp) {
			log.Infof(ctx, "a timestamp protection delayed GC of table %d", t.ID)
			return deadline
		}

		lifetime := timeutil.Until(deadline)
		if lifetime < 0 {
			if log.V(2) {
				log.Infof(ctx, "detected expired table %d", t.ID)
			}
			droppedTable.Status = jobspb.SchemaChangeGCProgress_DELETING
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
		if idxProgress.Status == jobspb.SchemaChangeGCProgress_DELETED {
			continue
		}

		sp := table.IndexSpan(execCfg.Codec, idxProgress.IndexID)

		ttlSeconds := getIndexTTL(tableTTL, zoneCfg, idxProgress.IndexID)

		deadlineNanos := indexDropTimes[idxProgress.IndexID] + int64(ttlSeconds)*time.Second.Nanoseconds()
		deadline := timeutil.Unix(0, deadlineNanos)
		if isProtected(ctx, protectedtsCache, indexDropTimes[idxProgress.IndexID], sp) {
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
			idxProgress.Status = jobspb.SchemaChangeGCProgress_DELETING
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

// Returns whether or not a key in the given spans is protected.
// TODO(pbardea): If the TTL for this index/table expired and we're only blocked
// on a protected timestamp, this may be useful information to surface to the
// user.
func isProtected(
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

// refreshTenant updates the status of tenant that is waiting to be GC'd. It
// returns whether or the tenant has expired or the duration until it expires.
func refreshTenant(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	dropTime int64,
	details *jobspb.SchemaChangeGCDetails,
	progress *jobspb.SchemaChangeGCProgress,
) (expired bool, deadline time.Time) {
	tenantTTLSeconds := execCfg.DefaultZoneConfig.GC.TTLSeconds
	tenID := details.Tenant.ID
	cfg := execCfg.SystemConfig.GetSystemConfig()
	zoneCfg, err := cfg.GetZoneConfigForObject(keys.MakeSQLCodec(roachpb.MakeTenantID(tenID)), 0)
	if err == nil {
		tenantTTLSeconds = zoneCfg.GC.TTLSeconds
	} else {
		log.Errorf(ctx, "zone config for tenants range: err = %+v", err)
	}

	deadlineNanos := dropTime + int64(tenantTTLSeconds)*time.Second.Nanoseconds()
	if timeutil.Now().UnixNano() >= deadlineNanos {
		progress.Tenant.Status = jobspb.SchemaChangeGCProgress_DELETING
		return true, time.Time{}
	}
	return false, timeutil.Unix(0, deadlineNanos)
}
