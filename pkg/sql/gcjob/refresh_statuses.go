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

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// refreshTables updates the status of tables/indexes that are waiting to be
// GC'd.
// It returns whether or not any index/table has expired and the duration until
// the next index/table expires.
func refreshTables(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	tableIDs []sqlbase.ID,
	tableDropTimes map[sqlbase.ID]int64,
	indexDropTimes map[sqlbase.IndexID]int64,
	progress *jobspb.WaitingForGCProgress,
) (expired bool, duration time.Duration) {
	duration = MaxSQLGCInterval

	for _, tableID := range tableIDs {
		var tableHasExpiredElem bool
		var newTimerDuration time.Duration
		tableHasExpiredElem, newTimerDuration = updateStatusForGCElements(
			ctx,
			execCfg,
			tableID,
			tableDropTimes, indexDropTimes,
			progress,
		)
		if tableHasExpiredElem {
			expired = true
		}
		if newTimerDuration < duration {
			duration = newTimerDuration
		}
	}

	return expired, duration
}

// updateStatusForGCElements updates the status for indexes on this table if any
// are waiting for GC. If the table is waiting for GC then the status of the table
// will be updated.
// It returns whether any indexes or the table have expired as well as the time
// until the next index expires if there are any more to drop.
func updateStatusForGCElements(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	tableID sqlbase.ID,
	tableDropTimes map[sqlbase.ID]int64,
	indexDropTimes map[sqlbase.IndexID]int64,
	progress *jobspb.WaitingForGCProgress,
) (expired bool, timeToNextTrigger time.Duration) {
	defTTL := execCfg.DefaultZoneConfig.GC.TTLSeconds
	cfg := execCfg.Gossip.GetSystemConfig()
	protectedtsCache := execCfg.ProtectedTimestampProvider

	earliestDeadline := int64(math.MaxInt64)

	if err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		table, err := sqlbase.GetTableDescFromID(ctx, txn, tableID)
		if err != nil {
			return err
		}

		zoneCfg, placeholder, _, err := sql.ZoneConfigHook(cfg, uint32(tableID))
		if err != nil {
			log.Errorf(ctx, "zone config for desc: %d, err = %+v", tableID, err)
			return nil
		}
		tableTTL := getTableTTL(defTTL, zoneCfg)
		if placeholder == nil {
			placeholder = zoneCfg
		}

		// Update the status of the table is the table was dropped.
		if table.Dropped() {
			deadline := updateTableStatus(ctx, int64(tableTTL), protectedtsCache, table, tableDropTimes, progress)
			if timeutil.Unix(0, deadline).Sub(timeutil.Now()) < 0 {
				expired = true
			} else if deadline < earliestDeadline {
				earliestDeadline = deadline
			}
		}

		// Update the status of any indexes waiting for GC.
		indexesExired, deadline := updateIndexesStatus(ctx, tableTTL, table, protectedtsCache, placeholder, indexDropTimes, progress)
		if indexesExired {
			expired = true
		}
		if deadline < earliestDeadline {
			earliestDeadline = deadline
		}

		return nil
	}); err != nil {
		log.Warningf(ctx, "error while calculating GC time for table %d, err: %+v", tableID, err)
		return false, MaxSQLGCInterval
	}
	// The timer should trigger again at this next time.
	nextTriggerTime := timeutil.Unix(0, earliestDeadline)
	timeToNextTrigger = nextTriggerTime.Sub(timeutil.Now())

	return expired, timeToNextTrigger
}

// updateTableStatus sets the status the table to DELETING if the GC TTL has
// expired.
func updateTableStatus(
	ctx context.Context,
	ttlSeconds int64,
	protectedtsCache protectedts.Cache,
	table *sqlbase.TableDescriptor,
	tableDropTimes map[sqlbase.ID]int64,
	progress *jobspb.WaitingForGCProgress,
) int64 {
	deadline := int64(math.MaxInt64)
	lifetime := ttlSeconds * time.Second.Nanoseconds()
	sp := table.TableSpan()

	for i, t := range progress.Tables {
		droppedTable := &progress.Tables[i]
		if droppedTable.ID != table.ID || droppedTable.Status != jobspb.WaitingForGCProgress_WAITING_FOR_GC {
			continue
		}

		deadline = tableDropTimes[t.ID] + lifetime
		if isProtected(ctx, protectedtsCache, deadline, sp) {
			return deadline
		}

		if timeutil.Until(timeutil.Unix(0, deadline)) < 0 {
			droppedTable.Status = jobspb.WaitingForGCProgress_DELETING
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
	tableTTL int32,
	table *sqlbase.TableDescriptor,
	protectedtsCache protectedts.Cache,
	placeholder *zonepb.ZoneConfig,
	indexDropTimes map[sqlbase.IndexID]int64,
	progress *jobspb.WaitingForGCProgress,
) (expired bool, soonestDeadline int64) {
	// Update the deadline for indexes that are being dropped, if any.
	soonestDeadline = int64(math.MaxInt64)
	for i := 0; i < len(progress.Indexes); i++ {
		idxProgress := &progress.Indexes[i]

		sp := table.IndexSpan(idxProgress.IndexID)

		ttlSeconds := getIndexTTL(tableTTL, placeholder, idxProgress.IndexID)

		deadline := indexDropTimes[idxProgress.IndexID] + int64(ttlSeconds)*time.Second.Nanoseconds()
		if isProtected(ctx, protectedtsCache, deadline, sp) {
			continue
		}
		lifetime := timeutil.Unix(0, deadline).Sub(timeutil.Now())
		if lifetime < 0 {
			expired = true
			idxProgress.Status = jobspb.WaitingForGCProgress_DELETING
		} else if deadline < soonestDeadline {
			soonestDeadline = deadline
		}
	}
	return expired, soonestDeadline
}

// Helpers.

func getIndexTTL(tableTTL int32, placeholder *zonepb.ZoneConfig, indexID sqlbase.IndexID) int32 {
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

// getUpdatedTables returns any tables who's TTL may have changed based on
// gossip changes. The zoneCfgFilter watches for changes in the zone config
// table and the descTableFilter watches for changes in the descriptor table.
func getUpdatedTables(
	ctx context.Context,
	cfg *config.SystemConfig,
	zoneCfgFilter gossip.SystemConfigDeltaFilter,
	descTableFilter gossip.SystemConfigDeltaFilter,
	details *jobspb.WaitingForGCDetails,
	progress *jobspb.WaitingForGCProgress,
) []sqlbase.ID {
	tablesToGC := make(map[sqlbase.ID]*jobspb.WaitingForGCProgress_TableProgress)
	for _, table := range progress.Tables {
		tablesToGC[table.ID] = &table
	}

	// Check to see if the zone cfg or any of the descriptors have been modified.
	var tablesToCheck []sqlbase.ID
	zoneCfgModified := false
	zoneCfgFilter.ForModified(cfg, func(kv roachpb.KeyValue) {
		zoneCfgModified = true
	})
	if zoneCfgModified {
		// If any zone config was modified, check all the remaining tables.
		return getAllTablesWaitingForGC(details, progress)
	}

	descTableFilter.ForModified(cfg, func(kv roachpb.KeyValue) {
		// Attempt to unmarshal config into a table/database descriptor.
		var descriptor sqlbase.Descriptor
		if err := kv.Value.GetProto(&descriptor); err != nil {
			log.Warningf(ctx, "%s: unable to unmarshal descriptor %v", kv.Key, kv.Value)
			return
		}
		switch union := descriptor.Union.(type) {
		case *sqlbase.Descriptor_Table:
			table := union.Table
			if err := table.MaybeFillInDescriptor(ctx, nil); err != nil {
				log.Errorf(ctx, "%s: failed to fill in table descriptor %v", kv.Key, table)
				return
			}
			if err := table.ValidateTable(); err != nil {
				log.Errorf(ctx, "%s: received invalid table descriptor: %s. Desc: %v",
					kv.Key, err, table,
				)
				return
			}
			// Check the expiration again for this descriptor if it is waiting for GC.
			if table, ok := tablesToGC[table.ID]; ok {
				if table.Status == jobspb.WaitingForGCProgress_WAITING_FOR_GC {
					tablesToCheck = append(tablesToCheck, table.ID)
				}
			}

		case *sqlbase.Descriptor_Database:
			// We don't care if the database descriptor changes as it doesn't have any
			// effect on the TTL of it's tables.
		}
	})

	return tablesToCheck
}

// setupConfigWatchers returns a filter to watch zone config changes, a filter
// to watch descriptor changes and a channel that is notified when there are
// changes.
func setupConfigWatchers(
	execCfg *sql.ExecutorConfig,
) (gossip.SystemConfigDeltaFilter, gossip.SystemConfigDeltaFilter, <-chan struct{}) {
	k := keys.MakeTablePrefix(uint32(keys.ZonesTableID))
	k = encoding.EncodeUvarintAscending(k, uint64(keys.ZonesTablePrimaryIndexID))
	zoneCfgFilter := gossip.MakeSystemConfigDeltaFilter(k)
	descKeyPrefix := keys.MakeTablePrefix(uint32(sqlbase.DescriptorTable.ID))
	descTableFilter := gossip.MakeSystemConfigDeltaFilter(descKeyPrefix)
	gossipUpdateC := execCfg.Gossip.RegisterSystemConfigChannel()
	return zoneCfgFilter, descTableFilter, gossipUpdateC
}
