// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var (
	// MaxSQLGCInterval is the longest the polling interval between checking if
	// elements should be GC'd.
	MaxSQLGCInterval = 5 * time.Minute
)

type waitForGCResumer struct {
	jobID int64
}

// getUpdatedTables returns any tables who's TTL may have changed based on
// gossip changes. The zoneCfgFilter watches for changes in the zone config
// table and the descTableFilter watches for changes in the descriptor table.
func (r *waitForGCResumer) getUpdatedTables(
	ctx context.Context,
	cfg *config.SystemConfig,
	zoneCfgFilter gossip.SystemConfigDeltaFilter,
	descTableFilter gossip.SystemConfigDeltaFilter,
	details *jobspb.WaitingForGCDetails,
	progress *jobspb.WaitingForGCProgress,
) ([]sqlbase.ID, error) {
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
	// We're already checking all the tables, so we can return now since we can't
	// check more.
	if zoneCfgModified {
		if len(progress.Indexes) > 0 {
			// If we're dropping indexes, we only need to check the parent ID.
			return []sqlbase.ID{details.ParentID}, nil
		}
		for _, table := range progress.Tables {
			// If dropping tables, check all the tables who are waiting to be GC'd.
			if table.Status == jobspb.WaitingForGCProgress_WAITING_FOR_GC {
				tablesToCheck = append(tablesToCheck, table.ID)
			}
		}
		return tablesToCheck, nil
	}

	// If a zone config change, we're going to check all the tables. If a zone
	// config did not change, check if any of the tables that are waiting on GC
	// have changed.
	// Put the table descriptors that have changed and that are waiting to GC into
	// tablesToCheck.
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

	return tablesToCheck, nil
}

func (r *waitForGCResumer) getAllRemainingTables(
	details *jobspb.WaitingForGCDetails, progress *jobspb.WaitingForGCProgress,
) []sqlbase.ID {
	allRemainingTableIDs := make([]sqlbase.ID, 0, len(progress.Tables))
	if len(details.Indexes) > 0 {
		allRemainingTableIDs = append(allRemainingTableIDs, details.ParentID)
	}
	for _, table := range progress.Tables {
		if table.Status == jobspb.WaitingForGCProgress_WAITING_FOR_GC {
			allRemainingTableIDs = append(allRemainingTableIDs, table.ID)
		}
	}

	return allRemainingTableIDs
}

// refreshTables forces a refresh of the Deadline property of all elements
// to drop on every table specified. This function returns whether one of
// elements waiting to be dropped has expired its TTL and the earliest deadline
// non-expired deadline is returned. The earliest deadline will be at most the
// MinSqlGcInterval.
func (r *waitForGCResumer) refreshTables(
	ctx context.Context,
	execCfg *ExecutorConfig,
	tableIDs []sqlbase.ID,
	tableDropTimes map[sqlbase.ID]int64,
	indexDropTimes map[sqlbase.IndexID]int64,
	progress *jobspb.WaitingForGCProgress,
) (bool, time.Duration, error) {
	duration := MaxSQLGCInterval

	expired := false
	for _, tableID := range tableIDs {
		var tableHasExpiredElem bool
		var newTimerDuration time.Duration
		var err error
		if tableHasExpiredElem, newTimerDuration, err = r.updateStatusForTableElements(
			ctx,
			execCfg,
			tableID,
			tableDropTimes, indexDropTimes,
			progress,
		); err != nil {
			return false, 0, err
		}
		if tableHasExpiredElem {
			expired = true
		}
		if newTimerDuration < duration {
		}
	}

	return expired, duration, nil
}

// updateStatusForTableElements updates the index and table Status indicating if
// they should be dropped. If an element has already expired, expired is true,
// earliestDeadline returns the time at which the next element is to GC (in the
// future), or MaxInt if there is nothing else to GC.
func (r *waitForGCResumer) updateStatusForTableElements(
	ctx context.Context,
	execCfg *ExecutorConfig,
	tableID sqlbase.ID,
	tableDropTimes map[sqlbase.ID]int64,
	indexDropTimes map[sqlbase.IndexID]int64,
	progress *jobspb.WaitingForGCProgress,
) (expired bool, timeToNextTrigger time.Duration, err error) {
	defTTL := execCfg.DefaultZoneConfig.GC.TTLSeconds
	cfg := execCfg.Gossip.GetSystemConfig()
	protectedtsCache := execCfg.ProtectedTimestampProvider

	earliestDeadline := int64(math.MaxInt64)

	if err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		table, err := sqlbase.GetTableDescFromID(ctx, txn, tableID)
		if err != nil {
			return err
		}

		zoneCfg, placeholder, _, err := ZoneConfigHook(cfg, uint32(tableID))
		if placeholder == nil {
			placeholder = zoneCfg
		}
		if err != nil {
			log.Errorf(ctx, "zone config for desc: %d, err = %+v", tableID, err)
			return nil
		}

		tableTTL := getTableTTL(defTTL, zoneCfg)

		// Check if the table is dropping.
		if table.Dropped() {
			deadline := r.updateTableStatus(ctx, int64(tableTTL), protectedtsCache, table, tableDropTimes, progress)
			if timeutil.Unix(0, deadline).Sub(timeutil.Now()) < 0 {
				expired = true
			} else if deadline < earliestDeadline {
				earliestDeadline = deadline
			}
		}

		indexesExired, deadline := updateIndexesStatus(ctx, tableTTL, table, protectedtsCache, placeholder, indexDropTimes, progress)
		if indexesExired {
			expired = true
		}
		if deadline < earliestDeadline {
			earliestDeadline = deadline
		}

		return nil
	}); err != nil {
		return false, 0, err
	}
	// The timer should trigger again at this next time.
	nextTriggerTime := timeutil.Unix(0, earliestDeadline)
	timeToNextTrigger = nextTriggerTime.Sub(timeutil.Now())

	return expired, timeToNextTrigger, nil
}

func updateIndexesStatus(
	ctx context.Context,
	tableTTL int32,
	table *sqlbase.TableDescriptor,
	protectedtsCache protectedts.Provider,
	placeholder *zonepb.ZoneConfig,
	indexDropTimes map[sqlbase.IndexID]int64,
	progress *jobspb.WaitingForGCProgress,
) (bool, int64) {
	// Update the deadline for indexes that are being dropped, if any.
	expired := false
	earliestDeadline := int64(math.MaxInt64)
	for i := 0; i < len(progress.Indexes); i++ {
		idxProgress := &progress.Indexes[i]

		sp := table.IndexSpan(idxProgress.IndexID)
		if isProtected(ctx, protectedtsCache, sp) {
			continue
		}

		ttlSeconds := getIndexTTL(tableTTL, placeholder, idxProgress.IndexID)

		deadline := indexDropTimes[idxProgress.IndexID] + int64(ttlSeconds)*time.Second.Nanoseconds()
		lifetime := timeutil.Unix(0, deadline).Sub(timeutil.Now())
		if lifetime < 0 {
			expired = true
			idxProgress.Status = jobspb.WaitingForGCProgress_DELETING
		} else if deadline < earliestDeadline {
			earliestDeadline = deadline
		}
	}
	return expired, earliestDeadline
}

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

// updateTableStatus sets the status of tables to DELETING if their GC TTL has
// expired.
func (r *waitForGCResumer) updateTableStatus(
	ctx context.Context,
	ttlSeconds int64,
	protectedtsCache protectedts.Provider,
	table *sqlbase.TableDescriptor,
	tableDropTimes map[sqlbase.ID]int64,
	progress *jobspb.WaitingForGCProgress,
) int64 {
	deadline := int64(math.MaxInt64)
	lifetime := ttlSeconds * time.Second.Nanoseconds()
	sp := table.TableSpan()

	if isProtected(ctx, protectedtsCache, sp) {
		return deadline
	}

	for i, t := range progress.Tables {
		droppedTable := &progress.Tables[i]
		if droppedTable.Status != jobspb.WaitingForGCProgress_WAITING_FOR_GC {
			continue
		}

		if droppedTable.ID == table.ID {
			deadline = tableDropTimes[t.ID] + lifetime
			if timeutil.Unix(0, deadline).Sub(timeutil.Now()) < 0 {
				droppedTable.Status = jobspb.WaitingForGCProgress_DELETING
			}
			break
		}
	}

	return deadline
}

// Returns whether or not a key in the given spans is protected.
func isProtected(ctx context.Context, protectedtsCache protectedts.Provider, sp roachpb.Span) bool {
	protected := false
	protectedtsCache.Iterate(ctx,
		sp.Key, sp.EndKey,
		func(r *ptpb.Record) (wantMore bool) {
			// If we encounter any protected timestamp records in this span, we
			// can't GC.
			protected = true
			return false
		})
	return protected
}

// Check if we are done GC'ing everything.
func (r *waitForGCResumer) isDoneGC(progress *jobspb.WaitingForGCProgress) bool {
	for _, index := range progress.Indexes {
		if index.Status != jobspb.WaitingForGCProgress_DELETED {
			return false
		}
	}
	for _, table := range progress.Tables {
		if table.Status != jobspb.WaitingForGCProgress_DELETED {
			return false
		}
	}

	return true
}

// initializeProgress converts the details provided into a progress payload that
// will be updated as the elements that need to be GC'd get processed.
func (r *waitForGCResumer) initializeProgress(
	ctx context.Context,
	execCfg *ExecutorConfig,
	details *jobspb.WaitingForGCDetails,
	progress *jobspb.WaitingForGCProgress,
) error {
	if len(progress.Tables) != len(details.Tables) || len(progress.Indexes) != len(details.Indexes) {
		for _, table := range details.Tables {
			progress.Tables = append(progress.Tables, jobspb.WaitingForGCProgress_TableProgress{ID: table.ID})
		}
		for _, index := range details.Indexes {
			progress.Indexes = append(progress.Indexes, jobspb.WaitingForGCProgress_IndexProgress{IndexID: index.IndexID})
		}

		// Write out new progress.
		if err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			job, err := execCfg.JobRegistry.LoadJobWithTxn(ctx, r.jobID, txn)
			if err != nil {
				return err
			}
			return job.SetProgress(ctx, *progress)
		}); err != nil {
			return err
		}
	}
	return nil
}

// Resume is part of the jobs.Resumer interface.
func (r waitForGCResumer) Resume(
	ctx context.Context, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	// Make sure to update the deadlines are updated before starting to listen for
	// zone config changes.
	p := phs.(PlanHookState)
	// TODO(pbardea): Wait for no versions.
	execCfg := p.ExecCfg()
	details, progress, err := r.getDetailsAndProgress(ctx, execCfg)
	if err != nil {
		return err
	}

	if err = r.initializeProgress(ctx, execCfg, details, progress); err != nil {
		return err
	}

	tableDropTimes := make(map[sqlbase.ID]int64, 0)
	for _, table := range details.Tables {
		tableDropTimes[table.ID] = table.DropTime
	}
	indexDropTimes := make(map[sqlbase.IndexID]int64, 0)
	for _, index := range details.Indexes {
		indexDropTimes[index.IndexID] = index.DropTime
	}

	k := keys.MakeTablePrefix(uint32(keys.ZonesTableID))
	k = encoding.EncodeUvarintAscending(k, uint64(keys.ZonesTablePrimaryIndexID))
	zoneCfgFilter := gossip.MakeSystemConfigDeltaFilter(k)
	descKeyPrefix := keys.MakeTablePrefix(uint32(sqlbase.DescriptorTable.ID))
	descTableFilter := gossip.MakeSystemConfigDeltaFilter(descKeyPrefix)

	gossipUpdateC := execCfg.Gossip.RegisterSystemConfigChannel()
	// Ensure that the deadlines of the elements we want to drop are up to date.
	expired, timerDuration, err := r.refreshTables(ctx, execCfg, r.getAllRemainingTables(details, progress), tableDropTimes, indexDropTimes, progress)
	if err != nil {
		return err
	}

	if expired {
		// Trigger immediately.
		timerDuration = 0
	}
	timer := time.NewTimer(timerDuration)

	for {
		select {
		case <-gossipUpdateC:
			if log.V(2) {
				log.Info(ctx, "received a new config")
			}
			updatedTables, err := r.getUpdatedTables(ctx, execCfg.Gossip.GetSystemConfig(), zoneCfgFilter, descTableFilter, details, progress)
			if err != nil {
				return err
			}
			expired, timerDuration, err = r.refreshTables(ctx, execCfg, updatedTables, tableDropTimes, indexDropTimes, progress)
			if err != nil {
				return err
			}

			// Reset the timer.
			timer = time.NewTimer(timerDuration)
		case <-timer.C:
			// Refresh the status of all tables in case any GC TTLs have changed that
			// gossip may have missed.
			remainingTables := r.getAllRemainingTables(details, progress)
			_, timerDuration, err = r.refreshTables(ctx, execCfg, remainingTables, tableDropTimes, indexDropTimes, progress)
			if err != nil {
				return err
			}

			// Currently all elements that can be dropped are dropping in the same
			// txn, and the job details payload is updated once at the end of the txn.
			if details.Indexes != nil {
				// Drop indexes.
				if details.ParentID == sqlbase.InvalidID {
					return errors.Errorf("expected a parent ID to be provided when GCing indexes.")
				}
				if err := gcIndexes(ctx, execCfg, details.ParentID, progress); err != nil {
					return err
				}
			} else if details.Tables != nil {
				// Drop tables.
				if err := dropTables(ctx, execCfg.DB, execCfg.DistSender, progress); err != nil {
					return err
				}

				// Drop database when all the tables have been GCed.
				if details.ParentID != sqlbase.InvalidID && r.isDoneGC(progress) {
					if err := deleteDatabaseZoneConfig(ctx, execCfg.DB, details.ParentID); err != nil {
						return err
					}
				}
			}

			// Persist any progress made.
			if err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
				job, err := execCfg.JobRegistry.LoadJobWithTxn(ctx, r.jobID, txn)
				if err != nil {
					return err
				}
				return job.SetProgress(ctx, *progress)
			}); err != nil {
				return err
			}

			if r.isDoneGC(progress) {
				return nil
			}

			// Schedule the next check for GC.
			timer = time.NewTimer(timerDuration)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (r waitForGCResumer) getDetailsAndProgress(
	ctx context.Context, execCfg *ExecutorConfig,
) (*jobspb.WaitingForGCDetails, *jobspb.WaitingForGCProgress, error) {
	var details jobspb.WaitingForGCDetails
	var progress *jobspb.WaitingForGCProgress
	var job *jobs.Job
	if err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		job, err = execCfg.JobRegistry.LoadJobWithTxn(ctx, r.jobID, txn)
		if err != nil {
			return err
		}
		details = job.Details().(jobspb.WaitingForGCDetails)
		jobProgress := job.Progress()
		progress = jobProgress.GetWaitingForGC()
		return nil
	}); err != nil {
		return nil, nil, err
	}
	return &details, progress, nil
}

// OnSuccess is part of the jobs.Resumer interface.
func (r waitForGCResumer) OnSuccess(context.Context, *client.Txn) error {
	return nil
}

// OnTerminal is part of the jobs.Resumer interface.
func (r waitForGCResumer) OnTerminal(context.Context, jobs.Status, chan<- tree.Datums) {
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (r waitForGCResumer) OnFailOrCancel(ctx context.Context, phs interface{}) error {
	return nil
}

// The job details should contain the following depending on the props:
// 1. Index deletions: One or more deletions of an index on a table.
//      details.Indexes -> the indexes to GC. These indexes must be
//      non-interleaved.
//      details.Tables -> length 1 with only the ID of the table which owns
//      these indexes.
//
// 2. Table deletions: The deletion of a single table.
//      details.Tables -> the tables to be deleted.
//
// 3. Database deletions: The deletion of a database and therefore all its tables.
//      details.Tables -> the IDs of the tables to GC.
//      details.DatabaseID -> the ID of the database to drop.
//
// This WaitForGC job should only be created for dropping:
// - Non-interleaved indexes
// - Entire tables
// - Entire databases
func init() {
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &waitForGCResumer{
			jobID: *job.ID(),
		}
	}
	jobs.RegisterConstructor(jobspb.TypeWaitingForGC, createResumerFn)
}
