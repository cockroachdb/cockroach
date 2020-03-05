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

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
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
	MinSqlGCInterval = 5 * time.Minute
)

type waitForGCResumer struct {
	jobID int64

	testingKnobs struct {
		beforeGC func()
	}
}

// getUpdatedTables returns any tables who's TTL may have changed based on
// gossip changes.
func (r *waitForGCResumer) getUpdatedTables(
	ctx context.Context,
	execCfg *ExecutorConfig,
	zoneCfgFilter gossip.SystemConfigDeltaFilter,
	cfgFilter gossip.SystemConfigDeltaFilter,
) ([]jobspb.WaitingForGCDetails_DroppedID, error) {
	job, err := execCfg.JobRegistry.LoadJob(ctx, r.jobID)
	if err != nil {
		return nil, err
	}
	details := job.Details().(jobspb.WaitingForGCDetails)

	cfg := execCfg.Gossip.GetSystemConfig()
	if log.V(2) {
		log.Info(ctx, "received a new config")
	}

	tablesToGC := make(map[sqlbase.ID]jobspb.WaitingForGCDetails_DroppedID)
	for _, table := range details.Tables {
		tablesToGC[table.ID] = table
	}

	// Check to see if the zone cfg or any of the descriptors have been modified.
	tablesToCheck := make([]jobspb.WaitingForGCDetails_DroppedID, 0)
	zoneCfgFilter.ForModified(cfg, func(kv roachpb.KeyValue) {
		// TODO(pbardea): We could make this smarter by only checking the tables for
		// zone configs that changed.
		// Check all the tables.
		tablesToCheck = details.Tables
	})
	// We're already checking all the tables, so we can return now since we can't
	// check more.
	if len(tablesToCheck) > 0 {
		return tablesToCheck, nil
	}

	// If a zone config change, we're going to check all the tables. If a zone
	// config did not change, check if any of the tables that are waiting on GC
	// have changed.
	// Put the table descriptors that have changed and that are waiting to GC into
	// tablesToCheck.
	cfgFilter.ForModified(cfg, func(kv roachpb.KeyValue) {
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
			// Update the deadline since the descriptor changed.
			if table, ok := tablesToGC[table.ID]; ok {
				tablesToCheck = append(tablesToCheck, table)
			}

		case *sqlbase.Descriptor_Database:
			// We don't care if the database descriptor changes as it doesn't have any
			// effect on the TTL of it's tables.
		}
	})

	return tablesToCheck, nil
}

// updateAllDeadlines forces a refresh of the Deadline property of all elements
// to drop on every table specified. This function returns whether one of
// elements waiting to be dropped has expired its TTL and the earliest deadline
// non-expired deadline is returned. The earliest deadline will be at most the
// minSqlGcIterval.
func (r *waitForGCResumer) updateDeadlineForTables(
	ctx context.Context,
	execCfg *ExecutorConfig,
	tables []jobspb.WaitingForGCDetails_DroppedID,
	details jobspb.WaitingForGCDetails,
) (bool, time.Duration, error) {
	minDuration := MinSqlGCInterval

	expired := false
	for _, table := range tables {
		if newTimerDuration, err := r.updateDeadlineForTable(ctx, execCfg, table.ID, details); err != nil {
			return false, 0, err
		} else {
			if newTimerDuration <= 0 {
				expired = true
			} else {
				if newTimerDuration < minDuration {
					minDuration = newTimerDuration
				}
			}
		}
	}

	return expired, minDuration, nil
}

// updateDeadlineForTable updates the index and table deadlines indicating when
// they should be dropped. The earliest of these times is returned.
func (r *waitForGCResumer) updateDeadlineForTable(
	ctx context.Context,
	execCfg *ExecutorConfig,
	tableID sqlbase.ID,
	details jobspb.WaitingForGCDetails,
) (time.Duration, error) {
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
		if err != nil {
			log.Errorf(ctx, "zone config for desc: %d, err = %+v", tableID, err)
			return nil
		}

		// Check if the table is dropping.
		if table.Dropped() {
			lifetime := int64(defTTL) * time.Second.Nanoseconds()
			if zoneCfg != nil {
				lifetime = int64(zoneCfg.GC.TTLSeconds) * time.Second.Nanoseconds()
			}
			sp := table.TableSpan()
			protected := false
			protectedtsCache.Iterate(ctx,
				sp.Key, sp.EndKey,
				func(r *ptpb.Record) (wantMore bool) {
					// If we encounter any protected timestamp records in this span, we
					// can't GC.
					protected = true
					return false
				})

			for i, t := range details.Tables {
				droppedTable := &details.Tables[i]
				if droppedTable.ID == table.ID {
					if protected {
						// This table is protected.
						droppedTable.Deadline = math.MaxInt64
					} else {
						droppedTable.Deadline = t.DropTime + lifetime
					}
					if droppedTable.Deadline < earliestDeadline {
						earliestDeadline = t.Deadline
					}
					break
				}
			}
		}

		ttlSeconds := defTTL
		if zoneCfg != nil {
			ttlSeconds = zoneCfg.GC.TTLSeconds
		}
		if placeholder == nil {
			placeholder = zoneCfg
		}

		// Update the deadline for indexes that are being dropped, if any.

		indexes := details.Indexes
		for i := 0; i < len(indexes); i++ {
			droppedIdx := &indexes[i]

			sp := table.IndexSpan(droppedIdx.IndexID)
			protected := false
			protectedtsCache.Iterate(ctx,
				sp.Key, sp.EndKey,
				func(r *ptpb.Record) (wantMore bool) {
					// If we encounter any protected timestamp records in this span, we
					// can't GC.
					protected = true
					return false
				})

			if placeholder != nil {
				if subzone := placeholder.GetSubzone(
					uint32(droppedIdx.IndexID), ""); subzone != nil && subzone.Config.GC != nil {
					ttlSeconds = subzone.Config.GC.TTLSeconds
				}
			}

			if protected {
				droppedIdx.Deadline = math.MaxInt64
			} else {
				droppedIdx.Deadline = droppedIdx.DropTime + int64(ttlSeconds)*time.Second.Nanoseconds()
			}

			if droppedIdx.Deadline < earliestDeadline {
				earliestDeadline = droppedIdx.Deadline
			}
		}
		details.Indexes = indexes

		// Update the jobs payload.
		job, err := execCfg.JobRegistry.LoadJobWithTxn(ctx, r.jobID, txn)
		if err != nil {
			return err
		}
		return job.WithTxn(txn).SetDetails(ctx, details)
	}); err != nil {
		return 0, err
	}
	// The timer should trigger again at this next time.
	nextTriggerTime := timeutil.Unix(0, earliestDeadline)
	timeUntilTrigger := nextTriggerTime.Sub(timeutil.Now())

	return timeUntilTrigger, nil
}

// Check if we are done GC'ing everything.
func (r *waitForGCResumer) isDoneGC(details jobspb.WaitingForGCDetails) bool {
	return len(details.Indexes) == 0 && len(details.Tables) == 0
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
	db := execCfg.DB
	var details jobspb.WaitingForGCDetails
	var job *jobs.Job
	if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		job, err = execCfg.JobRegistry.LoadJobWithTxn(ctx, r.jobID, txn)
		if err != nil {
			return err
		}
		details = job.Details().(jobspb.WaitingForGCDetails)
		return nil
	}); err != nil {
		return err
	}

	k := keys.MakeTablePrefix(uint32(keys.ZonesTableID))
	k = encoding.EncodeUvarintAscending(k, uint64(keys.ZonesTablePrimaryIndexID))
	zoneCfgFilter := gossip.MakeSystemConfigDeltaFilter(k)
	descKeyPrefix := keys.MakeTablePrefix(uint32(sqlbase.DescriptorTable.ID))
	cfgFilter := gossip.MakeSystemConfigDeltaFilter(descKeyPrefix)

	gossipUpdateC := execCfg.Gossip.RegisterSystemConfigChannel()
	// Ensure that the deadlines of the elements we want to drop are up to date.
	expired, timerDuration, err := r.updateDeadlineForTables(ctx, execCfg, details.Tables, details)
	if err != nil {
		return err
	}

	if expired {
		// Trigger immediately.
		timerDuration = time.Millisecond
	}
	timer := time.NewTimer(timerDuration)

	for {
		select {
		case <-gossipUpdateC:
			updatedTables, err := r.getUpdatedTables(ctx, execCfg, zoneCfgFilter, cfgFilter)
			if err != nil {
				return err
			}
			expired, timerDuration, err = r.updateDeadlineForTables(ctx, execCfg, updatedTables, details)
			if err != nil {
				return err
			}

			// Reset the timer.
			timer = time.NewTimer(timerDuration)
		case <-timer.C:
			if r.testingKnobs.beforeGC != nil {
				r.testingKnobs.beforeGC()
			}
			expired, timerDuration, err = r.updateDeadlineForTables(ctx, execCfg, details.Tables, details)
			if err != nil {
				return err
			}

			// Currently all elements that can be dropped are dropping in the same
			// txn, and the job details payload is updated once at the end of the txn.
			if details.Indexes != nil {
				// Drop indexes.
				if len(details.Tables) != 1 {
					return errors.Errorf("expected only the table that owns these indexes to be specified in the job details.")
				}
				// TODO(pbardea): Get table with version.
				var parentTable *sqlbase.TableDescriptor
				if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
					var err error
					parentTable, err = sqlbase.GetTableDescFromID(ctx, txn, details.Tables[0].ID)
					if err != nil {
						return err
					}
					return nil
				}); err != nil {
					return err
				}
				if err := gcIndexes(ctx, execCfg, parentTable, &details); err != nil {
					return err
				}
			} else if details.Tables != nil {
				// Drop tables.
				if err := dropTables(ctx, execCfg.DB, execCfg.DistSender, &details); err != nil {
					return err
				}

				// Drop database when all the tables have been GCed.
				if details.DatabaseID != sqlbase.InvalidID && len(details.Tables) == 0 {
					if err := deleteDatabaseZoneConfig(ctx, execCfg.DB, details.DatabaseID); err != nil {
						return err
					}
				}
			}

			if err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
				return job.WithTxn(txn).SetDetails(ctx, details)
			}); err != nil {
				return err
			}

			if r.isDoneGC(details) {
				return nil
			}

			// Check when the next GC should be.
			timer = time.NewTimer(timerDuration)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
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
