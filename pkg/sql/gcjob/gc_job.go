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
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var (
	// MaxSQLGCInterval is the longest the polling interval between checking if
	// elements should be GC'd.
	MaxSQLGCInterval = 5 * time.Minute
)

// SetSmallMaxGCIntervalForTest sets the MaxSQLGCInterval and then returns a closure
// that resets it.
// This is to be used in tests like:
//    defer SetSmallMaxGCIntervalForTest()
func SetSmallMaxGCIntervalForTest() func() {
	oldInterval := MaxSQLGCInterval
	MaxSQLGCInterval = 500 * time.Millisecond
	return func() {
		MaxSQLGCInterval = oldInterval
	}
}

type schemaChangeGCResumer struct {
	jobID int64
}

// performGC GCs any schema elements that are in the DELETING state and returns
// a bool indicating if it GC'd any elements.
func performGC(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	details *jobspb.SchemaChangeGCDetails,
	progress *jobspb.SchemaChangeGCProgress,
) (bool, error) {
	didGC := false
	if details.Indexes != nil {
		if didGCIndex, err := gcIndexes(ctx, execCfg, details.ParentID, progress); err != nil {
			return false, errors.Wrap(err, "attempting to GC indexes")
		} else if didGCIndex {
			didGC = true
		}
	} else if details.Tables != nil {
		if didGCTable, err := gcTables(ctx, execCfg, progress); err != nil {
			return false, errors.Wrap(err, "attempting to GC tables")
		} else if didGCTable {
			didGC = true
		}

		// Drop database zone config when all the tables have been GCed.
		if details.ParentID != sqlbase.InvalidID && isDoneGC(progress) {
			if err := deleteDatabaseZoneConfig(ctx, execCfg.DB, details.ParentID); err != nil {
				return false, errors.Wrap(err, "deleting database zone config")
			}
		}
	}
	return didGC, nil
}

// Resume is part of the jobs.Resumer interface.
func (r schemaChangeGCResumer) Resume(
	ctx context.Context, phs interface{}, _ chan<- tree.Datums,
) error {
	p := phs.(sql.PlanHookState)
	// TODO(pbardea): Wait for no versions.
	execCfg := p.ExecCfg()
	if fn := execCfg.GCJobTestingKnobs.RunBeforeResume; fn != nil {
		fn()
	}
	details, progress, err := initDetailsAndProgress(ctx, execCfg, r.jobID)
	if err != nil {
		return err
	}
	zoneCfgFilter, descTableFilter, gossipUpdateC := setupConfigWatchers(execCfg)
	tableDropTimes, indexDropTimes := getDropTimes(details)

	allTables := getAllTablesWaitingForGC(details, progress)
	expired, earliestDeadline := refreshTables(ctx, execCfg, allTables, tableDropTimes, indexDropTimes, r.jobID, progress)
	timerDuration := timeutil.Until(earliestDeadline)
	if expired {
		timerDuration = 0
	} else if timerDuration > MaxSQLGCInterval {
		timerDuration = MaxSQLGCInterval
	}
	timer := timeutil.NewTimer()
	defer timer.Stop()
	timer.Reset(timerDuration)

	for {
		select {
		case <-gossipUpdateC:
			// Upon notification of a gossip update, update the status of the relevant schema elements.
			if log.V(2) {
				log.Info(ctx, "received a new system config")
			}
			updatedTables := getUpdatedTables(ctx, execCfg.Gossip.GetSystemConfig(), zoneCfgFilter, descTableFilter, details, progress)
			if log.V(2) {
				log.Infof(ctx, "updating status for tables %+v", updatedTables)
			}
			expired, earliestDeadline = refreshTables(ctx, execCfg, updatedTables, tableDropTimes, indexDropTimes, r.jobID, progress)
			timerDuration := time.Until(earliestDeadline)
			if expired {
				timerDuration = 0
			} else if timerDuration > MaxSQLGCInterval {
				timerDuration = MaxSQLGCInterval
			}

			timer.Reset(timerDuration)
		case <-timer.C:
			timer.Read = true
			if log.V(2) {
				log.Info(ctx, "SchemaChangeGC timer triggered")
			}
			// Refresh the status of all tables in case any GC TTLs have changed.
			remainingTables := getAllTablesWaitingForGC(details, progress)
			_, earliestDeadline = refreshTables(ctx, execCfg, remainingTables, tableDropTimes, indexDropTimes, r.jobID, progress)

			if didWork, err := performGC(ctx, execCfg, details, progress); err != nil {
				return err
			} else if didWork {
				persistProgress(ctx, execCfg, r.jobID, progress)
			}

			if isDoneGC(progress) {
				return nil
			}

			// Schedule the next check for GC.
			timerDuration := time.Until(earliestDeadline)
			if timerDuration > MaxSQLGCInterval {
				timerDuration = MaxSQLGCInterval
			}
			timer.Reset(timerDuration)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (r schemaChangeGCResumer) OnFailOrCancel(context.Context, interface{}) error {
	return nil
}

func init() {
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &schemaChangeGCResumer{
			jobID: *job.ID(),
		}
	}
	jobs.RegisterConstructor(jobspb.TypeSchemaChangeGC, createResumerFn)
}
