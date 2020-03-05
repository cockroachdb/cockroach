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

type schemaChangeGCResumer struct {
	jobID int64
}

func performGC(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	jobID int64,
	details *jobspb.SchemaChangeGCDetails,
	progress *jobspb.SchemaChangeGCProgress,
) error {
	if details.Indexes != nil {
		if err := gcIndexes(ctx, execCfg, details.ParentID, progress); err != nil {
			return err
		}
	} else if details.Tables != nil {
		// Drop tables.
		if err := gcTables(ctx, execCfg, progress); err != nil {
			return errors.Wrap(err, "GCing tables")
		}

		// Drop database when all the tables have been GCed.
		if details.ParentID != sqlbase.InvalidID && isDoneGC(progress) {
			if err := deleteDatabaseZoneConfig(ctx, execCfg.DB, details.ParentID); err != nil {
				return err
			}
		}
	}

	persistProgress(ctx, execCfg, jobID, progress)
	return nil
}

// Resume is part of the jobs.Resumer interface.
func (r schemaChangeGCResumer) Resume(
	ctx context.Context, phs interface{}, _ chan<- tree.Datums,
) error {
	// Make sure to update the deadlines are updated before starting to listen for
	// zone config changes.
	p := phs.(sql.PlanHookState)
	// TODO(pbardea): Wait for no versions.
	execCfg := p.ExecCfg()
	details, progress, err := initDetailsAndProgress(ctx, execCfg, r.jobID)
	if err != nil {
		return err
	}
	zoneCfgFilter, descTableFilter, gossipUpdateC := setupConfigWatchers(execCfg)
	tableDropTimes, indexDropTimes := getDropTimes(details)

	allTables := getAllTablesWaitingForGC(details, progress)
	expired, earliestDeadline := refreshTables(ctx, execCfg, allTables, tableDropTimes, indexDropTimes, r.jobID, progress)
	timerDuration := time.Until(earliestDeadline)
	if expired {
		timerDuration = 0
	} else if timerDuration > MaxSQLGCInterval {
		timerDuration = MaxSQLGCInterval
	}
	timer := timeutil.NewTimer()
	defer timer.Stop()
	timer.Reset(timerDuration)

	// The main loop of the job. It listens for changes to gossip or table
	// descriptors, or until the periodic timer fires.
	for {
		select {
		case <-gossipUpdateC:
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
			// Refresh the status of all tables in case any GC TTLs have changed that
			// gossip may have missed.
			remainingTables := getAllTablesWaitingForGC(details, progress)
			_, earliestDeadline = refreshTables(ctx, execCfg, remainingTables, tableDropTimes, indexDropTimes, r.jobID, progress)

			if err := performGC(ctx, execCfg, r.jobID, details, progress); err != nil {
				return err
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

// This SchemaChangeGC job should only be created for GC'ing:
// - Non-interleaved indexes
// - Entire tables
// - Entire databases
func init() {
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &schemaChangeGCResumer{
			jobID: *job.ID(),
		}
	}
	jobs.RegisterConstructor(jobspb.TypeSchemaChangeGC, createResumerFn)
}
