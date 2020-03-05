// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package waiting_for_gc

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var (
	// MaxSQLGCInterval is the longest the polling interval between checking if
	// elements should be GC'd.
	MaxSQLGCInterval = 5 * time.Minute
)

type waitForGCResumer struct {
	jobID int64
}

func performGC(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	details *jobspb.WaitingForGCDetails,
	progress *jobspb.WaitingForGCProgress,
) error {
	if details.Indexes != nil {
		if err := gcIndexes(ctx, execCfg, details.ParentID, progress); err != nil {
			return err
		}
	} else if details.Tables != nil {
		// Drop tables.
		if err := dropTables(ctx, execCfg.DB, execCfg.DistSender, progress); err != nil {
			return err
		}

		// Drop database when all the tables have been GCed.
		if details.ParentID != sqlbase.InvalidID && isDoneGC(progress) {
			if err := deleteDatabaseZoneConfig(ctx, execCfg.DB, details.ParentID); err != nil {
				return err
			}
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
	p := phs.(sql.PlanHookState)
	// TODO(pbardea): Wait for no versions.
	execCfg := p.ExecCfg()
	details, progress, err := initDetailsAndProgress(ctx, execCfg, r.jobID)
	if err != nil {
		return err
	}
	zoneCfgFilter, descTableFilter, gossipUpdateC := setupConfigWatchers(execCfg)
	tableDropTimes, indexDropTimes := getDropTimes(details)

	allTables := getAllRemainingTables(details, progress)
	expired, timerDuration, err := refreshTables(ctx, execCfg, allTables, tableDropTimes, indexDropTimes, progress)
	if err != nil {
		return err
	}
	if expired {
		timerDuration = 0
	}
	timer := time.NewTimer(timerDuration)

	// The main loop of the job. It listens for changes to gossip or table
	// descriptors, or until the periodic timer fires.
	for {
		select {
		case <-gossipUpdateC:
			if log.V(2) {
				log.Info(ctx, "received a new config")
			}
			updatedTables, err := getUpdatedTables(ctx, execCfg.Gossip.GetSystemConfig(), zoneCfgFilter, descTableFilter, details, progress)
			if err != nil {
				return err
			}
			expired, timerDuration, err = refreshTables(ctx, execCfg, updatedTables, tableDropTimes, indexDropTimes, progress)
			if err != nil {
				return err
			}

			// Reset the timer.
			timer = time.NewTimer(timerDuration)
		case <-timer.C:
			// Refresh the status of all tables in case any GC TTLs have changed that
			// gossip may have missed.
			remainingTables := getAllRemainingTables(details, progress)
			_, timerDuration, err = refreshTables(ctx, execCfg, remainingTables, tableDropTimes, indexDropTimes, progress)
			if err != nil {
				return err
			}

			if err := performGC(ctx, execCfg, details, progress); err != nil {
				return err
			}

			if err := persistProgress(ctx, execCfg, r.jobID, progress); err != nil {
				return err
			}
			if isDoneGC(progress) {
				return nil
			}
			// Schedule the next check for GC.
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
//
// This WaitForGC job should only be created for GC'ing:
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
