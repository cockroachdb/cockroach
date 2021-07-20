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

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
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
	jobID jobspb.JobID
}

// performGC GCs any schema elements that are in the DELETING state and returns
// a bool indicating if it GC'd any elements.
func performGC(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	details *jobspb.SchemaChangeGCDetails,
	progress *jobspb.SchemaChangeGCProgress,
) error {
	if details.Tenant != nil {
		return errors.Wrapf(
			gcTenant(ctx, execCfg, details.Tenant.ID, progress),
			"attempting to GC tenant %+v", details.Tenant,
		)
	}
	if details.Indexes != nil {
		return errors.Wrap(gcIndexes(ctx, execCfg, details.ParentID, progress), "attempting to GC indexes")
	} else if details.Tables != nil {
		if err := gcTables(ctx, execCfg, progress); err != nil {
			return errors.Wrap(err, "attempting to GC tables")
		}

		// Drop database zone config when all the tables have been GCed.
		if details.ParentID != descpb.InvalidID && isDoneGC(progress) {
			if err := deleteDatabaseZoneConfig(ctx, execCfg.DB, execCfg.Codec, details.ParentID); err != nil {
				return errors.Wrap(err, "deleting database zone config")
			}
		}
	}
	return nil
}

// Resume is part of the jobs.Resumer interface.
func (r schemaChangeGCResumer) Resume(ctx context.Context, execCtx interface{}) (err error) {
	defer func() {
		if err != nil && !r.isPermanentGCError(err) {
			err = errors.Mark(err, jobs.NewRetryJobError("gc"))
		}
	}()
	p := execCtx.(sql.JobExecContext)
	// TODO(pbardea): Wait for no versions.
	execCfg := p.ExecCfg()
	if fn := execCfg.GCJobTestingKnobs.RunBeforeResume; fn != nil {
		if err := fn(r.jobID); err != nil {
			return err
		}
	}
	details, progress, err := initDetailsAndProgress(ctx, execCfg, r.jobID)
	if err != nil {
		return err
	}

	// If there are any interleaved indexes to drop as part of a table TRUNCATE
	// operation, then drop the indexes before waiting on the GC timer.
	if len(details.InterleavedIndexes) > 0 {
		// Before deleting any indexes, ensure that old versions of the table
		// descriptor are no longer in use.
		if err := sql.WaitToUpdateLeases(ctx, execCfg.LeaseManager, details.InterleavedTable.ID); err != nil {
			return err
		}
		interleavedIndexIDs := make([]descpb.IndexID, len(details.InterleavedIndexes))
		for i := range details.InterleavedIndexes {
			interleavedIndexIDs[i] = details.InterleavedIndexes[i].ID
		}
		if err := sql.TruncateInterleavedIndexes(
			ctx,
			execCfg,
			tabledesc.NewBuilder(details.InterleavedTable).BuildImmutableTable(),
			interleavedIndexIDs,
		); err != nil {
			return err
		}
	}

	tableDropTimes, indexDropTimes := getDropTimes(details)

	timer := timeutil.NewTimer()
	defer timer.Stop()
	timer.Reset(0)
	gossipUpdateC, cleanup := execCfg.GCJobNotifier.AddNotifyee(ctx)
	defer cleanup()
	for {
		select {
		case <-gossipUpdateC:
			if log.V(2) {
				log.Info(ctx, "received a new system config")
			}
		case <-timer.C:
			timer.Read = true
			if log.V(2) {
				log.Info(ctx, "SchemaChangeGC timer triggered")
			}
		case <-ctx.Done():
			return ctx.Err()
		}

		// Refresh the status of all elements in case any GC TTLs have changed.
		var expired bool
		earliestDeadline := timeutil.Unix(0, math.MaxInt64)
		if details.Tenant == nil {
			remainingTables := getAllTablesWaitingForGC(details, progress)
			expired, earliestDeadline = refreshTables(
				ctx, execCfg, remainingTables, tableDropTimes, indexDropTimes, r.jobID, progress,
			)
		} else {
			expired, earliestDeadline = refreshTenant(ctx, execCfg, details.Tenant.DropTime, details, progress)
		}
		timerDuration := time.Until(earliestDeadline)

		if expired {
			// Some elements have been marked as DELETING so save the progress.
			persistProgress(ctx, execCfg, r.jobID, progress, runningStatusGC(progress))
			if fn := execCfg.GCJobTestingKnobs.RunBeforePerformGC; fn != nil {
				if err := fn(r.jobID); err != nil {
					return err
				}
			}
			if err := performGC(ctx, execCfg, details, progress); err != nil {
				return err
			}
			persistProgress(ctx, execCfg, r.jobID, progress, sql.RunningStatusWaitingGC)

			// Trigger immediate re-run in case of more expired elements.
			timerDuration = 0
		}

		if isDoneGC(progress) {
			return nil
		}

		// Schedule the next check for GC.
		if timerDuration > MaxSQLGCInterval {
			timerDuration = MaxSQLGCInterval
		}
		timer.Reset(timerDuration)
	}
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (r schemaChangeGCResumer) OnFailOrCancel(context.Context, interface{}) error {
	return nil
}

// isPermanentGCError returns true if the error is a permanent job failure,
// which indicates that the failed GC job cannot be retried.
func (r *schemaChangeGCResumer) isPermanentGCError(err error) bool {
	// Currently we classify errors based on Schema Change function to backport
	// to 20.2 and 21.1. This functionality should be changed once #44594 is
	// implemented.
	return sql.IsPermanentSchemaChangeError(err)
}

func init() {
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &schemaChangeGCResumer{
			jobID: job.ID(),
		}
	}
	jobs.RegisterConstructor(jobspb.TypeSchemaChangeGC, createResumerFn)
}
