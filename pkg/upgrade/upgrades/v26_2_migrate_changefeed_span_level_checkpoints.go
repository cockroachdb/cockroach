// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobfrontier"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/span"
)

// migrateChangefeedSpanLevelCheckpoints converts span-level checkpoint (SLC)
// data stored in changefeed job progress to frontier persistence stored in
// job info storage. At the time this migration runs, no changefeeds will be
// writing new span-level checkpoints so we can be assured that the conversion
// will not be undone. We migrate running changefeeds as well instead of waiting
// for them to auto migrate so that any changefeed that is taking a while to
// update its checkpoint will not hold up the migration.
func migrateChangefeedSpanLevelCheckpoints(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	// Collect non-terminal changefeed job IDs. It's okay if we miss new ones
	// since V26_2_ChangefeedsStopWritingSpanLevelCheckpoint is already active
	// and no new SLC data is being written.
	var jobIDs []jobspb.JobID
	if err := d.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		var err error
		jobIDs, err = jobs.RunningJobs(
			ctx, d.Settings, jobspb.InvalidJobID, txn, jobspb.TypeChangefeed,
		)
		return err
	}); err != nil {
		return err
	}

	for _, jobID := range jobIDs {
		if err := maybeMigrateChangefeedSLC(ctx, d, jobID); err != nil {
			return err
		}
	}
	return nil
}

// maybeMigrateChangefeedSLC converts a single changefeed job's span-level
// checkpoint to frontier persistence, if it has one.
func maybeMigrateChangefeedSLC(
	ctx context.Context, d upgrade.TenantDeps, jobID jobspb.JobID,
) error {
	j, err := d.JobRegistry.LoadJob(ctx, jobID)
	if err != nil {
		if jobs.HasJobNotFoundError(err) {
			return nil
		}
		return err
	}

	return j.NoTxn().Update(ctx, func(
		txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
	) error {
		cfProgress := md.Progress.GetChangefeed()
		if cfProgress == nil || cfProgress.SpanLevelCheckpoint.IsEmpty() {
			return nil
		}

		// Load existing frontier so we can fold SLC entries into it.
		frontier, found, err := jobfrontier.Get(ctx, txn, jobID, "coordinator")
		if err != nil {
			return err
		}
		if !found {
			frontier, err = span.MakeFrontier()
			if err != nil {
				return err
			}
		}

		// Forward the frontier with each SLC entry.
		for ts, spans := range cfProgress.SpanLevelCheckpoint.All() {
			if err := frontier.AddSpansAt(ts, spans...); err != nil {
				return err
			}
		}

		if err := jobfrontier.Store(ctx, txn, jobID, "coordinator", frontier); err != nil {
			return err
		}

		log.Dev.Infof(ctx, "migrated span-level checkpoint to frontier for changefeed job %d",
			jobID)

		cfProgress.SpanLevelCheckpoint = nil
		ju.UpdateProgress(md.Progress)
		return nil
	})
}
