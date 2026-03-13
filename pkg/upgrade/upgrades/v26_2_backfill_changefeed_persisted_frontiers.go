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
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/errors"
)

// coordinatorFrontierName is the frontier name used by changefeeds.
// This must match the constant in changefeed_processors.go.
const coordinatorFrontierName = "coordinator"

func backfillChangefeedPersistedFrontiers(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	// Find all existing non-terminal changefeed jobs. It's okay to miss
	// any changefeeds created after the migration has started since
	// they'll all be dual-writing already.
	var jobIDs []jobspb.JobID
	if err := d.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		var err error
		jobIDs, err = jobs.RunningJobs(
			ctx, d.Settings, jobspb.InvalidJobID, txn, jobspb.TypeChangefeed)
		return err
	}); err != nil {
		return err
	}

	for _, jobID := range jobIDs {
		if err := backfillFrontierForJob(ctx, d, jobID); err != nil {
			return errors.Wrapf(err,
				"backfilling persisted frontier for changefeed job %d", jobID)
		}
	}
	return nil
}

func backfillFrontierForJob(ctx context.Context, d upgrade.TenantDeps, jobID jobspb.JobID) error {
	return d.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// Load the job progress.
		progressBytes, exists, err := jobs.InfoStorageForJob(txn, jobID).
			GetLegacyProgress(ctx, "backfill-changefeed-persisted-frontiers")
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}
		var progress jobspb.Progress
		if err := protoutil.Unmarshal(progressBytes, &progress); err != nil {
			return err
		}
		cfProgress := progress.GetChangefeed()
		if cfProgress == nil || cfProgress.SpanLevelCheckpoint == nil {
			return nil
		}

		// Load the existing persisted frontier, if any.
		frontier, found, err := jobfrontier.Get(ctx, txn, jobID, coordinatorFrontierName)
		if err != nil {
			return err
		}
		if !found {
			frontier, err = span.MakeFrontier()
			if err != nil {
				return err
			}
		}
		defer frontier.Release()

		// Forward the frontier with the span-level checkpoint data.
		for ts, spans := range cfProgress.SpanLevelCheckpoint.All() {
			if err := frontier.AddSpansAt(ts, spans...); err != nil {
				return err
			}
		}

		// Store the updated frontier.
		return jobfrontier.Store(ctx, txn, jobID, coordinatorFrontierName, frontier)
	})
}
