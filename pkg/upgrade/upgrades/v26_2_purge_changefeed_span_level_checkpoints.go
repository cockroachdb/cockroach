// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

func purgeChangefeedSpanLevelCheckpoints(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	// Find all non-terminal changefeed jobs. It's okay to miss
	// any changefeeds created after the migration has started since
	// they'll all be only doing frontier persistence.
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
		if err := purgeSpanLevelCheckpointForJob(ctx, d, jobID); err != nil {
			return errors.Wrapf(err,
				"purging span-level checkpoint for changefeed job %d", jobID)
		}
	}
	return nil
}

func purgeSpanLevelCheckpointForJob(
	ctx context.Context, d upgrade.TenantDeps, jobID jobspb.JobID,
) error {
	return d.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := jobs.InfoStorageForJob(txn, jobID)
		progressBytes, exists, err := infoStorage.GetLegacyProgress(
			ctx, "purge-changefeed-span-level-checkpoints")
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

		// Clear the span-level checkpoint and write back.
		cfProgress.SpanLevelCheckpoint = nil
		updatedBytes, err := protoutil.Marshal(&progress)
		if err != nil {
			return err
		}
		return infoStorage.WriteLegacyProgress(ctx, updatedBytes)
	})
}
