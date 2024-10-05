// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

func waitForSchemaChangerElementMigration(
	ctx context.Context, clusterVersion clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	initialJobListQuery := fmt.Sprintf(`

SELECT
    job_id
FROM
    [SHOW JOBS]
WHERE
    job_type = 'NEW SCHEMA CHANGE'
    AND status NOT IN ('%s', '%s', '%s', '%s', '%s')
`,
		string(jobs.StatusSucceeded),
		string(jobs.StatusFailed),
		string(jobs.StatusCanceled),
		string(jobs.StatusRevertFailed),
		string(jobs.StatusPaused))

	r := retry.StartWithCtx(ctx, retry.Options{
		InitialBackoff: 100 * time.Millisecond,
		Multiplier:     1.5,
	})
	jobsToWaitFor := make([]jobspb.JobID, 0)
	for r.Next() {
		waitForSomeJobs := false
		var rows []tree.Datums
		var newJobsToWaitFor []jobspb.JobID
		// Detect if any descriptors have deprecated elements
		err := d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
			newJobsToWaitFor = newJobsToWaitFor[:]
			var err error
			descs := txn.Descriptors()
			kvTxn := txn.KV()
			rows, err = d.InternalExecutor.QueryBufferedEx(ctx,
				"query-non-terminal-schema-changers",
				kvTxn, /* txn */
				sessiondata.NodeUserSessionDataOverride,
				initialJobListQuery)
			if err != nil {
				return err
			}
			jobsToWaitFor := make(map[jobspb.JobID]struct{})
			for _, row := range rows {
				jobsToWaitFor[jobspb.JobID(*row[0].(*tree.DInt))] = struct{}{}
			}
			ct, err := descs.GetAll(ctx, kvTxn)
			if err != nil {
				return err
			}
			return ct.ForEachDescriptor(func(desc catalog.Descriptor) error {
				if state := desc.GetDeclarativeSchemaChangerState(); state != nil {
					// Anything with an active job we will wait
					if _, found := jobsToWaitFor[state.JobID]; found {
						for _, elem := range state.Targets {
							if scpb.HasDeprecatedElements(clusterVersion, elem) {
								// This job is in progress we can wait for it
								// to be paused.
								waitForSomeJobs = true
								err := d.JobRegistry.PauseRequested(ctx, txn, state.JobID, "schema change migration")
								if err != nil {
									log.Warningf(ctx, "failed to pause job: %v", err)
								} else {
									newJobsToWaitFor = append(newJobsToWaitFor, state.JobID)
								}
								return nil
							}
						}
					} else {
						// Otherwise, migrate the state for the paused job.
						mutDesc, err := descs.MutableByID(kvTxn).Desc(ctx, desc.GetID())
						if err != nil {
							return err
						}
						scpb.MigrateDescriptorState(clusterVersion, mutDesc.GetParentID(), mutDesc.GetDeclarativeSchemaChangerState())
						err = descs.WriteDesc(ctx, false, mutDesc, kvTxn)
						if err != nil {
							return err
						}
					}
				}
				return nil
			})
		})
		if err != nil {
			return err
		}
		jobsToWaitFor = append(jobsToWaitFor, newJobsToWaitFor...)

		// Wait for all jobs using the old version.
		if waitForSomeJobs {
			err := d.JobRegistry.WaitForJobsIgnoringJobErrors(ctx, newJobsToWaitFor)
			if err != nil {
				return err
			}
			continue
		} else {
			// Retry until no jobs with the old state are left.
			break
		}
	}
	// Resume any jobs we paused earlier.
	for _, job := range jobsToWaitFor {
		err := d.JobRegistry.Unpause(ctx, nil, job)
		if err != nil {
			log.Warningf(ctx, "unable to unpause job: %v", err)
		}
	}
	return nil
}
