// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// waitForDelRangeInGCJob ensures that any running GC jobs have adopted the new
// DeleteRange protocol.
func waitForDelRangeInGCJob(
	ctx context.Context, _ clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	for r := retry.StartWithCtx(ctx, retry.Options{}); r.Next(); {
		if !storage.CanUseMVCCRangeTombstones(ctx, deps.Settings) {
			return nil
		}
		jobIDs, err := collectJobIDsFromQuery(
			ctx, deps.InternalExecutor, "wait-for-gc-job-upgrades", `
        WITH jobs AS (
                SELECT *
                  FROM (
                        SELECT id,
                               crdb_internal.pb_to_json('payload', payload) AS pl,
                               crdb_internal.pb_to_json('progress', progress) AS progress,
                               `+jobs.NextRunClause+` AS next_run,
                               args.max_delay as max_delay
                          FROM system.jobs,
                               (
                                SELECT (
                                        SELECT value
                                          FROM crdb_internal.cluster_settings
                                         WHERE variable = 'jobs.registry.retry.initial_delay'
                                       )::INTERVAL::FLOAT8 AS initial_delay,
                                       (
                                        SELECT value
                                          FROM crdb_internal.cluster_settings
                                         WHERE variable = 'jobs.registry.retry.max_delay'
                                       )::INTERVAL::FLOAT8 AS max_delay
                               ) AS args
                         WHERE status IN `+jobs.NonTerminalStatusTupleString+`
                       )
                 WHERE (pl->'schemaChangeGC') IS NOT NULL
                   AND (
                        next_run < (now() + '5m'::INTERVAL)
                        OR max_delay < '5m'::INTERVAL::FLOAT8
                   )
              ),
         tables AS (
                    SELECT id,
                           json_array_elements(pl->'schemaChangeGC'->'tables') AS pl,
                           json_array_elements(progress->'schemaChangeGC'->'tables') AS progress
                      FROM jobs
                ),
         indexes AS (
                    SELECT id,
                           json_array_elements(pl->'schemaChangeGC'->'indexes'),
                           json_array_elements(progress->'schemaChangeGC'->'indexes')
                      FROM jobs
                 ),
         elements AS (SELECT * FROM tables UNION ALL SELECT * FROM indexes)
  SELECT id
    FROM elements
-- While we are waiting for the GC TTL, the status will be WAITING_FOR_CLEAR because omitempty
-- set on this field it will not exist in the JSON output. Because tombstone adoption unconditionally
-- enabled by an earlier version, we should be safe to skip any job that hasn't started GCing yet.
   WHERE COALESCE(progress->>'status' NOT IN ('WAITING_FOR_MVCC_GC', 'CLEARED'), false)
GROUP BY id;
`)
		if err != nil || len(jobIDs) == 0 {
			return err
		}
		log.Infof(ctx, "waiting for %d GC jobs to adopt the new protocol: %v", len(jobIDs), jobIDs)
	}
	return ctx.Err()
}

// If there are any paused GC jobs, they are going to make this migration
// take forever, so we should detect them and give the user the opportunity
// to unpause them with a clear error.
func checkForPausedGCJobs(
	ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) (retErr error) {
	jobIDs, err := collectJobIDsFromQuery(
		ctx, deps.InternalExecutor, "check-for-paused-gc-jobs", `
SELECT job_id
  FROM crdb_internal.jobs
 WHERE job_type = 'SCHEMA CHANGE GC'
   AND status IN ('paused', 'pause-requested')`)
	if err != nil {
		return err
	}
	if len(jobIDs) > 0 {
		return errors.WithHint(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"paused GC jobs prevent upgrading GC job behavior: %v", jobIDs),
			"unpause the jobs to allow the upgrade to proceed")
	}
	return nil
}

// collectJobIDsFromQuery is a helper to execute a query which returns rows
// where the first column is a jobID and returns the job IDs from those rows.
func collectJobIDsFromQuery(
	ctx context.Context, ie isql.Executor, opName string, query string,
) (jobIDs []jobspb.JobID, retErr error) {
	it, err := ie.QueryIteratorEx(ctx, opName, nil, /* txn */
		sessiondata.NodeUserSessionDataOverride, query)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.CombineErrors(retErr, it.Close()) }()
	for ok, err := it.Next(ctx); ok && err == nil; ok, err = it.Next(ctx) {
		jobIDs = append(jobIDs, jobspb.JobID(tree.MustBeDInt(it.Cur()[0])))
	}
	return jobIDs, err
}
