// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/errors"
)

// waitForAllSchemaChanges waits for all schema changes to enter a
// terminal or paused state.
//
// Because this is intended for the mvcc-bulk-ops transition, it does
// not care about schema changes created while this migration is
// running because any such schema changes must already be using the
// new mvcc bulk operations
//
// Note that we do not use SHOW JOBS WHEN COMPLETE here to avoid
// blocking forever on PAUSED jobs. Jobs using old index backfills
// will fail on Resume.
func waitForAllSchemaChanges(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {

	initialJobListQuery := fmt.Sprintf(`

SELECT
    job_id
FROM
    [SHOW JOBS]
WHERE
    job_type = 'SCHEMA CHANGE'
    AND status NOT IN ('%s', '%s', '%s', '%s', '%s')
`,
		string(jobs.StatusSucceeded),
		string(jobs.StatusFailed),
		string(jobs.StatusCanceled),
		string(jobs.StatusRevertFailed),
		string(jobs.StatusPaused))
	rows, err := d.InternalExecutor.QueryBufferedEx(ctx,
		"query-non-terminal-schema-changers",
		nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		initialJobListQuery)
	if err != nil {
		return err
	}

	jobList := make([]jobspb.JobID, len(rows))
	for i, datums := range rows {
		if len(datums) != 1 {
			return errors.AssertionFailedf("unexpected number of columns: %d (expected 1)", len(datums))
		}
		d := datums[0]
		id, ok := d.(*tree.DInt)
		if !ok {
			return errors.AssertionFailedf("unexpected type for id column: %T (expected DInt)", d)
		}
		jobList[i] = jobspb.JobID(*id)
	}
	return d.JobRegistry.WaitForJobsIgnoringJobErrors(ctx, jobList)
}
