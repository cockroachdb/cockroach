// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func retryJobsWithExponentialBackoff(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps,
) error {
	const query = `
BEGIN;
	ALTER TABLE IF EXISTS system.jobs
			ADD COLUMN IF NOT EXISTS num_runs INT8 FAMILY claim DEFAULT 0,
			ADD COLUMN IF NOT EXISTS last_run TIMESTAMP FAMILY claim DEFAULT now();
	
	CREATE INDEX IF NOT EXISTS jobs_status_claim
		ON system.jobs (claim_session_id, created, status)
		STORING (num_runs, last_run, claim_instance_id)
		WHERE
			status
			IN (
					'running',
					'reverting',
					'pending',
					'pause-requested',
					'cancel-requested'
				);
COMMIT;
`
	log.Info(ctx, "Adding last_run and num_runs columns to system.jobs and adding creating an index")
	if _, err := d.InternalExecutor.Exec(ctx, "alter-jobs-table", nil, query); err != nil {
		return err
	}
	return nil
}
