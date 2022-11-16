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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

const addTypeColumnStmt = `
ALTER TABLE system.jobs
ADD COLUMN IF NOT EXISTS job_type STRING
FAMILY "fam_0_id_status_created_payload"
`

const addTypeColumnIdxStmt = `
CREATE INDEX IF NOT EXISTS jobs_job_type_idx
ON system.jobs (job_type)
`

const backfillTypeColumnStmt = `
UPDATE system.jobs
SET job_type = crdb_internal.job_payload_type(payload)
WHERE job_type IS NULL
`

func alterSystemJobsAddJobType(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	schemaChangeOps := []operation{
		{
			name:           "add-jobs-type-col",
			schemaList:     []string{"type"},
			query:          addTypeColumnStmt,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "add-jobs-type-col-idx",
			schemaList:     []string{"jobs_job_type_idx"},
			query:          addTypeColumnIdxStmt,
			schemaExistsFn: hasIndex,
		},
	}

	for _, op := range schemaChangeOps {
		if err := migrateTable(ctx, cs, d, op, keys.JobsTableID, systemschema.JobsTable); err != nil {
			return err
		}
	}

	return nil
}

func backfillJobTypeColumn(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	ie := d.InternalExecutor
	_, err := ie.Exec(ctx, "backfill-jobs-type-column", nil /* txn */, backfillTypeColumnStmt, username.RootUser)
	if err != nil {
		return err
	}
	return nil
}
