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

const addUserColumnStmt = `
ALTER TABLE system.jobs
ADD COLUMN IF NOT EXISTS username STRING
FAMILY "fam_0_id_status_created_payload"
`

const backfillUserColumnStmt = `
UPDATE system.jobs
SET username = crdb_internal.job_payload_user(payload)
WHERE username IS NULL
`

func alterSystemJobsAddUserColumn(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	op := operation{
		name:           "add-jobs-user-col",
		schemaList:     []string{"user"},
		query:          addUserColumnStmt,
		schemaExistsFn: hasColumn,
	}

	if err := migrateTable(ctx, cs, d, op, keys.JobsTableID, systemschema.JobsTable); err != nil {
		return err
	}

	return nil
}

func backfillJobsUserColumn(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	ie := d.InternalExecutorFactory.MakeInternalExecutorWithoutTxn()
	_, err := ie.Exec(ctx, "backfill-jobs-user-column", nil /* txn */, backfillUserColumnStmt, username.RootUser)
	if err != nil {
		return err
	}
	return nil
}
