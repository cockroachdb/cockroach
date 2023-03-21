// Copyright 2023 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

const addOwnerIDColumnToScheduledJobsTableStmt = `
ALTER TABLE system.scheduled_jobs
ADD COLUMN IF NOT EXISTS owner_id OID
FAMILY other
`

func alterScheduledJobsTableAddOwnerIDColumn(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	for _, op := range []operation{
		{
			name:           "add-owner-id-column-scheduled-jobs-table",
			schemaList:     []string{"owner_id"},
			query:          addOwnerIDColumnToScheduledJobsTableStmt,
			schemaExistsFn: columnExists,
		},
	} {
		if err := migrateTable(ctx, cs, d, op, keys.ScheduledJobsTableID, systemschema.ScheduledJobsTable); err != nil {
			return err
		}
	}

	return nil
}

const backfillOwnerIDColumnScheduledJobsTableStmt = `
UPDATE system.scheduled_jobs
SET owner_id = user_id
FROM system.users
WHERE owner_id IS NULL AND owner = username
LIMIT 1000
`

func backfillScheduledJobsTableOwnerIDColumn(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	ie := d.DB.Executor()
	for {
		rowsAffected, err := ie.ExecEx(ctx, "backfill-owner-id-col-scheduled-jobs-table", nil, /* txn */
			sessiondata.NodeUserSessionDataOverride,
			backfillOwnerIDColumnScheduledJobsTableStmt,
		)
		if err != nil {
			return err
		}
		if rowsAffected == 0 {
			break
		}
	}

	return nil
}
