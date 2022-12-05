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

// TODO should we put the different type for createStats?
const addTypeColumnStmt = `
ALTER table system.jobs
ADD COLUMN type STRING
FAMILY "fam_0_id_status_created_payload"
`

func alterSystemJobsAddJobType(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	op := operation{
		name:           "add-jobs-type-col",
		schemaList:     []string{"type"},
		query:          addTypeColumnStmt,
		schemaExistsFn: hasColumn,
	}
	if err := migrateTable(ctx, cs, d, op, keys.JobsTableID, systemschema.SQLInstancesTable()); err != nil {
		return err
	}

	return nil
}

const backfillTypeColumnStmt = `
UPDATE system.jobs
SET type = crdb_internal.payload_type(payload)
WHERE type IS NULL
`

func backfillSystemJobsTypeColumn(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {

	_, err := d.InternalExecutor.Exec(ctx, "backfill-jobs-type-column", nil /* txn */, backfillTypeColumnStmt, username.RootUser)
	if err != nil {
		return err
	}
	return nil
}
