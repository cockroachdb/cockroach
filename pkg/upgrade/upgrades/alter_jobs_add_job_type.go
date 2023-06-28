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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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
WITH updated AS (
	UPDATE system.jobs
	SET job_type = crdb_internal.job_payload_type(payload)
	WHERE id IN (
		SELECT id FROM system.jobs
		WHERE jobs.id > $1 AND job_type IS NULL
		ORDER BY jobs.id ASC
		LIMIT $2
	)
	RETURNING id
)
SELECT id FROM updated ORDER BY id DESC LIMIT 1
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
	resumeAfter := 0
	for batch, done := 0, false; !done; batch++ {
		if err := d.DB.KV().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			last, err := d.InternalExecutor.QueryBufferedEx(
				ctx,
				fmt.Sprintf("backfill-jobs-type-column-batch-%d", batch),
				txn,
				sessiondata.NodeUserSessionDataOverride,
				backfillTypeColumnStmt,
				resumeAfter,
				upgrade.JobsBackfillBatchSize,
			)

			if err != nil {
				return errors.Wrap(err, "failed to backfill system jobs type column")
			}
			if len(last) == 1 && len(last[0]) == 1 && last[0][0] != tree.DNull {
				resumeAfter = int(tree.MustBeDInt(last[0][0]))
			} else {
				done = true
			}
			log.Infof(ctx, "backfilling system.jobs job_type column, batch%d done; resume after %d, done %v", batch, resumeAfter, done)
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}
