// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// JobsBackfillBatchSize_22_2_20 is used to batch writes in the below migration.
// Batching writes across multiple transactions prevents this upgrade
// from failing continuously due to contention on system.jobs.
var JobsBackfillBatchSize_22_2_20 = envutil.EnvOrDefaultInt("COCKROACH_UPGRADE_22_2_20_BACKFILL_BATCH", 100)

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
WITH ids AS (
	SELECT id FROM system.jobs
	WHERE id > $1 AND job_type is NULL
	ORDER BY id ASC
	LIMIT $2
), updated AS (
	UPDATE system.jobs
	SET job_type = crdb_internal.job_payload_type(payload)
	WHERE id IN (SELECT id from ids)
	RETURNING id
) SELECT max(id) FROM updated
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
	var lastAdded int
	for batch, done := 0, false; !done; batch++ {
		if err := d.DB.KV().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			last, err := d.InternalExecutor.QueryBufferedEx(
				ctx,
				fmt.Sprintf("backfill-jobs-type-column-batch-%d", batch),
				txn,
				sessiondata.NodeUserSessionDataOverride,
				backfillTypeColumnStmt,
				resumeAfter,
				JobsBackfillBatchSize_22_2_20,
			)
			if err != nil {
				return errors.Wrap(err, "failed to backfill system jobs type column")
			}
			if len(last) == 1 && len(last[0]) == 1 && last[0][0] != tree.DNull {
				lastAdded = int(tree.MustBeDInt(last[0][0]))
			} else {
				done = true
			}
			log.Infof(ctx, "backfilling system.jobs job_type column, batch%d done; resume after %d, done %v", batch, resumeAfter, done)
			return nil
		}); err != nil {
			return err
		}
		resumeAfter = lastAdded
	}
	return nil
}
