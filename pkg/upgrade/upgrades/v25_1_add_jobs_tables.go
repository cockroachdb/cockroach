// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// addJobsTables adds the job_progress, job_progress_history, job_status and
// job_message tables.
func addJobsTables(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	return d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		if err := createSystemTable(
			ctx, d.DB, d.Settings, d.Codec,
			systemschema.SystemJobProgressTable,
			tree.LocalityLevelTable,
		); err != nil {
			return err
		}

		if err := createSystemTable(
			ctx, d.DB, d.Settings, d.Codec,
			systemschema.SystemJobProgressHistoryTable,
			tree.LocalityLevelTable,
		); err != nil {
			return err
		}

		if err := createSystemTable(
			ctx, d.DB, d.Settings, d.Codec,
			systemschema.SystemJobStatusTable,
			tree.LocalityLevelTable,
		); err != nil {
			return err
		}

		if err := createSystemTable(
			ctx, d.DB, d.Settings, d.Codec,
			systemschema.SystemJobMessageTable,
			tree.LocalityLevelTable,
		); err != nil {
			return err
		}

		return nil
	})
}

// addJobsColumns adds new columns to system.jobs.
func addJobsColumns(
	ctx context.Context, cv clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	return migrateTable(ctx, cv, d, operation{
		name:           "add-new-jobs-columns",
		schemaList:     []string{"owner", "description", "error_msg", "finished"},
		schemaExistsFn: columnExists,
		query: `ALTER TABLE system.jobs
			ADD COLUMN IF NOT EXISTS owner STRING NULL FAMILY "fam_0_id_status_created_payload",
			ADD COLUMN IF NOT EXISTS description STRING NULL FAMILY "fam_0_id_status_created_payload",
			ADD COLUMN IF NOT EXISTS error_msg STRING NULL FAMILY "fam_0_id_status_created_payload",
			ADD COLUMN IF NOT EXISTS finished TIMESTAMPTZ NULL FAMILY "fam_0_id_status_created_payload"
			`,
	},
		keys.JobsTableID,
		systemschema.JobsTable)
}

func TestingSetJobsBackfillPageSize(size int) {
	jobsBackfillPageSize = size
}

var jobsBackfillPageSize = 32

// backfills the new jobs tables and columns
func backfillJobsTablesAndColumns(
	ctx context.Context, cv clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	log.Infof(ctx, "backfilling new jobs tables and columns")
	jobsBackfilled := 0
	for {
		candidateRows, err := d.DB.Executor().QueryBufferedEx(ctx, "jobs-backfill-find", nil, sessiondata.NodeUserSessionDataOverride,
			`SELECT id FROM system.jobs WHERE owner IS NULL LIMIT $1`, jobsBackfillPageSize)
		if err != nil {
			return err
		}
		if len(candidateRows) == 0 {
			// All done!
			break
		}

		candidates := make([]int, len(candidateRows))
		for i := range candidateRows {
			candidates[i] = int(tree.MustBeDInt(candidateRows[i][0]))
		}
		var done int
		if err := d.DB.DescsTxn(ctx, func(ctx context.Context, tx descs.Txn) (retErr error) {
			// re-read the jobs in the txn, acquiring locks this time.
			todoRows, err := tx.QueryBufferedEx(ctx, "jobs-backfill-lock-job", tx.KV(), sessiondata.NodeUserSessionDataOverride,
				`SELECT id FROM system.jobs WHERE id = ANY($1) AND owner IS NULL FOR UPDATE`, candidates)
			if err != nil {
				return err
			}

			// No candidates are remained eligible as of txn; move on.
			if len(todoRows) == 0 {
				return nil
			}

			todo := candidates[:0]
			for i := range todoRows {
				todo = append(todo, int(tree.MustBeDInt(todoRows[i][0])))
			}
			// Lock the job infos to prevent concurrent modifications there as well.
			_, err = tx.QueryBufferedEx(ctx, "jobs-backfill-lock-info", tx.KV(), sessiondata.NodeUserSessionDataOverride,
				`SELECT job_id FROM system.job_info WHERE job_id = ANY($1) FOR UPDATE`, todo)
			if err != nil {
				return err
			}

			// Materialize the job details for the (now locked) todo jobs.
			q, err := tx.QueryIteratorEx(ctx, "jobs-backfill-read", tx.KV(), sessiondata.NodeUserSessionDataOverride,
				`SELECT
				job_id,
				description,
				user_name,
				finished,
				error,
				running_status,
				fraction_completed,
				high_water_timestamp
				FROM crdb_internal.jobs
				WHERE job_id = ANY($1)`, todo,
			)
			if err != nil {
				return err
			}
			defer func() {
				retErr = errors.CombineErrors(retErr, q.Close())
			}()

			done = 0
			for {
				ok, err := q.Next(ctx)
				if err != nil {
					return err
				}
				if !ok {
					break
				}

				row := q.Cur()

				// Update the job row.
				if _, err := tx.ExecEx(ctx, "jobs-backfill-jobs", tx.KV(),
					sessiondata.NodeUserSessionDataOverride,
					`UPDATE system.jobs
					SET description = $1,
					owner = $2,
					finished = $3,
					error_msg = NULLIF($4, '')
					WHERE id = $5`, row[1], row[2], row[3], row[4], row[0],
				); err != nil {
					return err
				}
				// If we see a running_status, we need to update the status.
				if row[5] != tree.DNull {
					if err := jobs.StatusStorage(tree.MustBeDInt(row[0])).Set(ctx, tx, string(tree.MustBeDString(row[5]))); err != nil {
						return err
					}
				}
				// If we see a fraction_completed or high_water_timestamp, we need to
				// update the progress.
				if row[6] != tree.DNull || row[7] != tree.DNull {
					var frac float64
					var ts hlc.Timestamp
					if row[6] != tree.DNull {
						frac = float64(tree.MustBeDFloat(row[6]))
					}
					if row[7] != tree.DNull {
						d := tree.MustBeDDecimal(row[7]).Decimal
						ts, err = hlc.DecimalToHLC(&d)
						if err != nil {
							return err
						}
					}
					if err := jobs.ProgressStorage(tree.MustBeDInt(row[0])).Set(ctx, tx, frac, ts); err != nil {
						return err
					}
				}
				done++
			}
			return nil
		}); err != nil {
			return err
		}

		jobsBackfilled += done
	}
	log.Infof(ctx, "finished backfilling new jobs tables and columns for %d jobs", jobsBackfilled)
	return nil
}
