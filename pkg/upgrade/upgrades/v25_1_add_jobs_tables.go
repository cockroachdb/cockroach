// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	totalRowsRow, err := d.DB.Executor().QueryRowEx(ctx, "jobs-backfill-count", nil, sessiondata.NodeUserSessionDataOverride,
		`SELECT count(*) FROM system.jobs WHERE owner IS NULL`)
	if err != nil {
		return err
	}
	totalRows := int(tree.MustBeDInt(totalRowsRow[0]))
	if totalRows == 0 {
		return nil
	}

	every := log.Every(time.Minute)
	log.Infof(ctx, "backfilling new jobs tables and columns for %d jobs", totalRows)
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
		for _, candidate := range candidateRows {
			id := int(tree.MustBeDInt(candidate[0]))
			var backfilled bool
			if err := d.DB.DescsTxn(ctx, func(ctx context.Context, tx descs.Txn) (retErr error) {
				backfilled = false
				// re-read the job in the txn, acquiring locks this time.
				found, err := tx.QueryRowEx(ctx, "jobs-backfill-lock-job", tx.KV(), sessiondata.NodeUserSessionDataOverride,
					`SELECT id FROM system.jobs WHERE id = $1 AND owner IS NULL FOR UPDATE`, id)
				if err != nil {
					return err
				}
				// The job isn't here anymore and needing backfill; move on.
				if found == nil {
					return nil
				}

				backfilled = true

				// Lock the job infos to prevent concurrent modifications there as well.
				_, err = tx.QueryBufferedEx(ctx, "jobs-backfill-lock-info", tx.KV(), sessiondata.NodeUserSessionDataOverride,
					`SELECT job_id FROM system.job_info WHERE job_id = $1 FOR UPDATE`, id)
				if err != nil {
					return err
				}

				// Materialize the job details for the (now locked) job row.
				row, err := tx.QueryRowEx(ctx, "jobs-backfill-read", tx.KV(), sessiondata.NodeUserSessionDataOverride,
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
					WHERE job_id = $1`, id,
				)
				if err != nil {
					return err
				}
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
				return nil
			}); err != nil {
				return err
			}
			if backfilled {
				jobsBackfilled++
			}
			if every.ShouldLog() {
				log.Infof(ctx, "backfilled new columns for %d of %d jobs so far", jobsBackfilled, totalRows)
				if jobID := d.OptionalJobID; jobID != 0 {
					frac := float32(jobsBackfilled) / float32(totalRows)
					if err := d.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
						// It would be nice to just ProgressStorage(jobID).Set() here, but
						// the new table is not used by reads until this backfill is marked
						// as complete, so we still need to use the old LoadJob/WithTxn API.
						j, err := d.JobRegistry.LoadClaimedJob(ctx, jobID)
						if err != nil {
							return err
						}
						return j.WithTxn(txn).FractionProgressed(ctx, jobs.FractionUpdater(frac))
					}); err != nil {
						log.Warningf(ctx, "failed to update progress for job %d: %v", jobID, err)
					}
				}
			}
		}
	}
	log.Infof(ctx, "finished backfilling new jobs tables and columns for %d jobs", jobsBackfilled)
	return nil
}
