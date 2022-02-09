// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ttljob

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var (
	defaultSelectBatchSize = settings.RegisterIntSetting(
		settings.TenantWritable,
		"sql.ttl.default_select_batch_size",
		"default amount of rows to select in a single query during a TTL job",
		500,
		settings.PositiveInt,
	).WithPublic()
	defaultDeleteBatchSize = settings.RegisterIntSetting(
		settings.TenantWritable,
		"sql.ttl.default_delete_batch_size",
		"default amount of rows to delete in a single query during a TTL job",
		100,
		settings.PositiveInt,
	).WithPublic()
)

type rowLevelTTLResumer struct {
	job *jobs.Job
	st  *cluster.Settings
}

var _ jobs.Resumer = (*rowLevelTTLResumer)(nil)

// Resume implements the jobs.Resumer interface.
func (t rowLevelTTLResumer) Resume(ctx context.Context, execCtx interface{}) error {
	jobExecCtx := execCtx.(sql.JobExecContext)
	ie := jobExecCtx.ExecCfg().InternalExecutor
	db := jobExecCtx.ExecCfg().DB
	var knobs sql.TTLTestingKnobs
	if ttlKnobs := jobExecCtx.ExecCfg().TTLTestingKnobs; ttlKnobs != nil {
		knobs = *ttlKnobs
	}

	details := t.job.Details().(jobspb.RowLevelTTLDetails)

	aostDuration := -time.Second * 30
	if knobs.AOSTDuration != nil {
		aostDuration = *knobs.AOSTDuration
	}
	aostTimestamp, err := tree.MakeDTimestampTZ(timeutil.Now().Add(aostDuration), time.Microsecond)
	if err != nil {
		return err
	}
	aostClause := fmt.Sprintf("AS OF SYSTEM TIME %s", aostTimestamp.String())

	// TODO(#75428): feature flag check, ttl pause check.
	// TODO(#75428): detect if the table has a schema change, in particular,
	// a PK change, a DROP TTL or a DROP TABLE should early exit the job.
	var ttlSettings descpb.TableDescriptor_RowLevelTTL
	var pkColumns []string
	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		desc, err := jobExecCtx.ExtendedEvalContext().Descs.GetImmutableTableByID(
			ctx,
			txn,
			details.TableID,
			tree.ObjectLookupFlagsWithRequired(),
		)
		if err != nil {
			return err
		}
		pkColumns = desc.GetPrimaryIndex().IndexDesc().KeyColumnNames

		ttl := desc.GetRowLevelTTL()
		if ttl == nil {
			return errors.Newf("unable to find TTL on table %s", desc.GetName())
		}
		ttlSettings = *ttl
		return nil
	}); err != nil {
		return err
	}

	columnNamesSQL := makeColumnNamesSQLFromColumns(pkColumns)
	// lastRowPK stores the last PRIMARY KEY that was seen.
	var lastRowPK []interface{}

	selectBatchSize := getSelectBatchSize(jobExecCtx.ExecCfg().SV(), ttlSettings)
	deleteBatchSize := getDeleteBatchSize(jobExecCtx.ExecCfg().SV(), ttlSettings)

	// TODO(#75428): break this apart by ranges to avoid multi-range operations.
	// TODO(#75428): add concurrency.
	// TODO(#75428): avoid regenerating strings for placeholders.
	// TODO(#75428): look at using a dist sql flow job
	for {
		// Step 1. Fetch some rows we want to delete using a historical
		// SELECT query.

		var filterClause string
		var rows sqlutil.InternalRows
		if len(lastRowPK) > 0 {
			// Generate (pk_col_1, pk_col_2, ...) > ($2, $3, ...), reserving
			// $1 for the now clause.
			filterClause = fmt.Sprintf("AND (%s) > (", columnNamesSQL)
			for i := range pkColumns {
				if i > 0 {
					filterClause += ", "
				}
				filterClause += fmt.Sprintf("$%d", i+2)
			}
			filterClause += ")"
		}

		if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			selectQuery := fmt.Sprintf(
				`SELECT %[1]s FROM [%[2]d AS tbl_name]
					%[3]s
					WHERE crdb_internal_expiration <= $1 %[4]s
					ORDER BY %[1]s
					LIMIT %[5]d
				`,
				columnNamesSQL,
				details.TableID,
				aostClause,
				filterClause,
				selectBatchSize,
			)
			args := append(
				[]interface{}{details.Cutoff},
				lastRowPK...,
			)
			var err error
			rows, err = ie.QueryIterator(
				ctx,
				"ttl_scanner",
				txn,
				selectQuery,
				args...,
			)
			return err
		}); err != nil {
			return errors.Wrapf(err, "error selecting rows to delete")
		}

		// Step 2. Delete the rows which have expired. We read through the iterator
		// until we have our deleteBatchSize, then run the delete.

		// Flatten the datums in deleteBatch and generate the placeholder string.
		// The result is (for a 2 column PK) something like:
		//   placeholderStr: ($2, $3), ($4, $5), ...
		//   deleteArgs: {cutoff, row1col1, row1col2, row2col1, row2col2, ...}
		// where we save $1 for crdb_internal_expiration < $1
		placeholderStr := ""
		deleteArgs := make([]interface{}, 0, len(pkColumns)*deleteBatchSize+1)
		deleteArgs = append(deleteArgs, details.Cutoff)
		numSelectedRows := 0
		numRowsForDelete := 0

		runDelete := func() error {
			if numRowsForDelete == 0 {
				return nil
			}

			if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				// TODO(#75428): configure admission priority
				deleteQuery := fmt.Sprintf(
					`DELETE FROM [%d AS tbl_name] WHERE crdb_internal_expiration <= $1 AND (%s) IN (%s)`,
					details.TableID,
					columnNamesSQL,
					placeholderStr,
				)

				_, err := ie.Exec(
					ctx,
					"ttl_delete",
					txn,
					deleteQuery,
					deleteArgs...,
				)
				if err != nil {
					return err
				}

				// Reset the args and string.
				deleteArgs = deleteArgs[:1]
				placeholderStr = ""
				numRowsForDelete = 0
				return err
			}); err != nil {
				return errors.Wrapf(err, "error during row deletion")
			}
			return nil
		}

		// Keep track of the last row's datums - we need this as our cursor
		// for the next SELECT batch.
		var lastRow tree.Datums
		for {
			hasNext, err := func() (bool, error) {
				next, err := rows.Next(ctx)
				if err != nil {
					return false, err
				}
				if !next {
					return false, nil
				}

				if len(deleteArgs) > 1 {
					placeholderStr += ", "
				}
				lastRow = rows.Cur()
				if len(pkColumns) != len(lastRow) {
					return false, errors.Newf(
						"number of pk columns (%d) != number of rows (%d)",
						len(pkColumns),
						len(lastRow),
					)
				}
				placeholderStr += "("
				for j := 0; j < len(pkColumns); j++ {
					if j > 0 {
						placeholderStr += ", "
					}
					placeholderStr += fmt.Sprintf("$%d", 2+numRowsForDelete*len(pkColumns)+j)
					deleteArgs = append(deleteArgs, lastRow[j])
				}
				placeholderStr += ")"

				numSelectedRows++
				numRowsForDelete++

				// If we are up to our delete batch size, run the delete.
				if numRowsForDelete == deleteBatchSize {
					if err := runDelete(); err != nil {
						return false, err
					}
				}
				return true, nil
			}()
			if err != nil {
				return errors.CombineErrors(err, rows.Close())
			}
			if !hasNext {
				if err := rows.Close(); err != nil {
					return err
				}
				break
			}
		}

		// Flush out the remaining batch.
		if err := runDelete(); err != nil {
			return err
		}

		// Step 3. Early exit if necessary. Otherwise, populate the lastRowPK so we
		// can start from this point in the next select batch.

		// If we selected less than the select batch size, we have selected every
		// row.
		if numSelectedRows < selectBatchSize {
			break
		}

		if lastRowPK == nil {
			lastRowPK = make([]interface{}, len(pkColumns))
		}
		for i := 0; i < len(pkColumns); i++ {
			lastRowPK[i] = lastRow[i]
		}
	}

	return nil
}

func getSelectBatchSize(sv *settings.Values, ttl descpb.TableDescriptor_RowLevelTTL) int {
	if bs := ttl.SelectBatchSize; bs != 0 {
		return int(bs)
	}
	return int(defaultSelectBatchSize.Get(sv))
}

func getDeleteBatchSize(sv *settings.Values, ttl descpb.TableDescriptor_RowLevelTTL) int {
	if bs := ttl.DeleteBatchSize; bs != 0 {
		return int(bs)
	}
	return int(defaultDeleteBatchSize.Get(sv))
}

// makeColumnNamesSQLFromColumns converts primary key columns into an escape string
// for an order by clause, e.g.:
//   {"a", "b"} => a, b
//   {"escape-me", "b"} => "escape-me", b
func makeColumnNamesSQLFromColumns(pkColumns []string) string {
	var b bytes.Buffer
	for i, pkColumn := range pkColumns {
		if i > 0 {
			b.WriteString(", ")
		}
		lexbase.EncodeRestrictedSQLIdent(&b, pkColumn, lexbase.EncNoFlags)
	}
	return b.String()
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (t rowLevelTTLResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	return nil
}

func init() {
	jobs.RegisterConstructor(jobspb.TypeRowLevelTTL, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &rowLevelTTLResumer{
			job: job,
			st:  settings,
		}
	})
}
