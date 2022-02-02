// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/ttlpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

type rowLevelTTLResumer struct {
	job *jobs.Job
	st  *cluster.Settings
}

var _ jobs.Resumer = (*rowLevelTTLResumer)(nil)

// Resume implements the jobs.Resumer interface.
func (t rowLevelTTLResumer) Resume(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(JobExecContext)
	ie := p.ExecCfg().InternalExecutor
	db := p.ExecCfg().DB
	var knobs TTLTestingKnobs
	if ttlKnobs := p.ExecCfg().TTLTestingKnobs; ttlKnobs != nil {
		knobs = *ttlKnobs
	}

	details := t.job.Details().(jobspb.RowLevelTTLDetails)

	// TODO(#75428): feature flag check, ttl pause check.
	// TODO(#75428): detect if the table has a schema change, in particular,
	// a PK change, a DROP TTL or a DROP TABLE should early exit the job.
	var pkColumns []string
	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		desc, err := p.ExtendedEvalContext().Descs.GetImmutableTableByID(
			ctx,
			txn,
			details.TableID,
			tree.ObjectLookupFlagsWithRequired(),
		)
		if err != nil {
			return err
		}
		pkColumns = desc.GetPrimaryIndex().IndexDesc().KeyColumnNames
		return nil
	}); err != nil {
		return err
	}

	selectClause := makeSelectClauseFromColumns(pkColumns)
	// lastRowPK stores the last PRIMARY KEY that was seen.
	var lastRowPK []interface{}

	const (
		selectBatchSize = 500
		deleteBatchSize = 100
	)

	// TODO(#75428): break this apart by ranges to avoid multi-range operations.
	// TODO(#75428): add concurrency.
	for {
		// Step 1. Fetch some rows we want to delete using a historical
		// SELECT query.
		var expiredRowsPKs []tree.Datums

		var filterClause string
		if len(lastRowPK) > 0 {
			// Generate (pk_col_1, pk_col_2, ...) > ($2, $3, ...), reserving
			// $1 for the now clause.
			filterClause = fmt.Sprintf("AND (%s) > (", selectClause)
			for i := range pkColumns {
				if i > 0 {
					filterClause += ", "
				}
				filterClause += fmt.Sprintf("$%d", i+2)
			}
			filterClause += ")"
		}

		aostClause := "AS OF SYSTEM TIME '-30s'"
		if knobs.DisableAOSTClause {
			aostClause = ""
		}
		if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			// TODO(#75428): configure select_batch_size
			q := fmt.Sprintf(
				`SELECT %[1]s FROM [%[2]d AS tbl_name]
					%[3]s
					WHERE crdb_internal_expiration <= $1 %[4]s
					ORDER BY %[1]s
					LIMIT %[5]d
				`,
				selectClause,
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
			expiredRowsPKs, err = ie.QueryBuffered(
				ctx,
				"ttl",
				txn,
				q,
				args...,
			)
			return err
		}); err != nil {
			return errors.Wrapf(err, "error selecting rows to delete")
		}

		// Step 2. Delete the rows which have expired.

		// TODO(#75428): configure delete_batch_size
		for startRowIdx := 0; startRowIdx < len(expiredRowsPKs); startRowIdx += deleteBatchSize {
			until := startRowIdx + deleteBatchSize
			if until > len(expiredRowsPKs) {
				until = len(expiredRowsPKs)
			}
			deleteBatch := expiredRowsPKs[startRowIdx:until]

			// Flatten the datums in deleteBatch and generate the placeholder string.
			// The result is (for a 2 column PK) something like:
			//   placeholderStr: ($2, $3), ($4, $5), ...
			//   args: {cutoff, row1col1, row1col2, row2col1, row2col2, ...}
			// where we save $1 for crdb_internal_expiration < $1
			args := make([]interface{}, len(pkColumns)*len(deleteBatch)+1)
			args[0] = details.Cutoff
			placeholderStr := ""
			for i, row := range deleteBatch {
				if i > 0 {
					placeholderStr += ", "
				}
				placeholderStr += "("
				for j := 0; j < len(pkColumns); j++ {
					if j > 0 {
						placeholderStr += ", "
					}
					placeholderStr += fmt.Sprintf("$%d", 2+i*len(pkColumns)+j)
					args[i*len(pkColumns)+j+1] = row[j]
				}
				placeholderStr += ")"
			}

			if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				// TODO(#75428): configure admission priority

				q := fmt.Sprintf(
					`DELETE FROM [%d AS tbl_name] WHERE crdb_internal_expiration <= $1 AND (%s) IN (%s)`,
					details.TableID,
					selectClause,
					placeholderStr,
				)

				_, err := ie.Exec(
					ctx,
					"ttl_delete",
					txn,
					q,
					args...,
				)
				return err
			}); err != nil {
				return errors.Wrapf(err, "error during row deletion")
			}
		}

		// Step 3. Early exit if necessary. Otherwise, populate the lastRowPK so we
		// can start from this point in the next select batch.

		// If we selected less than the select batch size, we have selected every
		// row.
		if len(expiredRowsPKs) < selectBatchSize {
			break
		}

		if lastRowPK == nil {
			lastRowPK = make([]interface{}, len(pkColumns))
		}
		lastRowIdx := len(expiredRowsPKs) - 1
		for i := 0; i < len(pkColumns); i++ {
			lastRowPK[i] = expiredRowsPKs[lastRowIdx][i]
		}
	}

	return nil
}

// makeSelectClauseFromColumns converts primary key columns into an escape string
// for an order by clause, e.g.:
//   {"a", "b"} => a, b
//   {"escape-me", "b"} => "escape-me", b
func makeSelectClauseFromColumns(pkColumns []string) string {
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

// newRowLevelTTLScheduledJob returns a *jobs.ScheduledJob for row level TTL
// for a given table.
func newRowLevelTTLScheduledJob(
	env scheduledjobs.JobSchedulerEnv, owner security.SQLUsername, tblID descpb.ID,
) (*jobs.ScheduledJob, error) {
	sj := jobs.NewScheduledJob(env)
	sj.SetScheduleLabel(fmt.Sprintf("row-level-ttl-%d", tblID))
	sj.SetOwner(owner)
	sj.SetScheduleDetails(jobspb.ScheduleDetails{
		Wait: jobspb.ScheduleDetails_WAIT,
		// If a job fails, try again at the allocated cron time.
		OnError: jobspb.ScheduleDetails_RETRY_SCHED,
	})
	// TODO(#75189): allow user to configure schedule.
	if err := sj.SetSchedule("@hourly"); err != nil {
		return nil, err
	}
	args := &ttlpb.ScheduledRowLevelTTLArgs{
		TableID: tblID,
	}
	any, err := pbtypes.MarshalAny(args)
	if err != nil {
		return nil, err
	}
	sj.SetExecutionDetails(
		tree.ScheduledRowLevelTTLExecutor.InternalName(),
		jobspb.ExecutionArguments{Args: any},
	)
	return sj, nil
}

type rowLevelTTLExecutor struct {
	metrics rowLevelTTLMetrics
}

var _ jobs.ScheduledJobController = (*rowLevelTTLExecutor)(nil)

type rowLevelTTLMetrics struct {
	*jobs.ExecutorMetrics
}

var _ metric.Struct = &rowLevelTTLMetrics{}

// MetricStruct implements metric.Struct interface.
func (m *rowLevelTTLMetrics) MetricStruct() {}

// OnDrop implements the jobs.ScheduledJobController interface.
func (s rowLevelTTLExecutor) OnDrop(
	ctx context.Context,
	scheduleControllerEnv scheduledjobs.ScheduleControllerEnv,
	env scheduledjobs.JobSchedulerEnv,
	schedule *jobs.ScheduledJob,
	txn *kv.Txn,
) error {
	return errors.WithHint(
		pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"cannot drop row level TTL schedule",
		),
		`use ALTER TABLE ... RESET (expire_after) instead`,
	)
}

// ExecuteJob implements the jobs.ScheduledJobController interface.
func (s rowLevelTTLExecutor) ExecuteJob(
	ctx context.Context,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
	txn *kv.Txn,
) error {
	args := &ttlpb.ScheduledRowLevelTTLArgs{}
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return err
	}

	p, cleanup := cfg.PlanHookMaker(
		fmt.Sprintf("invoke-row-level-ttl-%d", args.TableID),
		txn,
		security.NodeUserName(),
	)
	defer cleanup()

	if _, err := createRowLevelTTLJob(
		ctx,
		&jobs.CreatedByInfo{
			ID:   sj.ScheduleID(),
			Name: jobs.CreatedByScheduledJobs,
		},
		txn,
		cfg.InternalExecutor,
		p.(*planner).ExecCfg().JobRegistry,
		*args,
	); err != nil {
		s.metrics.NumFailed.Inc(1)
		return err
	}
	s.metrics.NumStarted.Inc(1)
	return nil
}

// NotifyJobTermination implements the jobs.ScheduledJobController interface.
func (s rowLevelTTLExecutor) NotifyJobTermination(
	ctx context.Context,
	jobID jobspb.JobID,
	jobStatus jobs.Status,
	details jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
	ex sqlutil.InternalExecutor,
	txn *kv.Txn,
) error {
	if jobStatus == jobs.StatusFailed {
		jobs.DefaultHandleFailedRun(
			sj,
			"row level ttl for table [%d] job failed",
			details.(ttlpb.ScheduledRowLevelTTLArgs).TableID,
		)
		s.metrics.NumFailed.Inc(1)
		return nil
	}

	if jobStatus == jobs.StatusSucceeded {
		s.metrics.NumSucceeded.Inc(1)
	}

	sj.SetScheduleStatus(string(jobStatus))
	return nil
}

// Metrics implements the jobs.ScheduledJobController interface.
func (s rowLevelTTLExecutor) Metrics() metric.Struct {
	return &s.metrics
}

// GetCreateScheduleStatement implements the jobs.ScheduledJobController interface.
func (s rowLevelTTLExecutor) GetCreateScheduleStatement(
	ctx context.Context,
	env scheduledjobs.JobSchedulerEnv,
	txn *kv.Txn,
	schedule *jobs.ScheduledJob,
	ex sqlutil.InternalExecutor,
) (string, error) {
	args := &ttlpb.ScheduledRowLevelTTLArgs{}
	if err := pbtypes.UnmarshalAny(schedule.ExecutionArgs().Args, args); err != nil {
		return "", err
	}

	// TODO(#75428): consider using table name instead - we would need to pass in descCol from the planner.
	return fmt.Sprintf("ALTER TABLE [%d as T] WITH (expire_after = ...)", args.TableID), nil
}

func createRowLevelTTLJob(
	ctx context.Context,
	createdByInfo *jobs.CreatedByInfo,
	txn *kv.Txn,
	ie sqlutil.InternalExecutor,
	jobRegistry *jobs.Registry,
	ttlDetails ttlpb.ScheduledRowLevelTTLArgs,
) (jobspb.JobID, error) {
	record := jobs.Record{
		Description: "ttl",
		Username:    security.NodeUserName(),
		Details: jobspb.RowLevelTTLDetails{
			TableID: ttlDetails.TableID,
			Cutoff:  timeutil.Now(),
		},
		Progress:  jobspb.RowLevelTTLProgress{},
		CreatedBy: createdByInfo,
	}

	jobID := jobRegistry.MakeJobID()
	if _, err := jobRegistry.CreateAdoptableJobWithTxn(ctx, record, jobID, txn); err != nil {
		return jobspb.InvalidJobID, err
	}
	return jobID, nil
}

func init() {
	jobs.RegisterConstructor(jobspb.TypeRowLevelTTL, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &rowLevelTTLResumer{
			job: job,
			st:  settings,
		}
	})

	jobs.RegisterScheduledJobExecutorFactory(
		tree.ScheduledRowLevelTTLExecutor.InternalName(),
		func() (jobs.ScheduledJobExecutor, error) {
			m := jobs.MakeExecutorMetrics(tree.ScheduledRowLevelTTLExecutor.InternalName())
			return &rowLevelTTLExecutor{
				metrics: rowLevelTTLMetrics{
					ExecutorMetrics: &m,
				},
			}, nil
		},
	)
}
