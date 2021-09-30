// Copyright 2021 The Cockroach Authors.
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
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/ttlpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

type ttlResumer struct {
	job *jobs.Job
	st  *cluster.Settings
}

var _ jobs.Resumer = (*ttlResumer)(nil)

func (t ttlResumer) Resume(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(JobExecContext)
	ie := p.ExecCfg().InternalExecutor
	db := p.ExecCfg().DB
	details := t.job.Details().(jobspb.TTLDetails)
	cn := tree.Name(details.ColumnName)
	var lastRows []interface{}
	var pks []string
	var pkStr string
	var pkRevStr string
	const batchSize = 1000

	metrics := p.ExecCfg().JobRegistry.MetricsStruct().TTL

	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		desc, err := p.ExtendedEvalContext().Descs.GetImmutableTableByID(ctx, txn, details.TableID, tree.ObjectLookupFlagsWithRequired())
		if err != nil {
			return err
		}
		pks = desc.GetPrimaryIndex().IndexDesc().KeyColumnNames
		pkStr = strings.Join(pks, ", ")
		//We have to delete backwards...
		for i, pk := range pks {
			if i > 0 {
				pkRevStr += ", "
			}
			pkRevStr += pk
			pkRevStr += " ASC"
		}
		return nil
	}); err != nil {
		return err
	}

	untilTS := timeutil.Unix(details.UntilUnix, 0)
	for {
		var rows []tree.Datums
		if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			startTime := timeutil.Now()
			defer func() {
				metrics.DeletionTotalNanos.RecordValue(timeutil.Now().Sub(startTime).Nanoseconds())
			}()

			// TODO(XXX): order by directions.
			// TODO(XXX): prevent full table scans?
			_, err := ie.Exec(ctx, "ttl-begin", txn, "SET TRANSACTION AS OF SYSTEM TIME follower_read_timestamp()")
			if err != nil {
				return err
			}

			// ((i = 10 AND j = 100 AND k > 1000)) OR (i = 10 AND j > 10) OR (i > 0))
			var filterClause string
			if len(lastRows) > 0 {
				filterClause += " AND ("
				for i := 0; i < len(lastRows); i++ {
					if i > 0 {
						filterClause += " OR "
					}
					filterClause += "("
					until := len(lastRows) - i
					for j := 0; j < until; j++ {
						if j > 0 {
							filterClause += " AND "
						}
						sign := "="
						if j == until-1 {
							sign = ">"
						}
						filterClause += fmt.Sprintf("%s %s $%d", pks[j], sign, j+2)
					}
					filterClause += ")"
				}
				filterClause += ")"
			}
			q := fmt.Sprintf(
				`SELECT %[1]s FROM [%[2]d AS t] WHERE %[3]s < $1%[5]s ORDER BY %[6]s LIMIT %[4]d`,
				pkStr,
				details.TableID,
				cn.String(),
				batchSize,
				filterClause,
				pkRevStr,
			)
			args := append(
				[]interface{}{untilTS},
				lastRows...,
			)
			//fmt.Printf("initial query:%s\nargs: %#v\n", q, args)
			selectStartTime := timeutil.Now()
			rows, err = ie.QueryBuffered(
				ctx,
				"ttl",
				txn,
				q,
				args...,
			)
			metrics.DeletionSelectNanos.RecordValue(timeutil.Now().Sub(selectStartTime).Nanoseconds())
			return err
		}); err != nil {
			return err
		}

		// If we have no rows found, we're done.
		if len(rows) == 0 {
			break
		}

		lastRowIdx := len(rows) - 1
		lastRows = make([]interface{}, len(pks))
		for i := 0; i < len(pks); i++ {
			lastRows[i] = rows[lastRowIdx][i]
		}

		// TODO(XXX): account for schema changes.
		if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			placeholderVals := make([]interface{}, len(pks)*len(rows))
			placeholderStr := ""
			for i, row := range rows {
				if i > 0 {
					placeholderStr += ", "
				}
				placeholderStr += "("
				for j := 0; j < len(pks); j++ {
					if j > 0 {
						placeholderStr += ", "
					}
					placeholderStr += fmt.Sprintf("$%d", 1+i*len(pks)+j)
					placeholderVals[i*len(pks)+j] = row[j]
				}
				placeholderStr += ")"
			}
			// TODO(XXX): we should probably do a secondary check here if we decide against strict TTL.
			q := fmt.Sprintf(`DELETE FROM [%d AS t] WHERE (%s) IN (%s)`, details.TableID, pkStr, placeholderStr)
			//	fmt.Printf("%s\n", q)
			deletionStartTime := timeutil.Now()
			if _, err := ie.Exec(
				ctx,
				"ttl_delete",
				txn,
				q,
				placeholderVals...,
			); err != nil {
				return err
			}
			metrics.DeletionDeleteNanos.RecordValue(timeutil.Now().Sub(deletionStartTime).Nanoseconds())
			metrics.RowDeletions.Inc(int64(len(rows)))
			return nil
		}); err != nil {
			return err
		}

		if len(rows) < batchSize {
			break
		}
	}
	return nil
}

func (t ttlResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	// TODO(XXX): what do we do here?
	return nil
}

// NewTTLScheduledJob XXX.
// TODO(XXX): should this be per table, or global?
func NewTTLScheduledJob(
	env scheduledjobs.JobSchedulerEnv, owner security.SQLUsername, tblID descpb.ID, ttlColumn string,
) (*jobs.ScheduledJob, error) {
	sj := jobs.NewScheduledJob(env)
	sj.SetScheduleLabel(fmt.Sprintf("TTL %d", tblID))
	sj.SetOwner(owner)
	sj.SetScheduleDetails(jobspb.ScheduleDetails{
		// TODO(XXX): is this what we want?
		Wait:    jobspb.ScheduleDetails_WAIT,
		OnError: jobspb.ScheduleDetails_RETRY_SCHED,
	})
	// Let's do minutely!
	// TODO(XXX): allow user to configure schedule.
	if err := sj.SetSchedule("* * * * *"); err != nil {
		return nil, err
	}
	args := &ttlpb.TTLDetails{
		TableID:   tblID,
		TTLColumn: ttlColumn,
	}
	any, err := pbtypes.MarshalAny(args)
	if err != nil {
		return nil, err
	}
	// TODO(XXX): when should the first run be?
	sj.SetNextRun(env.Now().Add(time.Second * 10))
	sj.SetExecutionDetails(
		tree.ScheduledTTLExecutor.InternalName(),
		jobspb.ExecutionArguments{Args: any},
	)
	return sj, nil
}

type scheduledTTLExecutor struct {
	metrics ttlMetrics
}

func (s scheduledTTLExecutor) OnDrop(
	ctx context.Context,
	scheduleControllerEnv scheduledjobs.ScheduleControllerEnv,
	env scheduledjobs.JobSchedulerEnv,
	schedule *jobs.ScheduledJob,
	txn *kv.Txn,
) error {
	return errors.Newf("XXX undroppable")
}

type ttlMetrics struct {
	*jobs.ExecutorMetrics
}

var _ metric.Struct = &ttlMetrics{}

// MetricStruct implements metric.Struct interface.
func (m *ttlMetrics) MetricStruct() {}

func (s scheduledTTLExecutor) ExecuteJob(
	ctx context.Context,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
	txn *kv.Txn,
) error {
	p, cleanup := cfg.PlanHookMaker("invoke-sql-stats-compact", txn, security.NodeUserName())
	defer cleanup()

	args := &ttlpb.TTLDetails{}
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return err
	}

	_, err := CreateTTLJob(
		ctx,
		&jobs.CreatedByInfo{
			ID:   sj.ScheduleID(),
			Name: jobs.CreatedByScheduledJobs,
		},
		txn,
		cfg.InternalExecutor,
		p.(*planner).ExecCfg().JobRegistry,
		*args,
	)
	return err
}

func CreateTTLJob(
	ctx context.Context,
	createdByInfo *jobs.CreatedByInfo,
	txn *kv.Txn,
	ie sqlutil.InternalExecutor,
	jobRegistry *jobs.Registry,
	ttlDetails ttlpb.TTLDetails,
) (jobspb.JobID, error) {
	// TODO(XXX): check existing job.
	/*
		if err := CheckExistingCompactionJob(ctx, nil, ie, txn); err != nil {
			return jobspb.InvalidJobID, err
		}
	*/
	record := jobs.Record{
		Description: "ttl",
		Username:    security.NodeUserName(),
		// TODO(XXX): unify details
		Details: jobspb.TTLDetails{
			TableID:    ttlDetails.TableID,
			ColumnName: ttlDetails.TTLColumn,
			UntilUnix:  timeutil.Now().Unix(),
		},
		Progress:  jobspb.TTLProgress{},
		CreatedBy: createdByInfo,
	}

	jobID := jobRegistry.MakeJobID()
	if _, err := jobRegistry.CreateAdoptableJobWithTxn(ctx, record, jobID, txn); err != nil {
		return jobspb.InvalidJobID, err
	}
	return jobID, nil
}

func (s scheduledTTLExecutor) NotifyJobTermination(
	ctx context.Context,
	jobID jobspb.JobID,
	jobStatus jobs.Status,
	details jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
	ex sqlutil.InternalExecutor,
	txn *kv.Txn,
) error {
	sj.SetScheduleStatus(string(jobStatus))
	return nil
}

func (s scheduledTTLExecutor) Metrics() metric.Struct {
	return &s.metrics
}

func (s scheduledTTLExecutor) GetCreateScheduleStatement(
	ctx context.Context,
	env scheduledjobs.JobSchedulerEnv,
	txn *kv.Txn,
	schedule *jobs.ScheduledJob,
	ex sqlutil.InternalExecutor,
) (string, error) {
	return "IMPLEMENT ME XXX", nil
}

func init() {
	jobs.RegisterConstructor(jobspb.TypeTTL, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &ttlResumer{
			job: job,
			st:  settings,
		}
	})

	jobs.RegisterScheduledJobExecutorFactory(
		tree.ScheduledTTLExecutor.InternalName(),
		func() (jobs.ScheduledJobExecutor, error) {
			m := jobs.MakeExecutorMetrics(tree.ScheduledTTLExecutor.InternalName())
			return &scheduledTTLExecutor{
				metrics: ttlMetrics{
					ExecutorMetrics: &m,
				},
			}, nil
		})
}
