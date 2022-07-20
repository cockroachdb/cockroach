// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schematelemetrycontroller

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
	"github.com/robfig/cron/v3"
)

const schemaTelemetryScheduleName = "sql-schema-telemetry"

// SchemaTelemetryRecurrence is the cron-tab string specifying the recurrence
// for schema telemetry job.
var SchemaTelemetryRecurrence = settings.RegisterValidatedStringSetting(
	settings.TenantWritable,
	"sql.schema.telemetry.recurrence",
	"cron-tab recurrence for SQL schema telemetry job",
	"@daily", /* defaultValue */
	func(_ *settings.Values, s string) error {
		if _, err := cron.ParseStandard(s); err != nil {
			return errors.Wrap(err, "invalid cron expression")
		}
		return nil
	},
).WithPublic()

// ErrDuplicatedSchedules indicates that there is already a schedule for sql
// stats compaction job existing in the system.scheduled_jobs table.
var ErrDuplicatedSchedules = errors.New("creating multiple schema telemetry schedules is disallowed")

// Controller implements the SQL Stats subsystem control plane. This exposes
// administrative interfaces that can be consumed by other parts of the database
// (e.g. status server, builtins) to control the behavior of the SQL Stats
// subsystem.
type Controller struct {
	db        *kv.DB
	ie        sqlutil.InternalExecutor
	ieMonitor *mon.BytesMonitor
	st        *cluster.Settings
	jr        *jobs.Registry
}

// NewController is a constructor for *Controller.
func NewController(
	db *kv.DB,
	ie sqlutil.InternalExecutor,
	ieMonitor *mon.BytesMonitor,
	st *cluster.Settings,
	jr *jobs.Registry,
) *Controller {
	return &Controller{
		db:        db,
		ie:        ie,
		ieMonitor: ieMonitor,
		st:        st,
		jr:        jr,
	}
}

// CreateSchemaTelemetrySchedule implements eval.SchemaTelemetryController.
func (c *Controller) CreateSchemaTelemetrySchedule(ctx context.Context) error {
	return c.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		_, err := CreateSchemaTelemetrySchedule(ctx, c.ie, txn, c.st)
		return err
	})
}

// CreateSchemaTelemetryJob implements eval.SchemaTelemetryController.
func (c *Controller) CreateSchemaTelemetryJob(
	ctx context.Context, createdByName string, createdByID int64,
) (id int64, _ error) {
	if err := c.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		jobID, err := CreateSchemaTelemetryJob(ctx, c.jr, txn, createdByName, createdByID)
		id = int64(jobID)
		return err
	}); err != nil {
		return 0, err
	}
	c.jr.NotifyToAdoptJobs()
	return id, nil
}

// CreateSchemaTelemetrySchedule registers the schema telemetry job with
// the scheduled job subsystem so that the schema telemetry job can be run
// periodically. This is done during the cluster startup upgrade.
func CreateSchemaTelemetrySchedule(
	ctx context.Context, ie sqlutil.InternalExecutor, txn *kv.Txn, st *cluster.Settings,
) (*jobs.ScheduledJob, error) {
	scheduleExists, err := checkExistingSchemaTelemetrySchedule(ctx, ie, txn)
	if err != nil {
		return nil, err
	}
	if scheduleExists {
		return nil, ErrDuplicatedSchedules
	}

	scheduledJob := jobs.NewScheduledJob(scheduledjobs.ProdJobSchedulerEnv)

	schedule := SchemaTelemetryRecurrence.Get(&st.SV)
	if err := scheduledJob.SetSchedule(schedule); err != nil {
		return nil, err
	}

	scheduledJob.SetScheduleDetails(jobspb.ScheduleDetails{
		Wait:    jobspb.ScheduleDetails_SKIP,
		OnError: jobspb.ScheduleDetails_RETRY_SCHED,
	})

	scheduledJob.SetScheduleLabel(schemaTelemetryScheduleName)
	scheduledJob.SetOwner(username.NodeUserName())

	args, err := pbtypes.MarshalAny(&ScheduledSchemaTelemetryExecutionArgs{})
	if err != nil {
		return nil, err
	}
	scheduledJob.SetExecutionDetails(
		tree.ScheduledSchemaTelemetryExecutor.InternalName(),
		jobspb.ExecutionArguments{Args: args},
	)

	scheduledJob.SetScheduleStatus(string(jobs.StatusPending))
	if err = scheduledJob.Create(ctx, ie, txn); err != nil {
		return nil, err
	}

	return scheduledJob, nil
}

func checkExistingSchemaTelemetrySchedule(
	ctx context.Context, ie sqlutil.InternalExecutor, txn *kv.Txn,
) (exists bool, _ error) {
	row, err := ie.QueryRowEx(
		ctx,
		"check-existing-schema-telemetry-schedule",
		txn,
		sessiondata.InternalExecutorOverride{User: username.NodeUserName()},
		`SELECT count(*) FROM system.scheduled_jobs WHERE schedule_name = $1`,
		schemaTelemetryScheduleName,
	)

	if err != nil {
		return false /* exists */, err
	}

	if row == nil {
		return false /* exists */, errors.AssertionFailedf("unexpected empty result when querying system.scheduled_job")
	}

	if len(row) != 1 {
		return false /* exists */, errors.AssertionFailedf("unexpectedly received %d columns", len(row))
	}

	// Defensively check the count.
	return tree.MustBeDInt(row[0]) > 0, nil /* err */
}

// CreateSchemaTelemetryJob creates a new schema telemetry job.
func CreateSchemaTelemetryJob(
	ctx context.Context, jr *jobs.Registry, txn *kv.Txn, createdByName string, createdByID int64,
) (jobID jobspb.JobID, err error) {
	record := jobs.Record{
		Description: "SQL schema telemetry",
		Username:    username.NodeUserName(),
		Details:     jobspb.SchemaTelemetryDetails{},
		Progress:    jobspb.SchemaTelemetryProgress{},
		CreatedBy: &jobs.CreatedByInfo{
			ID:   createdByID,
			Name: createdByName,
		},
	}
	jobID = jr.MakeJobID()
	_, err = jr.CreateAdoptableJobWithTxn(ctx, record, jobID, txn)
	return jobID, err
}
