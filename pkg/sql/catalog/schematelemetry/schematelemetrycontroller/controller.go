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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
	"github.com/robfig/cron/v3"
)

// SchemaTelemetryScheduleName is the name of the schema telemetry schedule.
const SchemaTelemetryScheduleName = "sql-schema-telemetry"

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

// ErrDuplicatedSchedules indicates that there is already a schedule for SQL
// schema telemetry jobs existing in the system.scheduled_jobs table.
var ErrDuplicatedSchedules = errors.New("creating multiple schema telemetry schedules is disallowed")

// ErrVersionGate indicates that SQL schema telemetry jobs or schedules are
// not supported by the current cluster version.
var ErrVersionGate = errors.New("SQL schema telemetry jobs or schedules not supported by current cluster version")

// Controller implements the SQL Schema telemetry subsystem control plane.
// This exposes administrative interfaces that can be consumed by other parts
// of the database (e.g. status server, builtins) to control the behavior of the
// SQL schema telemetry subsystem.
type Controller struct {
	scheduleController
	jr *jobs.Registry
}

// scheduleController is distinct from Controller which implements the full
// control plane, because tenant migrations don't have access to the jobs
// registry.
type scheduleController struct {
	db *kv.DB
	ie sqlutil.InternalExecutor
	st *cluster.Settings
}

// NewController is a constructor for *Controller.
//
// This constructor needs to be called in the sql package when creating a new
// sql.Server. This is the reason why it and the definition of the Controller
// object live in their own package separate from schematelemetry.
func NewController(
	db *kv.DB, ie sqlutil.InternalExecutor, st *cluster.Settings, jr *jobs.Registry,
) *Controller {
	return &Controller{
		scheduleController: scheduleController{
			db: db,
			ie: ie,
			st: st,
		},
		jr: jr,
	}
}

// CreateSchemaTelemetryJob is part of the eval.SchemaTelemetryController
// interface.
func (c *Controller) CreateSchemaTelemetryJob(
	ctx context.Context, createdByName string, createdByID int64,
) (id int64, _ error) {
	if !c.st.Version.IsActive(ctx, clusterversion.SQLSchemaTelemetryScheduledJobs) {
		return 0, ErrVersionGate
	}
	var j *jobs.Job
	if err := c.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		r := CreateSchemaTelemetryJobRecord(createdByName, createdByID)
		j, err = c.jr.CreateJobWithTxn(ctx, r, c.jr.MakeJobID(), txn)
		return err
	}); err != nil {
		return 0, err
	}
	return int64(j.ID()), nil
}

// CreateSchemaTelemetryJobRecord creates a record for a schema telemetry job.
func CreateSchemaTelemetryJobRecord(createdByName string, createdByID int64) jobs.Record {
	return jobs.Record{
		Description: "SQL schema telemetry",
		Username:    username.NodeUserName(),
		Details:     jobspb.SchemaTelemetryDetails{},
		Progress:    jobspb.SchemaTelemetryProgress{},
		CreatedBy: &jobs.CreatedByInfo{
			ID:   createdByID,
			Name: createdByName,
		},
	}
}

// CreateSchemaTelemetrySchedule registers the schema telemetry job with
// the scheduled job subsystem so that the schema telemetry job can be run
// periodically. This is done during the cluster startup upgrade.
func CreateSchemaTelemetrySchedule(
	ctx context.Context, ie sqlutil.InternalExecutor, txn *kv.Txn, st *cluster.Settings,
) (*jobs.ScheduledJob, error) {
	id, err := GetSchemaTelemetryScheduleID(ctx, ie, txn)
	if err != nil {
		return nil, err
	}
	if id != 0 {
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

	scheduledJob.SetScheduleLabel(SchemaTelemetryScheduleName)
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

// GetSchemaTelemetryScheduleID returns the ID of the schema telemetry schedule
// if it exists, 0 if it does not exist yet.
func GetSchemaTelemetryScheduleID(
	ctx context.Context, ie sqlutil.InternalExecutor, txn *kv.Txn,
) (id int64, _ error) {
	row, err := ie.QueryRowEx(
		ctx,
		"check-existing-schema-telemetry-schedule",
		txn,
		sessiondata.InternalExecutorOverride{User: username.NodeUserName()},
		`SELECT schedule_id FROM system.scheduled_jobs WHERE schedule_name = $1 ORDER BY schedule_id ASC LIMIT 1`,
		SchemaTelemetryScheduleName,
	)
	if err != nil || row == nil {
		return 0, err
	}
	if len(row) != 1 {
		return 0, errors.AssertionFailedf("unexpectedly received %d columns", len(row))
	}
	// Defensively check the type.
	v, ok := tree.AsDInt(row[0])
	if !ok {
		return 0, errors.AssertionFailedf("unexpectedly received non-integer value %v", row[0])
	}
	return int64(v), nil
}

// EnsureScheduleAtStartup ensures that a SQL schema telemetry job schedule
// exists during sql.Server startup.
func (sc *scheduleController) EnsureScheduleAtStartup(ctx context.Context) {
	if !sc.st.Version.IsActive(ctx, clusterversion.SQLSchemaTelemetryScheduledJobs) {
		log.Infof(ctx, "%s", ErrVersionGate.Error())
		return
	}
	if err := sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		_, err := sc.ensureSchedule(ctx, txn)
		return err
	}); err != nil {
		log.Errorf(ctx, "failed to ensure existence of SQL schema telemetry schedule: %s", err)
	}
}

// RegisterClusterSettingHook ensures that the SQL schema telemetry schedule
// recurrence is updated following changes to the corresponding cluster setting.
func (sc *scheduleController) RegisterClusterSettingHook(ctx context.Context) {
	SchemaTelemetryRecurrence.SetOnChange(&sc.st.SV, func(ctx context.Context) {
		if !sc.st.Version.IsActive(ctx, clusterversion.SQLSchemaTelemetryScheduledJobs) {
			log.Infof(ctx, "%s", ErrVersionGate.Error())
			return
		}
		sc.onClusterSettingChange(ctx)
	})
}

func (sc *scheduleController) ensureSchedule(
	ctx context.Context, txn *kv.Txn,
) (sj *jobs.ScheduledJob, _ error) {
	id, err := GetSchemaTelemetryScheduleID(ctx, sc.ie, txn)
	if err != nil {
		return nil, err
	}
	if id == 0 {
		sj, err = CreateSchemaTelemetrySchedule(ctx, sc.ie, txn, sc.st)
	} else {
		sj, err = jobs.LoadScheduledJob(ctx, scheduledjobs.ProdJobSchedulerEnv, id, sc.ie, txn)
	}
	if err != nil {
		return nil, err
	}
	return sj, nil
}

func (sc *scheduleController) onClusterSettingChange(ctx context.Context) {
	if err := sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		sj, err := sc.ensureSchedule(ctx, txn)
		if err != nil || sj == nil {
			return err
		}
		cronExpr := SchemaTelemetryRecurrence.Get(&sc.st.SV)
		if err = sj.SetSchedule(cronExpr); err != nil {
			return err
		}
		sj.SetScheduleStatus(string(jobs.StatusPending))
		return sj.Update(ctx, sc.ie, txn)
	}); err != nil {
		log.Errorf(ctx, "failed to change SQL schema telemetry schedule recurrence: %s", err)
	}
}

// EnsureScheduleAndRegisterClusterSettingHook is a convenient entry point
// for the migration associated with the SQL schema telemetry version.
func EnsureScheduleAndRegisterClusterSettingHook(
	ctx context.Context, db *kv.DB, ie sqlutil.InternalExecutor, st *cluster.Settings,
) error {
	sc := scheduleController{db: db, ie: ie, st: st}
	sc.RegisterClusterSettingHook(ctx)
	return sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		_, err := sc.ensureSchedule(ctx, txn)
		return err
	})
}
