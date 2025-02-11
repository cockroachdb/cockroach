// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schematelemetrycontroller

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
	"github.com/robfig/cron/v3"
)

// SchemaTelemetryScheduleName is the name of the schema telemetry schedule.
const SchemaTelemetryScheduleName = "sql-schema-telemetry"

// SchemaTelemetryRecurrence is the cron-tab string specifying the recurrence
// for schema telemetry job.
var SchemaTelemetryRecurrence = settings.RegisterStringSetting(
	settings.SystemVisible,
	"sql.schema.telemetry.recurrence",
	"cron-tab recurrence for SQL schema telemetry job",
	"@weekly", /* defaultValue */
	settings.WithValidateString(func(_ *settings.Values, s string) error {
		if _, err := cron.ParseStandard(s); err != nil {
			return errors.Wrap(err, "invalid cron expression")
		}
		return nil
	}),
	settings.WithPublic,
)

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
	db        isql.DB
	mon       *mon.BytesMonitor
	st        *cluster.Settings
	jr        *jobs.Registry
	clusterID func() uuid.UUID
}

// NewController is a constructor for *Controller.
//
// This constructor needs to be called in the sql package when creating a new
// sql.Server. This is the reason why it and the definition of the Controller
// object live in their own package separate from schematelemetry.
func NewController(
	db isql.DB,
	mon *mon.BytesMonitor,
	st *cluster.Settings,
	jr *jobs.Registry,
	clusterID func() uuid.UUID,
) *Controller {
	return &Controller{
		db:        db,
		mon:       mon,
		st:        st,
		jr:        jr,
		clusterID: clusterID,
	}
}

// Start kicks off the async task which acts on schedule update notifications
// and registers the change hook on the schedule recurrence cluster setting.
func (c *Controller) Start(ctx context.Context, stopper *stop.Stopper) {
	// ch is used to notify a goroutine to ensure the schedule exists and
	// update its recurrence.
	ch := make(chan struct{}, 1)
	stopper.AddCloser(stop.CloserFn(func() { c.mon.Stop(ctx) }))
	// Start a goroutine that ensures the presence of the schema telemetry
	// schedule and updates its recurrence.
	_ = stopper.RunAsyncTask(ctx, "schema-telemetry-schedule-updater", func(ctx context.Context) {
		stopCtx, cancel := stopper.WithCancelOnQuiesce(ctx)
		defer cancel()
		for {
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-ch:
				updateSchedule(stopCtx, c.db, c.st, c.clusterID())
			}
		}
	})
	notify := func(name string) {
		_ = stopper.RunAsyncTask(ctx, "schema-telemetry-schedule-"+name, func(ctx context.Context) {
			// Notify only if the channel is empty, don't bother if another update
			// is already pending.
			select {
			case ch <- struct{}{}:
			default:
			}
		})
	}
	// Trigger a schedule update to ensure it exists at startup.
	notify("ensure-at-startup")

	// Add a change hook on the recurrence cluster setting that will notify
	// a schedule update.
	SchemaTelemetryRecurrence.SetOnChange(&c.st.SV, func(ctx context.Context) {
		notify("notify-recurrence-change")
	})
}

func updateSchedule(ctx context.Context, db isql.DB, st *cluster.Settings, clusterID uuid.UUID) {
	retryOptions := retry.Options{
		InitialBackoff: time.Second,
		MaxBackoff:     10 * time.Minute,
	}
	for r := retry.StartWithCtx(ctx, retryOptions); r.Next(); {
		if err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			// Ensure schedule exists.
			var sj *jobs.ScheduledJob
			{
				id, err := GetSchemaTelemetryScheduleID(ctx, txn)
				if err != nil {
					return err
				}
				if id == 0 {
					sj, err = CreateSchemaTelemetrySchedule(ctx, txn, st, clusterID)
				} else {
					sj, err = jobs.ScheduledJobTxn(txn).Load(ctx, scheduledjobs.ProdJobSchedulerEnv, id)
				}
				if err != nil {
					return err
				}
			}
			// Update schedule with new recurrence, if different.
			cronExpr := scheduledjobs.MaybeRewriteCronExpr(
				clusterID, SchemaTelemetryRecurrence.Get(&st.SV),
			)
			if sj.ScheduleExpr() == cronExpr {
				return nil
			}
			if err := sj.SetScheduleAndNextRun(cronExpr); err != nil {
				return err
			}
			sj.SetScheduleStatus(string(jobs.StatePending))
			return jobs.ScheduledJobTxn(txn).Update(ctx, sj)
		}); err != nil && ctx.Err() == nil {
			log.Warningf(ctx, "failed to update SQL schema telemetry schedule: %s", err)
		} else {
			return
		}
	}
}

// CreateSchemaTelemetryJob is part of the eval.SchemaTelemetryController
// interface.
func (c *Controller) CreateSchemaTelemetryJob(
	ctx context.Context, createdByName string, createdByID int64,
) (id int64, _ error) {
	var j *jobs.Job
	if err := c.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) (err error) {
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
	ctx context.Context, txn isql.Txn, st *cluster.Settings, clusterID uuid.UUID,
) (*jobs.ScheduledJob, error) {
	id, err := GetSchemaTelemetryScheduleID(ctx, txn)
	if err != nil {
		return nil, err
	}
	if id != 0 {
		return nil, ErrDuplicatedSchedules
	}

	scheduledJob := jobs.NewScheduledJob(scheduledjobs.ProdJobSchedulerEnv)

	schedule := SchemaTelemetryRecurrence.Get(&st.SV)
	if err := scheduledJob.SetScheduleAndNextRun(schedule); err != nil {
		return nil, err
	}

	scheduledJob.SetScheduleDetails(jobspb.ScheduleDetails{
		Wait:                   jobspb.ScheduleDetails_SKIP,
		OnError:                jobspb.ScheduleDetails_RETRY_SCHED,
		ClusterID:              clusterID,
		CreationClusterVersion: st.Version.ActiveVersion(ctx),
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

	scheduledJob.SetScheduleStatus(string(jobs.StatePending))
	if err = jobs.ScheduledJobTxn(txn).Create(ctx, scheduledJob); err != nil {
		return nil, err
	}

	return scheduledJob, nil
}

// GetSchemaTelemetryScheduleID returns the ID of the schema telemetry schedule
// if it exists, 0 if it does not exist yet.
func GetSchemaTelemetryScheduleID(
	ctx context.Context, txn isql.Txn,
) (id jobspb.ScheduleID, _ error) {
	row, err := txn.QueryRowEx(
		ctx,
		"check-existing-schema-telemetry-schedule",
		txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
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
	return jobspb.ScheduleID(v), nil
}
