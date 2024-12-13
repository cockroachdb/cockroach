// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/robfig/cron/v3"
)

// scheduledJobRecord is a reflective representation of a row in
// a system.scheduled_job table.
// Each field in this struct has a tag specifying the column
// name in the system.scheduled_job table containing the data for the field.
// Do not manipulate these fields directly, use methods in the ScheduledJob.
type scheduledJobRecord struct {
	ScheduleID      jobspb.ScheduleID         `col:"schedule_id"`
	ScheduleLabel   string                    `col:"schedule_name"`
	Owner           username.SQLUsername      `col:"owner"`
	NextRun         time.Time                 `col:"next_run"`
	ScheduleState   jobspb.ScheduleState      `col:"schedule_state"`
	ScheduleExpr    string                    `col:"schedule_expr"`
	ScheduleDetails jobspb.ScheduleDetails    `col:"schedule_details"`
	ExecutorType    string                    `col:"executor_type"`
	ExecutionArgs   jobspb.ExecutionArguments `col:"execution_args"`
}

// ScheduledJob  is a representation of the scheduled job.
// This struct can marshal/unmarshal changes made to the underlying system.scheduled_job table.
type ScheduledJob struct {
	env scheduledjobs.JobSchedulerEnv

	// The "record" for this schedule job.  Do not access this field
	// directly (except in tests); Use Get/Set methods on ScheduledJob instead.
	rec scheduledJobRecord

	// The time this scheduled job was supposed to run.
	// This field is initialized to rec.NextRun when the scheduled job record
	// is loaded from the table.
	scheduledTime time.Time

	// Set of changes to this job that need to be persisted.
	dirty map[string]struct{}
}

// NewScheduledJob creates and initializes ScheduledJob.
func NewScheduledJob(env scheduledjobs.JobSchedulerEnv) *ScheduledJob {
	return &ScheduledJob{
		env:   env,
		dirty: make(map[string]struct{}),
	}
}

// scheduledJobNotFoundError is returned from load when the scheduled job does
// not exist.
type scheduledJobNotFoundError struct {
	scheduleID jobspb.ScheduleID
}

// Error makes scheduledJobNotFoundError an error.
func (e *scheduledJobNotFoundError) Error() string {
	return fmt.Sprintf("scheduled job with ID %d does not exist", e.scheduleID)
}

// HasScheduledJobNotFoundError returns true if the error contains a
// scheduledJobNotFoundError.
func HasScheduledJobNotFoundError(err error) bool {
	return errors.HasType(err, (*scheduledJobNotFoundError)(nil))
}

func ScheduledJobDB(db isql.DB) ScheduledJobStorage {
	return scheduledJobStorageDB{db: db}
}

func ScheduledJobTxn(txn isql.Txn) ScheduledJobStorage {
	return scheduledJobStorageTxn{txn: txn}
}

type ScheduledJobStorage interface {
	// Load loads scheduled job record from the database.
	Load(ctx context.Context, env scheduledjobs.JobSchedulerEnv, id jobspb.ScheduleID) (*ScheduledJob, error)

	// DeleteByID removes this schedule with the given ID.
	// If an error is returned, it is callers responsibility to handle it (e.g. rollback transaction).
	DeleteByID(ctx context.Context, env scheduledjobs.JobSchedulerEnv, id jobspb.ScheduleID) error

	// Create persists this schedule in the system.scheduled_jobs table.
	// Sets j.scheduleID to the ID of the newly created schedule.
	// Only the values initialized in this schedule are written to the specified transaction.
	// If an error is returned, it is callers responsibility to handle it (e.g. rollback transaction).
	Create(ctx context.Context, j *ScheduledJob) error

	// Delete removes this schedule.
	// If an error is returned, it is callers responsibility to handle it (e.g. rollback transaction).
	Delete(ctx context.Context, j *ScheduledJob) error

	// Update saves changes made to this schedule.
	// If an error is returned, it is callers responsibility to handle it (e.g. rollback transaction).
	Update(ctx context.Context, j *ScheduledJob) error
}

// ScheduleID returns schedule ID.
func (j *ScheduledJob) ScheduleID() jobspb.ScheduleID {
	return j.rec.ScheduleID
}

// ScheduleLabel returns schedule label.
func (j *ScheduledJob) ScheduleLabel() string {
	return j.rec.ScheduleLabel
}

// SetScheduleLabel updates schedule label.
func (j *ScheduledJob) SetScheduleLabel(label string) {
	j.rec.ScheduleLabel = label
	j.markDirty("schedule_name")
}

// Owner returns schedule owner.
func (j *ScheduledJob) Owner() username.SQLUsername {
	return j.rec.Owner
}

// SetOwner updates schedule owner.
func (j *ScheduledJob) SetOwner(owner username.SQLUsername) {
	j.rec.Owner = owner
	j.markDirty("owner")
}

// NextRun returns the next time this schedule supposed to execute.
// A sentinel value of time.Time{} indicates this schedule is paused.
func (j *ScheduledJob) NextRun() time.Time {
	return j.rec.NextRun
}

// ScheduledRunTime returns the time this schedule was supposed to execute.
// This value reflects the 'next_run' value loaded from the system.scheduled_jobs table,
// prior to any mutations to the 'next_run' value.
func (j *ScheduledJob) ScheduledRunTime() time.Time {
	return j.scheduledTime
}

// IsPaused returns true if this schedule is paused.
func (j *ScheduledJob) IsPaused() bool {
	return j.rec.NextRun == time.Time{}
}

// ExecutorType returns executor type for this schedule.
func (j *ScheduledJob) ExecutorType() string {
	return j.rec.ExecutorType
}

// ExecutionArgs returns ExecutionArgs set for this schedule.
func (j *ScheduledJob) ExecutionArgs() *jobspb.ExecutionArguments {
	return &j.rec.ExecutionArgs
}

// SetScheduleAndNextRun updates periodicity of this schedule, and updates this schedules
// next run time.
func (j *ScheduledJob) SetScheduleAndNextRun(scheduleExpr string) error {
	j.rec.ScheduleExpr = scheduleExpr
	j.markDirty("schedule_expr")
	return j.ScheduleNextRun()
}

// SetScheduleExpr updates schedule expression for this schedule without updating next run time.
func (j *ScheduledJob) SetScheduleExpr(scheduleExpr string) {
	j.rec.ScheduleExpr = scheduleExpr
	j.markDirty("schedule_expr")
}

// HasRecurringSchedule returns true if this schedule job runs periodically.
func (j *ScheduledJob) HasRecurringSchedule() bool {
	return len(j.rec.ScheduleExpr) > 0
}

// Frequency returns how often this schedule executes.
func (j *ScheduledJob) Frequency() (time.Duration, error) {
	if !j.HasRecurringSchedule() {
		return 0, errors.Newf(
			"schedule %d is not periodic", j.rec.ScheduleID)
	}
	expr, err := cron.ParseStandard(j.rec.ScheduleExpr)
	if err != nil {
		return 0, errors.Wrapf(err,
			"parsing schedule expression: %q; it must be a valid cron expression",
			j.rec.ScheduleExpr)
	}

	next := expr.Next(j.env.Now())
	nextNext := expr.Next(next)
	return nextNext.Sub(next), nil
}

// ScheduleNextRun updates next run based on job schedule.
func (j *ScheduledJob) ScheduleNextRun() error {
	if !j.HasRecurringSchedule() {
		return errors.Newf(
			"cannot set next run for schedule %d (empty schedule)", j.rec.ScheduleID)
	}
	expr, err := cron.ParseStandard(j.rec.ScheduleExpr)
	if err != nil {
		return errors.Wrapf(err, "parsing schedule expression: %q", j.rec.ScheduleExpr)
	}
	j.SetNextRun(expr.Next(j.env.Now()))
	return nil
}

// SetNextRun updates next run time for this schedule.
func (j *ScheduledJob) SetNextRun(t time.Time) {
	j.rec.NextRun = t
	j.markDirty("next_run")
}

// ScheduleDetails returns schedule configuration information.
func (j *ScheduledJob) ScheduleDetails() *jobspb.ScheduleDetails {
	return &j.rec.ScheduleDetails
}

// SetScheduleDetails updates schedule configuration.
func (j *ScheduledJob) SetScheduleDetails(details jobspb.ScheduleDetails) {
	j.rec.ScheduleDetails = details
	j.markDirty("schedule_details")
}

// SetScheduleStatus sets schedule status.
func (j *ScheduledJob) SetScheduleStatus(msg string) {
	j.rec.ScheduleState.Status = msg
	j.markDirty("schedule_state")
}

// SetScheduleStatusf sets schedule status.
func (j *ScheduledJob) SetScheduleStatusf(format string, args ...interface{}) {
	j.rec.ScheduleState.Status = fmt.Sprintf(format, args...)
	j.markDirty("schedule_state")
}

// ScheduleStatus returns schedule status.
func (j *ScheduledJob) ScheduleStatus() string {
	return j.rec.ScheduleState.Status
}

// ClearScheduleStatus clears schedule status.
func (j *ScheduledJob) ClearScheduleStatus() {
	j.rec.ScheduleState.Status = ""
	j.markDirty("schedule_state")
}

// ScheduleExpr returns the schedule expression for this schedule.
func (j *ScheduledJob) ScheduleExpr() string {
	return j.rec.ScheduleExpr
}

// Pause pauses this schedule.
// Use ScheduleNextRun to unpause.
func (j *ScheduledJob) Pause() {
	j.rec.NextRun = time.Time{}
	j.markDirty("next_run")
}

// SetExecutionDetails sets execution specific fields for this schedule.
func (j *ScheduledJob) SetExecutionDetails(executor string, args jobspb.ExecutionArguments) {
	j.rec.ExecutorType = executor
	j.rec.ExecutionArgs = args
	j.markDirty("executor_type", "execution_args")
}

// ClearDirty clears the dirty map making this object appear as if it was just loaded.
func (j *ScheduledJob) ClearDirty() {
	j.dirty = make(map[string]struct{})
}

// InitFromDatums initializes this ScheduledJob object based on datums and column names.
func (j *ScheduledJob) InitFromDatums(datums []tree.Datum, cols []colinfo.ResultColumn) error {
	if len(datums) != len(cols) {
		return errors.Errorf(
			"datums length != columns length: %d != %d", len(datums), len(cols))
	}

	modified := false

	for i, col := range cols {
		datum := tree.UnwrapDOidWrapper(datums[i])
		if datum == tree.DNull {
			// Skip over any null values
			continue
		}

		switch col.Name {
		case "schedule_id":
			dint, ok := datum.(*tree.DInt)
			if !ok {
				return errors.Newf("expected %q to be %T got %T", col.Name, dint, datum)
			}
			j.rec.ScheduleID = jobspb.ScheduleID(*dint)

		case "schedule_name":
			dstring, ok := datum.(*tree.DString)
			if !ok {
				return errors.Newf("expected %q to be %T got %T", col.Name, dstring, datum)
			}
			j.rec.ScheduleLabel = string(*dstring)

		case "owner":
			dstring, ok := datum.(*tree.DString)
			if !ok {
				return errors.Newf("expected %q to be %T got %T", col.Name, dstring, datum)
			}
			j.rec.Owner = username.MakeSQLUsernameFromPreNormalizedString(string(*dstring))

		case "next_run":
			dtime, ok := datum.(*tree.DTimestampTZ)
			if !ok {
				return errors.Newf("expected %q to be %T got %T", col.Name, dtime, datum)
			}
			j.rec.NextRun = dtime.Time

		case "schedule_state":
			dbytes, ok := datum.(*tree.DBytes)
			if !ok {
				return errors.Newf("expected %q to be %T got %T", col.Name, dbytes, datum)
			}
			if err := protoutil.Unmarshal([]byte(*dbytes), &j.rec.ScheduleState); err != nil {
				return err
			}

		case "schedule_expr":
			dstring, ok := datum.(*tree.DString)
			if !ok {
				return errors.Newf("expected %q to be %T got %T", col.Name, dstring, datum)
			}
			j.rec.ScheduleExpr = string(*dstring)

		case "schedule_details":
			dbytes, ok := datum.(*tree.DBytes)
			if !ok {
				return errors.Newf("expected %q to be %T got %T", col.Name, dbytes, datum)
			}
			if err := protoutil.Unmarshal([]byte(*dbytes), &j.rec.ScheduleDetails); err != nil {
				return err
			}

		case "executor_type":
			dstring, ok := datum.(*tree.DString)
			if !ok {
				return errors.Newf("expected %q to be %T got %T", col.Name, dstring, datum)
			}
			j.rec.ExecutorType = string(*dstring)

		case "execution_args":
			dbytes, ok := datum.(*tree.DBytes)
			if !ok {
				return errors.Newf("expected %q to be %T got %T", col.Name, dbytes, datum)
			}
			if err := protoutil.Unmarshal([]byte(*dbytes), &j.rec.ExecutionArgs); err != nil {
				return err
			}

		default:
			// Ignore any unrecognized fields. This behavior is historical and tested
			// but it's unclear why unrecognized fields would appear.
			continue
		}

		modified = true

	}

	if !modified {
		return errors.Newf("no fields initialized")
	}

	j.scheduledTime = j.rec.NextRun

	return nil
}

type scheduledJobStorageDB struct{ db isql.DB }

func (s scheduledJobStorageDB) DeleteByID(
	ctx context.Context, env scheduledjobs.JobSchedulerEnv, id jobspb.ScheduleID,
) error {
	return s.run(ctx, func(ctx context.Context, txn scheduledJobStorageTxn) error {
		return txn.DeleteByID(ctx, env, id)
	})
}

func (s scheduledJobStorageDB) Load(
	ctx context.Context, env scheduledjobs.JobSchedulerEnv, id jobspb.ScheduleID,
) (*ScheduledJob, error) {
	var j *ScheduledJob
	if err := s.run(ctx, func(
		ctx context.Context, txn scheduledJobStorageTxn,
	) (err error) {
		j, err = txn.Load(ctx, env, id)
		return err
	}); err != nil {
		return nil, err
	}
	return j, nil
}

func (s scheduledJobStorageDB) run(
	ctx context.Context, f func(ctx context.Context, txn scheduledJobStorageTxn) error,
) error {
	return s.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) (err error) {
		return f(ctx, scheduledJobStorageTxn{txn})
	})
}

func (s scheduledJobStorageDB) runAction(
	ctx context.Context,
	f func(scheduledJobStorageTxn, context.Context, *ScheduledJob) error,
	j *ScheduledJob,
) error {
	return s.run(ctx, func(ctx context.Context, txn scheduledJobStorageTxn) error {
		return f(txn, ctx, j)
	})
}

func (s scheduledJobStorageDB) Create(ctx context.Context, j *ScheduledJob) error {
	return s.runAction(ctx, scheduledJobStorageTxn.Create, j)
}

func (s scheduledJobStorageDB) Delete(ctx context.Context, j *ScheduledJob) error {
	return s.runAction(ctx, scheduledJobStorageTxn.Delete, j)
}

func (s scheduledJobStorageDB) Update(ctx context.Context, j *ScheduledJob) error {
	return s.runAction(ctx, scheduledJobStorageTxn.Update, j)
}

type scheduledJobStorageTxn struct{ txn isql.Txn }

func (s scheduledJobStorageTxn) DeleteByID(
	ctx context.Context, env scheduledjobs.JobSchedulerEnv, id jobspb.ScheduleID,
) error {
	_, err := s.txn.ExecEx(
		ctx,
		"delete-schedule",
		s.txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf(
			"DELETE FROM %s WHERE schedule_id = $1",
			env.ScheduledJobsTableName(),
		),
		id,
	)
	return err
}

func (s scheduledJobStorageTxn) Load(
	ctx context.Context, env scheduledjobs.JobSchedulerEnv, id jobspb.ScheduleID,
) (*ScheduledJob, error) {
	row, cols, err := s.txn.QueryRowExWithCols(ctx, "lookup-schedule", s.txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf("SELECT * FROM %s WHERE schedule_id = %d",
			env.ScheduledJobsTableName(), id))

	if err != nil {
		return nil, errors.CombineErrors(err, &scheduledJobNotFoundError{scheduleID: id})
	}
	if row == nil {
		return nil, &scheduledJobNotFoundError{scheduleID: id}
	}

	j := NewScheduledJob(env)
	if err := j.InitFromDatums(row, cols); err != nil {
		return nil, err
	}
	return j, nil
}

func (s scheduledJobStorageTxn) Create(ctx context.Context, j *ScheduledJob) error {
	if j.rec.ScheduleID != 0 {
		return errors.New("cannot specify schedule id when creating new cron job")
	}
	if j.rec.ScheduleDetails.ClusterID.Equal(uuid.UUID{}) {
		return errors.New("scheduled job created without a cluster ID")
	}
	if j.rec.ScheduleDetails.CreationClusterVersion.Equal(clusterversion.ClusterVersion{}) {
		return errors.New("scheduled job created without a cluster version")
	}
	if !j.isDirty() {
		return errors.New("no settings specified for scheduled job")
	}

	cols, qargs, err := j.marshalChanges()
	if err != nil {
		return err
	}

	row, retCols, err := s.txn.QueryRowExWithCols(ctx, "sched-create", s.txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf("INSERT INTO %s (%s) VALUES(%s) RETURNING schedule_id",
			j.env.ScheduledJobsTableName(), strings.Join(cols, ","), generatePlaceholders(len(qargs))),
		qargs...,
	)

	if err != nil {
		return errors.Wrapf(err, "failed to create new schedule")
	}
	if row == nil {
		return errors.New("failed to create new schedule")
	}

	return j.InitFromDatums(row, retCols)
}

func (s scheduledJobStorageTxn) Delete(ctx context.Context, j *ScheduledJob) error {
	if j.rec.ScheduleID == 0 {
		return errors.New("cannot delete schedule: missing schedule id")
	}
	_, err := s.txn.ExecEx(ctx, "sched-delete", s.txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf("DELETE FROM %s WHERE schedule_id = %d",
			j.env.ScheduledJobsTableName(), j.ScheduleID()),
	)

	return err
}

func (s scheduledJobStorageTxn) Update(ctx context.Context, j *ScheduledJob) error {
	if !j.isDirty() {
		return nil
	}

	if j.rec.ScheduleID == 0 {
		return errors.New("cannot update schedule: missing schedule id")
	}

	cols, qargs, err := j.marshalChanges()
	if err != nil {
		return err
	}

	if len(qargs) == 0 {
		return nil // Nothing changed.
	}

	n, err := s.txn.ExecEx(ctx, "sched-update", s.txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf("UPDATE %s SET (%s) = (%s) WHERE schedule_id = %d",
			j.env.ScheduledJobsTableName(), strings.Join(cols, ","),
			generatePlaceholders(len(qargs)), j.ScheduleID()),
		qargs...,
	)

	if err != nil {
		return err
	}

	if n != 1 {
		return fmt.Errorf("expected to update 1 schedule, updated %d instead", n)
	}

	return nil
}

var _ ScheduledJobStorage = (*scheduledJobStorageTxn)(nil)
var _ ScheduledJobStorage = (*scheduledJobStorageDB)(nil)

// marshalChanges marshals all changes in the in-memory representation and returns
// the names of the columns and marshaled values.
// If no error is returned, the job is not considered to be modified anymore.
// If the error is returned, this job object should no longer be used.
func (j *ScheduledJob) marshalChanges() ([]string, []interface{}, error) {
	var cols []string
	var qargs []interface{}

	for col := range j.dirty {
		var arg tree.Datum
		var err error

		switch col {
		case `schedule_name`:
			arg = tree.NewDString(j.rec.ScheduleLabel)
		case `owner`:
			arg = tree.NewDString(j.rec.Owner.Normalized())
		case `next_run`:
			if (j.rec.NextRun == time.Time{}) {
				arg = tree.DNull
			} else {
				arg, err = tree.MakeDTimestampTZ(j.rec.NextRun, time.Microsecond)
			}
		case `schedule_state`:
			arg, err = marshalProto(&j.rec.ScheduleState)
		case `schedule_expr`:
			arg = tree.NewDString(j.rec.ScheduleExpr)
		case `schedule_details`:
			arg, err = marshalProto(&j.rec.ScheduleDetails)
		case `executor_type`:
			arg = tree.NewDString(j.rec.ExecutorType)
		case `execution_args`:
			arg, err = marshalProto(&j.rec.ExecutionArgs)
		default:
			return nil, nil, errors.Newf("cannot marshal column %q", col)
		}

		if err != nil {
			return nil, nil, err
		}
		cols = append(cols, col)
		qargs = append(qargs, arg)
	}

	j.dirty = make(map[string]struct{})
	return cols, qargs, nil
}

// markDirty marks specified columns as dirty.
func (j *ScheduledJob) markDirty(cols ...string) {
	for _, col := range cols {
		j.dirty[col] = struct{}{}
	}
}

func (j *ScheduledJob) isDirty() bool {
	return len(j.dirty) > 0
}

// generates "$1,$2,..." placeholders for the specified 'n' number of arguments.
func generatePlaceholders(n int) string {
	placeholders := strings.Builder{}
	for i := 1; i <= n; i++ {
		if i > 1 {
			placeholders.WriteByte(',')
		}
		placeholders.WriteString(fmt.Sprintf("$%d", i))
	}
	return placeholders.String()
}

// marshalProto is a helper to serialize protocol message.
func marshalProto(message protoutil.Message) (tree.Datum, error) {
	data := make([]byte, message.Size())
	if _, err := message.MarshalTo(data); err != nil {
		return nil, err
	}
	return tree.NewDBytes(tree.DBytes(data)), nil
}
