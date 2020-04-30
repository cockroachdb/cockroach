// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/gorhill/cronexpr"
)

// serializer is an interface describing conversions to/from tree.Datum
type serializer interface {
	FromDatum(d tree.Datum) error
	ToDatum() (tree.Datum, error)
}

// scheduledJob is an in-memory representation of scheduled job.
type scheduledJob struct {
	env        cronEnv
	schedID    intSerializer
	jobName    strSerializer
	owner      strSerializer
	schedExpr  strSerializer
	nextRun    tzSerializer
	jobDetails jobDetailsSerializer
	execSpec   scheduleDetailsSerializer
	changeInfo changeInfoSerializer
}

// SetSchedule updates job schedule as well as its nextRun.
func (j *scheduledJob) SetSchedule(scheduleExpr string) error {
	j.schedExpr.Set(scheduleExpr)
	return j.ScheduleNextRun()
}

// ScheduleNextRun updates next run based on job schedule.
func (j *scheduledJob) ScheduleNextRun() error {
	expr, err := cronexpr.Parse(j.schedExpr.Value())
	if err != nil {
		return errors.Wrapf(err, "parsing schedule expression: %q", j.schedExpr.Value())
	}

	j.nextRun.Set(expr.Next(j.env.Now()))
	return nil
}

// AddChangeReason adds change information to this job.
// Arguments are interpreted same as printf.
// If there are too many changes already recorded, trims older changes.
func (j *scheduledJob) AddChangeReason(reasonFmt string, args ...interface{}) {
	ci := j.changeInfo.Mutable()
	if len(ci.Changes) > 10 {
		ci.Changes = ci.Changes[1:]
	}
	ci.Changes = append(ci.Changes, jobspb.ScheduleChangeInfo_Change{
		Time:   j.env.Now().UnixNano(),
		Reason: fmt.Sprintf(reasonFmt, args...),
	})
}

// Pause pauses the job.
func (j *scheduledJob) Pause(reason string) {
	j.nextRun.Set(time.Time{})
	j.AddChangeReason(reason)
}

// Unpause resumes running the job.
func (j *scheduledJob) Unpause(reason string) error {
	if err := j.SetSchedule(j.schedExpr.string); err != nil {
		return err
	}
	j.AddChangeReason(reason)
	return nil
}

// Creates new cron job.  Sets j.schedID to the ID of the newly created schedule.
// Only the values initialized in the job are written to the specified transaction, using executor.
// If an error is returned, it is callers responsibility to handle it (e.g. rollback transaction).
func (j *scheduledJob) Create(ctx context.Context, ex sqlutil.InternalExecutor, txn *kv.Txn) error {
	if j.schedID.changed {
		return errors.New("cannot specify schedule id when creating new cron job")
	}

	cols, qargs, err := j.marshalChanges()
	if err != nil {
		return err
	}

	row, err := ex.QueryRowEx(ctx, "cron-create", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		fmt.Sprintf("INSERT INTO %s (%s) VALUES(%s) RETURNING sched_id",
			j.env.CrontabTableName(), strings.Join(cols, ","), generatePlaceholders(len(qargs))),
		qargs...,
	)

	if err != nil {
		return err
	}
	if row == nil {
		// That really shouldn't happen.
		return errors.New("failed to create new schedule")
	}

	if err := j.schedID.FromDatum(row[0]); err != nil {
		return err
	}
	return nil
}

// Update saves changes made to this job.
// If an error is returned, it is callers responsibility to handle it (e.g. rollback transaction).
func (j *scheduledJob) Update(ctx context.Context, ex sqlutil.InternalExecutor, txn *kv.Txn) error {
	if j.schedID.changed {
		return errors.New("schedule ID is immutable")
	}

	if j.schedID.Value() == 0 {
		return errors.New("cannot update job: missing schedule id")
	}

	cols, qargs, err := j.marshalChanges()
	if err != nil {
		return err
	}

	if len(qargs) == 0 {
		return nil // Nothing changed.
	}

	n, err := ex.ExecEx(ctx, "cron-update", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		fmt.Sprintf("UPDATE %s SET (%s) = (%s) WHERE sched_id = %d",
			j.env.CrontabTableName(), strings.Join(cols, ","),
			generatePlaceholders(len(qargs)), j.schedID.Value()),
		qargs...,
	)

	if n != 1 {
		return fmt.Errorf("expected to update 1 schedule, updated %d instead", n)
	}

	return nil
}

// FromDatums is a helper to unmarshal scheduled job record.
// Requires the number of datums and the number of fields in this job record are the same.
//  Ex: j.FromDatums(row, &j.jobName, &j.schedExpr, ...)
func (j *scheduledJob) FromDatums(datums []tree.Datum, fields ...serializer) error {
	if len(datums) != len(fields) {
		return errors.Errorf("datums length != field length: %d != %d", len(datums), len(fields))
	}

	for i := 0; i < len(datums); i++ {
		if err := fields[i].FromDatum(datums[i]); err != nil {
			return err
		}
	}
	return nil
}

// marhalChanges marshals all changes in the in-memory representation and returns
// the names of the columns and marshaled values.
// If no error is returned, the job is not considered to be modified anymore.
// If the error is returned, this job object should no longer be used.
func (j *scheduledJob) marshalChanges() ([]string, []interface{}, error) {
	var cols []string
	var qargs []interface{}

	addChange := func(col string, changed *bool, val serializer) error {
		var err error
		if *changed {
			*changed = false
			cols = append(cols, col)
			datum, err := val.ToDatum()
			if err == nil {
				qargs = append(qargs, datum)
			}
		}
		return err
	}

	if err := addChange("job_name", &j.jobName.changed, &j.jobName); err != nil {
		return nil, nil, err
	}

	if err := addChange("owner", &j.owner.changed, &j.owner); err != nil {
		return nil, nil, err
	}

	if err := addChange("schedule_expr", &j.schedExpr.changed, &j.schedExpr); err != nil {
		return nil, nil, err
	}

	if err := addChange("next_run", &j.nextRun.changed, &j.nextRun); err != nil {
		return nil, nil, err
	}

	if err := addChange("job_details", &j.jobDetails.changed, &j.jobDetails); err != nil {
		return nil, nil, err
	}

	if err := addChange("exec_spec", &j.execSpec.changed, &j.execSpec); err != nil {
		return nil, nil, err
	}

	if err := addChange("change_info", &j.changeInfo.changed, &j.changeInfo); err != nil {
		return nil, nil, err
	}
	return cols, qargs, nil
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

// NotifyJobCompletion is a callback invoked by the system job before it completes.
// This callback must be invoked by the jobs system when the job enters its terminal state.
//
// The 'txn' transaction argument is the transaction the job will use to update its
// state (e.g. status, etc).  If any changes need to be made to the scheduled job record,
// those changes are applied to the same transaction -- that is, they are applied atomically
// with the job status changes.
func NotifyJobCompletion(
	ctx context.Context, env cronEnv, md *JobMetadata, ex sqlutil.InternalExecutor, txn *kv.Txn,
) error {
	if !md.Status.Terminal() {
		return errors.Newf(
			"job completion expects terminal state, found %s instead for job %d", md.Status, md.ID)
	}

	if md.Status == StatusFailed && md.Payload != nil && md.Payload.ScheduleID > 0 {
		return handleFailedJob(ctx, env, md, ex, txn)
	}

	return nil
}

const retryFailedJobAfter = time.Minute

func handleFailedJob(
	ctx context.Context, env cronEnv, md *JobMetadata, ex sqlutil.InternalExecutor, txn *kv.Txn,
) error {
	row, err := ex.QueryRowEx(ctx, "lookup-schedule", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		fmt.Sprintf("SELECT sched_id, exec_spec FROM %s WHERE sched_id = $1", env.CrontabTableName()),
		md.Payload.ScheduleID,
	)
	if err != nil {
		return err
	}

	if row == nil {
		log.Errorf(ctx,
			"Job %d referenced non-existing scheduled %d", md.ID, md.Payload.ScheduleID)
		return nil
	}

	j := &scheduledJob{env: env}
	if err := j.FromDatums(row, &j.schedID, &j.execSpec); err != nil {
		return errors.Wrapf(err,
			"failed to parse execSpec for schedule %d", md.Payload.ScheduleID)
	}

	switch j.execSpec.Value().OnError {
	case jobspb.ScheduleDetails_RETRY_SOON:
		j.AddChangeReason("retrying due to job %d failure", md.ID)
		j.nextRun.Set(env.Now().Add(retryFailedJobAfter)) // TODO(yevgeniy): backoff
	case jobspb.ScheduleDetails_PAUSE_SCHED:
		j.Pause(fmt.Sprintf("schedule paused due job %d failure", md.ID))
	default:
		// Nothing: ScheduleDetails_RETRY_SCHED already handled since
		// the next run was set when we started running scheduled job.
		return nil
	}

	return j.Update(ctx, ex, txn)
}

// intSerializer implements serializer for int64
type intSerializer struct {
	int64
	changed bool
}

// strSerializer implements serializer for strings
type strSerializer struct {
	string
	changed bool
}

// tzSerializer implements serializer for time
type tzSerializer struct {
	time.Time
	changed bool
}

// jobDetailsSerializer implements serializer for ScheduledJobDetails
type jobDetailsSerializer struct {
	*jobspb.ScheduledJobDetails
	changed bool
}

// scheduleDetailsSerializer implements serializer for ScheduleDetails
type scheduleDetailsSerializer struct {
	*jobspb.ScheduleDetails
	changed bool
}

// changeInfoSerializer implements serializer for ScheduleChangeInfo
type changeInfoSerializer struct {
	*jobspb.ScheduleChangeInfo
	changed bool
}

var _ serializer = &intSerializer{}
var _ serializer = &strSerializer{}
var _ serializer = &tzSerializer{}
var _ serializer = &jobDetailsSerializer{}
var _ serializer = &scheduleDetailsSerializer{}
var _ serializer = &changeInfoSerializer{}

func (i *intSerializer) FromDatum(d tree.Datum) error {
	if num, ok := d.(*tree.DInt); ok {
		i.int64 = int64(*num)
		return nil
	}
	return fmt.Errorf("expected int, found %T instead", d)
}

func (i *intSerializer) ToDatum() (tree.Datum, error) {
	return tree.NewDInt(tree.DInt(i.int64)), nil
}

func (i *intSerializer) Value() int64 {
	return i.int64
}

func (i *intSerializer) Set(v int64) {
	i.int64 = v
	i.changed = true
}

func (s *strSerializer) FromDatum(d tree.Datum) error {
	if d == tree.DNull {
		return nil
	}

	if str, ok := d.(*tree.DString); ok {
		s.string = string(*str)
		return nil
	}
	return fmt.Errorf("expected string, found %T instead", d)
}

func (s *strSerializer) ToDatum() (tree.Datum, error) {
	if len(s.string) == 0 {
		return tree.DNull, nil
	}
	return tree.NewDString(s.string), nil
}

func (s *strSerializer) Set(val string) {
	s.string = val
	s.changed = true
}

func (s *strSerializer) Value() string {
	return s.string
}

func (t *tzSerializer) FromDatum(d tree.Datum) error {
	if d == tree.DNull {
		return nil
	}

	if ts, ok := d.(*tree.DTimestampTZ); ok {
		t.Time = ts.Time
		return nil
	}
	return fmt.Errorf("expected string, found %T instead", d)
}

func (t *tzSerializer) ToDatum() (tree.Datum, error) {
	if t.Time.Equal(time.Time{}) {
		return tree.DNull, nil
	}
	return tree.MakeDTimestampTZ(t.Time, time.Nanosecond)
}

func (t *tzSerializer) Set(time time.Time) {
	t.Time = time
	t.changed = true
}

func (t *tzSerializer) Value() time.Time {
	return t.Time
}

func unmarshalProto(d tree.Datum, message protoutil.Message) error {
	if d == tree.DNull {
		return nil
	}
	if data, ok := d.(*tree.DBytes); ok {
		return protoutil.Unmarshal([]byte(*data), message)
	}
	return fmt.Errorf("expected bytes, found %T instead", d)
}

func marshalProto(message protoutil.Message) (tree.Datum, error) {
	data := make([]byte, message.Size())
	if _, err := message.MarshalTo(data); err != nil {
		return nil, err
	}
	return tree.NewDBytes(tree.DBytes(data)), nil
}

func (s *jobDetailsSerializer) Mutable() *jobspb.ScheduledJobDetails {
	s.changed = true
	if s.ScheduledJobDetails == nil {
		s.ScheduledJobDetails = &jobspb.ScheduledJobDetails{}
	}
	return s.ScheduledJobDetails
}

func (s *jobDetailsSerializer) Value() *jobspb.ScheduledJobDetails {
	return s.ScheduledJobDetails
}

func (s *jobDetailsSerializer) ToDatum() (tree.Datum, error) {
	return marshalProto(s.ScheduledJobDetails)
}

func (s *jobDetailsSerializer) FromDatum(d tree.Datum) error {
	s.ScheduledJobDetails = &jobspb.ScheduledJobDetails{}
	if d == tree.DNull {
		return nil
	}
	return unmarshalProto(d, s.ScheduledJobDetails)
}

func (s *scheduleDetailsSerializer) Mutable() *jobspb.ScheduleDetails {
	s.changed = true
	if s.ScheduleDetails == nil {
		s.ScheduleDetails = &jobspb.ScheduleDetails{}
	}
	return s.ScheduleDetails
}

func (s *scheduleDetailsSerializer) Value() *jobspb.ScheduleDetails {
	return s.ScheduleDetails
}

func (s *scheduleDetailsSerializer) FromDatum(d tree.Datum) error {
	s.ScheduleDetails = &jobspb.ScheduleDetails{}
	if d == tree.DNull {
		return nil
	}
	return unmarshalProto(d, s.ScheduleDetails)
}

func (s *scheduleDetailsSerializer) ToDatum() (tree.Datum, error) {
	return marshalProto(s.ScheduleDetails)
}

func (s *changeInfoSerializer) Mutable() *jobspb.ScheduleChangeInfo {
	s.changed = true
	if s.ScheduleChangeInfo == nil {
		s.ScheduleChangeInfo = &jobspb.ScheduleChangeInfo{}
	}
	return s.ScheduleChangeInfo
}

func (s *changeInfoSerializer) Value() *jobspb.ScheduleChangeInfo {
	return s.ScheduleChangeInfo
}

func (s *changeInfoSerializer) FromDatum(d tree.Datum) error {
	s.ScheduleChangeInfo = &jobspb.ScheduleChangeInfo{}
	if d == tree.DNull {
		return nil
	}
	return unmarshalProto(d, s.ScheduleChangeInfo)
}

func (s *changeInfoSerializer) ToDatum() (tree.Datum, error) {
	return marshalProto(s.ScheduleChangeInfo)
}
