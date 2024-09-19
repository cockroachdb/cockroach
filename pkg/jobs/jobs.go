// Copyright 2017 The Cockroach Authors.
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
	"bytes"
	"context"
	gojson "encoding/json"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/gogo/protobuf/jsonpb"
)

// Job manages logging the progress of long-running system processes, like
// backups and restores, to the system.jobs table.
type Job struct {
	// TODO(benesch): avoid giving Job a reference to Registry. This will likely
	// require inverting control: rather than having the worker call Created,
	// Started, etc., have Registry call a setupFn and a workFn as appropriate.
	registry *Registry

	id        jobspb.JobID
	createdBy *CreatedByInfo
	session   sqlliveness.Session
	mu        struct {
		syncutil.Mutex
		payload  jobspb.Payload
		progress jobspb.Progress
		status   Status
		runStats *RunStats
	}
}

// CreatedByInfo encapsulates the type and the ID of the system which created
// this job.
type CreatedByInfo struct {
	Name string
	ID   int64
}

// ScheduleID return ID as a [jobspb.ScheduleID] iff Name is
// [CreatedByScheduledJobs]. Otherwise it returns [jobspb.InvalidScheduleID],
// the zero value.
func (i *CreatedByInfo) ScheduleID() jobspb.ScheduleID {
	if i.Name == CreatedByScheduledJobs {
		return jobspb.ScheduleID(i.ID)
	}
	return jobspb.InvalidScheduleID
}

// Record bundles together the user-managed fields in jobspb.Payload.
type Record struct {
	JobID         jobspb.JobID
	Description   string
	Statements    []string
	Username      username.SQLUsername
	DescriptorIDs descpb.IDs
	Details       jobspb.Details
	Progress      jobspb.ProgressDetails
	RunningStatus RunningStatus
	// NonCancelable is used to denote when a job cannot be canceled. This field
	// will not be respected in mixed version clusters where some nodes have
	// a version < 20.1, so it can only be used in cases where all nodes having
	// versions >= 20.1 is guaranteed.
	NonCancelable bool
	// CreatedBy, if set, annotates this record with the information on
	// this job creator.
	CreatedBy *CreatedByInfo
	// MaximumPTSAge specifies the maximum age of PTS record held by a job.
	// 0 means no limit.
	MaximumPTSAge time.Duration
}

// AppendDescription appends description to this records Description with a
// ';' separator.
func (r *Record) AppendDescription(description string) {
	if len(r.Description) == 0 {
		r.Description = description
		return
	}
	r.Description = r.Description + "; " + description
}

// SetNonCancelable sets NonCancelable of this Record to the value returned from
// updateFn.
func (r *Record) SetNonCancelable(ctx context.Context, updateFn NonCancelableUpdateFn) {
	r.NonCancelable = updateFn(ctx, r.NonCancelable)
}

// StartableJob is a job created with a transaction to be started later.
// See Registry.CreateStartableJob
type StartableJob struct {
	*Job
	txn        *kv.Txn
	resumer    Resumer
	resumerCtx context.Context
	cancel     context.CancelFunc
	execDone   chan struct{}
	execErr    error
	starts     int64 // used to detect multiple calls to Start()
}

// TraceableJob is associated with a Job object, and can be used to configure
// the root tracing span that will be tied to the jobs' execution.
// By default, a job will create a `noop` root span that will discard all
// recordings.
type TraceableJob interface {
	// ForceRealSpan forces the registry to create a real Span instead of a
	// low-overhead non-recordable noop span.
	ForceRealSpan() bool
	// DumpTraceAfterRun determines whether the job's trace is dumped to disk at
	// the end of every adoption.
	DumpTraceAfterRun() bool
}

func init() {
	// NB: This exists to make the jobs payload usable during testrace. See the
	// comment on protoutil.Clone and the implementation of Marshal when run under
	// race.
	var jobPayload jobspb.Payload
	jobsDetailsInterfaceType := reflect.TypeOf(&jobPayload.Details).Elem()
	var jobProgress jobspb.Progress
	jobsProgressDetailsInterfaceType := reflect.TypeOf(&jobProgress.Details).Elem()
	protoutil.RegisterUnclonableType(jobsDetailsInterfaceType, reflect.Array)
	protoutil.RegisterUnclonableType(jobsProgressDetailsInterfaceType, reflect.Array)

}

// Status represents the status of a job in the system.jobs table.
type Status string

// SafeValue implements redact.SafeValue.
func (s Status) SafeValue() {}

var _ redact.SafeValue = Status("")

// RunningStatus represents the more detailed status of a running job in
// the system.jobs table.
type RunningStatus string

// SafeValue implements redact.SafeValue.
func (s RunningStatus) SafeValue() {}

var _ redact.SafeValue = RunningStatus("")

const (
	// StatusPending is `for jobs that have been created but on which work has
	// not yet started.
	StatusPending Status = "pending"
	// StatusRunning is for jobs that are currently in progress.
	StatusRunning Status = "running"
	// StatusPaused is for jobs that are not currently performing work, but have
	// saved their state and can be resumed by the user later.
	StatusPaused Status = "paused"
	// StatusFailed is for jobs that failed.
	StatusFailed Status = "failed"
	// StatusReverting is for jobs that failed or were canceled and their changes are being
	// being reverted.
	StatusReverting Status = "reverting"
	// StatusSucceeded is for jobs that have successfully completed.
	StatusSucceeded Status = "succeeded"
	// StatusCanceled is for jobs that were explicitly canceled by the user and
	// cannot be resumed.
	StatusCanceled Status = "canceled"
	// StatusCancelRequested is for jobs that were requested to be canceled by
	// the user but may be still running Resume. The node that is running the job
	// will change it to StatusReverting the next time it runs maybeAdoptJobs.
	StatusCancelRequested Status = "cancel-requested"
	// StatusPauseRequested is for jobs that were requested to be paused by the
	// user but may be still resuming or reverting. The node that is running the
	// job will change its state to StatusPaused the next time it runs
	// maybeAdoptJobs and will stop running it.
	StatusPauseRequested Status = "pause-requested"
	// StatusRevertFailed is for jobs that encountered an non-retryable error when
	// reverting their changes. Manual cleanup is required when a job ends up in
	// this state.
	StatusRevertFailed Status = "revert-failed"
)

var (
	errJobCanceled = errors.New("job canceled by user")
)

// HasErrJobCanceled returns true if the error contains the error set as the
// job's FinalResumeError when it has been canceled.
func HasErrJobCanceled(err error) bool {
	return errors.Is(err, errJobCanceled)
}

// Terminal returns whether this status represents a "terminal" state: a state
// after which the job should never be updated again.
func (s Status) Terminal() bool {
	return s == StatusFailed || s == StatusSucceeded || s == StatusCanceled || s == StatusRevertFailed
}

// ID returns the ID of the job.
func (j *Job) ID() jobspb.JobID {
	return j.id
}

// Session returns the underlying sqlliveness.Session associated with the job.
func (j *Job) Session() sqlliveness.Session {
	return j.session
}

// CreatedBy returns name/id of this job creator.  This will be nil if this information
// was not set.
func (j *Job) CreatedBy() *CreatedByInfo {
	return j.createdBy
}

// taskName is the name for the async task on the registry stopper that will
// execute this job.
func (j *Job) taskName() string {
	return fmt.Sprintf(`job-%d`, j.ID())
}

// Started marks the tracked job as started by updating status to running in
// jobs table.
func (u Updater) started(ctx context.Context) error {
	sp := tracing.SpanFromContext(ctx)
	traceID := tracingpb.TraceID(0)
	if sp != nil {
		traceID = sp.TraceID()
	}
	return u.Update(ctx, func(_ isql.Txn, md JobMetadata, ju *JobUpdater) error {
		if md.Status != StatusPending && md.Status != StatusRunning {
			return errors.Errorf("job with status %s cannot be marked started", md.Status)
		}
		if md.Payload.StartedMicros == 0 {
			ju.UpdateStatus(StatusRunning)
			md.Payload.StartedMicros = timeutil.ToUnixMicros(u.now())
			ju.UpdatePayload(md.Payload)
		}
		// md.RunStats can be nil because of the timing of version-update when exponential-backoff
		// gets activated. It may happen that backoff is not activated when Update() function was
		// called, which will cause to not populate md.RunStats. However, when the code reaches this
		// point, version update may have been updated to enable backoff. In this case, we can skip
		// updating num_runs and last_run, treating this job run as if backoff was not activated.
		//
		// TODO (sajjad): Update this comment after version 22.2 has been released.
		if md.RunStats != nil {
			ju.UpdateRunStats(md.RunStats.NumRuns+1, u.now())
		}
		if traceID != 0 && md.Progress != nil && md.Progress.TraceID != traceID {
			md.Progress.TraceID = traceID
			ju.UpdateProgress(md.Progress)
		}
		return nil
	})
}

// CheckStatus verifies the status of the job and returns an error if the job's
// status isn't Running or Reverting.
func (u Updater) CheckStatus(ctx context.Context) error {
	return u.Update(ctx, func(_ isql.Txn, md JobMetadata, _ *JobUpdater) error {
		return md.CheckRunningOrReverting()
	})
}

// RunningStatus updates the detailed status of a job currently in progress.
// It sets the job's RunningStatus field to the value returned by runningStatusFn
// and persists runningStatusFn's modifications to the job's details, if any.
func (u Updater) RunningStatus(ctx context.Context, runningStatus RunningStatus) error {
	return u.Update(ctx, func(_ isql.Txn, md JobMetadata, ju *JobUpdater) error {
		if err := md.CheckRunningOrReverting(); err != nil {
			return err
		}
		md.Progress.RunningStatus = string(runningStatus)
		ju.UpdateProgress(md.Progress)
		return nil
	})
}

// NonCancelableUpdateFn is a callback that computes a job's non-cancelable
// status given its current one.
type NonCancelableUpdateFn func(ctx context.Context, nonCancelable bool) bool

// FractionProgressedFn is a callback that computes a job's completion fraction
// given its details. It is safe to modify details in the callback; those
// modifications will be automatically persisted to the database record.
type FractionProgressedFn func(ctx context.Context, details jobspb.ProgressDetails) float32

// FractionUpdater returns a FractionProgressedFn that returns its argument.
func FractionUpdater(f float32) FractionProgressedFn {
	return func(ctx context.Context, details jobspb.ProgressDetails) float32 {
		return f
	}
}

// FractionProgressed updates the progress of the tracked job. It sets the job's
// FractionCompleted field to the value returned by progressedFn and persists
// progressedFn's modifications to the job's progress details, if any.
//
// Jobs for which progress computations do not depend on their details can
// use the FractionUpdater helper to construct a ProgressedFn.
func (u Updater) FractionProgressed(ctx context.Context, progressedFn FractionProgressedFn) error {
	return u.Update(ctx, func(_ isql.Txn, md JobMetadata, ju *JobUpdater) error {
		if err := md.CheckRunningOrReverting(); err != nil {
			return err
		}
		fractionCompleted := progressedFn(ctx, md.Progress.Details)

		if !build.IsRelease() {
			// We allow for slight floating-point rounding
			// inaccuracies. We only want to error in non-release
			// builds because in large production installations the
			// method at least one job uses to calculate process can
			// result in substantial floating point inaccuracy.
			if fractionCompleted < 0.0 || fractionCompleted > 1.01 {
				return errors.Errorf(
					"fraction completed %f is outside allowable range [0.0, 1.01]",
					fractionCompleted,
				)
			}
		}

		// Clamp to [0.0, 1.0].
		if fractionCompleted > 1.0 {
			log.VInfof(ctx, 1, "clamping fraction completed %f to [0.0, 1.0]", fractionCompleted)
			fractionCompleted = 1.0
		} else if fractionCompleted < 0.0 {
			log.VInfof(ctx, 1, "clamping fraction completed %f to [0.0, 1.0]", fractionCompleted)
			fractionCompleted = 0
		}

		md.Progress.Progress = &jobspb.Progress_FractionCompleted{
			FractionCompleted: fractionCompleted,
		}
		ju.UpdateProgress(md.Progress)
		return nil
	})
}

// CancelRequested sets the status of the tracked job to cancel-requested. It
// does not directly cancel the job; like job.Paused, it expects the job to call
// job.Progressed soon, observe a "job is cancel-requested" error, and abort.
// Further the node the runs the job will actively cancel it when it notices
// that it is in state StatusCancelRequested and will move it to state
// StatusReverting.
func (u Updater) CancelRequested(ctx context.Context) error {
	return u.Update(ctx, func(txn isql.Txn, md JobMetadata, ju *JobUpdater) error {
		return ju.CancelRequestedWithReason(ctx, md, errJobCanceled)
	})
}

// onPauseRequestFunc is a function used to perform action on behalf of a job
// implementation when a pause is requested.
type onPauseRequestFunc func(ctx context.Context, md JobMetadata, ju *JobUpdater) error

// PauseRequestedWithFunc sets the status of the tracked job to pause-requested.
// It does not directly pause the job; it expects the node that runs the job will
// actively cancel it when it notices that it is in state StatusPauseRequested
// and will move it to state StatusPaused. If a function is passed, it will be
// used to update the job state. If the job has builtin logic to run upon
// pausing, it will be ignored; use PauseRequested if you want that logic to run.
func (u Updater) PauseRequestedWithFunc(
	ctx context.Context, fn onPauseRequestFunc, reason string,
) error {
	return u.Update(ctx, func(txn isql.Txn, md JobMetadata, ju *JobUpdater) error {
		return ju.PauseRequestedWithFunc(ctx, txn, md, fn, reason)
	})
}

// reverted sets the status of the tracked job to reverted.
func (u Updater) reverted(
	ctx context.Context, err error, fn func(context.Context, isql.Txn) error,
) error {
	sp := tracing.SpanFromContext(ctx)
	traceID := tracingpb.TraceID(0)
	if sp != nil {
		traceID = sp.TraceID()
	}

	return u.Update(ctx, func(txn isql.Txn, md JobMetadata, ju *JobUpdater) error {
		if md.Status != StatusReverting &&
			md.Status != StatusCancelRequested &&
			md.Status != StatusRunning &&
			md.Status != StatusPending {
			return fmt.Errorf("job with status %s cannot be reverted", md.Status)
		}
		if md.Status != StatusReverting {
			if fn != nil {
				if err := fn(ctx, txn); err != nil {
					return err
				}
			}
			if err != nil {
				md.Payload.Error = err.Error()
				encodedErr := errors.EncodeError(ctx, err)
				md.Payload.FinalResumeError = &encodedErr
				ju.UpdatePayload(md.Payload)
			} else {
				if md.Payload.FinalResumeError == nil {
					return errors.AssertionFailedf(
						"tried to mark job as reverting, but no error was provided or recorded")
				}
			}
			ju.UpdateStatus(StatusReverting)
		}
		// md.RunStats will be nil if clusterversion.RetryJobsWithExponentialBackoff
		// was not active when Update was called above. In this case, we skip updating
		// the runStats, treating this job run as if backoff is not active.
		//
		// TODO (sajjad): Update this comment after version 22.2 has been released.
		if md.RunStats != nil {
			// We can reach here due to a failure or due to the job being canceled.
			// We should reset the exponential backoff parameters if the job was not
			// canceled. Note that md.Status will be StatusReverting if the job
			// was canceled.
			numRuns := md.RunStats.NumRuns + 1
			if md.Status != StatusReverting {
				// Reset the number of runs to speed up reverting.
				numRuns = 1
			}
			ju.UpdateRunStats(numRuns, u.now())
		}
		if traceID != 0 && md.Progress != nil && md.Progress.TraceID != traceID {
			md.Progress.TraceID = traceID
			ju.UpdateProgress(md.Progress)
		}
		return nil
	})
}

// Canceled sets the status of the tracked job to cancel.
func (u Updater) canceled(ctx context.Context) error {
	return u.Update(ctx, func(txn isql.Txn, md JobMetadata, ju *JobUpdater) error {
		if md.Status == StatusCanceled {
			return nil
		}
		if md.Status != StatusReverting {
			return fmt.Errorf("job with status %s cannot be requested to be canceled", md.Status)
		}
		ju.UpdateStatus(StatusCanceled)
		var now time.Time
		if u.j.registry.knobs.StubTimeNow != nil {
			now = u.j.registry.knobs.StubTimeNow()
		} else {
			now = u.j.registry.clock.Now().GoTime()
		}
		md.Payload.FinishedMicros = timeutil.ToUnixMicros(now)
		ju.UpdatePayload(md.Payload)
		return nil
	})
}

// Failed marks the tracked job as having failed with the given error.
func (u Updater) failed(ctx context.Context, err error) error {
	return u.Update(ctx, func(txn isql.Txn, md JobMetadata, ju *JobUpdater) error {
		// TODO(spaskob): should we fail if the terminal state is not StatusFailed?
		if md.Status.Terminal() {
			// Already done - do nothing.
			return nil
		}

		// TODO (sajjad): We don't have any checks for state transitions here. Consequently,
		// a pause-requested job can transition to failed, which may or may not be
		// acceptable depending on the job.
		ju.UpdateStatus(StatusFailed)

		// Truncate all errors to avoid large rows in the jobs
		// table.
		const (
			jobErrMaxRuneCount    = 1024
			jobErrTruncatedMarker = " -- TRUNCATED"
		)
		errStr := err.Error()
		if len(errStr) > jobErrMaxRuneCount {
			errStr = util.TruncateString(errStr, jobErrMaxRuneCount) + jobErrTruncatedMarker
		}
		md.Payload.Error = errStr

		md.Payload.FinishedMicros = timeutil.ToUnixMicros(u.now())
		ju.UpdatePayload(md.Payload)
		return nil
	})
}

// RevertFailed marks the tracked job as having failed during revert with the
// given error. Manual cleanup is required when the job is in this state.
func (u Updater) revertFailed(
	ctx context.Context, err error, fn func(context.Context, isql.Txn) error,
) error {
	return u.Update(ctx, func(txn isql.Txn, md JobMetadata, ju *JobUpdater) error {
		if md.Status != StatusReverting {
			return fmt.Errorf("job with status %s cannot fail during a revert", md.Status)
		}
		if fn != nil {
			if err := fn(ctx, txn); err != nil {
				return err
			}
		}
		ju.UpdateStatus(StatusRevertFailed)
		md.Payload.FinishedMicros = timeutil.ToUnixMicros(u.j.registry.clock.Now().GoTime())
		md.Payload.Error = err.Error()
		ju.UpdatePayload(md.Payload)
		return nil
	})
}

// succeeded marks the tracked job as having succeeded and sets its fraction
// completed to 1.0.
func (u Updater) succeeded(ctx context.Context, fn func(context.Context, isql.Txn) error) error {
	return u.Update(ctx, func(txn isql.Txn, md JobMetadata, ju *JobUpdater) error {
		if md.Status == StatusSucceeded {
			return nil
		}
		if md.Status != StatusRunning && md.Status != StatusPending {
			return errors.Errorf("job with status %s cannot be marked as succeeded", md.Status)
		}
		if fn != nil {
			if err := fn(ctx, txn); err != nil {
				return err
			}
		}
		ju.UpdateStatus(StatusSucceeded)
		md.Payload.FinishedMicros = timeutil.ToUnixMicros(u.j.registry.clock.Now().GoTime())
		ju.UpdatePayload(md.Payload)
		md.Progress.Progress = &jobspb.Progress_FractionCompleted{
			FractionCompleted: 1.0,
		}
		ju.UpdateProgress(md.Progress)
		return nil
	})
}

// SetDetails sets the details field of the currently running tracked job.
func (u Updater) SetDetails(ctx context.Context, details interface{}) error {
	return u.Update(ctx, func(txn isql.Txn, md JobMetadata, ju *JobUpdater) error {
		if err := md.CheckRunningOrReverting(); err != nil {
			return err
		}
		md.Payload.Details = jobspb.WrapPayloadDetails(details)
		ju.UpdatePayload(md.Payload)
		return nil
	})
}

// SetProgress sets the details field of the currently running tracked job.
func (u Updater) SetProgress(ctx context.Context, details interface{}) error {
	return u.Update(ctx, func(txn isql.Txn, md JobMetadata, ju *JobUpdater) error {
		if err := md.CheckRunningOrReverting(); err != nil {
			return err
		}
		md.Progress.Details = jobspb.WrapProgressDetails(details)
		ju.UpdateProgress(md.Progress)
		return nil
	})
}

// Payload returns the most recently sent Payload for this Job.
func (j *Job) Payload() jobspb.Payload {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.mu.payload
}

// Progress returns the most recently sent Progress for this Job.
func (j *Job) Progress() jobspb.Progress {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.mu.progress
}

// Details returns the details from the most recently sent Payload for this Job.
func (j *Job) Details() jobspb.Details {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.mu.payload.UnwrapDetails()
}

// Status returns the status of the job. It will be "" if the status has
// not been set or the job has never been loaded.
func (j *Job) Status() Status {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.mu.status
}

// FractionCompleted returns completion according to the in-memory job state.
func (j *Job) FractionCompleted() float32 {
	progress := j.Progress()
	return progress.GetFractionCompleted()
}

// MarkIdle marks the job as Idle.  Idleness should not be toggled frequently
// (no more than ~twice a minute) as the action is logged.
func (j *Job) MarkIdle(isIdle bool) {
	j.registry.MarkIdle(j, isIdle)
}

// JobNotFoundError is returned from load when the job does not exist.
type JobNotFoundError struct {
	jobID     jobspb.JobID
	sessionID sqlliveness.SessionID
}

// Error makes JobNotFoundError an error.
func (e *JobNotFoundError) Error() string {
	if e.sessionID != "" {
		return fmt.Sprintf("job with ID %d does not exist with claim session id %q", e.jobID, e.sessionID.String())
	}
	return fmt.Sprintf("job with ID %d does not exist", e.jobID)
}

// HasJobNotFoundError returns true if the error contains a JobNotFoundError.
func HasJobNotFoundError(err error) bool {
	return errors.HasType(err, (*JobNotFoundError)(nil))
}

func (j *Job) loadJobPayloadAndProgress(
	ctx context.Context, st *cluster.Settings, txn isql.Txn,
) (*jobspb.Payload, *jobspb.Progress, error) {
	if txn == nil {
		return nil, nil, errors.New("cannot load job payload and progress with a nil txn")
	}

	payload := &jobspb.Payload{}
	progress := &jobspb.Progress{}
	infoStorage := j.InfoStorage(txn)

	payloadBytes, exists, err := infoStorage.GetLegacyPayload(ctx)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to get payload for job %d", j.ID())
	}
	if !exists {
		return nil, nil, errors.Wrap(&JobNotFoundError{jobID: j.ID()}, "job payload not found in system.job_info")
	}
	if err := protoutil.Unmarshal(payloadBytes, payload); err != nil {
		return nil, nil, err
	}

	progressBytes, exists, err := infoStorage.GetLegacyProgress(ctx)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to get progress for job %d", j.ID())
	}
	if !exists {
		return nil, nil, errors.Wrap(&JobNotFoundError{jobID: j.ID()}, "job progress not found in system.job_info")
	}
	if err := protoutil.Unmarshal(progressBytes, progress); err != nil {
		return nil, nil, &JobNotFoundError{jobID: j.ID()}
	}

	return payload, progress, nil
}

func (u Updater) load(ctx context.Context) (retErr error) {
	if u.txn == nil {
		return u.j.registry.db.Txn(ctx, func(
			ctx context.Context, txn isql.Txn,
		) error {
			u.txn = txn
			return u.load(ctx)
		})
	}
	ctx, sp := tracing.ChildSpan(ctx, "load-job")
	defer sp.Finish()

	var payload *jobspb.Payload
	var progress *jobspb.Progress
	var createdBy *CreatedByInfo
	var status Status
	j := u.j
	defer func() {
		if retErr != nil {
			return
		}
		j.mu.Lock()
		defer j.mu.Unlock()
		j.mu.payload = *payload
		j.mu.progress = *progress
		j.mu.status = status
		j.createdBy = createdBy
	}()

	const (
		queryNoSessionID   = "SELECT created_by_type, created_by_id, status FROM system.jobs WHERE id = $1"
		queryWithSessionID = queryNoSessionID + " AND claim_session_id = $2"
	)
	sess := sessiondata.NodeUserSessionDataOverride

	var err error
	var row tree.Datums
	if j.session == nil {
		row, err = u.txn.QueryRowEx(ctx, "load-job-query", u.txn.KV(), sess,
			queryNoSessionID, j.ID())
	} else {
		row, err = u.txn.QueryRowEx(ctx, "load-job-query", u.txn.KV(), sess,
			queryWithSessionID, j.ID(), j.session.ID().UnsafeBytes())
	}
	if err != nil {
		return err
	}
	if row == nil {
		return &JobNotFoundError{jobID: j.ID()}
	}
	createdBy, err = unmarshalCreatedBy(row[0], row[1])
	if err != nil {
		return err
	}
	status, err = unmarshalStatus(row[2])
	if err != nil {
		return err
	}

	payload, progress, err = j.loadJobPayloadAndProgress(ctx, j.registry.settings, u.txn)
	return err
}

// UnmarshalPayload unmarshals and returns the Payload encoded in the input
// datum, which should be a tree.DBytes.
func UnmarshalPayload(datum tree.Datum) (*jobspb.Payload, error) {
	payload := &jobspb.Payload{}
	bytes, ok := datum.(*tree.DBytes)
	if !ok {
		return nil, errors.Errorf(
			"job: failed to unmarshal payload as DBytes (was %T)", datum)
	}
	if err := protoutil.Unmarshal([]byte(*bytes), payload); err != nil {
		return nil, err
	}
	return payload, nil
}

// UnmarshalProgress unmarshals and returns the Progress encoded in the input
// datum, which should be a tree.DBytes.
func UnmarshalProgress(datum tree.Datum) (*jobspb.Progress, error) {
	progress := &jobspb.Progress{}
	bytes, ok := datum.(*tree.DBytes)
	if !ok {
		return nil, errors.Errorf(
			"job: failed to unmarshal Progress as DBytes (was %T)", datum)
	}
	if err := protoutil.Unmarshal([]byte(*bytes), progress); err != nil {
		return nil, err
	}
	return progress, nil
}

// unmarshalCreatedBy unmarshals and returns created_by_type and created_by_id datums
// which may be tree.DNull, or tree.DString and tree.DInt respectively.
func unmarshalCreatedBy(createdByType, createdByID tree.Datum) (*CreatedByInfo, error) {
	if createdByType == tree.DNull || createdByID == tree.DNull {
		return nil, nil
	}
	if ds, ok := createdByType.(*tree.DString); ok {
		if id, ok := createdByID.(*tree.DInt); ok {
			return &CreatedByInfo{Name: string(*ds), ID: int64(*id)}, nil
		}
		return nil, errors.Errorf(
			"job: failed to unmarshal created_by_type as DInt (was %T)", createdByID)
	}
	return nil, errors.Errorf(
		"job: failed to unmarshal created_by_type as DString (was %T)", createdByType)
}

func unmarshalStatus(datum tree.Datum) (Status, error) {
	statusString, ok := datum.(*tree.DString)
	if !ok {
		return "", errors.AssertionFailedf("expected string status, but got %T", datum)
	}
	return Status(*statusString), nil
}

// getRunStats returns the RunStats for a job. If they are not set, it will
// return a zero-value.
func (j *Job) getRunStats() (rs RunStats) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.mu.runStats != nil {
		rs = *j.mu.runStats
	}
	return rs
}

// Start will resume the job. The transaction used to create the StartableJob
// must be committed. If a non-nil error is returned, the job was not started
// and nothing will be send on errCh. Clients must not start jobs more than
// once.
func (sj *StartableJob) Start(ctx context.Context) (err error) {
	if alreadyStarted := sj.recordStart(); alreadyStarted {
		return errors.AssertionFailedf(
			"StartableJob %d cannot be started more than once", sj.ID())
	}

	if sj.session == nil {
		return errors.AssertionFailedf(
			"StartableJob %d cannot be started without sqlliveness session", sj.ID())
	}

	defer func() {
		if err != nil {
			sj.registry.unregister(sj.ID())
		}
	}()
	if !sj.txn.IsCommitted() {
		return fmt.Errorf("cannot resume %T job which is not committed", sj.resumer)
	}

	if err := sj.registry.stopper.RunAsyncTask(ctx, sj.taskName(), func(_ context.Context) {
		resumeCtx, cancel := sj.registry.stopper.WithCancelOnQuiesce(sj.resumerCtx)
		defer cancel()
		sj.execErr = sj.registry.runJob(resumeCtx, sj.resumer, sj.Job, StatusRunning, sj.taskName())
		close(sj.execDone)
	}); err != nil {
		return err
	}

	return nil
}

// AwaitCompletion waits for the job to finish execution, or context cancellation.
// Requires Start() has been called.
// The returned error code is the error code returned by the job execution itself.
// Nil error code implies successful completion. The non-nil value does not
// imply the job failed -- it just means that this registry encountered an error
// while executing this job.  The job may still be running (on another node), and
// may complete successfully.
func (sj *StartableJob) AwaitCompletion(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-sj.execDone:
		return sj.execErr
	}
}

// ReportExecutionResults sends execution results to the specified channel.
// The method should be called after the job completes successfully.
// Calling this method prior to completion may return partial results.
func (sj *StartableJob) ReportExecutionResults(
	ctx context.Context, resultsCh chan<- tree.Datums,
) error {
	if reporter, ok := sj.resumer.(JobResultsReporter); ok {
		return reporter.ReportResults(ctx, resultsCh)
	}
	return errors.AssertionFailedf("job does not produce results")
}

// CleanupOnRollback will unregister the job in the case that the creating
// transaction has been rolled back. It will return an assertion failure if
// the transaction used to create this job is committed. It will return an
// error but proceed with cleanup if the transaction is still open under
// the assumption that this is being called due to an unrecoverable error
// that prevented the transaction from being cleaned up.
func (sj *StartableJob) CleanupOnRollback(ctx context.Context) error {

	if sj.txn.IsCommitted() && ctx.Err() == nil {
		return errors.AssertionFailedf(
			"cannot call CleanupOnRollback for a StartableJob created by a committed transaction")
	}

	// Note that we check the context error because when a context is canceled in
	// (*kv.DB).Txn() we move the cleanup to async. That async cleanup may not
	// have happened either due to a race or due to the server shutting down.
	// Another issue is that the cleanup may fail with an ambiguous error due to
	// networking problems leading the transaction in an undefined state.
	// Given that, proceed to clean up regardless.

	sj.registry.unregister(sj.ID())
	if sj.cancel != nil {
		sj.cancel()
	}

	// This is an error, but it's likely due to some shutdown behavior so do not
	// mark it as an assertion failure.
	if !sj.txn.Sender().TxnStatus().IsFinalized() && ctx.Err() == nil {
		return errors.New(
			"cannot call CleanupOnRollback for a StartableJob with a non-finalized transaction")
	}
	return nil
}

// Cancel will mark the job as canceled and release its resources in the
// Registry.
func (sj *StartableJob) Cancel(ctx context.Context) error {
	alreadyStarted := sj.recordStart() // prevent future start attempts
	defer func() {
		if alreadyStarted {
			sj.registry.cancelRegisteredJobContext(sj.ID())
		} else {
			sj.registry.unregister(sj.ID())
		}
	}()
	return sj.Job.NoTxn().CancelRequested(ctx)
}

func (sj *StartableJob) recordStart() (alreadyStarted bool) {
	return atomic.AddInt64(&sj.starts, 1) != 1
}

// ParseRetriableExecutionErrorLogFromJSON inverts the output of
// FormatRetriableExecutionErrorLogToJSON.
func ParseRetriableExecutionErrorLogFromJSON(
	log []byte,
) ([]*jobspb.RetriableExecutionFailure, error) {
	var jsonArr []gojson.RawMessage
	if err := gojson.Unmarshal(log, &jsonArr); err != nil {
		return nil, errors.Wrap(err, "failed to decode json array for execution log")
	}
	ret := make([]*jobspb.RetriableExecutionFailure, len(jsonArr))

	json := jsonpb.Unmarshaler{AllowUnknownFields: true}
	var reader bytes.Reader
	for i, data := range jsonArr {
		msgI, err := protoreflect.NewMessage("cockroach.sql.jobs.jobspb.RetriableExecutionFailure")
		if err != nil {
			return nil, errors.WithAssertionFailure(err)
		}
		msg := msgI.(*jobspb.RetriableExecutionFailure)
		reader.Reset(data)
		if err := json.Unmarshal(&reader, msg); err != nil {
			return nil, err
		}
		ret[i] = msg
	}
	return ret, nil
}

// FormatRetriableExecutionErrorLogToJSON extracts the events
// stored in the payload, formats them into a json array. This function
// is intended for use with crdb_internal.jobs. Note that the error will
// be flattened into a string and stored in the TruncatedError field.
func FormatRetriableExecutionErrorLogToJSON(
	ctx context.Context, log []*jobspb.RetriableExecutionFailure,
) (*tree.DJSON, error) {
	ab := json.NewArrayBuilder(len(log))
	for i := range log {
		ev := *log[i]
		if ev.Error != nil {
			ev.TruncatedError = errors.DecodeError(ctx, *ev.Error).Error()
			ev.Error = nil
		}
		msg, err := protoreflect.MessageToJSON(&ev, protoreflect.FmtFlags{
			EmitDefaults: false,
		})
		if err != nil {
			return nil, err
		}
		ab.Add(msg)
	}
	return tree.NewDJSON(ab.Build()), nil
}

// FormatRetriableExecutionErrorLogToStringArray extracts the events
// stored in the payload, formats them into strings and returns them as an
// array of strings. This function is intended for use with crdb_internal.jobs.
func FormatRetriableExecutionErrorLogToStringArray(
	ctx context.Context, log []*jobspb.RetriableExecutionFailure,
) *tree.DArray {
	arr := tree.NewDArray(types.String)
	for _, ev := range log {
		if ev == nil { // no reason this should happen, but be defensive
			continue
		}
		var cause error
		if ev.Error != nil {
			cause = errors.DecodeError(ctx, *ev.Error)
		} else {
			cause = fmt.Errorf("(truncated) %s", ev.TruncatedError)
		}
		msg := formatRetriableExecutionFailure(
			ev.InstanceID,
			Status(ev.Status),
			timeutil.FromUnixMicros(ev.ExecutionStartMicros),
			timeutil.FromUnixMicros(ev.ExecutionEndMicros),
			cause,
		)
		// We really don't care about errors here. I'd much rather see nothing
		// in my log than crash.
		_ = arr.Append(tree.NewDString(msg))
	}
	return arr
}

// GetJobTraceID returns the current trace ID of the job from the job progress.
func GetJobTraceID(ctx context.Context, db isql.DB, jobID jobspb.JobID) (tracingpb.TraceID, error) {
	var traceID tracingpb.TraceID
	if err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		jobInfo := InfoStorageForJob(txn, jobID)
		progressBytes, exists, err := jobInfo.GetLegacyProgress(ctx)
		if err != nil {
			return err
		}
		if !exists {
			return errors.New("progress not found")
		}
		var progress jobspb.Progress
		if err := protoutil.Unmarshal(progressBytes, &progress); err != nil {
			return errors.Wrap(err, "failed to unmarshal progress bytes")
		}
		traceID = progress.TraceID
		return nil
	}); err != nil {
		return 0, errors.Wrapf(err, "failed to fetch trace ID for job %d", jobID)
	}

	return traceID, nil
}

// LoadJobProgress returns the job progress from the info table. Note that the
// progress can be nil if none is recorded.
func LoadJobProgress(
	ctx context.Context, db isql.DB, jobID jobspb.JobID,
) (*jobspb.Progress, error) {
	var (
		progressBytes []byte
		exists        bool
	)
	if err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := InfoStorageForJob(txn, jobID)
		var err error
		progressBytes, exists, err = infoStorage.GetLegacyProgress(ctx)
		return err
	}); err != nil || !exists {
		return nil, err
	}
	progress := &jobspb.Progress{}
	if err := protoutil.Unmarshal(progressBytes, progress); err != nil {
		return nil, err
	}
	return progress, nil
}
