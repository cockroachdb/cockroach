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
	"context"
	"fmt"
	"reflect"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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
	sessionID sqlliveness.SessionID
	mu        struct {
		syncutil.Mutex
		payload  jobspb.Payload
		progress jobspb.Progress
	}
}

// CreatedByInfo encapsulates they type and the ID of the system which created
// this job.
type CreatedByInfo struct {
	Name string
	ID   int64
}

// Record bundles together the user-managed fields in jobspb.Payload.
type Record struct {
	Description   string
	Statements    []string
	Username      security.SQLUsername
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
	ForceRealSpan()
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

// SafeFormat implements redact.SafeFormatter.
func (s Status) SafeFormat(sp redact.SafePrinter, verb rune) {
	sp.SafeString(redact.SafeString(s))
}

var _ redact.SafeFormatter = Status("")

// RunningStatus represents the more detailed status of a running job in
// the system.jobs table.
type RunningStatus string

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
// job's FinalResumError when it has been canceled.
func HasErrJobCanceled(err error) bool {
	return errors.Is(err, errJobCanceled)
}

// deprecatedIsOldSchemaChangeJob returns whether the provided payload is for a
// job that is a 19.2-style schema change, and therefore cannot be run or
// updated in 20.1 (without first having undergone a migration).
// TODO (lucy): The plan is to mark all 19.2 jobs as failed in a 20.2 startup
// migration. Once we do that, this can remain as an assertion.
func deprecatedIsOldSchemaChangeJob(payload *jobspb.Payload) bool {
	schemaChangeDetails, ok := payload.UnwrapDetails().(jobspb.SchemaChangeDetails)
	return ok && schemaChangeDetails.FormatVersion < jobspb.JobResumerFormatVersion
}

// Terminal returns whether this status represents a "terminal" state: a state
// after which the job should never be updated again.
func (s Status) Terminal() bool {
	return s == StatusFailed || s == StatusSucceeded || s == StatusCanceled || s == StatusRevertFailed
}

// InvalidStatusError is the error returned when the desired operation is
// invalid given the job's current status.
type InvalidStatusError struct {
	id     jobspb.JobID
	status Status
	op     string
	err    string
}

func (e *InvalidStatusError) Error() string {
	if e.err != "" {
		return fmt.Sprintf("cannot %s %s job (id %d, err: %q)", e.op, e.status, e.id, e.err)
	}
	return fmt.Sprintf("cannot %s %s job (id %d)", e.op, e.status, e.id)
}

// SimplifyInvalidStatusError unwraps an *InvalidStatusError into an error
// message suitable for users. Other errors are returned as passed.
func SimplifyInvalidStatusError(err error) error {
	if ierr := (*InvalidStatusError)(nil); errors.As(err, &ierr) {
		return errors.Errorf("job %s", ierr.status)
	}
	return err
}

// ID returns the ID of the job.
func (j *Job) ID() jobspb.JobID {
	return j.id
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

// Started marks the tracked job as started.
func (j *Job) started(ctx context.Context, txn *kv.Txn) error {
	return j.Update(ctx, txn, func(_ *kv.Txn, md JobMetadata, ju *JobUpdater) error {
		if md.Status != StatusPending && md.Status != StatusRunning {
			return errors.Errorf("job with status %s cannot be marked started", md.Status)
		}
		// TODO(spaskob): Remove this status change after we stop supporting
		// pending job states.
		ju.UpdateStatus(StatusRunning)
		md.Payload.StartedMicros = timeutil.ToUnixMicros(j.registry.clock.Now().GoTime())
		ju.UpdatePayload(md.Payload)
		return nil
	})
}

// CheckStatus verifies the status of the job and returns an error if the job's
// status isn't Running or Reverting.
func (j *Job) CheckStatus(ctx context.Context, txn *kv.Txn) error {
	return j.Update(ctx, txn, func(_ *kv.Txn, md JobMetadata, _ *JobUpdater) error {
		return md.CheckRunningOrReverting()
	})
}

// CheckTerminalStatus returns true if the job is in a terminal status.
func (j *Job) CheckTerminalStatus(ctx context.Context, txn *kv.Txn) bool {
	err := j.Update(ctx, txn, func(_ *kv.Txn, md JobMetadata, _ *JobUpdater) error {
		if !md.Status.Terminal() {
			return &InvalidStatusError{md.ID, md.Status, "checking that job status is success", md.Payload.Error}
		}
		return nil
	})

	return err == nil
}

// RunningStatus updates the detailed status of a job currently in progress.
// It sets the job's RunningStatus field to the value returned by runningStatusFn
// and persists runningStatusFn's modifications to the job's details, if any.
func (j *Job) RunningStatus(
	ctx context.Context, txn *kv.Txn, runningStatusFn RunningStatusFn,
) error {
	return j.Update(ctx, txn, func(_ *kv.Txn, md JobMetadata, ju *JobUpdater) error {
		if err := md.CheckRunningOrReverting(); err != nil {
			return err
		}
		runningStatus, err := runningStatusFn(ctx, md.Progress.Details)
		if err != nil {
			return err
		}
		md.Progress.RunningStatus = string(runningStatus)
		ju.UpdateProgress(md.Progress)
		return nil
	})
}

// SetDescription updates the description of a created job.
func (j *Job) SetDescription(ctx context.Context, txn *kv.Txn, updateFn DescriptionUpdateFn) error {
	return j.Update(ctx, txn, func(_ *kv.Txn, md JobMetadata, ju *JobUpdater) error {
		prev := md.Payload.Description
		desc, err := updateFn(ctx, prev)
		if err != nil {
			return err
		}
		if prev != desc {
			md.Payload.Description = desc
			ju.UpdatePayload(md.Payload)
		}
		return nil
	})
}

// SetNonCancelable updates the NonCancelable field of a created job.
func (j *Job) SetNonCancelable(
	ctx context.Context, txn *kv.Txn, updateFn NonCancelableUpdateFn,
) error {
	return j.Update(ctx, txn, func(_ *kv.Txn, md JobMetadata, ju *JobUpdater) error {
		prev := md.Payload.Noncancelable
		newStatus := updateFn(ctx, prev)
		if prev != newStatus {
			md.Payload.Noncancelable = newStatus
			ju.UpdatePayload(md.Payload)
		}
		return nil
	})
}

// RunningStatusFn is a callback that computes a job's running status
// given its details. It is safe to modify details in the callback; those
// modifications will be automatically persisted to the database record.
type RunningStatusFn func(ctx context.Context, details jobspb.Details) (RunningStatus, error)

// DescriptionUpdateFn is a callback that computes a job's description
// given its current one.
type DescriptionUpdateFn func(ctx context.Context, description string) (string, error)

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
func (j *Job) FractionProgressed(
	ctx context.Context, txn *kv.Txn, progressedFn FractionProgressedFn,
) error {
	return j.Update(ctx, txn, func(_ *kv.Txn, md JobMetadata, ju *JobUpdater) error {
		if err := md.CheckRunningOrReverting(); err != nil {
			return err
		}
		fractionCompleted := progressedFn(ctx, md.Progress.Details)
		// allow for slight floating-point rounding inaccuracies
		if fractionCompleted > 1.0 && fractionCompleted < 1.01 {
			fractionCompleted = 1.0
		}
		if fractionCompleted < 0.0 || fractionCompleted > 1.0 {
			return errors.Errorf(
				"job %d: fractionCompleted %f is outside allowable range [0.0, 1.0]",
				j.ID(), fractionCompleted,
			)
		}
		md.Progress.Progress = &jobspb.Progress_FractionCompleted{
			FractionCompleted: fractionCompleted,
		}
		ju.UpdateProgress(md.Progress)
		return nil
	})
}

// paused sets the status of the tracked job to paused. It is called by the
// registry adoption loop by the node currently running a job to move it from
// pauseRequested to paused.
func (j *Job) paused(
	ctx context.Context, txn *kv.Txn, fn func(context.Context, *kv.Txn) error,
) error {
	return j.Update(ctx, txn, func(txn *kv.Txn, md JobMetadata, ju *JobUpdater) error {
		if md.Status == StatusPaused {
			// Already paused - do nothing.
			return nil
		}
		if md.Status != StatusPauseRequested {
			return fmt.Errorf("job with status %s cannot be set to paused", md.Status)
		}
		if fn != nil {
			if err := fn(ctx, txn); err != nil {
				return err
			}
		}
		ju.UpdateStatus(StatusPaused)
		return nil
	})
}

// unpaused sets the status of the tracked job to running or reverting iff the
// job is currently paused. It does not directly resume the job; rather, it
// expires the job's lease so that a Registry adoption loop detects it and
// resumes it.
func (j *Job) unpaused(ctx context.Context, txn *kv.Txn) error {
	return j.Update(ctx, txn, func(txn *kv.Txn, md JobMetadata, ju *JobUpdater) error {
		if md.Status == StatusRunning || md.Status == StatusReverting {
			// Already resumed - do nothing.
			return nil
		}
		if md.Status != StatusPaused {
			return fmt.Errorf("job with status %s cannot be resumed", md.Status)
		}
		// We use the absence of error to determine what state we should
		// resume into.
		if md.Payload.FinalResumeError == nil {
			ju.UpdateStatus(StatusRunning)
		} else {
			ju.UpdateStatus(StatusReverting)
		}
		ju.UpdatePayload(md.Payload)
		return nil
	})
}

// cancelRequested sets the status of the tracked job to cancel-requested. It
// does not directly cancel the job; like job.Paused, it expects the job to call
// job.Progressed soon, observe a "job is cancel-requested" error, and abort.
// Further the node the runs the job will actively cancel it when it notices
// that it is in state StatusCancelRequested and will move it to state
// StatusReverting.
func (j *Job) cancelRequested(
	ctx context.Context, txn *kv.Txn, fn func(context.Context, *kv.Txn) error,
) error {
	return j.Update(ctx, txn, func(txn *kv.Txn, md JobMetadata, ju *JobUpdater) error {
		// Don't allow 19.2-style schema change jobs to undergo changes in job state
		// before they undergo a migration to make them properly runnable in 20.1 and
		// later versions. While we could support cancellation in principle, the
		// point is to cut down on the number of possible states that the migration
		// could encounter.
		//
		// TODO (lucy): Remove this in 20.2.
		if deprecatedIsOldSchemaChangeJob(md.Payload) {
			return errors.Newf(
				"schema change job was created in earlier version, and cannot be " +
					"canceled in this version until the upgrade is finalized and an internal migration is complete")
		}

		if md.Payload.Noncancelable {
			return errors.Newf("job %d: not cancelable", j.ID())
		}
		if md.Status == StatusCancelRequested || md.Status == StatusCanceled {
			return nil
		}
		if md.Status != StatusPending && md.Status != StatusRunning && md.Status != StatusPaused {
			return fmt.Errorf("job with status %s cannot be requested to be canceled", md.Status)
		}
		if md.Status == StatusPaused && md.Payload.FinalResumeError != nil {
			decodedErr := errors.DecodeError(ctx, *md.Payload.FinalResumeError)
			return fmt.Errorf("job %d is paused and has non-nil FinalResumeError %s hence cannot be canceled and should be reverted", j.ID(), decodedErr.Error())
		}
		if fn != nil {
			if err := fn(ctx, txn); err != nil {
				return err
			}
		}
		ju.UpdateStatus(StatusCancelRequested)
		return nil
	})
}

// onPauseRequestFunc is a function used to perform action on behalf of a job
// implementation when a pause is requested.
type onPauseRequestFunc func(
	ctx context.Context, planHookState interface{}, txn *kv.Txn, progress *jobspb.Progress,
) error

// pauseRequested sets the status of the tracked job to pause-requested. It does
// not directly pause the job; it expects the node that runs the job will
// actively cancel it when it notices that it is in state StatusPauseRequested
// and will move it to state StatusPaused.
func (j *Job) pauseRequested(ctx context.Context, txn *kv.Txn, fn onPauseRequestFunc) error {
	return j.Update(ctx, txn, func(txn *kv.Txn, md JobMetadata, ju *JobUpdater) error {
		// Don't allow 19.2-style schema change jobs to undergo changes in job state
		// before they undergo a migration to make them properly runnable in 20.1 and
		// later versions.
		//
		// In particular, schema change jobs could not be paused in 19.2, so allowing
		// pausing here could break backward compatibility during an upgrade by
		// forcing 19.2 nodes to deal with a schema change job in a state that wasn't
		// possible in 19.2.
		//
		// TODO (lucy): Remove this in 20.2.
		if deprecatedIsOldSchemaChangeJob(md.Payload) {
			return errors.Newf(
				"schema change job was created in earlier version, and cannot be " +
					"paused in this version until the upgrade is finalized and an internal migration is complete")
		}

		if md.Status == StatusPauseRequested || md.Status == StatusPaused {
			return nil
		}
		if md.Status != StatusPending && md.Status != StatusRunning && md.Status != StatusReverting {
			return fmt.Errorf("job with status %s cannot be requested to be paused", md.Status)
		}
		if fn != nil {
			execCtx, cleanup := j.registry.execCtx("pause request", j.Payload().UsernameProto.Decode())
			defer cleanup()
			if err := fn(ctx, execCtx, txn, md.Progress); err != nil {
				return err
			}
			ju.UpdateProgress(md.Progress)
		}
		ju.UpdateStatus(StatusPauseRequested)
		log.Infof(ctx, "job %d: pause requested recorded", j.ID())
		return nil
	})
}

// reverted sets the status of the tracked job to reverted.
func (j *Job) reverted(
	ctx context.Context, txn *kv.Txn, err error, fn func(context.Context, *kv.Txn) error,
) error {
	return j.Update(ctx, txn, func(txn *kv.Txn, md JobMetadata, ju *JobUpdater) error {
		if md.Status == StatusReverting {
			return nil
		}
		if md.Status != StatusCancelRequested && md.Status != StatusRunning && md.Status != StatusPending {
			return fmt.Errorf("job with status %s cannot be reverted", md.Status)
		}
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
		return nil
	})
}

// Canceled sets the status of the tracked job to cancel.
func (j *Job) canceled(
	ctx context.Context, txn *kv.Txn, fn func(context.Context, *kv.Txn) error,
) error {
	return j.Update(ctx, txn, func(txn *kv.Txn, md JobMetadata, ju *JobUpdater) error {
		if md.Status == StatusCanceled {
			return nil
		}
		if md.Status != StatusReverting {
			return fmt.Errorf("job with status %s cannot be requested to be canceled", md.Status)
		}
		if fn != nil {
			if err := fn(ctx, txn); err != nil {
				return err
			}
		}
		ju.UpdateStatus(StatusCanceled)
		md.Payload.FinishedMicros = timeutil.ToUnixMicros(j.registry.clock.Now().GoTime())
		ju.UpdatePayload(md.Payload)
		return nil
	})
}

// Failed marks the tracked job as having failed with the given error.
func (j *Job) failed(
	ctx context.Context, txn *kv.Txn, err error, fn func(context.Context, *kv.Txn) error,
) error {
	return j.Update(ctx, txn, func(txn *kv.Txn, md JobMetadata, ju *JobUpdater) error {
		// TODO(spaskob): should we fail if the terminal state is not StatusFailed?
		if md.Status.Terminal() {
			// Already done - do nothing.
			return nil
		}
		if fn != nil {
			if err := fn(ctx, txn); err != nil {
				return err
			}
		}
		ju.UpdateStatus(StatusFailed)
		md.Payload.Error = err.Error()
		md.Payload.FinishedMicros = timeutil.ToUnixMicros(j.registry.clock.Now().GoTime())
		ju.UpdatePayload(md.Payload)
		return nil
	})
}

// RevertFailed marks the tracked job as having failed during revert with the
// given error. Manual cleanup is required when the job is in this state.
func (j *Job) revertFailed(
	ctx context.Context, txn *kv.Txn, err error, fn func(context.Context, *kv.Txn) error,
) error {
	return j.Update(ctx, txn, func(txn *kv.Txn, md JobMetadata, ju *JobUpdater) error {
		if md.Status != StatusReverting {
			return fmt.Errorf("job with status %s cannot fail during a revert", md.Status)
		}
		if fn != nil {
			if err := fn(ctx, txn); err != nil {
				return err
			}
		}
		ju.UpdateStatus(StatusRevertFailed)
		md.Payload.FinishedMicros = timeutil.ToUnixMicros(j.registry.clock.Now().GoTime())
		md.Payload.Error = err.Error()
		ju.UpdatePayload(md.Payload)
		return nil
	})
}

// succeeded marks the tracked job as having succeeded and sets its fraction
// completed to 1.0.
func (j *Job) succeeded(
	ctx context.Context, txn *kv.Txn, fn func(context.Context, *kv.Txn) error,
) error {
	return j.Update(ctx, txn, func(txn *kv.Txn, md JobMetadata, ju *JobUpdater) error {
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
		md.Payload.FinishedMicros = timeutil.ToUnixMicros(j.registry.clock.Now().GoTime())
		ju.UpdatePayload(md.Payload)
		md.Progress.Progress = &jobspb.Progress_FractionCompleted{
			FractionCompleted: 1.0,
		}
		ju.UpdateProgress(md.Progress)
		return nil
	})
}

// SetDetails sets the details field of the currently running tracked job.
func (j *Job) SetDetails(ctx context.Context, txn *kv.Txn, details interface{}) error {
	return j.Update(ctx, txn, func(txn *kv.Txn, md JobMetadata, ju *JobUpdater) error {
		if err := md.CheckRunningOrReverting(); err != nil {
			return err
		}
		md.Payload.Details = jobspb.WrapPayloadDetails(details)
		ju.UpdatePayload(md.Payload)
		return nil
	})
}

// SetProgress sets the details field of the currently running tracked job.
func (j *Job) SetProgress(ctx context.Context, txn *kv.Txn, details interface{}) error {
	return j.Update(ctx, txn, func(txn *kv.Txn, md JobMetadata, ju *JobUpdater) error {
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

// FractionCompleted returns completion according to the in-memory job state.
func (j *Job) FractionCompleted() float32 {
	progress := j.Progress()
	return progress.GetFractionCompleted()
}

// MakeSessionBoundInternalExecutor makes an internal executor, for use in a job
// resumer, and sets it with the provided session data. See the comment on
// sessionBoundInternalExecutorFactory for a more detailed explanation of why
// this exists.
func (j *Job) MakeSessionBoundInternalExecutor(
	ctx context.Context, sd *sessiondata.SessionData,
) sqlutil.InternalExecutor {
	return j.registry.sessionBoundInternalExecutorFactory(ctx, sd)
}

func (j *Job) runInTxn(
	ctx context.Context, txn *kv.Txn, fn func(context.Context, *kv.Txn) error,
) error {
	if txn != nil {
		// Don't run fn in a retry loop because we need retryable errors to
		// propagate up to the transaction's properly-scoped retry loop.
		return fn(ctx, txn)
	}
	return j.registry.db.Txn(ctx, fn)
}

// JobNotFoundError is returned from load when the job does not exist.
type JobNotFoundError struct {
	jobID jobspb.JobID
}

// Error makes JobNotFoundError an error.
func (e *JobNotFoundError) Error() string {
	return fmt.Sprintf("job with ID %d does not exist", e.jobID)
}

// HasJobNotFoundError returns true if the error contains a JobNotFoundError.
func HasJobNotFoundError(err error) bool {
	return errors.HasType(err, (*JobNotFoundError)(nil))
}

func (j *Job) load(ctx context.Context, txn *kv.Txn) error {
	var payload *jobspb.Payload
	var progress *jobspb.Progress
	var createdBy *CreatedByInfo

	if err := j.runInTxn(ctx, txn, func(ctx context.Context, txn *kv.Txn) error {
		const stmt = "SELECT payload, progress, created_by_type, created_by_id FROM system.jobs WHERE id = $1"
		row, err := j.registry.ex.QueryRowEx(
			ctx, "load-job-query", txn, sessiondata.InternalExecutorOverride{User: security.RootUserName()},
			stmt, j.ID())
		if err != nil {
			return err
		}
		if row == nil {
			return &JobNotFoundError{jobID: j.ID()}
		}
		payload, err = UnmarshalPayload(row[0])
		if err != nil {
			return err
		}
		progress, err = UnmarshalProgress(row[1])
		if err != nil {
			return err
		}
		createdBy, err = unmarshalCreatedBy(row[2], row[3])
		return err
	}); err != nil {
		return err
	}
	j.mu.payload = *payload
	j.mu.progress = *progress
	j.createdBy = createdBy
	return nil
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

// unnarshalCreatedBy unrmarshals and returns created_by_type and created_by_id datums
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

// CurrentStatus returns the current job status from the jobs table or error.
func (j *Job) CurrentStatus(ctx context.Context, txn *kv.Txn) (Status, error) {
	var statusString tree.DString
	if err := j.runInTxn(ctx, txn, func(ctx context.Context, txn *kv.Txn) error {
		const selectStmt = "SELECT status FROM system.jobs WHERE id = $1"
		row, err := j.registry.ex.QueryRow(ctx, "job-status", txn, selectStmt, j.ID())
		if err != nil {
			return errors.Wrapf(err, "job %d: can't query system.jobs", j.ID())
		}
		if row == nil {
			return errors.Errorf("job %d: not found in system.jobs", j.ID())
		}

		statusString = tree.MustBeDString(row[0])
		return nil
	}); err != nil {
		return "", err
	}
	return Status(statusString), nil
}

// Start will resume the job. The transaction used to create the StartableJob
// must be committed. If a non-nil error is returned, the job was not started
// and nothing will be send on errCh. Clients must not start jobs more than
// once.
func (sj *StartableJob) Start(ctx context.Context) (err error) {
	if starts := atomic.AddInt64(&sj.starts, 1); starts != 1 {
		return errors.AssertionFailedf(
			"StartableJob %d cannot be started more than once", sj.ID())
	}

	if sj.sessionID == "" {
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

	if err := sj.started(ctx, nil /* txn */); err != nil {
		return err
	}

	if err := sj.registry.stopper.RunAsyncTask(ctx, sj.taskName(), func(ctx context.Context) {
		sj.execErr = sj.registry.runJob(sj.resumerCtx, sj.resumer, sj.Job, StatusRunning, sj.taskName())
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
	defer sj.registry.unregister(sj.ID())
	return sj.registry.CancelRequested(ctx, nil, sj.ID())
}
