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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

// Job manages logging the progress of long-running system processes, like
// backups and restores, to the system.jobs table.
type Job struct {
	// TODO(benesch): avoid giving Job a reference to Registry. This will likely
	// require inverting control: rather than having the worker call Created,
	// Started, etc., have Registry call a setupFn and a workFn as appropriate.
	registry *Registry

	id  *int64
	txn *client.Txn
	mu  struct {
		syncutil.Mutex
		payload  jobspb.Payload
		progress jobspb.Progress
	}
}

// Record bundles together the user-managed fields in jobspb.Payload.
type Record struct {
	Description   string
	Statement     string
	Username      string
	DescriptorIDs sqlbase.IDs
	Details       jobspb.Details
	Progress      jobspb.ProgressDetails
	RunningStatus RunningStatus
}

// Status represents the status of a job in the system.jobs table.
type Status string

// RunningStatus represents the more detailed status of a running job in
// the system.jobs table.
type RunningStatus string

const (
	// StatusPending is for jobs that have been created but on which work has
	// not yet started.
	StatusPending Status = "pending"
	// StatusRunning is for jobs that are currently in progress.
	StatusRunning Status = "running"
	// StatusPaused is for jobs that are not currently performing work, but have
	// saved their state and can be resumed by the user later.
	StatusPaused Status = "paused"
	// StatusFailed is for jobs that failed.
	StatusFailed Status = "failed"
	// StatusSucceeded is for jobs that have successfully completed.
	StatusSucceeded Status = "succeeded"
	// StatusCanceled is for jobs that were explicitly canceled by the user and
	// cannot be resumed.
	StatusCanceled Status = "canceled"
)

// Terminal returns whether this status represents a "terminal" state: a state
// after which the job should never be updated again.
func (s Status) Terminal() bool {
	return s == StatusFailed || s == StatusSucceeded || s == StatusCanceled
}

// InvalidStatusError is the error returned when the desired operation is
// invalid given the job's current status.
type InvalidStatusError struct {
	id     int64
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
	ierr, ok := err.(*InvalidStatusError)
	if !ok {
		return err
	}
	return errors.Errorf("job %s", ierr.status)
}

// ID returns the ID of the job that this Job is currently tracking. This will
// be nil if Created has not yet been called.
func (j *Job) ID() *int64 {
	return j.id
}

// Created records the creation of a new job in the system.jobs table and
// remembers the assigned ID of the job in the Job. The job information is read
// from the Record field at the time Created is called.
func (j *Job) Created(ctx context.Context) error {
	if j.ID() != nil {
		return errors.Errorf("job already created with ID %v", *j.ID())
	}
	return j.insert(ctx, j.registry.makeJobID(), nil /* lease */)
}

// Started marks the tracked job as started.
func (j *Job) Started(ctx context.Context) error {
	return j.Update(ctx, func(_ *client.Txn, md JobMetadata, ju *JobUpdater) error {
		if md.Status != StatusPending {
			// Already started - do nothing.
			return nil
		}
		ju.UpdateStatus(StatusRunning)
		md.Payload.StartedMicros = timeutil.ToUnixMicros(j.registry.clock.Now().GoTime())
		ju.UpdatePayload(md.Payload)
		return nil
	})
}

// CheckStatus verifies the status of the job and returns an error if the job's
// status isn't Running.
func (j *Job) CheckStatus(ctx context.Context) error {
	return j.Update(ctx, func(_ *client.Txn, md JobMetadata, _ *JobUpdater) error {
		return md.CheckRunning()
	})
}

// CheckTerminalStatus returns true if the job is in a terminal status.
func (j *Job) CheckTerminalStatus(ctx context.Context) bool {
	err := j.Update(ctx, func(_ *client.Txn, md JobMetadata, _ *JobUpdater) error {
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
func (j *Job) RunningStatus(ctx context.Context, runningStatusFn RunningStatusFn) error {
	return j.Update(ctx, func(_ *client.Txn, md JobMetadata, ju *JobUpdater) error {
		if err := md.CheckRunning(); err != nil {
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
func (j *Job) SetDescription(ctx context.Context, updateFn DescriptionUpdateFn) error {
	return j.Update(ctx, func(_ *client.Txn, md JobMetadata, ju *JobUpdater) error {
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

// RunningStatusFn is a callback that computes a job's running status
// given its details. It is safe to modify details in the callback; those
// modifications will be automatically persisted to the database record.
type RunningStatusFn func(ctx context.Context, details jobspb.Details) (RunningStatus, error)

// DescriptionUpdateFn is a callback that computes a job's description
// given its current one.
type DescriptionUpdateFn func(ctx context.Context, description string) (string, error)

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

// HighWaterProgressedFn is a callback that computes a job's high-water mark
// given its details. It is safe to modify details in the callback; those
// modifications will be automatically persisted to the database record.
type HighWaterProgressedFn func(ctx context.Context, details jobspb.ProgressDetails) hlc.Timestamp

// FractionProgressed updates the progress of the tracked job. It sets the job's
// FractionCompleted field to the value returned by progressedFn and persists
// progressedFn's modifications to the job's progress details, if any.
//
// Jobs for which progress computations do not depend on their details can
// use the FractionUpdater helper to construct a ProgressedFn.
func (j *Job) FractionProgressed(ctx context.Context, progressedFn FractionProgressedFn) error {
	return j.Update(ctx, func(_ *client.Txn, md JobMetadata, ju *JobUpdater) error {
		if err := md.CheckRunning(); err != nil {
			return err
		}
		fractionCompleted := progressedFn(ctx, md.Progress.Details)
		// allow for slight floating-point rounding inaccuracies
		if fractionCompleted > 1.0 && fractionCompleted < 1.01 {
			fractionCompleted = 1.0
		}
		if fractionCompleted < 0.0 || fractionCompleted > 1.0 {
			return errors.Errorf(
				"Job: fractionCompleted %f is outside allowable range [0.0, 1.0] (job %d)",
				fractionCompleted, j.id,
			)
		}
		md.Progress.Progress = &jobspb.Progress_FractionCompleted{
			FractionCompleted: fractionCompleted,
		}
		ju.UpdateProgress(md.Progress)
		return nil
	})
}

// HighWaterProgressed updates the progress of the tracked job. It sets the
// job's HighWater field to the value returned by progressedFn and persists
// progressedFn's modifications to the job's progress details, if any.
func (j *Job) HighWaterProgressed(ctx context.Context, progressedFn HighWaterProgressedFn) error {
	return j.Update(ctx, func(txn *client.Txn, md JobMetadata, ju *JobUpdater) error {
		if err := md.CheckRunning(); err != nil {
			return err
		}
		highWater := progressedFn(ctx, md.Progress.Details)
		if highWater.Less(hlc.Timestamp{}) {
			return errors.Errorf(
				"Job: high-water %s is outside allowable range > 0.0 (job %d)",
				highWater, j.id,
			)
		}
		md.Progress.Progress = &jobspb.Progress_HighWater{
			HighWater: &highWater,
		}
		ju.UpdateProgress(md.Progress)
		return nil
	})
}

// Paused sets the status of the tracked job to paused. It does not directly
// pause the job; instead, it expects the job to call job.Progressed soon,
// observe a "job is paused" error, and abort further work.
func (j *Job) paused(ctx context.Context) error {
	return j.Update(ctx, func(txn *client.Txn, md JobMetadata, ju *JobUpdater) error {
		if md.Status == StatusPaused {
			// Already paused - do nothing.
			return nil
		}
		if md.Status.Terminal() {
			return &InvalidStatusError{*j.id, md.Status, "pause", md.Payload.Error}
		}
		ju.UpdateStatus(StatusPaused)
		return nil
	})
}

// Resumed sets the status of the tracked job to running iff the job is
// currently paused. It does not directly resume the job; rather, it expires the
// job's lease so that a Registry adoption loop detects it and resumes it.
func (j *Job) resumed(ctx context.Context) error {
	return j.Update(ctx, func(txn *client.Txn, md JobMetadata, ju *JobUpdater) error {
		if md.Status == StatusRunning {
			// Already resumed - do nothing.
			return nil
		}
		if md.Status != StatusPaused {
			if md.Payload.Error != "" {
				return fmt.Errorf("job with status %s %q cannot be resumed", md.Status, md.Payload.Error)
			}
			return fmt.Errorf("job with status %s cannot be resumed", md.Status)
		}
		ju.UpdateStatus(StatusRunning)
		// NB: A nil lease indicates the job is not resumable, whereas an empty
		// lease is always considered expired.
		md.Payload.Lease = &jobspb.Lease{}
		ju.UpdatePayload(md.Payload)
		return nil
	})
}

// Canceled sets the status of the tracked job to canceled. It does not directly
// cancel the job; like job.Paused, it expects the job to call job.Progressed
// soon, observe a "job is canceled" error, and abort further work.
func (j *Job) canceled(ctx context.Context, fn func(context.Context, *client.Txn) error) error {
	return j.Update(ctx, func(txn *client.Txn, md JobMetadata, ju *JobUpdater) error {
		if md.Status == StatusCanceled {
			// Already canceled - do nothing.
			return nil
		}
		if md.Status != StatusPaused && md.Status.Terminal() {
			if md.Payload.Error != "" {
				return fmt.Errorf("job with status %s %q cannot be canceled", md.Status, md.Payload.Error)
			}
			return fmt.Errorf("job with status %s cannot be canceled", md.Status)
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

// NoopFn is an empty function that can be used for Failed and Succeeded. It indicates
// no transactional callback should be made during these operations.
var NoopFn = func(context.Context, *client.Txn) error { return nil }

// Failed marks the tracked job as having failed with the given error.
func (j *Job) Failed(
	ctx context.Context, err error, fn func(context.Context, *client.Txn) error,
) error {
	return j.Update(ctx, func(txn *client.Txn, md JobMetadata, ju *JobUpdater) error {
		if md.Status.Terminal() {
			// Already done - do nothing.
			return nil
		}
		if err := fn(ctx, txn); err != nil {
			return err
		}
		ju.UpdateStatus(StatusFailed)
		md.Payload.Error = err.Error()
		md.Payload.FinishedMicros = timeutil.ToUnixMicros(j.registry.clock.Now().GoTime())
		ju.UpdatePayload(md.Payload)
		return nil
	})
}

// Succeeded marks the tracked job as having succeeded and sets its fraction
// completed to 1.0.
func (j *Job) Succeeded(ctx context.Context, fn func(context.Context, *client.Txn) error) error {
	return j.Update(ctx, func(txn *client.Txn, md JobMetadata, ju *JobUpdater) error {
		if md.Status.Terminal() {
			// Already done - do nothing.
			return nil
		}
		if err := fn(ctx, txn); err != nil {
			return err
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
func (j *Job) SetDetails(ctx context.Context, details interface{}) error {
	return j.Update(ctx, func(txn *client.Txn, md JobMetadata, ju *JobUpdater) error {
		md.Payload.Details = jobspb.WrapPayloadDetails(details)
		ju.UpdatePayload(md.Payload)
		return nil
	})
}

// SetProgress sets the details field of the currently running tracked job.
func (j *Job) SetProgress(ctx context.Context, details interface{}) error {
	return j.Update(ctx, func(txn *client.Txn, md JobMetadata, ju *JobUpdater) error {
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

// WithTxn sets the transaction that this Job will use for its next operation.
// If the transaction is nil, the Job will create a one-off transaction instead.
// If you use WithTxn, this Job will no longer be threadsafe.
func (j *Job) WithTxn(txn *client.Txn) *Job {
	j.txn = txn
	return j
}

func (j *Job) runInTxn(ctx context.Context, fn func(context.Context, *client.Txn) error) error {
	if j.txn != nil {
		defer func() { j.txn = nil }()
		// Don't run fn in a retry loop because we need retryable errors to
		// propagate up to the transaction's properly-scoped retry loop.
		return fn(ctx, j.txn)
	}
	return j.registry.db.Txn(ctx, fn)
}

func (j *Job) load(ctx context.Context) error {
	var payload *jobspb.Payload
	var progress *jobspb.Progress
	if err := j.runInTxn(ctx, func(ctx context.Context, txn *client.Txn) error {
		const stmt = "SELECT payload, progress FROM system.jobs WHERE id = $1"
		row, err := j.registry.ex.QueryRow(ctx, "log-job", txn, stmt, *j.id)
		if err != nil {
			return err
		}
		if row == nil {
			return fmt.Errorf("job with ID %d does not exist", *j.id)
		}
		payload, err = UnmarshalPayload(row[0])
		if err != nil {
			return err
		}
		progress, err = UnmarshalProgress(row[1])
		return err
	}); err != nil {
		return err
	}
	j.mu.payload = *payload
	j.mu.progress = *progress
	return nil
}

func (j *Job) insert(ctx context.Context, id int64, lease *jobspb.Lease) error {
	if j.id != nil {
		// Already created - do nothing.
		return nil
	}

	j.mu.payload.Lease = lease

	if err := j.runInTxn(ctx, func(ctx context.Context, txn *client.Txn) error {
		// Note: although the following uses ReadTimestamp and
		// ReadTimestamp can diverge from the value of now() throughout a
		// transaction, this may be OK -- we merely required ModifiedMicro
		// to be equal *or greater* than previously inserted timestamps
		// computed by now(). For now ReadTimestamp can only move forward
		// and the assertion ReadTimestamp >= now() holds at all times.
		j.mu.progress.ModifiedMicros = timeutil.ToUnixMicros(txn.ReadTimestamp().GoTime())
		payloadBytes, err := protoutil.Marshal(&j.mu.payload)
		if err != nil {
			return err
		}
		progressBytes, err := protoutil.Marshal(&j.mu.progress)
		if err != nil {
			return err
		}

		const stmt = "INSERT INTO system.jobs (id, status, payload, progress) VALUES ($1, $2, $3, $4)"
		_, err = j.registry.ex.Exec(ctx, "job-insert", txn, stmt, id, StatusPending, payloadBytes, progressBytes)
		return err
	}); err != nil {
		return err
	}
	j.id = &id
	return nil
}

func (j *Job) adopt(ctx context.Context, oldLease *jobspb.Lease) error {
	return j.Update(ctx, func(txn *client.Txn, md JobMetadata, ju *JobUpdater) error {
		if md.Status != StatusRunning && md.Status != StatusPending {
			return errors.Errorf("job %d has status %v which is not elligible for adopting", *j.id, md.Status)
		}
		if !md.Payload.Lease.Equal(oldLease) {
			return errors.Errorf("current lease %v did not match expected lease %v",
				md.Payload.Lease, oldLease)
		}
		md.Payload.Lease = j.registry.newLease()
		ju.UpdatePayload(md.Payload)
		ju.UpdateStatus(StatusRunning)
		return nil
	})
}

// UnmarshalPayload unmarshals and returns the Payload encoded in the input
// datum, which should be a tree.DBytes.
func UnmarshalPayload(datum tree.Datum) (*jobspb.Payload, error) {
	payload := &jobspb.Payload{}
	bytes, ok := datum.(*tree.DBytes)
	if !ok {
		return nil, errors.Errorf(
			"Job: failed to unmarshal payload as DBytes (was %T)", datum)
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
			"Job: failed to unmarshal Progress as DBytes (was %T)", datum)
	}
	if err := protoutil.Unmarshal([]byte(*bytes), progress); err != nil {
		return nil, err
	}
	return progress, nil
}
