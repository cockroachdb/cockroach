// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package jobs

import (
	"context"
	gosql "database/sql"
	"fmt"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	// RunningStatusDrainingNames is for jobs that are currently in progress and
	// are draining names.
	RunningStatusDrainingNames RunningStatus = "draining names"
	// RunningStatusWaitingGC is for jobs that are currently in progress and
	// are waiting for the GC interval to expire
	RunningStatusWaitingGC RunningStatus = "waiting for GC TTL"
	// RunningStatusCompaction is for jobs that are currently in progress and
	// undergoing RocksDB compaction
	RunningStatusCompaction RunningStatus = "RocksDB compaction"
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
	return j.insert(ctx, j.registry.makeJobID(), nil /* lease */)
}

// Started marks the tracked job as started.
func (j *Job) Started(ctx context.Context) error {
	return j.update(ctx, func(_ *client.Txn, status *Status, payload *jobspb.Payload, _ *jobspb.Progress) (bool, error) {
		if *status != StatusPending {
			// Already started - do nothing.
			return false, nil
		}
		*status = StatusRunning
		payload.StartedMicros = timeutil.ToUnixMicros(timeutil.Now())
		return true, nil
	})
}

// RunningStatus updates the detailed status of a job currently in progress.
// It sets the job's RunningStatus field to the value returned by runningStatusFn
// and persists runningStatusFn's modifications to the job's details, if any.
func (j *Job) RunningStatus(ctx context.Context, runningStatusFn RunningStatusFn) error {
	return j.updateRow(ctx, updateProgressAndDetails,
		func(_ *client.Txn, status *Status, payload *jobspb.Payload, progress *jobspb.Progress) (bool, error) {
			if *status != StatusRunning {
				return false, &InvalidStatusError{*j.id, *status, "update progress on", payload.Error}
			}
			runningStatus, err := runningStatusFn(ctx, progress.Details)
			if err != nil {
				return false, err
			}
			progress.RunningStatus = string(runningStatus)
			return true, nil
		},
	)
}

// SetDescription updates the description of a created job.
func (j *Job) SetDescription(ctx context.Context, updateFn DescriptionUpdateFn) error {
	return j.updateRow(ctx, updateProgressAndDetails,
		func(_ *client.Txn, status *Status, payload *jobspb.Payload, _ *jobspb.Progress) (bool, error) {
			prev := payload.Description
			desc, err := updateFn(ctx, prev)
			if err != nil {
				return false, err
			}
			payload.Description = desc
			return prev != desc, nil
		},
	)
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

// FractionDetailProgressedFn is a callback that computes a job's completion
// fraction given its details. It is safe to modify details in the callback;
// those modifications will be automatically persisted to the database record.
type FractionDetailProgressedFn func(ctx context.Context, details jobspb.Details, progress jobspb.ProgressDetails) float32

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
	return j.updateRow(ctx, updateProgressOnly,
		func(_ *client.Txn, status *Status, payload *jobspb.Payload, progress *jobspb.Progress) (bool, error) {
			if *status != StatusRunning {
				return false, &InvalidStatusError{*j.id, *status, "update progress on", payload.Error}
			}
			fractionCompleted := progressedFn(ctx, progress.Details)
			if fractionCompleted < 0.0 || fractionCompleted > 1.0 {
				return false, errors.Errorf(
					"Job: fractionCompleted %f is outside allowable range [0.0, 1.0] (job %d)",
					fractionCompleted, j.id,
				)
			}
			progress.Progress = &jobspb.Progress_FractionCompleted{
				FractionCompleted: fractionCompleted,
			}
			return true, nil
		},
	)
}

// FractionDetailProgressed is similar to Progressed but also updates the job's Details.
func (j *Job) FractionDetailProgressed(
	ctx context.Context, progressedFn FractionDetailProgressedFn,
) error {
	return j.update(ctx, func(_ *client.Txn, status *Status, payload *jobspb.Payload, progress *jobspb.Progress) (bool, error) {
		if *status != StatusRunning {
			return false, &InvalidStatusError{*j.id, *status, "update progress on", payload.Error}
		}
		fractionCompleted := progressedFn(ctx, payload.Details, progress.Details)
		if fractionCompleted < 0.0 || fractionCompleted > 1.0 {
			return false, errors.Errorf(
				"Job: fractionCompleted %f is outside allowable range [0.0, 1.0] (job %d)",
				fractionCompleted, j.id,
			)
		}
		progress.Progress = &jobspb.Progress_FractionCompleted{
			FractionCompleted: fractionCompleted,
		}
		return true, nil
	})
}

// HighWaterProgressed updates the progress of the tracked job. It sets the
// job's HighWater field to the value returned by progressedFn and persists
// progressedFn's modifications to the job's progress details, if any.
func (j *Job) HighWaterProgressed(ctx context.Context, progressedFn HighWaterProgressedFn) error {
	return j.updateRow(ctx, updateProgressOnly,
		func(_ *client.Txn, status *Status, payload *jobspb.Payload, progress *jobspb.Progress) (bool, error) {
			if *status != StatusRunning {
				return false, &InvalidStatusError{*j.id, *status, "update progress on", payload.Error}
			}
			highWater := progressedFn(ctx, progress.Details)
			if highWater.Less(hlc.Timestamp{}) {
				return false, errors.Errorf(
					"Job: high-water %s is outside allowable range > 0.0 (job %d)",
					highWater, j.id,
				)
			}
			progress.Progress = &jobspb.Progress_HighWater{
				HighWater: &highWater,
			}
			return true, nil
		},
	)
}

// Paused sets the status of the tracked job to paused. It does not directly
// pause the job; instead, it expects the job to call job.Progressed soon,
// observe a "job is paused" error, and abort further work.
func (j *Job) paused(ctx context.Context) error {
	return j.update(ctx, func(_ *client.Txn, status *Status, payload *jobspb.Payload, _ *jobspb.Progress) (bool, error) {
		if *status == StatusPaused {
			// Already paused - do nothing.
			return false, nil
		}
		if status.Terminal() {
			return false, &InvalidStatusError{*j.id, *status, "pause", payload.Error}
		}
		*status = StatusPaused
		return true, nil
	})
}

// Resumed sets the status of the tracked job to running iff the job is
// currently paused. It does not directly resume the job; rather, it expires the
// job's lease so that a Registry adoption loop detects it and resumes it.
func (j *Job) resumed(ctx context.Context) error {
	return j.update(ctx, func(_ *client.Txn, status *Status, payload *jobspb.Payload, _ *jobspb.Progress) (bool, error) {
		if *status == StatusRunning {
			// Already resumed - do nothing.
			return false, nil
		}
		if *status != StatusPaused {
			if payload.Error != "" {
				return false, fmt.Errorf("job with status %s %q cannot be resumed", *status, payload.Error)
			}
			return false, fmt.Errorf("job with status %s cannot be resumed", *status)
		}
		*status = StatusRunning
		// NB: A nil lease indicates the job is not resumable, whereas an empty
		// lease is always considered expired.
		payload.Lease = &jobspb.Lease{}
		return true, nil
	})
}

// Canceled sets the status of the tracked job to canceled. It does not directly
// cancel the job; like job.Paused, it expects the job to call job.Progressed
// soon, observe a "job is canceled" error, and abort further work.
func (j *Job) canceled(
	ctx context.Context, fn func(context.Context, *client.Txn, *Job) error,
) error {
	return j.update(ctx, func(txn *client.Txn, status *Status, payload *jobspb.Payload, _ *jobspb.Progress) (bool, error) {
		if *status == StatusCanceled {
			// Already canceled - do nothing.
			return false, nil
		}
		if *status != StatusPaused && status.Terminal() {
			if payload.Error != "" {
				return false, fmt.Errorf("job with status %s %q cannot be canceled", *status, payload.Error)
			}
			return false, fmt.Errorf("job with status %s cannot be canceled", *status)
		}
		*status = StatusCanceled
		if fn != nil {
			if err := fn(ctx, txn, j); err != nil {
				return false, err
			}
		}
		payload.FinishedMicros = timeutil.ToUnixMicros(timeutil.Now())
		return true, nil
	})
}

// NoopFn is used in place of a nil for Failed and Succeeded. It indicates
// no transactional callback should be made during these operations.
var NoopFn func(context.Context, *client.Txn, *Job) error

// Failed marks the tracked job as having failed with the given error.
func (j *Job) Failed(
	ctx context.Context, err error, fn func(context.Context, *client.Txn, *Job) error,
) error {
	return j.update(ctx, func(txn *client.Txn, status *Status, payload *jobspb.Payload, _ *jobspb.Progress) (bool, error) {
		if status.Terminal() {
			// Already done - do nothing.
			return false, nil
		}
		*status = StatusFailed
		if fn != nil {
			if err := fn(ctx, txn, j); err != nil {
				return false, err
			}
		}
		payload.Error = err.Error()
		payload.FinishedMicros = timeutil.ToUnixMicros(timeutil.Now())
		return true, nil
	})
}

// Succeeded marks the tracked job as having succeeded and sets its fraction
// completed to 1.0.
func (j *Job) Succeeded(
	ctx context.Context, fn func(context.Context, *client.Txn, *Job) error,
) error {
	return j.update(ctx, func(txn *client.Txn, status *Status, payload *jobspb.Payload, progress *jobspb.Progress) (bool, error) {
		if status.Terminal() {
			// Already done - do nothing.
			return false, nil
		}
		*status = StatusSucceeded
		if fn != nil {
			if err := fn(ctx, txn, j); err != nil {
				return false, err
			}
		}
		payload.FinishedMicros = timeutil.ToUnixMicros(timeutil.Now())
		progress.Progress = &jobspb.Progress_FractionCompleted{
			FractionCompleted: 1.0,
		}
		return true, nil
	})
}

// SetDetails sets the details field of the currently running tracked job.
func (j *Job) SetDetails(ctx context.Context, details interface{}) error {
	return j.update(ctx, func(_ *client.Txn, _ *Status, payload *jobspb.Payload, _ *jobspb.Progress) (bool, error) {
		payload.Details = jobspb.WrapPayloadDetails(details)
		return true, nil
	})
}

// SetProgress sets the details field of the currently running tracked job.
func (j *Job) SetProgress(ctx context.Context, details interface{}) error {
	return j.updateRow(ctx, updateProgressOnly,
		func(_ *client.Txn, _ *Status, _ *jobspb.Payload, progress *jobspb.Progress) (bool, error) {
			progress.Details = jobspb.WrapProgressDetails(details)
			return true, nil
		},
	)
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
		// Note: although the following uses OrigTimestamp and
		// OrigTimestamp can diverge from the value of now() throughout a
		// transaction, this may be OK -- we merely required ModifiedMicro
		// to be equal *or greater* than previously inserted timestamps
		// computed by now(). For now OrigTimestamp can only move forward
		// and the assertion OrigTimestamp >= now() holds at all times.
		j.mu.progress.ModifiedMicros = timeutil.ToUnixMicros(txn.OrigTimestamp().GoTime())
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

func (j *Job) update(
	ctx context.Context,
	updateFn func(*client.Txn, *Status, *jobspb.Payload, *jobspb.Progress) (bool, error),
) error {
	return j.updateRow(ctx, updateProgressAndDetails, updateFn)
}

const updateProgressOnly, updateProgressAndDetails = true, false

func (j *Job) updateRow(
	ctx context.Context,
	progressOnly bool,
	updateFn func(*client.Txn, *Status, *jobspb.Payload, *jobspb.Progress) (bool, error),
) error {
	if j.id == nil {
		return errors.New("Job: cannot update: job not created")
	}

	var payload *jobspb.Payload
	var progress *jobspb.Progress
	if err := j.runInTxn(ctx, func(ctx context.Context, txn *client.Txn) error {
		const selectStmt = "SELECT status, payload, progress FROM system.jobs WHERE id = $1"
		row, err := j.registry.ex.QueryRow(ctx, "log-job", txn, selectStmt, *j.id)
		if err != nil {
			return err
		}
		statusString, ok := row[0].(*tree.DString)
		if !ok {
			return errors.Errorf("Job: expected string status on job %d, but got %T", *j.id, statusString)
		}
		status := Status(*statusString)
		payload, err = UnmarshalPayload(row[1])
		if err != nil {
			return err
		}

		progress, err = UnmarshalProgress(row[2])
		if err != nil {
			return err
		}

		doUpdate, err := updateFn(txn, &status, payload, progress)
		if err != nil {
			return err
		}
		if !doUpdate {
			return nil
		}

		progress.ModifiedMicros = timeutil.ToUnixMicros(timeutil.Now())
		progressBytes, err := protoutil.Marshal(progress)
		if err != nil {
			return err
		}

		if progressOnly {
			const updateStmt = "UPDATE system.jobs SET progress = $1 WHERE id = $2"
			updateArgs := []interface{}{progressBytes, *j.id}
			n, err := j.registry.ex.Exec(ctx, "job-update", txn, updateStmt, updateArgs...)
			if err != nil {
				return err
			}
			if n != 1 {
				return errors.Errorf("Job: expected exactly one row affected, but %d rows affected by job update", n)
			}
			return nil
		}

		payloadBytes, err := protoutil.Marshal(payload)
		if err != nil {
			return err
		}

		const updateStmt = "UPDATE system.jobs SET status = $1, payload = $2, progress = $3 WHERE id = $4"
		updateArgs := []interface{}{status, payloadBytes, progressBytes, *j.id}
		n, err := j.registry.ex.Exec(ctx, "job-update", txn, updateStmt, updateArgs...)
		if err != nil {
			return err
		}
		if n != 1 {
			return errors.Errorf("Job: expected exactly one row affected, but %d rows affected by job update", n)
		}
		return nil
	}); err != nil {
		return err
	}
	if payload != nil {
		j.mu.Lock()
		j.mu.payload = *payload
		j.mu.Unlock()
	}
	if progress != nil {
		j.mu.Lock()
		j.mu.progress = *progress
		j.mu.Unlock()
	}
	return nil
}

func (j *Job) adopt(ctx context.Context, oldLease *jobspb.Lease) error {
	return j.update(ctx, func(_ *client.Txn, status *Status, payload *jobspb.Payload, progress *jobspb.Progress) (bool, error) {
		if *status != StatusRunning {
			return false, errors.Errorf("job %d no longer running", *j.id)
		}
		if !payload.Lease.Equal(oldLease) {
			return false, errors.Errorf("current lease %v did not match expected lease %v",
				payload.Lease, oldLease)
		}
		payload.Lease = j.registry.newLease()
		return true, nil
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

// RunAndWaitForTerminalState runs a closure and potentially tracks its progress
// using the system.jobs table.
//
// If the closure returns before a jobs entry is created, the closure's error is
// passed back with no job information. Otherwise, the first jobs entry created
// after the closure starts is polled until it enters a terminal state and that
// job's id, status, and error are returned.
//
// TODO(dan): Return a *Job instead of just the id and status.
//
// TODO(dan): This assumes that the next entry to the jobs table was made by
// this closure, but this assumption is quite racy. See if we can do something
// better.
func RunAndWaitForTerminalState(
	ctx context.Context, sqlDB *gosql.DB, execFn func(context.Context) error,
) (int64, Status, error) {
	begin := timeutil.Now()

	execErrCh := make(chan error, 1)
	go func() {
		err := execFn(ctx)
		log.Warningf(ctx, "exec returned so attempting to track via jobs: err %+v", err)
		execErrCh <- err
	}()

	var jobID int64
	var execErr error
	for r := retry.StartWithCtx(ctx, retry.Options{}); ; {
		select {
		case <-ctx.Done():
			return 0, "", ctx.Err()
		case execErr = <-execErrCh:
			// The closure finished, try to fetch a job id one more time. Close
			// and nil out execErrCh so it blocks from now on.
			close(execErrCh)
			execErrCh = nil
		case <-r.NextCh(): // Fallthrough.
		}
		err := sqlDB.QueryRow(`SELECT id FROM system.jobs WHERE created > $1`, begin).Scan(&jobID)
		if err == nil {
			break
		}
		if execDone := execErrCh == nil; err == gosql.ErrNoRows && !execDone {
			continue
		}
		if execErr != nil {
			return 0, "", errors.Wrap(execErr, "exec failed before job was created")
		}
		return 0, "", errors.Wrap(err, "no jobs found")
	}

	for r := retry.StartWithCtx(ctx, retry.Options{}); ; {
		select {
		case <-ctx.Done():
			return jobID, "", ctx.Err()
		case execErr = <-execErrCh:
			// The closure finished, this is a nice hint to wake up, but it only
			// works once. Close and nil out execErrCh so it blocks from now on.
			close(execErrCh)
			execErrCh = nil
		case <-r.NextCh(): // Fallthrough.
		}

		var status Status
		var jobErr gosql.NullString
		var fractionCompleted float64
		err := sqlDB.QueryRow(`
       SELECT status, error, fraction_completed
         FROM [SHOW JOBS]
        WHERE job_id = $1`, jobID).Scan(
			&status, &jobErr, &fractionCompleted,
		)
		if err != nil {
			return jobID, "", errors.Wrapf(err, "getting status of job %d", jobID)
		}
		if !status.Terminal() {
			if log.V(1) {
				log.Infof(ctx, "job %d: status=%s, progress=%0.3f, created %s ago",
					jobID, status, fractionCompleted, timeutil.Since(begin))
			}
			continue
		}
		if jobErr.Valid && len(jobErr.String) > 0 {
			return jobID, status, errors.New(jobErr.String)
		}
		return jobID, status, nil
	}
}
