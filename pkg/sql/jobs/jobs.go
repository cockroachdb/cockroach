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
	"strings"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Job manages logging the progress of long-running system processes, like
// backups and restores, to the system.jobs table.
//
// The Record field can be directly modified before Created is called. Updates
// to the Record field after the job has been created will not be written to the
// database, however, even when calling e.g. Started or Succeeded.
type Job struct {
	// TODO(benesch): avoid giving Job a reference to Registry. This will likely
	// require inverting control: rather than having the worker call Created,
	// Started, etc., have Registry call a setupFn and a workFn as appropriate.
	registry *Registry

	id     *int64
	Record Record
	txn    *client.Txn

	mu struct {
		syncutil.Mutex
		payload Payload
	}
}

// Details is a marker interface for job details proto structs.
type Details interface{}

var _ Details = BackupDetails{}
var _ Details = RestoreDetails{}
var _ Details = SchemaChangeDetails{}

// Record stores the job fields that are not automatically managed by Job.
type Record struct {
	Description   string
	Username      string
	DescriptorIDs sqlbase.IDs
	Details       Details
}

// Status represents the status of a job in the system.jobs table.
type Status string

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
}

func (e *InvalidStatusError) Error() string {
	return fmt.Sprintf("cannot %s %s job (id %d)", e.op, e.status, e.id)
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
	return j.created(ctx, j.registry.makeJobID(), nil)
}

func (j *Job) created(ctx context.Context, id int64, lease *Lease) error {
	payload := &Payload{
		Description:   j.Record.Description,
		Username:      j.Record.Username,
		DescriptorIDs: j.Record.DescriptorIDs,
		Details:       WrapPayloadDetails(j.Record.Details),
		Lease:         lease,
	}
	return j.insert(ctx, id, payload)
}

// Started marks the tracked job as started.
func (j *Job) Started(ctx context.Context) error {
	return j.update(ctx, func(_ *client.Txn, status *Status, payload *Payload) (bool, error) {
		if *status != StatusPending {
			// Already started - do nothing.
			return false, nil
		}
		*status = StatusRunning
		payload.StartedMicros = timeutil.ToUnixMicros(timeutil.Now())
		return true, nil
	})
}

// ProgressedFn is a callback that computes a job's completion fraction
// given its details. It is safe to modify details in the callback; those
// modifications will be automatically persisted to the database record.
type ProgressedFn func(ctx context.Context, details Details) float32

// FractionUpdater returns a ProgressedFn that returns its argument.
func FractionUpdater(f float32) ProgressedFn {
	return func(ctx context.Context, details Details) float32 {
		return f
	}
}

// Progressed updates the progress of the tracked job. It sets the job's
// FractionCompleted field to the value returned by progressedFn and persists
// progressedFn's modifications to the job's details, if any.
//
// Jobs for which progress computations do not depend on their details can
// use the FractionUpdater helper to construct a ProgressedFn.
func (j *Job) Progressed(ctx context.Context, progressedFn ProgressedFn) error {
	return j.update(ctx, func(_ *client.Txn, status *Status, payload *Payload) (bool, error) {
		if *status != StatusRunning {
			return false, &InvalidStatusError{*j.id, *status, "update progress on"}
		}
		payload.FractionCompleted = progressedFn(ctx, payload.Details)
		if payload.FractionCompleted < 0.0 || payload.FractionCompleted > 1.0 {
			return false, errors.Errorf(
				"Job: fractionCompleted %f is outside allowable range [0.0, 1.0] (job %d)",
				payload.FractionCompleted, j.id,
			)
		}
		return true, nil
	})
}

// Paused sets the status of the tracked job to paused. It does not directly
// pause the job; instead, it expects the job to call job.Progressed soon,
// observe a "job is paused" error, and abort further work.
func (j *Job) paused(ctx context.Context) error {
	return j.update(ctx, func(_ *client.Txn, status *Status, payload *Payload) (bool, error) {
		if *status == StatusPaused {
			// Already paused - do nothing.
			return false, nil
		}
		if status.Terminal() {
			return false, &InvalidStatusError{*j.id, *status, "pause"}
		}
		*status = StatusPaused
		return true, nil
	})
}

// Resumed sets the status of the tracked job to running iff the job is
// currently paused. It does not directly resume the job; rather, it expires the
// job's lease so that a Registry adoption loop detects it and resumes it.
func (j *Job) resumed(ctx context.Context) error {
	return j.update(ctx, func(_ *client.Txn, status *Status, payload *Payload) (bool, error) {
		if *status == StatusRunning {
			// Already resumed - do nothing.
			return false, nil
		}
		if *status != StatusPaused {
			return false, fmt.Errorf("job with status %s cannot be resumed", *status)
		}
		*status = StatusRunning
		// NB: A nil lease indicates the job is not resumable, whereas an empty
		// lease is always considered expired.
		payload.Lease = &Lease{}
		return true, nil
	})
}

// Canceled sets the status of the tracked job to canceled. It does not directly
// cancel the job; like job.Paused, it expects the job to call job.Progressed
// soon, observe a "job is canceled" error, and abort further work.
func (j *Job) canceled(
	ctx context.Context, fn func(context.Context, *client.Txn, *Job) error,
) error {
	return j.update(ctx, func(txn *client.Txn, status *Status, payload *Payload) (bool, error) {
		if *status == StatusCanceled {
			// Already canceled - do nothing.
			return false, nil
		}
		if *status != StatusPaused && status.Terminal() {
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
	return j.update(ctx, func(txn *client.Txn, status *Status, payload *Payload) (bool, error) {
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
	return j.update(ctx, func(txn *client.Txn, status *Status, payload *Payload) (bool, error) {
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
		payload.FractionCompleted = 1.0
		return true, nil
	})
}

// SetDetails sets the details field of the currently running tracked job.
func (j *Job) SetDetails(ctx context.Context, details interface{}) error {
	return j.update(ctx, func(_ *client.Txn, _ *Status, payload *Payload) (bool, error) {
		payload.Details = WrapPayloadDetails(details)
		return true, nil
	})
}

// Payload returns the most recently sent Payload for this Job. Will return an
// empty Payload until Created() is called on a new Job.
func (j *Job) Payload() Payload {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.mu.payload
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

func (j *Job) initialize(payload *Payload) (err error) {
	j.Record.Description = payload.Description
	j.Record.Username = payload.Username
	j.Record.DescriptorIDs = payload.DescriptorIDs
	if j.Record.Details, err = payload.UnwrapDetails(); err != nil {
		return err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	j.mu.payload = *payload
	return nil
}

func (j *Job) load(ctx context.Context) error {
	var payload *Payload
	if err := j.runInTxn(ctx, func(ctx context.Context, txn *client.Txn) error {
		const stmt = "SELECT payload FROM system.jobs WHERE id = $1"
		row, err := j.registry.ex.QueryRowInTransaction(ctx, "log-job", txn, stmt, *j.id)
		if err != nil {
			return err
		}
		if row == nil {
			return fmt.Errorf("job with ID %d does not exist", *j.id)
		}
		payload, err = UnmarshalPayload(row[0])
		return err
	}); err != nil {
		return err
	}
	return j.initialize(payload)
}

func (j *Job) insert(ctx context.Context, id int64, payload *Payload) error {
	if j.id != nil {
		// Already created - do nothing.
		return nil
	}

	if err := j.runInTxn(ctx, func(ctx context.Context, txn *client.Txn) error {
		// Note: although the following uses OrigTimestamp and
		// OrigTimestamp can diverge from the value of now() throughout a
		// transaction, this may be OK -- we merely required ModifiedMicro
		// to be equal *or greater* than previously inserted timestamps
		// computed by now(). For now OrigTimestamp can only move forward
		// and the assertion OrigTimestamp >= now() holds at all times.
		payload.ModifiedMicros = timeutil.ToUnixMicros(txn.OrigTimestamp().GoTime())
		payloadBytes, err := protoutil.Marshal(payload)
		if err != nil {
			return err
		}

		const stmt = "INSERT INTO system.jobs (id, status, payload) VALUES ($1, $2, $3)"
		_, err = j.registry.ex.ExecuteStatementInTransaction(ctx, "job-insert", txn, stmt, id, StatusPending, payloadBytes)
		return err
	}); err != nil {
		return err
	}
	j.mu.payload = *payload
	j.id = &id
	return nil
}

func (j *Job) update(
	ctx context.Context, updateFn func(*client.Txn, *Status, *Payload) (doUpdate bool, err error),
) error {
	if j.id == nil {
		return errors.New("Job: cannot update: job not created")
	}

	var payload *Payload
	if err := j.runInTxn(ctx, func(ctx context.Context, txn *client.Txn) error {
		const selectStmt = "SELECT status, payload FROM system.jobs WHERE id = $1"
		row, err := j.registry.ex.QueryRowInTransaction(ctx, "log-job", txn, selectStmt, *j.id)
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

		doUpdate, err := updateFn(txn, &status, payload)
		if err != nil {
			return err
		}
		if !doUpdate {
			return nil
		}

		payload.ModifiedMicros = timeutil.ToUnixMicros(timeutil.Now())
		payloadBytes, err := protoutil.Marshal(payload)
		if err != nil {
			return err
		}

		const updateStmt = "UPDATE system.jobs SET status = $1, payload = $2 WHERE id = $3"
		updateArgs := []interface{}{status, payloadBytes, *j.id}
		n, err := j.registry.ex.ExecuteStatementInTransaction(
			ctx, "job-update", txn, updateStmt, updateArgs...)
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
	return nil
}

func (j *Job) adopt(ctx context.Context, oldLease *Lease) error {
	return j.update(ctx, func(_ *client.Txn, status *Status, payload *Payload) (bool, error) {
		if *status != StatusRunning {
			return false, errors.Errorf("job %d no longer running", *j.id)
		}
		if !payload.Lease.Equal(oldLease) {
			return false, errors.Errorf("current lease %v did not match expected lease %v",
				payload.Lease, oldLease)
		}
		payload.Lease = j.registry.newLease()
		if err := j.initialize(payload); err != nil {
			return false, err
		}
		return true, nil
	})
}

// Type returns the payload's job type.
func (p *Payload) Type() Type {
	return detailsType(p.Details)
}

func detailsType(d isPayload_Details) Type {
	switch d.(type) {
	case *Payload_Backup:
		return TypeBackup
	case *Payload_Restore:
		return TypeRestore
	case *Payload_SchemaChange:
		return TypeSchemaChange
	case *Payload_Import:
		return TypeImport
	default:
		panic(fmt.Sprintf("Payload.Type called on a payload with an unknown details type: %T", d))
	}
}

// WrapPayloadDetails wraps a Details object in the protobuf wrapper struct
// necessary to make it usable as the Details field of a Payload.
//
// Providing an unknown details type indicates programmer error and so causes a
// panic.
func WrapPayloadDetails(details Details) interface {
	isPayload_Details
} {
	switch d := details.(type) {
	case BackupDetails:
		return &Payload_Backup{Backup: &d}
	case RestoreDetails:
		return &Payload_Restore{Restore: &d}
	case SchemaChangeDetails:
		return &Payload_SchemaChange{SchemaChange: &d}
	case ImportDetails:
		return &Payload_Import{Import: &d}
	default:
		panic(fmt.Sprintf("jobs.WrapPayloadDetails: unknown details type %T", d))
	}
}

// UnwrapDetails returns the details object stored within the payload's Details
// field, discarding the protobuf wrapper struct.
//
// Unlike in WrapPayloadDetails, an unknown details type may simply indicate
// that the Payload originated on a node aware of more details types, and so the
// error is returned to the caller.
func (p *Payload) UnwrapDetails() (Details, error) {
	switch d := p.Details.(type) {
	case *Payload_Backup:
		return *d.Backup, nil
	case *Payload_Restore:
		return *d.Restore, nil
	case *Payload_SchemaChange:
		return *d.SchemaChange, nil
	case *Payload_Import:
		return *d.Import, nil
	default:
		return nil, errors.Errorf("jobs.Payload: unsupported details type %T", d)
	}
}

// UnmarshalPayload unmarshals and returns the Payload encoded in the input
// datum, which should be a tree.DBytes.
func UnmarshalPayload(datum tree.Datum) (*Payload, error) {
	payload := &Payload{}
	bytes, ok := datum.(*tree.DBytes)
	if !ok {
		return nil, errors.Errorf(
			"Job: failed to unmarshal payload as DBytes (was %T)", bytes)
	}
	if err := protoutil.Unmarshal([]byte(*bytes), payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func (t Type) String() string {
	// Protobufs, by convention, use CAPITAL_SNAKE_CASE for enum identifiers.
	// Since Type's string representation is used as a SHOW JOBS output column, we
	// simply swap underscores for spaces in the identifier for very SQL-esque
	// names, like "BACKUP" and "SCHEMA CHANGE".
	return strings.Replace(Type_name[int32(t)], "_", " ", -1)
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
        WHERE id = $1`, jobID).Scan(
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

// Completed returns the total complete percent of processing this table. There
// are two phases: sampling, SST writing. The SST phase is divided into two
// stages: read CSV, write SST. Thus, the entire progress can be measured
// by (sampling + read csv + write sst) progress. Since there are multiple
// distSQL processors running these stages, we assign slots to each one, and
// they are in charge of updating their portion of the progress. Since we read
// over CSV files twice (once for sampling, once for SST writing), we must
// indicate which phase we are in. This is done using the SamplingProgress
// slice, which is empty if we are in the second stage (and can thus be
// implied as complete).
func (d ImportDetails_Table) Completed() float32 {
	const (
		// These ratios are approximate after running simple benchmarks.
		samplingPhaseContribution = 0.1
		readStageContribution     = 0.65
		writeStageContribution    = 0.25
	)

	sum := func(fs []float32) float32 {
		var total float32
		for _, f := range fs {
			total += f
		}
		return total
	}
	sampling := sum(d.SamplingProgress) * samplingPhaseContribution
	if len(d.SamplingProgress) == 0 {
		// SamplingProgress is empty iff we are in the second phase. If so, the
		// first phase is implied as fully complete.
		sampling = samplingPhaseContribution
	}
	read := sum(d.ReadProgress) * readStageContribution
	write := sum(d.WriteProgress) * writeStageContribution
	completed := sampling + read + write
	// Float addition can round such that the sum is > 1.
	if completed > 1 {
		completed = 1
	}
	return completed
}
