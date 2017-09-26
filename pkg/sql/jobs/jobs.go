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
	gosql "database/sql"
	"fmt"
	"strings"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
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

	id       *int64
	Record   Record
	txn      *client.Txn
	cancelFn func()

	mu struct {
		syncutil.Mutex
		payload  Payload
		canceled bool
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

// WithoutCancel indicates that the job should not have its leasing and
// cancelation managed by Registry. This is only a temporary measure; eventually
// all jobs will use the Registry's leasing and cancelation.
var WithoutCancel func()

// Created records the creation of a new job in the system.jobs table and
// remembers the assigned ID of the job in the Job. The job information is read
// from the Record field at the time Created is called. If cancelFn is not nil,
// the Registry will automatically acquire a lease for this job and invoke
// cancelFn if the lease expires.
func (j *Job) Created(ctx context.Context, cancelFn func()) error {
	payload := &Payload{
		Description:   j.Record.Description,
		Username:      j.Record.Username,
		DescriptorIDs: j.Record.DescriptorIDs,
		Details:       WrapPayloadDetails(j.Record.Details),
	}
	if cancelFn != nil {
		payload.Lease = j.registry.newLease()
	}
	if err := j.insert(ctx, payload); err != nil {
		return err
	}
	if cancelFn != nil {
		j.cancelFn = cancelFn
		if err := j.registry.register(*j.id, j); err != nil {
			return err
		}
	}
	return nil
}

// Started marks the tracked job as started.
func (j *Job) Started(ctx context.Context) error {
	return j.update(ctx, func(status *Status, payload *Payload) (bool, error) {
		if *status != StatusPending {
			// Already started - do nothing.
			return false, nil
		}
		*status = StatusRunning
		payload.StartedMicros = timeutil.ToUnixMicros(timeutil.Now())
		return true, nil
	})
}

// ProgressedFn is a callback that allows arbitrary modifications to a job's
// details when updating its progress.
type ProgressedFn func(ctx context.Context, details interface{})

// Noop is a nil ProgressedFn.
var Noop ProgressedFn

// Progressed updates the progress of the tracked job to fractionCompleted. A
// fractionCompleted that is less than the currently-recorded fractionCompleted
// will be silently ignored. If progressedFn is non-nil, it will be invoked with
// a pointer to the job's details to allow for modifications to the details
// before the job is saved. If no such modifications are required, pass Noop
// instead of nil for readability.
func (j *Job) Progressed(
	ctx context.Context, fractionCompleted float32, progressedFn ProgressedFn,
) error {
	if fractionCompleted < 0.0 || fractionCompleted > 1.0 {
		return errors.Errorf(
			"Job: fractionCompleted %f is outside allowable range [0.0, 1.0] (job %d)",
			fractionCompleted, j.id,
		)
	}
	return j.update(ctx, func(status *Status, payload *Payload) (bool, error) {
		if *status != StatusRunning {
			return false, &InvalidStatusError{*j.id, *status, "update progress on"}
		}
		if fractionCompleted > payload.FractionCompleted {
			payload.FractionCompleted = fractionCompleted
		}
		if progressedFn != nil {
			progressedFn(ctx, payload.Details)
		}
		return true, nil
	})
}

func isControllable(p *Payload, op string) error {
	switch typ := p.Type(); typ {
	case TypeSchemaChange:
		return pgerror.UnimplementedWithIssueErrorf(
			16018, "schema change jobs do not support %s", op)
	case TypeImport:
		return pgerror.UnimplementedWithIssueErrorf(
			18139, "import jobs do not support %s", op)
	case TypeBackup:
	case TypeRestore:
	default:
		return fmt.Errorf("%s jobs do not support %s", strings.ToLower(typ.String()), op)
	}
	if p.Lease == nil {
		return fmt.Errorf("job created by node without %s support", op)
	}
	return nil
}

// Paused sets the status of the tracked job to paused. It does not directly
// pause the job; instead, it expects the job to call job.Progressed soon,
// observe a "job is paused" error, and abort further work.
func (j *Job) Paused(ctx context.Context) error {
	return j.update(ctx, func(status *Status, payload *Payload) (bool, error) {
		if err := isControllable(payload, "PAUSE"); err != nil {
			return false, err
		}
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
func (j *Job) Resumed(ctx context.Context) error {
	return j.update(ctx, func(status *Status, payload *Payload) (bool, error) {
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
func (j *Job) Canceled(ctx context.Context) error {
	return j.update(ctx, func(status *Status, payload *Payload) (bool, error) {
		if err := isControllable(payload, "CANCEL"); err != nil {
			return false, err
		}
		if *status == StatusCanceled {
			// Already canceled - do nothing.
			return false, nil
		}
		if *status != StatusPaused && status.Terminal() {
			return false, fmt.Errorf("job with status %s cannot be canceled", *status)
		}
		*status = StatusCanceled
		payload.FinishedMicros = timeutil.ToUnixMicros(timeutil.Now())
		return true, nil
	})
}

// Failed marks the tracked job as having failed with the given error. Any
// errors encountered while updating the jobs table are logged but not returned,
// under the assumption that the the caller is already handling a more important
// error and doesn't care about this one.
func (j *Job) Failed(ctx context.Context, err error) {
	// To simplify cleanup routines, it is not an error to call Failed on a job
	// that was never Created.
	if j.id == nil {
		return
	}
	internalErr := j.update(ctx, func(status *Status, payload *Payload) (bool, error) {
		if status.Terminal() {
			// Already done - do nothing.
			return false, nil
		}
		*status = StatusFailed
		payload.Error = err.Error()
		payload.FinishedMicros = timeutil.ToUnixMicros(timeutil.Now())
		return true, nil
	})
	if internalErr != nil {
		log.Errorf(ctx, "Job: ignoring error %v while logging failure for job %d: %+v",
			err, *j.id, internalErr)
	}
	j.registry.unregister(*j.id)
}

// Succeeded marks the tracked job as having succeeded and sets its fraction
// completed to 1.0.
func (j *Job) Succeeded(ctx context.Context) error {
	defer j.registry.unregister(*j.id)
	return j.update(ctx, func(status *Status, payload *Payload) (bool, error) {
		if status.Terminal() {
			// Already done - do nothing.
			return false, nil
		}
		*status = StatusSucceeded
		payload.FinishedMicros = timeutil.ToUnixMicros(timeutil.Now())
		payload.FractionCompleted = 1.0
		return true, nil
	})
}

// FinishedWith is a shortcut for automatically calling Succeeded or Failed
// based on the presence of err. Any non-nil error is taken to mean that the job
// has failed. The error returned, if any, is serious enough that it should not
// be logged and ignored.
//
// TODO(benesch): Fix this wonky API. Once schema change leases are managed by
// this package, replace Succeeded, Failed, and FinishedWith with an API like
//
//      func (r *Registry) RunJob(setupFn func() Details, workFn func() error) (result, error)
//
// where RunJob handles writing to system.jobs automatically.
func (j *Job) FinishedWith(ctx context.Context, err error) error {
	j.mu.Lock()
	canceled := j.mu.canceled
	j.mu.Unlock()
	if canceled {
		// The registry canceled the job because its lease expired. This job will be
		// retried, potentially on another node, so we must report an ambiguous
		// result to the client because we don't know its fate.
		//
		// NB: Canceled jobs are automatically unregistered, so no need to call
		// j.registry.unregistered.
		//
		// TODO(benesch): In rare cases, this can return an ambiguous result error
		// when the result was not, in fact, ambigious. Specifically, if the job
		// succeeds or fails with a non-lease-related error immediately before it
		// loses its lease, we'll have knowledge of its true status but will blindly
		// report an ambiguous result. Ideally, we'd additionally check for
		// `errors.Cause(err) == context.Canceled`, but that yields false negatives
		// when using errors.Wrapf, and we'd much rather have false positives (too
		// many ambiguous results) than false negatives (too few ambiguous results).
		return roachpb.NewAmbiguousResultError("job lease expired")
	}
	if err, ok := errors.Cause(err).(*InvalidStatusError); ok &&
		(err.status == StatusPaused || err.status == StatusCanceled) {
		// If we couldn't operate on the job because it was paused or canceled, send
		// the more understandable "job paused" or "job canceled" error message to
		// the user.
		//
		// NB: Since we're not calling Succeeded or Failed, we need to manually
		// unregister the job.
		j.registry.unregister(*j.id)
		return fmt.Errorf("job %s", err.status)
	}
	if err != nil {
		j.Failed(ctx, err)
	} else if err := j.Succeeded(ctx); err != nil {
		// No callers of Succeeded do anything but log the error, so that behavior
		// is baked into the API of FinishedWith.
		log.Errorf(ctx, "ignoring error while marking job %d as successful: %+v", *j.id, err)
	}
	return nil
}

// SetDetails sets the details field of the currently running tracked job.
func (j *Job) SetDetails(ctx context.Context, details interface{}) error {
	return j.update(ctx, func(_ *Status, payload *Payload) (bool, error) {
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

// DB returns the *client.DB associated with this job.
func (j *Job) DB() *client.DB {
	return j.registry.db
}

// Gossip returns the *gossip.Gossip associated with this job.
func (j *Job) Gossip() *gossip.Gossip {
	return j.registry.gossip
}

// NodeID returns the roachpb.NodeID associated with this job.
func (j *Job) NodeID() roachpb.NodeID {
	return j.registry.nodeID.Get()
}

// ClusterID returns the uuid.UUID cluster ID associated with this job.
func (j *Job) ClusterID() uuid.UUID {
	return j.registry.clusterID()
}

func (j *Job) runInTxn(
	ctx context.Context, retryable func(context.Context, *client.Txn) error,
) error {
	if j.txn != nil {
		defer func() { j.txn = nil }()
		return j.txn.Exec(ctx, client.TxnExecOptions{AutoRetry: true},
			func(ctx context.Context, txn *client.Txn, _ *client.TxnExecOptions) error {
				return retryable(ctx, txn)
			})
	}
	return j.registry.db.Txn(ctx, retryable)
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

func (j *Job) insert(ctx context.Context, payload *Payload) error {
	if j.id != nil {
		// Already created - do nothing.
		return nil
	}

	var row parser.Datums
	if err := j.runInTxn(ctx, func(ctx context.Context, txn *client.Txn) error {
		payload.ModifiedMicros = timeutil.ToUnixMicros(txn.Proto().OrigTimestamp.GoTime())
		payloadBytes, err := protoutil.Marshal(payload)
		if err != nil {
			return err
		}

		const stmt = "INSERT INTO system.jobs (status, payload) VALUES ($1, $2) RETURNING id"
		row, err = j.registry.ex.QueryRowInTransaction(ctx, "job-insert", txn, stmt, StatusPending, payloadBytes)
		return err
	}); err != nil {
		return err
	}
	j.mu.payload = *payload
	j.id = (*int64)(row[0].(*parser.DInt))

	return nil
}

func (j *Job) update(
	ctx context.Context, updateFn func(*Status, *Payload) (doUpdate bool, err error),
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
		statusString, ok := row[0].(*parser.DString)
		if !ok {
			return errors.Errorf("Job: expected string status on job %d, but got %T", *j.id, statusString)
		}
		status := Status(*statusString)
		payload, err = UnmarshalPayload(row[1])
		if err != nil {
			return err
		}

		doUpdate, err := updateFn(&status, payload)
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
	return j.update(ctx, func(status *Status, payload *Payload) (bool, error) {
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

func (j *Job) cancel() {
	j.cancelFn()
	j.mu.Lock()
	defer j.mu.Unlock()
	j.mu.canceled = true
}

// Type returns the payload's job type.
func (p *Payload) Type() Type {
	switch p.Details.(type) {
	case *Payload_Backup:
		return TypeBackup
	case *Payload_Restore:
		return TypeRestore
	case *Payload_SchemaChange:
		return TypeSchemaChange
	case *Payload_Import:
		return TypeImport
	default:
		panic("Payload.Type called on a payload with an unknown details type")
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
// datum, which should be a parser.DBytes.
func UnmarshalPayload(datum parser.Datum) (*Payload, error) {
	payload := &Payload{}
	bytes, ok := datum.(*parser.DBytes)
	if !ok {
		return nil, errors.Errorf(
			"Job: failed to unmarshal payload as DBytes (was %T)", bytes)
	}
	if err := proto.Unmarshal([]byte(*bytes), payload); err != nil {
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
