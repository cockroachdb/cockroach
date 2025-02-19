// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// UpdateFn is the callback passed to Job.Update. It is called from the context
// of a transaction and is passed the current metadata for the job. The callback
// can modify metadata using the JobUpdater and the changes will be persisted
// within the same transaction.
//
// The function is free to modify contents of JobMetadata in place (but the
// changes will be ignored unless JobUpdater is used).
type UpdateFn func(txn isql.Txn, md JobMetadata, ju *JobUpdater) error

type Updater struct {
	j            *Job
	txn          isql.Txn
	txnDebugName string
}

func (j *Job) NoTxn() Updater {
	return Updater{j: j}
}

func (j *Job) DebugNameNoTxn(txnDebugName string) Updater {
	return Updater{j: j, txnDebugName: txnDebugName}
}

func (j *Job) WithTxn(txn isql.Txn) Updater {
	return Updater{j: j, txn: txn}
}

func (u Updater) update(ctx context.Context, updateFn UpdateFn) (retErr error) {
	if u.txn == nil {
		return u.j.registry.db.Txn(ctx, func(
			ctx context.Context, txn isql.Txn,
		) error {
			if u.txnDebugName != "" {
				txn.KV().SetDebugName(u.txnDebugName)
			}
			u.txn = txn
			return u.update(ctx, updateFn)
		})
	}
	ctx, sp := tracing.ChildSpan(ctx, "update-job")
	defer sp.Finish()

	var payload *jobspb.Payload
	var progress *jobspb.Progress
	var state State
	j := u.j
	defer func() {
		if retErr != nil && !HasJobNotFoundError(retErr) {
			retErr = errors.Wrapf(retErr, "job %d", j.id)
			return
		}
		j.mu.Lock()
		defer j.mu.Unlock()
		if payload != nil {
			j.mu.payload = *payload
		}
		if progress != nil {
			j.mu.progress = *progress
		}
		if state != "" {
			j.mu.state = state
		}
	}()

	const loadJobQuery = `
WITH
  latestpayload AS (
    SELECT job_id, value
    FROM system.job_info AS payload
    WHERE info_key = 'legacy_payload' AND job_id = $1
    ORDER BY written DESC LIMIT 1
  ),
  latestprogress AS (
    SELECT job_id, value
    FROM system.job_info AS progress
    WHERE info_key = 'legacy_progress' AND job_id = $1
    ORDER BY written DESC LIMIT 1
  )
SELECT status, payload.value AS payload, progress.value AS progress,
       claim_session_id, COALESCE(last_run, created), COALESCE(num_runs, 0)
FROM system.jobs AS j
INNER JOIN latestpayload AS payload ON j.id = payload.job_id
LEFT JOIN latestprogress AS progress ON j.id = progress.job_id
WHERE id = $1
`
	row, err := u.txn.QueryRowEx(
		ctx, "select-job", u.txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		loadJobQuery, j.ID(),
	)
	if err != nil {
		return err
	}
	if row == nil {
		return &JobNotFoundError{jobID: j.ID()}
	}

	if state, err = unmarshalState(row[0]); err != nil {
		return err
	}
	if payload, err = UnmarshalPayload(row[1]); err != nil {
		return err
	}
	if progress, err = UnmarshalProgress(row[2]); err != nil {
		return err
	}
	beforeProgress := *progress
	beforePayload := *payload
	if j.session != nil {
		if row[3] == tree.DNull {
			return errors.Errorf(
				"with state %q: expected session %q but found NULL",
				state, j.session.ID())
		}
		storedSession := []byte(*row[3].(*tree.DBytes))
		if !bytes.Equal(storedSession, j.session.ID().UnsafeBytes()) {
			return errors.Errorf(
				"with state %q: expected session %q but found %q",
				state, j.session.ID(), sqlliveness.SessionID(storedSession))
		}
	} else {
		log.VInfof(ctx, 1, "job %d: update called with no session ID", j.ID())
	}

	lastRun, ok := row[4].(*tree.DTimestamp)
	if !ok {
		return errors.AssertionFailedf("expected timestamp last_run, but got %T", lastRun)
	}
	numRuns, ok := row[5].(*tree.DInt)
	if !ok {
		return errors.AssertionFailedf("expected int num_runs, but got %T", numRuns)
	}

	md := JobMetadata{
		ID:       j.ID(),
		State:    state,
		Payload:  payload,
		Progress: progress,
	}

	var ju JobUpdater
	if err := updateFn(u.txn, md, &ju); err != nil {
		return err
	}

	// a job state is considered updated if:
	//  1. the state of the updated metadata is not empty
	//  2. the state of the updated metadata is not equal to old state
	//  3. the state of the updated metadata and the old state is running
	// #1 should be sufficient to determine whether a state change has happened
	// as the state field of ju.md is not empty only when JobMetadata.Updatestate is
	// called, and this is only called in places where a state change is happening.
	// Since this may not be in the case in the future we add condition #2. #3 is
	// required when a job starts because it may already have a "running" state.
	//
	if ju.md.State != "" &&
		(ju.md.State != state || (ju.md.State == StateRunning && state == StateRunning)) {
		u.txn.KV().AddCommitTrigger(func(ctx context.Context) {
			p := ju.md.Payload
			// In some cases, ju.md.Payload may be nil, such as a cancel-requested state update.
			// In this case, payload is used.
			if p == nil {
				p = payload
			}
			// If run stats has been updated, use the updated run stats.
			LogStateChangeStructured(ctx, md.ID, p.Type().String(), p, state, ju.md.State)
		})
	}
	if j.registry.knobs.BeforeUpdate != nil {
		if err := j.registry.knobs.BeforeUpdate(md, ju.md); err != nil {
			return err
		}
	}

	if !ju.hasUpdates() {
		return nil
	}

	// Build a statement of the following form, depending on which properties
	// need updating:
	//
	//   UPDATE system.jobs
	//   SET
	//     [status = $2,]
	//     [payload = $y,]
	//     [progress = $z]
	//   WHERE
	//     id = $1

	var setters []string
	params := []interface{}{j.ID()} // $1 is always the job ID.
	addSetter := func(column string, value interface{}) {
		params = append(params, value)
		setters = append(setters, fmt.Sprintf("%s = $%d", column, len(params)))
	}

	if ju.md.State != "" {
		addSetter("status", ju.md.State)
	}

	var payloadBytes []byte
	if ju.md.Payload != nil {
		payload = ju.md.Payload
		var err error
		payloadBytes, err = protoutil.Marshal(payload)
		if err != nil {
			return err
		}
	}

	var progressBytes []byte
	if ju.md.Progress != nil {
		progress = ju.md.Progress
		progress.ModifiedMicros = timeutil.ToUnixMicros(u.now())
		var err error
		progressBytes, err = protoutil.Marshal(progress)
		if err != nil {
			return err
		}
	}

	if len(setters) != 0 {
		updateStmt := fmt.Sprintf(
			"UPDATE system.jobs SET %s WHERE id = $1",
			strings.Join(setters, ", "),
		)
		n, err := u.txn.ExecEx(
			ctx, "job-update", u.txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			updateStmt, params...,
		)
		if err != nil {
			return err
		}
		if n != 1 {
			return errors.Errorf(
				"expected exactly one row affected, but %d rows affected by job update", n,
			)
		}
	}

	// Insert the job payload and progress into the system.jobs_info table.
	infoStorage := j.InfoStorage(u.txn)
	infoStorage.claimChecked = true
	if payloadBytes != nil {
		if err := infoStorage.WriteLegacyPayload(ctx, payloadBytes); err != nil {
			return err
		}
	}
	if progressBytes != nil {
		if err := infoStorage.WriteLegacyProgress(ctx, progressBytes); err != nil {
			return err
		}
	}

	v, err := u.txn.GetSystemSchemaVersion(ctx)
	if err != nil {
		return err
	}
	if v.AtLeast(clusterversion.V25_1_AddJobsTables.Version()) {
		if ju.md.State != "" && ju.md.State != state {
			if err := j.Messages().Record(ctx, u.txn, "state", string(ju.md.State)); err != nil {
				return err
			}
			// If we are changing state, we should clear out the status, unless
			// we are about to set it to something instead.
			if progress == nil || progress.StatusMessage == "" {
				if err := j.StatusStorage().Clear(ctx, u.txn); err != nil {
					return err
				}
			}
		}

		if progress != nil {
			var ts hlc.Timestamp
			if hwm := progress.GetHighWater(); hwm != nil {
				ts = *hwm
			}

			if err := j.ProgressStorage().Set(ctx, u.txn, float64(progress.GetFractionCompleted()), ts); err != nil {
				return err
			}

			if progress.StatusMessage != beforeProgress.StatusMessage {
				if err := j.StatusStorage().Set(ctx, u.txn, progress.StatusMessage); err != nil {
					return err
				}
			}

			if progress.TraceID != beforeProgress.TraceID {
				if err := j.Messages().Record(ctx, u.txn, "trace-id", fmt.Sprintf("%d", progress.TraceID)); err != nil {
					return err
				}
			}
		}
	}
	if v.AtLeast(clusterversion.V25_1_AddJobsColumns.Version()) {

		vals := []interface{}{j.ID()}

		var update strings.Builder

		if payloadBytes != nil {
			if beforePayload.Description != payload.Description {
				if update.Len() > 0 {
					update.WriteString(", ")
				}
				vals = append(vals, payload.Description)
				fmt.Fprintf(&update, "description = $%d", len(vals))
			}

			if beforePayload.UsernameProto.Decode() != payload.UsernameProto.Decode() {
				if update.Len() > 0 {
					update.WriteString(", ")
				}
				vals = append(vals, payload.UsernameProto.Decode().Normalized())
				fmt.Fprintf(&update, "owner = $%d", len(vals))
			}

			if beforePayload.Error != payload.Error {
				if update.Len() > 0 {
					update.WriteString(", ")
				}
				vals = append(vals, payload.Error)
				fmt.Fprintf(&update, "error_msg = $%d", len(vals))
			}

			if beforePayload.FinishedMicros != payload.FinishedMicros {
				if update.Len() > 0 {
					update.WriteString(", ")
				}
				vals = append(vals, time.UnixMicro(payload.FinishedMicros))
				fmt.Fprintf(&update, "finished = $%d", len(vals))
			}

		}
		if len(vals) > 1 {
			stmt := fmt.Sprintf("UPDATE system.jobs SET %s WHERE id = $1", update.String())
			if _, err := u.txn.ExecEx(
				ctx, "job-update-row", u.txn.KV(),
				sessiondata.NodeUserSessionDataOverride,
				stmt, vals...,
			); err != nil {
				return err
			}
		}
	}

	return nil
}

// JobMetadata groups the job metadata values passed to UpdateFn.
type JobMetadata struct {
	ID       jobspb.JobID
	State    State
	Payload  *jobspb.Payload
	Progress *jobspb.Progress
}

// CheckRunningOrReverting returns an InvalidStatusError if md.Status is not
// StatusRunning or StatusReverting.
func (md *JobMetadata) CheckRunningOrReverting() error {
	if md.State != StateRunning && md.State != StateReverting {
		return &InvalidStateError{md.ID, md.State, "update progress on", md.Payload.Error}
	}
	return nil
}

// JobUpdater accumulates changes to job metadata that are to be persisted.
type JobUpdater struct {
	md JobMetadata
}

// UpdateState sets a new status (to be persisted).
func (ju *JobUpdater) UpdateState(state State) {
	ju.md.State = state
}

// UpdatePayload sets a new Payload (to be persisted).
//
// WARNING: the payload can be large (resulting in a large KV for each version);
// it shouldn't be updated frequently.
func (ju *JobUpdater) UpdatePayload(payload *jobspb.Payload) {
	ju.md.Payload = payload
}

// UpdateProgress sets a new Progress (to be persisted).
func (ju *JobUpdater) UpdateProgress(progress *jobspb.Progress) {
	ju.md.Progress = progress
}

func (ju *JobUpdater) hasUpdates() bool {
	return ju.md != JobMetadata{}
}

func (ju *JobUpdater) PauseRequested(
	ctx context.Context, txn isql.Txn, md JobMetadata, reason string,
) error {
	return ju.PauseRequestedWithFunc(ctx, txn, md, nil /* fn */, reason)
}

func (ju *JobUpdater) PauseRequestedWithFunc(
	ctx context.Context, txn isql.Txn, md JobMetadata, fn onPauseRequestFunc, reason string,
) error {
	if md.State == StatePauseRequested || md.State == StatePaused {
		return nil
	}
	if md.State != StatePending && md.State != StateRunning && md.State != StateReverting {
		return fmt.Errorf("job with state %s cannot be requested to be paused", md.State)
	}
	if fn != nil {
		if err := fn(ctx, md, ju); err != nil {
			return err
		}
	}
	ju.UpdateState(StatePauseRequested)
	md.Payload.PauseReason = reason
	ju.UpdatePayload(md.Payload)
	log.Infof(ctx, "job %d: pause requested recorded with reason %s", md.ID, reason)
	return nil
}

// Unpaused sets the state of the tracked job to running or reverting iff the
// job is currently paused. It does not directly resume the job.
func (ju *JobUpdater) Unpaused(_ context.Context, md JobMetadata) error {
	if md.State == StateRunning || md.State == StateReverting {
		// Already resumed - do nothing.
		return nil
	}
	if md.State != StatePaused {
		return fmt.Errorf("job with state %s cannot be resumed", md.State)
	}
	// We use the absence of error to determine what state we should
	// resume into.
	if md.Payload.FinalResumeError == nil {
		ju.UpdateState(StateRunning)
	} else {
		ju.UpdateState(StateReverting)
	}
	return nil
}

func (ju *JobUpdater) CancelRequested(ctx context.Context, md JobMetadata) error {
	return ju.CancelRequestedWithReason(ctx, md, errJobCanceled)
}

func (ju *JobUpdater) CancelRequestedWithReason(
	ctx context.Context, md JobMetadata, reason error,
) error {
	if md.Payload.Noncancelable {
		return errors.Newf("job %d: not cancelable", md.ID)
	}
	if md.State == StateCancelRequested || md.State == StateCanceled {
		return nil
	}
	if md.State != StatePending && md.State != StateRunning && md.State != StatePaused {
		return fmt.Errorf("job with state %s cannot be requested to be canceled", md.State)
	}
	if md.State == StatePaused && md.Payload.FinalResumeError != nil {
		decodedErr := errors.DecodeError(ctx, *md.Payload.FinalResumeError)
		return errors.Wrapf(decodedErr, "job %d is paused and has non-nil FinalResumeError "+
			"hence cannot be canceled and should be reverted", md.ID)
	}
	if !errors.Is(reason, errJobCanceled) {
		md.Payload.Error = reason.Error()
		ju.UpdatePayload(md.Payload)
	}
	ju.UpdateState(StateCancelRequested)
	return nil
}

// Update is used to read the metadata for a job and potentially update it.
//
// The updateFn is called in the context of a transaction and is passed the
// current metadata for the job. It can choose to update parts of the metadata
// using the JobUpdater, causing them to be updated within the same transaction.
//
// Sample usage:
//
//	err := j.Update(ctx, func(_ *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
//	  if md.State != StateRunning {
//	    return errors.New("job no longer running")
//	  }
//	  ju.UpdateState(StatePaused)
//	  // <modify md.Payload>
//	  ju.UpdatePayload(md.Payload)
//	}
//
// Note that there are various convenience wrappers (like FractionProgressed)
// defined in jobs.go.
func (u Updater) Update(ctx context.Context, updateFn UpdateFn) error {
	return u.update(ctx, updateFn)
}

func (u Updater) now() time.Time {
	return u.j.registry.clock.Now().GoTime()
}
