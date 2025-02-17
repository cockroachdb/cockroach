// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// Functions in this file are exported for test code convience and
// should not be used in production code.

// CancelRequested marks the job as cancel-requested using the
// specified txn (may be nil).
func (r *Registry) CancelRequested(ctx context.Context, txn isql.Txn, id jobspb.JobID) error {
	job, err := r.LoadJobWithTxn(ctx, id, txn)
	if err != nil {
		return err
	}
	return job.WithTxn(txn).CancelRequested(ctx)
}

// Started is a wrapper around the internal function that moves a job to the
// started state.
func (j *Job) Started(ctx context.Context) error {
	return j.NoTxn().started(ctx)
}

// Reverted is a wrapper around the internal function that moves a job to the
// reverting state.
func (j *Job) Reverted(ctx context.Context, err error) error {
	return j.NoTxn().reverted(ctx, err, nil)
}

// Paused is a helper to the paused state.
func (j *Job) Paused(ctx context.Context) error {
	return j.NoTxn().Update(ctx, func(txn isql.Txn, md JobMetadata, ju *JobUpdater) error {
		if md.State == StatePaused {
			// Already paused - do nothing.
			return nil
		}
		if md.State != StatePauseRequested {
			return errors.Newf("job with state %s cannot be set to paused", md.State)
		}
		ju.UpdateState(StatePaused)
		return nil
	})
}

// Failed is a wrapper around the internal function that moves a job to the
// failed state.
func (j *Job) Failed(ctx context.Context, causingErr error) error {
	return j.NoTxn().failed(ctx, causingErr)
}

// Succeeded is a wrapper around the internal function that moves a job to the
// succeeded state.
func (j *Job) Succeeded(ctx context.Context) error {
	return j.NoTxn().succeeded(ctx, nil /* fn */)
}

// TestingCurrentState returns the current job state from the jobs table or error.
func (j *Job) TestingCurrentState(ctx context.Context) (State, error) {
	var stateString tree.DString
	const selectStmt = "SELECT status FROM system.jobs WHERE id = $1"
	row, err := j.registry.db.Executor().QueryRow(ctx, "job-status", nil, selectStmt, j.ID())
	if err != nil {
		return "", errors.Wrapf(err, "job %d: can't query system.jobs", j.ID())
	}
	if row == nil {
		return "", errors.Errorf("job %d: not found in system.jobs", j.ID())
	}

	stateString = tree.MustBeDString(row[0])
	return State(stateString), nil
}

const (
	AdoptQuery               = claimQuery
	CancelQuery              = pauseAndCancelUpdate
	RemoveClaimsQuery        = removeClaimsForDeadSessionsQuery
	ProcessJobsQuery         = processQuery
	IntervalBaseSettingKey   = intervalBaseSettingKey
	AdoptIntervalSettingKey  = adoptIntervalSettingKey
	CancelIntervalSettingKey = cancelIntervalSettingKey
	GcIntervalSettingKey     = gcIntervalSettingKey
	RetentionTimeSettingKey  = retentionTimeSettingKey
	DefaultAdoptInterval     = defaultAdoptInterval
)

var (
	AdoptIntervalSetting            = adoptIntervalSetting
	CancelIntervalSetting           = cancelIntervalSetting
	CancellationsUpdateLimitSetting = cancellationsUpdateLimitSetting
	GcIntervalSetting               = gcIntervalSetting
)
