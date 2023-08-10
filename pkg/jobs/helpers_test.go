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

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// FakeResumer calls optional callbacks during the job lifecycle.
type FakeResumer struct {
	OnResume      func(context.Context) error
	FailOrCancel  func(context.Context) error
	Success       func() error
	PauseRequest  onPauseRequestFunc
	TraceRealSpan bool
}

func (d FakeResumer) ForceRealSpan() bool {
	return d.TraceRealSpan
}

func (d FakeResumer) DumpTraceAfterRun() bool {
	return true
}

var _ Resumer = FakeResumer{}

func (d FakeResumer) Resume(ctx context.Context, execCtx interface{}) error {
	if d.OnResume != nil {
		if err := d.OnResume(ctx); err != nil {
			return err
		}
	}
	if d.Success != nil {
		return d.Success()
	}
	return nil
}

func (d FakeResumer) OnFailOrCancel(ctx context.Context, _ interface{}, _ error) error {
	if d.FailOrCancel != nil {
		return d.FailOrCancel(ctx)
	}
	return nil
}

// OnPauseRequestFunc forwards the definition for use in tests.
type OnPauseRequestFunc = onPauseRequestFunc

var _ PauseRequester = FakeResumer{}

func (d FakeResumer) OnPauseRequest(
	ctx context.Context, execCtx interface{}, txn isql.Txn, details *jobspb.Progress,
) error {
	if d.PauseRequest == nil {
		return nil
	}
	return d.PauseRequest(ctx, execCtx, txn, details)
}

func (r *Registry) CancelRequested(ctx context.Context, txn isql.Txn, id jobspb.JobID) error {
	return r.cancelRequested(ctx, txn, id)
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

// Paused is a wrapper around the internal function that moves a job to the
// paused state.
func (j *Job) Paused(ctx context.Context) error {
	return j.NoTxn().paused(ctx, nil /* fn */)
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

// TestingCurrentStatus returns the current job status from the jobs table or error.
func (j *Job) TestingCurrentStatus(ctx context.Context) (Status, error) {
	var statusString tree.DString
	const selectStmt = "SELECT status FROM system.jobs WHERE id = $1"
	row, err := j.registry.db.Executor().QueryRow(ctx, "job-status", nil, selectStmt, j.ID())
	if err != nil {
		return "", errors.Wrapf(err, "job %d: can't query system.jobs", j.ID())
	}
	if row == nil {
		return "", errors.Errorf("job %d: not found in system.jobs", j.ID())
	}

	statusString = tree.MustBeDString(row[0])
	return Status(statusString), nil
}

const (
	AdoptQuery                     = claimQuery
	CancelQuery                    = pauseAndCancelUpdate
	RemoveClaimsQuery              = removeClaimsForDeadSessionsQuery
	ProcessJobsQuery               = processQueryWithBackoff
	IntervalBaseSettingKey         = intervalBaseSettingKey
	AdoptIntervalSettingKey        = adoptIntervalSettingKey
	CancelIntervalSettingKey       = cancelIntervalSettingKey
	GcIntervalSettingKey           = gcIntervalSettingKey
	RetentionTimeSettingKey        = retentionTimeSettingKey
	DefaultAdoptInterval           = defaultAdoptInterval
	ExecutionErrorsMaxEntriesKey   = executionErrorsMaxEntriesKey
	ExecutionErrorsMaxEntrySizeKey = executionErrorsMaxEntrySizeKey
)

var (
	AdoptIntervalSetting            = adoptIntervalSetting
	CancelIntervalSetting           = cancelIntervalSetting
	CancellationsUpdateLimitSetting = cancellationsUpdateLimitSetting
	GcIntervalSetting               = gcIntervalSetting
)
