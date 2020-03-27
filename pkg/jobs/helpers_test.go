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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func ResetConstructors() func() {
	old := make(map[jobspb.Type]Constructor)
	for k, v := range constructors {
		old[k] = v
	}
	return func() { constructors = old }
}

// FakeResumer calls optional callbacks during the job lifecycle.
type FakeResumer struct {
	OnResume     func(context.Context, chan<- tree.Datums) error
	FailOrCancel func(context.Context) error
	Success      func() error
	PauseRequest onPauseRequestFunc
}

var _ Resumer = FakeResumer{}

func (d FakeResumer) Resume(
	ctx context.Context, _ interface{}, resultsCh chan<- tree.Datums,
) error {
	if d.OnResume != nil {
		if err := d.OnResume(ctx, resultsCh); err != nil {
			return err
		}
	}
	if d.Success != nil {
		return d.Success()
	}
	return nil
}

func (d FakeResumer) OnFailOrCancel(ctx context.Context, _ interface{}) error {
	if d.FailOrCancel != nil {
		return d.FailOrCancel(ctx)
	}
	return nil
}

// OnPauseRequestFunc forwards the definition for use in tests.
type OnPauseRequestFunc = onPauseRequestFunc

var _ PauseRequester = FakeResumer{}

func (d FakeResumer) OnPauseRequest(
	ctx context.Context, phs interface{}, txn *kv.Txn, details *jobspb.Progress,
) error {
	if d.PauseRequest == nil {
		return nil
	}
	return d.PauseRequest(ctx, phs, txn, details)
}

// Started is a wrapper around the internal function that moves a job to the
// started state.
func (j *Job) Started(ctx context.Context) error {
	return j.started(ctx)
}

// Created is a wrapper around the internal function that creates the initial
// job state.
func (j *Job) Created(ctx context.Context) error {
	return j.created(ctx)
}

// Paused is a wrapper around the internal function that moves a job to the
// paused state.
func (j *Job) Paused(ctx context.Context) error {
	return j.paused(ctx, nil /* fn */)
}

// Failed is a wrapper around the internal function that moves a job to the
// failed state.
func (j *Job) Failed(ctx context.Context, causingErr error) error {
	return j.failed(ctx, causingErr, nil /* fn */)
}

// Succeeded is a wrapper around the internal function that moves a job to the
// succeeded state.
func (j *Job) Succeeded(ctx context.Context) error {
	return j.succeeded(ctx, nil /* fn */)
}
