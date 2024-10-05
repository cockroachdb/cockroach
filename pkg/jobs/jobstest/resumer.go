// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobstest

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
)

// FakeResumer calls optional callbacks during the job lifecycle.
type FakeResumer struct {
	OnResume      func(context.Context) error
	FailOrCancel  func(context.Context) error
	Success       func() error
	PauseRequest  func(ctx context.Context, planHookState interface{}, txn isql.Txn, progress *jobspb.Progress) error
	TraceRealSpan bool
}

func (d FakeResumer) ForceRealSpan() bool {
	return d.TraceRealSpan
}

func (d FakeResumer) DumpTraceAfterRun() bool {
	return true
}

func (d FakeResumer) Resume(ctx context.Context, _ interface{}) error {
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

func (d FakeResumer) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

func (d FakeResumer) OnPauseRequest(
	ctx context.Context, execCtx interface{}, txn isql.Txn, details *jobspb.Progress,
) error {
	if d.PauseRequest == nil {
		return nil
	}
	return d.PauseRequest(ctx, execCtx, txn, details)
}
