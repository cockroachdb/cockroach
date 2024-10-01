// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobstest

import "context"

// FakeResumer calls optional callbacks during the job lifecycle.
type FakeResumer struct {
	OnResume      func(context.Context) error
	FailOrCancel  func(context.Context) error
	Success       func() error
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
