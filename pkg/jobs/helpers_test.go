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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
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
	OnResume     func() error
	FailOrCancel func() error
	Success      func() error
	Terminal     func()
}

func (d FakeResumer) Resume(_ context.Context, _ interface{}, _ chan<- tree.Datums) error {
	if d.OnResume != nil {
		return d.OnResume()
	}
	return nil
}

func (d FakeResumer) OnFailOrCancel(_ context.Context, _ interface{}) error {
	if d.FailOrCancel != nil {
		return d.FailOrCancel()
	}
	return nil
}

func (d FakeResumer) OnSuccess(_ context.Context, _ *client.Txn) error {
	if d.Success != nil {
		return d.Success()
	}
	return nil
}

func (d FakeResumer) OnTerminal(_ context.Context, _ Status, _ chan<- tree.Datums) {
	if d.Terminal != nil {
		d.Terminal()
	}
}

var _ Resumer = FakeResumer{}
