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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func ResetResumeHooks() func() {
	oldResumeHooks := resumeHooks
	return func() { resumeHooks = oldResumeHooks }
}

// FakeResumer calls optional callbacks during the job lifecycle.
type FakeResumer struct {
	OnResume func(job *Job) error
	Fail     func(job *Job) error
	Success  func(job *Job) error
	Terminal func(job *Job)
}

func (d FakeResumer) Resume(
	_ context.Context, job *Job, _ interface{}, _ chan<- tree.Datums,
) error {
	if d.OnResume != nil {
		return d.OnResume(job)
	}
	return nil
}

func (d FakeResumer) OnFailOrCancel(_ context.Context, _ *client.Txn, job *Job) error {
	if d.Fail != nil {
		return d.Fail(job)
	}
	return nil
}

func (d FakeResumer) OnSuccess(_ context.Context, _ *client.Txn, job *Job) error {
	if d.Success != nil {
		return d.Success(job)
	}
	return nil
}

func (d FakeResumer) OnTerminal(_ context.Context, job *Job, _ Status, _ chan<- tree.Datums) {
	if d.Terminal != nil {
		d.Terminal(job)
	}
}

var _ Resumer = FakeResumer{}
