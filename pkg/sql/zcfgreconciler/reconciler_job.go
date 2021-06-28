// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package zcfgreconciler

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/errors"
)

func init() {
	jobs.RegisterConstructor(jobspb.TypeAutoZoneConfigReconciliation,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &resumer{job: job}
		})
}

type resumer struct {
	job *jobs.Job
}

var _ jobs.Resumer = (*resumer)(nil)

// Resume implements the jobs.Resumer interface.
func (z *resumer) Resume(ctx context.Context, _ interface{}) error {
	// TODO(arul): This doesn't do anything yet.
	<-ctx.Done()
	return ctx.Err()
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (z *resumer) OnFailOrCancel(ctx context.Context, _ interface{}) error {
	return errors.AssertionFailedf("zone config reconciliation job can never fail or be canceled")
}
