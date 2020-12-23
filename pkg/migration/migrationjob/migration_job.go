// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package migrationjob contains the jobs.Resumer implementation
// used for long-running migrations.
package migrationjob

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
)

func init() {
	jobs.RegisterConstructor(jobspb.TypeLongRunningMigration, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &resumer{j: job}
	})
}

type resumer struct {
	j *jobs.Job
}

var _ jobs.Resumer = (*resumer)(nil)

func (r resumer) Resume(ctx context.Context, execCtxI interface{}) error {
	// TODO(ajwerner): add some check to see if we're done.
	execCtx := execCtxI.(sql.JobExecContext)
	pl := r.j.Payload()
	cv := *pl.GetLongRunningMigration().ClusterVersion
	m, ok := migration.GetMigration(cv)
	if !ok {
		return nil
	}
	return m.Run(ctx, cv, execCtx.MigrationCluster())
}

// The long-running migration resumer has no reverting logic.
func (r resumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	return nil
}
