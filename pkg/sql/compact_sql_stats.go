// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type sqlStatsCompactionResumer struct {
	*jobs.Job
	compactionExecutor *persistedsqlstats.StatsCompactor
}

var _ jobs.Resumer = &sqlStatsCompactionResumer{}

// Resume implements the jobs.Resumer interface.
func (r *sqlStatsCompactionResumer) Resume(ctx context.Context, execCtx interface{}) error {
	log.Infof(ctx, "starting sql stats compaction job")
	p := execCtx.(JobExecContext)
	ie := p.ExecCfg().InternalExecutor
	db := p.ExecCfg().DB

	// We check for concurrently running SQL Stats compaction jobs. We only allow
	// one job to be running at the same time.
	if err := persistedsqlstats.CheckExistingCompactionJob(ctx, r.Job, ie, nil /* txn */); err != nil {
		if errors.Is(err, persistedsqlstats.ErrConcurrentSQLStatsCompaction) {
			log.Infof(ctx, "exiting due to a running sql stats compaction job")
			return nil
		}
		return err
	}

	jobDetails, ok := r.Job.Details().(jobspb.SQLStatsCompactionDetails)
	if !ok {
		return errors.AssertionFailedf("invalid job detail payload for sql stats compaction")
	}
	r.compactionExecutor.Init(ie, db, jobDetails.DisableFollowerRead)
	return r.compactionExecutor.DeleteOldestEntries(ctx)
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (r *sqlStatsCompactionResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	return nil
}

func init() {
	jobs.RegisterConstructor(jobspb.TypeSQLStatsCompaction, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &sqlStatsCompactionResumer{
			Job: job,
			compactionExecutor: &persistedsqlstats.StatsCompactor{
				Settings: settings,
			},
		}
	})
}
