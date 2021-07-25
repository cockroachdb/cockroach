// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigjob

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

func init() {
	jobs.RegisterConstructor(jobspb.TypeAutoSpanConfigReconciliation,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &resumer{job: job}
		})
}

type resumer struct {
	job *jobs.Job
}

var _ jobs.Resumer = (*resumer)(nil)

// Resume implements the jobs.Resumer interface.
func (r *resumer) Resume(ctx context.Context, execCtxI interface{}) error {
	execCtx := execCtxI.(sql.JobExecContext)
	rc := execCtx.ConfigReconciliationJobDeps()

	var updateCh <-chan spanconfig.Update

	maxAttempts := 100
	err := retry.WithMaxAttempts(
		ctx,
		retry.Options{
			InitialBackoff: 10 * time.Second,
			MaxBackoff:     5 * time.Minute,
			Multiplier:     2,
		},
		maxAttempts,
		func() error {
			var err error
			updateCh, err = rc.WatchForSQLUpdates(ctx)
			if err != nil {
				log.Errorf(ctx, "error initializing sql watcher: %v", err)
			}
			return err
		})
	if err != nil {
		// TODO(zcfg-pod): How do we want to deal with this scenario? Can we bounce
		// the job to another pod so that `Resume` starts all over again?
		log.Errorf(ctx, "could not initialize watcher after %d attempts", maxAttempts)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case update := <-updateCh:
			// TODO(zcfg-pod): add the ability to batch updates here instead of
			// sending them one by one.
			var toUpdate []roachpb.SpanConfigEntry
			var toDelete []roachpb.Span
			if update.Deleted {
				toDelete = append(toDelete, update.Entry.Span)
			} else {
				toUpdate = append(toUpdate, roachpb.SpanConfigEntry{
					Span:   update.Entry.Span,
					Config: update.Entry.Config,
				})
			}
			if err := rc.UpdateSpanConfigEntries(ctx, toUpdate, toDelete); err != nil {
				log.Errorf(ctx, "config reconciliation error: %v", err)
			}
		}
	}
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (r *resumer) OnFailOrCancel(context.Context, interface{}) error {
	return errors.AssertionFailedf("span config reconciliation job can never fail or be canceled")
}
