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
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

	updates, err := rc.Watch(ctx)
	if err != nil {
		log.Errorf(ctx, "error initializing sql watcher: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case update := <-updates:
			var toUpdate []roachpb.SpanConfigEntry
			var toDelete []roachpb.Span
			if update.Deleted {
				toDelete = append(toDelete, update.Entry.Span)
			} else {
				if err != nil {
					log.Errorf(ctx, "couldn't convert to span config %v", err)
				}
				toUpdate = append(toUpdate, roachpb.SpanConfigEntry{
					Span:   update.Entry.Span,
					Config: update.Entry.Config,
				})
			}
			if err := rc.UpdateSpanConfigEntries(ctx, toUpdate, toDelete); err != nil {
				log.Errorf(ctx, "config reconciliation error: %v", err)
			}
		default:
			time.Sleep(1 * time.Second)
		}
	}
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (r *resumer) OnFailOrCancel(context.Context, interface{}) error {
	return errors.AssertionFailedf("span config reconciliation job can never fail or be canceled")
}
