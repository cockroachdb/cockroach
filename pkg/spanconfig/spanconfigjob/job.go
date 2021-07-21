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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigreconciler"
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

	reconciler := spanconfigreconciler.NewReconciler(execCtx.ExecCfg())

	closerCh := make(chan struct{})
	// When the job starts up initially, try doing a full reconciliation until
	// it succeeds.
	err := retry.WithMaxAttempts(
		ctx,
		retry.Options{
			InitialBackoff: 10 * time.Second,
			MaxBackoff:     5 * time.Minute,
			Multiplier:     2,
			Closer:         closerCh,
		},
		10000, /* Just a large number */
		func() error {
			select {
			case <-ctx.Done():
				close(closerCh)
				return ctx.Err()
			default:
			}

			entries, err := reconciler.FullReconcile(ctx)
			if err != nil {
				log.Errorf(ctx, "full reconciliation error: %v", err)
			}
			var update []roachpb.SpanConfigEntry
			var delete []roachpb.Span

			for _, entry := range entries {
				config, err := entry.Config.GenerateSpanConfig()
				if err != nil {
					log.Errorf(ctx, "error converting entry with span %v to a span config: %v", entry.Span, err)
					return err
				}

				update = append(update, roachpb.SpanConfigEntry{
					Span:   entry.Span,
					Config: config,
				})
			}

			rc := execCtx.ConfigReconciliationJobDeps()
			if err := rc.UpdateSpanConfigEntries(ctx, update, delete); err != nil {
				log.Errorf(ctx, "config reconciliation error: %v", err)
			}
			return err
		},
	)

	if err != nil {
		log.Infof(ctx, "error retrying full reconciliation on job startup: %v", err)
	}

	// TODO(zcfgs-pod): Hookt up a rangefeed over system.{descriptor,zones} to
	// react to changes beyond the full reconciliation, instead of this
	// placeholder write over the same keyspan over and over again.
	// TODO(zcfgs-pod): How do we test this?
	nameSpaceTableStart := execCtx.ExecCfg().Codec.TablePrefix(keys.NamespaceTableID)
	nameSpaceTableSpan := roachpb.Span{
		Key:    nameSpaceTableStart,
		EndKey: nameSpaceTableStart.PrefixEnd(),
	}
	for {

		var update []roachpb.SpanConfigEntry
		var delete []roachpb.Span
		update = append(update, roachpb.SpanConfigEntry{
			Span:   nameSpaceTableSpan,
			Config: roachpb.SpanConfig{},
		})

		// NB: We cannot return from this method, as that would put the job in
		// an un-revertable, non-running state.
		rc := execCtx.ConfigReconciliationJobDeps()
		if err := rc.UpdateSpanConfigEntries(ctx, update, delete); err != nil {
			log.Errorf(ctx, "config reconciliation error: %v", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			time.Sleep(1 * time.Second)
		}
	}
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (r *resumer) OnFailOrCancel(context.Context, interface{}) error {
	return errors.AssertionFailedf("span config reconciliation job can never fail or be canceled")
}
