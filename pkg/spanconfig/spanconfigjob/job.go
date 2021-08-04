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
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigstore"
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
	retryOpts := retry.Options{
		InitialBackoff: 10 * time.Second,
		MaxBackoff:     time.Minute,
		Multiplier:     2,
	}
	err := retry.WithMaxAttempts(ctx, retryOpts, math.MaxInt32, func() error {
		// TODO(zcfgs-pod): Checkpoint progress; if we restart and if the
		// watcher has sufficient history we can avoid a full reconciliation.
		var err error
		updateCh, err = rc.WatchForSQLUpdates(ctx)
		if err != nil {
			log.Errorf(ctx, "error initializing sql watcher: %v", err)
		}
		log.Infof(ctx, "initialized sql watcher")
		return err
	})
	if err != nil {
		log.Fatalf(ctx, "could not initialize watcher: %v", err)
	}

	// Populate a local cache of our span configs from KV.
	store := spanconfigstore.NewWithoutCoalesce()
	{
		prefix := execCtx.ExecCfg().Codec.TenantPrefix()
		sp := roachpb.Span{
			Key:    prefix,
			EndKey: prefix.PrefixEnd(),
		}
		entries, err := rc.GetSpanConfigEntriesFor(ctx, sp)
		if err != nil {
			// TODO(zcfgs-pod): What do we do here?
			log.Errorf(ctx, "config reconciliation error: %v", err)
		}

		for _, entry := range entries {
			store.Apply(spanconfig.Update{Entry: entry})
		}
	}

	// TODO(zcfgs-pod): Before reading off the updateCh, we want to do a full
	// reconciliation. If tables were GC-ed while we were dormant, we're not
	// going to learn about it through the updateCh. We'll want to purge
	// everything other than what we're seeing based on
	// system.{descriptors,zones}.

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case update := <-updateCh:
			// TODO(zcfgs-pod): Add the ability to batch updates here instead of
			// sending them one by one.
			upsert, delete := store.Diff(update)
			err := rc.UpdateSpanConfigEntries(ctx, upsert, delete)
			if errors.Is(err, context.Canceled) {
				continue
			}
			if err != nil {
				// TODO(zcfgs-pod): What do we do here?
				log.Errorf(ctx, "config reconciliation error: %v", err)
				continue
			}

			// Update our local cache with the results.
			for _, deleteSp := range delete {
				update := spanconfig.Update{Deleted: true}
				update.Entry.Span = deleteSp
				store.Apply(update)
			}
			for _, entry := range upsert {
				store.Apply(spanconfig.Update{Entry: entry})
			}
		}
	}
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (r *resumer) OnFailOrCancel(context.Context, interface{}) error {
	return errors.AssertionFailedf("span config reconciliation job can never fail or be canceled")
}
