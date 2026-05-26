// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// TxnPushNotifier enqueues a PushTxnEvent for all processors yielded by
// visitProcessors every interval.
type TxnPushNotifier struct {
	interval        time.Duration
	settings        *cluster.Settings
	scheduler       *Scheduler
	visitProcessors func(func(int64))
}

func NewTxnPushNotifier(
	interval time.Duration,
	settings *cluster.Settings,
	scheduler *Scheduler,
	visitProcessors func(func(int64)),
) *TxnPushNotifier {
	return &TxnPushNotifier{
		interval:        interval,
		settings:        settings,
		scheduler:       scheduler,
		visitProcessors: visitProcessors,
	}
}

func (t *TxnPushNotifier) Start(ctx context.Context, stopper *stop.Stopper) error {
	taskOpts := stop.TaskOpts{
		TaskName: "rangefeed-txn-push-notifier",
		SpanOpt:  stop.SterileRootSpan,
	}
	ctx, hdl, err := stopper.GetHandle(ctx, taskOpts)
	if err != nil {
		return err
	}
	go func(ctx context.Context, hdl *stop.Handle) {
		defer hdl.Activate(ctx).Release(ctx)

		ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		ticker := time.NewTicker(t.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if !PushTxnsEnabled.Get(&t.settings.SV) {
					continue
				}

				batch := t.scheduler.NewEnqueueBatch()
				t.visitProcessors(batch.Add)
				t.scheduler.EnqueueBatch(batch, PushTxnQueued)
				batch.Close()
			case <-ctx.Done():
				return
			}
		}
	}(ctx, hdl)
	return nil
}
