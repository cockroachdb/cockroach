// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// startAssertEngineHealth starts a goroutine that periodically verifies that
// syncing the engines is possible within maxSyncDuration. If not,
// the process is terminated (with an attempt at a descriptive message).
func (n *Node) startAssertEngineHealth(
	ctx context.Context, engines []storage.Engine, settings *cluster.Settings,
) {
	maxSyncDuration := storage.MaxSyncDuration.Get(&settings.SV)
	fatalOnExceeded := storage.MaxSyncDurationFatalOnExceeded.Get(&settings.SV)
	_ = n.stopper.RunAsyncTask(ctx, "engine-health", func(ctx context.Context) {
		t := timeutil.NewTimer()
		t.Reset(0)

		for {
			select {
			case <-t.C:
				t.Read = true
				t.Reset(10 * time.Second)
				n.assertEngineHealth(ctx, engines, maxSyncDuration, fatalOnExceeded)
			case <-n.stopper.ShouldQuiesce():
				return
			}
		}
	})
}

func guaranteedExitFatal(ctx context.Context, msg string, args ...interface{}) {
	// NB: log.Shout sets up a timer that guarantees process termination.
	log.Health.Shoutf(ctx, severity.FATAL, msg, args...)
}

func (n *Node) assertEngineHealth(
	ctx context.Context, engines []storage.Engine, maxDuration time.Duration, fatalOnExceeded bool,
) {
	for _, eng := range engines {
		func() {
			t := time.AfterFunc(maxDuration, func() {
				n.metrics.DiskStalls.Inc(1)
				m := eng.GetMetrics()
				logger := log.Warningf
				if fatalOnExceeded {
					logger = guaranteedExitFatal
				}
				// NB: the disk-stall-detected roachtest matches on this message.
				logger(ctx, "disk stall detected: unable to write to %s within %s\n%s",
					eng, storage.MaxSyncDuration, m)
			})
			defer t.Stop()
			if err := storage.WriteSyncNoop(ctx, eng); err != nil {
				log.Fatalf(ctx, "%v", err)
			}
		}()
	}
}
