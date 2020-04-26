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

	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// maxSyncDuration is the threshold above which an observed engine sync duration
// triggers either a warning or a fatal error.
var maxSyncDuration = envutil.EnvOrDefaultDuration("COCKROACH_ENGINE_MAX_SYNC_DURATION", 10*time.Second)

// maxSyncDurationFatalOnExceeded defaults to false due to issues such as
// https://github.com/cockroachdb/cockroach/issues/34860#issuecomment-469262019.
// Similar problems have been known to occur during index backfill and, possibly,
// IMPORT/RESTORE.
var maxSyncDurationFatalOnExceeded = envutil.EnvOrDefaultBool("COCKROACH_ENGINE_MAX_SYNC_DURATION_FATAL", false)

// startAssertEngineHealth starts a goroutine that periodically verifies that
// syncing the engines is possible within maxSyncDuration. If not,
// the process is terminated (with an attempt at a descriptive message).
func (n *Node) startAssertEngineHealth(ctx context.Context, engines []storage.Engine) {
	n.stopper.RunWorker(ctx, func(ctx context.Context) {
		t := timeutil.NewTimer()
		t.Reset(0)

		for {
			select {
			case <-t.C:
				t.Read = true
				t.Reset(10 * time.Second)
				n.assertEngineHealth(ctx, engines, maxSyncDuration)
			case <-n.stopper.ShouldQuiesce():
				return
			}
		}
	})
}

func guaranteedExitFatal(ctx context.Context, msg string, args ...interface{}) {
	// NB: log.Shout sets up a timer that guarantees process termination.
	log.Shoutf(ctx, log.Severity_FATAL, msg, args...)
}

func (n *Node) assertEngineHealth(
	ctx context.Context, engines []storage.Engine, maxDuration time.Duration,
) {
	for _, eng := range engines {
		func() {
			t := time.AfterFunc(maxDuration, func() {
				n.metrics.DiskStalls.Inc(1)
				stats := "\n" + eng.GetCompactionStats()
				logger := log.Warningf
				if maxSyncDurationFatalOnExceeded {
					logger = guaranteedExitFatal
				}
				// NB: the disk-stall-detected roachtest matches on this message.
				logger(ctx, "disk stall detected: unable to write to %s within %s %s",
					eng, maxSyncDuration, stats,
				)
			})
			defer t.Stop()
			if err := storage.WriteSyncNoop(ctx, eng); err != nil {
				log.Fatalf(ctx, "%v", err)
			}
		}()
	}
}
