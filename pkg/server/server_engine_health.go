// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package server

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
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
func startAssertEngineHealth(ctx context.Context, stopper *stop.Stopper, engines []engine.Engine) {
	stopper.RunWorker(ctx, func(ctx context.Context) {
		t := timeutil.NewTimer()
		t.Reset(0)

		for {
			select {
			case <-t.C:
				t.Read = true
				t.Reset(10 * time.Second)
				assertEngineHealth(ctx, engines, maxSyncDuration)
			case <-stopper.ShouldQuiesce():
				return
			}
		}
	})
}

func guaranteedExitFatal(ctx context.Context, msg string, args ...interface{}) {
	// NB: log.Shout sets up a timer that guarantees process termination.
	log.Shout(ctx, log.Severity_FATAL, fmt.Sprintf(msg, args...))
}

func assertEngineHealth(ctx context.Context, engines []engine.Engine, maxDuration time.Duration) {
	for _, eng := range engines {
		func() {
			t := time.AfterFunc(maxDuration, func() {
				var stats string
				if rocks, ok := eng.(*engine.RocksDB); ok {
					stats = "\n" + rocks.GetCompactionStats()
				}
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
			if err := engine.WriteSyncNoop(ctx, eng); err != nil {
				log.Fatal(ctx, err)
			}
		}()
	}
}
