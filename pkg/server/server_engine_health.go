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

var engineHealthCheckInterval = envutil.EnvOrDefaultDuration("COCKROACH_ENGINE_HEALTH_CHECK_INTERVAL", 5*time.Second)

func startAssertEngineHealth(ctx context.Context, stopper *stop.Stopper, engines []engine.Engine) {
	stopper.RunWorker(ctx, func(ctx context.Context) {
		t := timeutil.NewTimer()
		t.Reset(0)

		for {
			select {
			case <-t.C:
				t.Read = true
				t.Reset(engineHealthCheckInterval)
				assertLoggingHealth(ctx, engineHealthCheckInterval)
				assertEngineHealth(ctx, engines, engineHealthCheckInterval)
			case <-stopper.ShouldQuiesce():
				return
			}
		}
	})
}

// Don't change this string without also changing the disk-stall-detection
// roachtest which greps for it.
const diskStallDetected = "disk stall detected"

func assertEngineHealth(ctx context.Context, engines []engine.Engine, maxDuration time.Duration) {
	for _, eng := range engines {
		func() {
			t := time.AfterFunc(maxDuration, func() {
				log.Shout(ctx, log.Severity_FATAL, fmt.Sprintf(
					"%s: unable to write to %s within %s",
					diskStallDetected, eng, engineHealthCheckInterval,
				))
			})
			defer t.Stop()
			if err := engine.WriteSyncNoop(ctx, eng); err != nil {
				log.Fatal(ctx, err)
			}
		}()
	}
}

// assertLoggingHealth flushes all the log streams and fatally exits the process
// if this does not succeed within a reasonable amount of time.
func assertLoggingHealth(ctx context.Context, maxDuration time.Duration) {
	timer := time.AfterFunc(maxDuration, func() {
		log.Shout(ctx, log.Severity_FATAL, fmt.Sprintf(
			"%s: unable to sync log files within %s",
			diskStallDetected, engineHealthCheckInterval,
		))
	})
	defer timer.Stop()

	log.Flush()
}
