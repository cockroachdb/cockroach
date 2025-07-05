// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package load

import (
	"context"
	"time"

	"github.com/VividCortex/ewma"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

const runtimeCPUPeriod = 1 * time.Second
const runtimeCPUTrackedSeconds = 5 * 60

type RuntimeLoadStats struct {
	// CPURateUsage is the rate of CPU usage in nanoseconds per second over
	// approx. runtimeCPUTrackedSeconds.
	CPURateUsage int64
	// CPURateCapacity is the capacity of the CPU in nanoseconds per second.
	CPURateCapacity int64
}

type RuntimeLoadMonitor struct {
	mu struct {
		syncutil.Mutex
		first, second       int64
		lastCPURateCapacity int64
		// TODO(kvoli): We will want an interface where we call back with the delta
		// (usr+sys)@t - (usr+sys)@t-1 and the capacity, And then allow the
		// callback to decide what to do with that information. For now, we just
		// track the exponentially weighted moving average with a 5 minute avg age
		// (recorded every second) here and return it in Stats().
		movingAverage ewma.MovingAverage
	}
}

func NewRuntimeLoadMonitor() *RuntimeLoadMonitor {
	rlm := &RuntimeLoadMonitor{}
	rlm.mu.movingAverage = ewma.NewMovingAverage(runtimeCPUTrackedSeconds)
	return rlm
}

func (rlm *RuntimeLoadMonitor) Stats() RuntimeLoadStats {
	rlm.mu.Lock()
	defer rlm.mu.Unlock()

	return RuntimeLoadStats{
		CPURateUsage:    int64(rlm.mu.movingAverage.Value()),
		CPURateCapacity: rlm.mu.lastCPURateCapacity,
	}
}

func (rlm *RuntimeLoadMonitor) Start(ctx context.Context, stopper *stop.Stopper) error {
	return stopper.RunAsyncTask(ctx, "kvserver-runtime-monitor", func(ctx context.Context) {
		ticker := time.NewTicker(runtimeCPUPeriod)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-stopper.ShouldQuiesce():
				return
			case <-ticker.C:
				func() {
					rlm.mu.Lock()
					defer rlm.mu.Unlock()
					// TODO: Is the call to gather CPU capacity expensive? If so, we
					// should only check the capacity every n ticks, say 10.
					rlm.mu.lastCPURateCapacity = int64(status.GetCPUCapacity()) * time.Second.Nanoseconds()
					utimeMillis, stimeMillis, err := status.GetProcCPUTime(ctx)
					if err != nil {
						panic(err)
					}
					// User and System CPU time are in milliseconds, convert to
					// nanoseconds.
					utime := utimeMillis * 1e6
					stime := stimeMillis * 1e6
					rlm.mu.second = rlm.mu.first
					rlm.mu.first = utime + stime
					rlm.mu.movingAverage.Add(float64(rlm.mu.first-rlm.mu.second) / runtimeCPUPeriod.Seconds())
				}()
			}
		}
	})
}
