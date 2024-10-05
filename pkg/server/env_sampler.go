// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"os"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/goroutinedumper"
	"github.com/cockroachdb/cockroach/pkg/server/profiler"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type sampleEnvironmentCfg struct {
	st                   *cluster.Settings
	stopper              *stop.Stopper
	minSampleInterval    time.Duration
	goroutineDumpDirName string
	heapProfileDirName   string
	cpuProfileDirName    string
	runtime              *status.RuntimeStatSampler
	sessionRegistry      *sql.SessionRegistry
	rootMemMonitor       *mon.BytesMonitor
}

// startSampleEnvironment starts a periodic loop that samples the environment and,
// when appropriate, creates goroutine and/or heap dumps.
func startSampleEnvironment(
	ctx context.Context,
	settings *cluster.Settings,
	stopper *stop.Stopper,
	goroutineDumpDirName string,
	heapProfileDirName string,
	cpuProfileDirName string,
	runtimeSampler *status.RuntimeStatSampler,
	sessionRegistry *sql.SessionRegistry,
	rootMemMonitor *mon.BytesMonitor,
	testingKnobs base.TestingKnobs,
) error {
	metricsSampleInterval := base.DefaultMetricsSampleInterval
	if p, ok := testingKnobs.Server.(*TestingKnobs); ok && p.EnvironmentSampleInterval != time.Duration(0) {
		metricsSampleInterval = p.EnvironmentSampleInterval
	}
	cfg := sampleEnvironmentCfg{
		st:                   settings,
		stopper:              stopper,
		minSampleInterval:    metricsSampleInterval,
		goroutineDumpDirName: goroutineDumpDirName,
		heapProfileDirName:   heapProfileDirName,
		cpuProfileDirName:    cpuProfileDirName,
		runtime:              runtimeSampler,
		sessionRegistry:      sessionRegistry,
		rootMemMonitor:       rootMemMonitor,
	}
	// Immediately record summaries once on server startup.

	// Initialize a goroutine dumper if we have an output directory
	// specified.
	var goroutineDumper *goroutinedumper.GoroutineDumper
	if cfg.goroutineDumpDirName != "" {
		hasValidDumpDir := true
		if err := os.MkdirAll(cfg.goroutineDumpDirName, 0755); err != nil {
			// This is possible when running with only in-memory stores;
			// in that case the start-up code sets the output directory
			// to the current directory (.). If running the process
			// from a directory which is not writable, we won't
			// be able to create a sub-directory here.
			log.Warningf(ctx, "cannot create goroutine dump dir -- goroutine dumps will be disabled: %v", err)
			hasValidDumpDir = false
		}
		if hasValidDumpDir {
			var err error
			goroutineDumper, err = goroutinedumper.NewGoroutineDumper(ctx, cfg.goroutineDumpDirName, cfg.st)
			if err != nil {
				return errors.Wrap(err, "starting goroutine dumper worker")
			}
		}
	}

	// Initialize a heap profiler if we have an output directory
	// specified.
	var heapProfiler *profiler.HeapProfiler
	var memMonitoringProfiler *profiler.MemoryMonitoringProfiler
	var nonGoAllocProfiler *profiler.NonGoAllocProfiler
	var statsProfiler *profiler.StatsProfiler
	var queryProfiler *profiler.ActiveQueryProfiler
	var cpuProfiler *profiler.CPUProfiler
	if cfg.heapProfileDirName != "" {
		hasValidDumpDir := true
		if err := os.MkdirAll(cfg.heapProfileDirName, 0755); err != nil {
			// This is possible when running with only in-memory stores;
			// in that case the start-up code sets the output directory
			// to the current directory (.). If wrunning the process
			// from a directory which is not writable, we won't
			// be able to create a sub-directory here.
			log.Warningf(ctx, "cannot create memory dump dir -- memory profile dumps will be disabled: %v", err)
			hasValidDumpDir = false
		}

		if hasValidDumpDir {
			var err error
			heapProfiler, err = profiler.NewHeapProfiler(ctx, cfg.heapProfileDirName, cfg.st)
			if err != nil {
				return errors.Wrap(err, "starting heap profiler worker")
			}
			memMonitoringProfiler, err = profiler.NewMemoryMonitoringProfiler(ctx, cfg.heapProfileDirName, cfg.st)
			if err != nil {
				return errors.Wrap(err, "starting memory monitoring profiler worker")
			}
			nonGoAllocProfiler, err = profiler.NewNonGoAllocProfiler(ctx, cfg.heapProfileDirName, cfg.st)
			if err != nil {
				return errors.Wrap(err, "starting non-go alloc profiler worker")
			}
			statsProfiler, err = profiler.NewStatsProfiler(ctx, cfg.heapProfileDirName, cfg.st)
			if err != nil {
				return errors.Wrap(err, "starting memory stats collector worker")
			}
			queryProfiler, err = profiler.NewActiveQueryProfiler(ctx, cfg.heapProfileDirName, cfg.st)
			if err != nil {
				log.Warningf(ctx, "failed to start query profiler worker: %v", err)
			}
			cpuProfiler, err = profiler.NewCPUProfiler(ctx, cfg.cpuProfileDirName, cfg.st)
			if err != nil {
				log.Warningf(ctx, "failed to start cpu profiler worker: %v", err)
			}
		}
	}

	return cfg.stopper.RunAsyncTaskEx(ctx,
		stop.TaskOpts{TaskName: "mem-logger", SpanOpt: stop.SterileRootSpan},
		func(ctx context.Context) {
			var goMemStats atomic.Value // *status.GoMemStats
			goMemStats.Store(&status.GoMemStats{})
			var collectingMemStats int32 // atomic, 1 when stats call is ongoing

			timer := timeutil.NewTimer()
			defer timer.Stop()
			timer.Reset(cfg.minSampleInterval)

			for {
				select {
				case <-cfg.stopper.ShouldQuiesce():
					return
				case <-timer.C:
					timer.Read = true
					timer.Reset(cfg.minSampleInterval)

					// We read the heap stats on another goroutine and give up after 1s.
					// This is necessary because as of Go 1.12, runtime.ReadMemStats()
					// "stops the world" and that requires first waiting for any current GC
					// run to finish. With a large heap and under extreme conditions, a
					// single GC run may take longer than the default sampling period of
					// 10s. Under normal operations and with more recent versions of Go,
					// this hasn't been observed to be a problem.
					statsCollected := make(chan struct{})
					if atomic.CompareAndSwapInt32(&collectingMemStats, 0, 1) {
						if err := cfg.stopper.RunAsyncTaskEx(ctx,
							stop.TaskOpts{TaskName: "get-mem-stats"},
							func(ctx context.Context) {
								var ms status.GoMemStats
								runtime.ReadMemStats(&ms.MemStats)
								ms.Collected = timeutil.Now()
								log.VEventf(ctx, 2, "memstats: %+v", ms)

								goMemStats.Store(&ms)
								atomic.StoreInt32(&collectingMemStats, 0)
								close(statsCollected)
							}); err != nil {
							close(statsCollected)
						}
					}

					select {
					case <-statsCollected:
						// Good; we managed to read the Go memory stats quickly enough.
					case <-time.After(time.Second):
					}

					curStats := goMemStats.Load().(*status.GoMemStats)
					cgoStats := status.GetCGoMemStats(ctx)
					cfg.runtime.SampleEnvironment(ctx, curStats, cgoStats)

					if goroutineDumper != nil {
						goroutineDumper.MaybeDump(ctx, cfg.st, cfg.runtime.Goroutines.Value())
					}
					if heapProfiler != nil {
						heapProfiler.MaybeTakeProfile(ctx, cfg.runtime.GoAllocBytes.Value())
						memMonitoringProfiler.MaybeTakeMemoryMonitoringDump(ctx, cfg.runtime.GoAllocBytes.Value(), cfg.rootMemMonitor, cfg.st)
						nonGoAllocProfiler.MaybeTakeProfile(ctx, cfg.runtime.CgoTotalBytes.Value())
						statsProfiler.MaybeTakeProfile(ctx, cfg.runtime.RSSBytes.Value(), curStats, cgoStats)
					}
					if queryProfiler != nil {
						queryProfiler.MaybeDumpQueries(ctx, cfg.sessionRegistry, cfg.st)
					}
					if cpuProfiler != nil {
						cpuProfiler.MaybeTakeProfile(ctx, int64(cfg.runtime.CPUCombinedPercentNorm.Value()*100))
					}
				}
			}
		})
}
