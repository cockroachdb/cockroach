// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/goroutinedumper"
	"github.com/cockroachdb/cockroach/pkg/server/profiler"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/gcassist"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/goexectrace"
	"github.com/cockroachdb/errors"
)

var jemallocPurgeOverhead = settings.RegisterIntSetting(
	settings.SystemVisible,
	"server.jemalloc_purge_overhead_percent",
	"a purge of jemalloc dirty pages is issued once the overhead exceeds this percent (0 disables purging)",
	20,
	settings.NonNegativeInt,
)

var jemallocPurgePeriod = settings.RegisterDurationSettingWithExplicitUnit(
	settings.SystemVisible,
	"server.jemalloc_purge_period",
	"minimum amount of time that must pass between two jemalloc dirty page purges (0 disables purging)",
	2*time.Minute,
)

var gcAssistEnabled = settings.RegisterBoolSetting(
	settings.SystemVisible,
	"server.gc_assist.enabled",
	"set to false to dynamically disable GC assist in the Go runtime "+
		"(requires CockroachDB's forked Go runtime; no-op otherwise)",
	false, // disabled by default to reduce tail latencies
)

var gcPressureWarningThreshold = settings.RegisterFloatSetting(
	settings.SystemVisible,
	"server.gc_pressure.warning_threshold",
	"gc cpu fraction (0.0-1.0) above which a warning event is emitted "+
		"after sustained samples; set to 0 to disable GC pressure warnings",
	0.25,
	settings.FloatInRange(0, 1),
)

const (
	// gcPressureSustainedSamples is the number of consecutive
	// above-threshold samples required before warning. This filters
	// out transient GC spikes (e.g. during bulk ingestion).
	gcPressureSustainedSamples = 3
	// gcPressureCooldown is the minimum time between consecutive
	// GC pressure warnings to avoid log noise during prolonged pressure.
	gcPressureCooldown = 5 * time.Minute
)

// gcPressureChecker detects sustained GC pressure by monitoring
// the GC CPU ratio. When the ratio exceeds the configured threshold
// for gcPressureSustainedSamples consecutive samples, a warning is
// emitted. A cooldown prevents excessive warnings.
type gcPressureChecker struct {
	consecutiveAbove int
	lastWarning      time.Time
}

// check reports whether a GC pressure warning should be emitted.
// It returns true when gcCPURatio has been at or above threshold
// for gcPressureSustainedSamples consecutive calls and the cooldown
// has elapsed since the last warning.
func (c *gcPressureChecker) check(gcCPURatio, threshold float64, now time.Time) bool {
	if gcCPURatio >= threshold {
		c.consecutiveAbove++
	} else {
		c.consecutiveAbove = 0
		return false
	}

	if c.consecutiveAbove < gcPressureSustainedSamples {
		return false
	}
	if !c.lastWarning.IsZero() &&
		now.Sub(c.lastWarning) < gcPressureCooldown {
		return false
	}
	c.lastWarning = now
	// Reset so the checker must re-observe gcPressureSustainedSamples
	// new above-threshold samples before firing again. Combined with the
	// cooldown, the effective minimum re-fire interval is
	// gcPressureCooldown + gcPressureSustainedSamples*sampleInterval.
	c.consecutiveAbove = 0
	return true
}

type sampleEnvironmentCfg struct {
	st                    *cluster.Settings
	stopper               *stop.Stopper
	minSampleInterval     time.Duration
	heapProfileDirName    string
	cpuProfileDirName     string
	executionTraceDirName string
	runtime               *status.RuntimeStatSampler
	sessionRegistry       *sql.SessionRegistry
	rootMemMonitor        *mon.BytesMonitor
	cgoMemTarget          uint64
}

// startSampleEnvironment starts a periodic loop that samples the environment and,
// when appropriate, creates goroutine and/or heap dumps.
//
// The pebbleCacheSize is used to determine a target for CGO memory allocation.
func startSampleEnvironment(
	ctx context.Context,
	srvCfg *BaseConfig,
	pebbleCacheSize int64,
	stopper *stop.Stopper,
	runtimeSampler *status.RuntimeStatSampler,
	goroutineDumper *goroutinedumper.GoroutineDumper,
	sessionRegistry *sql.SessionRegistry,
	rootMemMonitor *mon.BytesMonitor,
	logMetadata func(ctx context.Context, source string),
) error {
	metricsSampleInterval := base.DefaultMetricsSampleInterval
	if p, ok := srvCfg.TestingKnobs.Server.(*TestingKnobs); ok && p.EnvironmentSampleInterval != time.Duration(0) {
		metricsSampleInterval = p.EnvironmentSampleInterval
	}
	cfg := sampleEnvironmentCfg{
		st:                    srvCfg.Settings,
		stopper:               stopper,
		minSampleInterval:     metricsSampleInterval,
		heapProfileDirName:    srvCfg.HeapProfileDirName,
		cpuProfileDirName:     srvCfg.CPUProfileDirName,
		executionTraceDirName: srvCfg.ExecutionTraceDirName,
		runtime:               runtimeSampler,
		sessionRegistry:       sessionRegistry,
		rootMemMonitor:        rootMemMonitor,
		cgoMemTarget:          max(uint64(pebbleCacheSize), 128*1024*1024),
	}
	// Initialize a heap profiler if we have an output directory
	// specified.
	var heapProfiler *profiler.HeapProfiler
	var memMonitoringProfiler *profiler.MemoryMonitoringProfiler
	var nonGoAllocProfiler *profiler.NonGoAllocProfiler
	var statsProfiler *profiler.StatsProfiler
	var queryProfiler *profiler.ActiveQueryProfiler
	var cpuProfiler *profiler.CPUProfiler
	var ashReportProfiler *profiler.ASHReportProfiler
	if cfg.heapProfileDirName != "" {
		hasValidDumpDir := true
		if err := os.MkdirAll(cfg.heapProfileDirName, 0755); err != nil {
			// This is possible when running with only in-memory stores;
			// in that case the start-up code sets the output directory
			// to the current directory (.). If wrunning the process
			// from a directory which is not writable, we won't
			// be able to create a sub-directory here.
			log.Dev.Warningf(ctx, "cannot create memory dump dir -- memory profile dumps will be disabled: %v", err)
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
				log.Dev.Warningf(ctx, "failed to start query profiler worker: %v", err)
			}
			cpuProfiler, err = profiler.NewCPUProfiler(ctx, cfg.cpuProfileDirName, cfg.st)
			if err != nil {
				log.Dev.Warningf(ctx, "failed to start cpu profiler worker: %v", err)
			}
			ashReportProfiler, err = profiler.NewASHReportProfiler(ctx, cfg.heapProfileDirName, cfg.st)
			if err != nil {
				log.Dev.Warningf(ctx, "failed to start ASH report profiler: %v", err)
			}
		}
	}

	simpleFlightRecorder, err := goexectrace.NewFlightRecorder(cfg.st, 10*time.Second, cfg.executionTraceDirName, logMetadata)
	if err != nil {
		log.Dev.Warningf(ctx, "failed to initialize flight recorder: %v", err)
	} else {
		err = simpleFlightRecorder.Start(ctx, cfg.stopper)
		if err != nil {
			log.Dev.Warningf(ctx, "failed to start flight recorder: %v", err)
		}
	}

	// Apply the initial value of the GC assist setting and install a
	// callback for dynamic updates.
	gcassist.SetEnabled(gcAssistEnabled.Get(&cfg.st.SV))
	gcAssistEnabled.SetOnChange(&cfg.st.SV, func(ctx context.Context) {
		gcassist.SetEnabled(gcAssistEnabled.Get(&cfg.st.SV))
	})

	return cfg.stopper.RunAsyncTaskEx(ctx,
		stop.TaskOpts{TaskName: "mem-logger", SpanOpt: stop.SterileRootSpan},
		func(ctx context.Context) {
			var timer timeutil.Timer
			defer timer.Stop()
			timer.Reset(cfg.minSampleInterval)

			var gcChecker gcPressureChecker

			for {
				select {
				case <-cfg.stopper.ShouldQuiesce():
					return
				case <-timer.C:
					timer.Reset(cfg.minSampleInterval)

					cgoStats := status.GetCGoMemStats(ctx)
					cfg.runtime.SampleEnvironment(ctx, cgoStats)

					threshold :=
						gcPressureWarningThreshold.Get(&cfg.st.SV)
					if threshold > 0 {
						gcCPU := cfg.runtime.GcCPUPercent.Value()
						if gcChecker.check(gcCPU, threshold, timeutil.Now()) {
							goAlloc := uint64(cfg.runtime.GoAllocBytes.Value())
							goLimit := uint64(cfg.runtime.GoLimitBytes.Value())
							ev := &eventpb.GCPressureDetected{
								NodeID:        int32(srvCfg.IDContainer.Get()),
								GCCPUFraction: gcCPU,
								GoAllocBytes:  goAlloc,
								GoLimitBytes:  goLimit,
							}
							ev.CommonDetails().Timestamp =
								timeutil.Now().UnixNano()
							log.StructuredEvent(
								ctx, severity.WARNING, ev,
							)
							log.Ops.Warningf(ctx,
								"GC pressure detected: GC using %.1f%% "+
									"of available CPU "+
									"(heap: %d MiB, limit: %d MiB)",
								gcCPU*100,
								goAlloc/(1024*1024),
								goLimit/(1024*1024),
							)
						}
					}

					// Maybe purge jemalloc dirty pages.
					if overhead, period := jemallocPurgeOverhead.Get(&cfg.st.SV), jemallocPurgePeriod.Get(&cfg.st.SV); overhead > 0 && period > 0 {
						status.CGoMemMaybePurge(ctx, cgoStats.CGoAllocatedBytes, cgoStats.CGoTotalBytes, cfg.cgoMemTarget, int(overhead), period)
					}

					if goroutineDumper != nil {
						if goroutineDumper.MaybeDump(ctx, cfg.st, cfg.runtime.Goroutines.Value()) {
							if ashReportProfiler != nil {
								ashReportProfiler.WriteReport(ctx, profiler.TriggerGoroutineDump)
							}
						}
					}
					if heapProfiler != nil {
						heapProfiler.MaybeTakeProfile(ctx, cfg.runtime.GoAllocBytes.Value())
						memMonitoringProfiler.MaybeTakeMemoryMonitoringDump(ctx, cfg.runtime.GoAllocBytes.Value(), cfg.rootMemMonitor, cfg.st)
						nonGoAllocProfiler.MaybeTakeProfile(ctx, cfg.runtime.CgoTotalBytes.Value())
						statsProfiler.MaybeTakeProfile(ctx, cfg.runtime.RSSBytes.Value(), cgoStats)
					}
					if queryProfiler != nil {
						queryProfiler.MaybeDumpQueries(ctx, cfg.sessionRegistry, cfg.st)
					}
					if cpuProfiler != nil {
						if cpuProfiler.MaybeTakeProfile(ctx, int64(cfg.runtime.CPUCombinedPercentNorm.Value()*100)) {
							if ashReportProfiler != nil {
								ashReportProfiler.WriteReport(ctx, profiler.TriggerCPUProfile)
							}
						}
					}
				}
			}
		})
}

func maybeNewGoroutineDumper(
	ctx context.Context, goroutineDumpDirName string, st *cluster.Settings,
) (*goroutinedumper.GoroutineDumper, error) {
	if goroutineDumpDirName == "" {
		return nil, nil
	}
	if err := os.MkdirAll(goroutineDumpDirName, 0755); err != nil {
		// This is possible when running with only in-memory stores; in that case
		// the start-up code sets the output directory to the current directory (.).
		// If running the process from a directory which is not writable, we won't
		// be able to create a sub-directory here.
		log.Dev.Warningf(ctx, "cannot create goroutine dump dir -- goroutine dumps will be disabled: %v", err)
		return nil, nil
	}
	return goroutinedumper.NewGoroutineDumper(ctx, goroutineDumpDirName, st)
}
