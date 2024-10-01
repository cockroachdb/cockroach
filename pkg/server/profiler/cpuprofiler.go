// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package profiler

import (
	"context"
	"os"
	"runtime/pprof"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/dumpstore"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/errors"
)

var maxCombinedCPUProfFileSize = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"server.cpu_profile.total_dump_size_limit",
	"maximum combined disk size of preserved CPU profiles",
	128<<20, // 128MiB
)

var cpuUsageCombined = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"server.cpu_profile.cpu_usage_combined_threshold",
	"a threshold beyond which if the combined cpu usage is above, "+
		"then a cpu profile can be triggered. If a value over 100 is set, "+
		"the profiler will never take a profile and conversely, if a value"+
		"of 0 is set, a profile will be taken every time the cpu profile"+
		"interval has passed or the provided usage is increasing",
	65,
	settings.NonNegativeInt,
)

var cpuProfileInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"server.cpu_profile.interval",
	// NB: this is not the entire explanation - it's when we stop taking into
	// account the high water mark seen. Without this, if CPU ever reaches 100%,
	// we'll never take another profile.
	"duration after which the high water mark resets and a new cpu profile can be taken",
	20*time.Minute,
	settings.PositiveDuration,
)

var cpuProfileDuration = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"server.cpu_profile.duration",
	"the duration for how long a cpu profile is taken",
	10*time.Second,
	settings.PositiveDuration,
)

const cpuProfFileNamePrefix = "cpuprof"

// CPUProfiler is used to take CPU profiles.
// Similar to the heapprofiler, MaybeTakeProfile()
// is intended to be called periodically and, unlike the
// heapprofiler, has a highWaterMarkBytes floor based on cpuUsageCombined
// which makes it more particular about when to take profiles.
type CPUProfiler struct {
	// profiler provides the common values and methods used across all of the
	// profilers. In particular, the CPUProfiler provides control of when to take
	// profiles via the cluster settings defined above when initializing profiler.
	profiler profiler
	st       *cluster.Settings
}

// NewCPUProfiler creates a new CPUProfiler. dir indicates the directory which
// dumps are stored.
func NewCPUProfiler(ctx context.Context, dir string, st *cluster.Settings) (*CPUProfiler, error) {
	if dir == "" {
		return nil, errors.New("directory to store dumps could not be determined")
	}
	// Make the directory if it doesn't already exist.
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	log.Infof(ctx, "writing cpu profile dumps to %s", log.SafeManaged(dir))
	dumpStore := dumpstore.NewStore(dir, maxCombinedCPUProfFileSize, st)
	cp := &CPUProfiler{
		profiler: makeProfiler(
			newProfileStore(dumpStore, cpuProfFileNamePrefix, heapFileNameSuffix, st),
			func() int64 { return cpuUsageCombined.Get(&st.SV) },
			func() time.Duration { return cpuProfileInterval.Get(&st.SV) },
		),
		st: st,
	}
	return cp, nil
}

// MaybeTakeProfile takes a cpu profile if cpu usage is high enough.
func (cp *CPUProfiler) MaybeTakeProfile(ctx context.Context, currentCpuUsage int64) {
	defer func() {
		if p := recover(); p != nil {
			logcrash.ReportPanic(ctx, &cp.st.SV, p, 1)
		}
	}()
	cp.profiler.maybeTakeProfile(ctx, currentCpuUsage, cp.takeCPUProfile)
}

func (cp *CPUProfiler) takeCPUProfile(
	ctx context.Context, path string, _ ...interface{},
) (success bool) {
	if err := debug.CPUProfileDo(cp.st, cluster.CPUProfileWithLabels, func() error {
		// Try writing a CPU profile.
		f, err := os.Create(path)
		if err != nil {
			log.Warningf(ctx, "error creating go cpu profile %s: %v", path, err)
			return err
		}
		defer f.Close()
		// Start the new profile.
		if err := pprof.StartCPUProfile(f); err != nil {
			return err
		}
		defer pprof.StopCPUProfile()
		dur := cpuProfileDuration.Get(&cp.st.SV)
		log.Infof(ctx, "taking CPU profile for %.2fs", dur.Seconds())
		select {
		case <-ctx.Done():
		case <-time.After(cpuProfileDuration.Get(&cp.st.SV)):
		}
		return nil
	}); err != nil {
		// Only log the errors, since errors can occur due to cpu profiles being taken
		// elsewhere.
		log.Infof(ctx, "error during CPU profile: %s", err)
		return false
	}
	return true
}
