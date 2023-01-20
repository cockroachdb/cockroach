// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cpuprofiler

import (
	"bytes"
	"context"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/dumpstore"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var MaxCombinedCPUProfFileSize = settings.RegisterByteSizeSetting(
	settings.TenantWritable,
	"server.cpu_profile.total_dump_size_limit",
	"maximum combined disk size of preserved CPU profiles",
	128<<20, // 128MiB
)

var cpuUsageCombined = settings.RegisterFloatSetting(
	settings.TenantWritable,
	"server.cpu_profile.cpu_usage_combined_threshold",
	"a threshold beyond which if the combined cpu usage is above, "+
		"then a cpu profile can be triggered",
	0.8,
)

var cpuProfileInterval = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"server.cpu_profile.interval",
	"rate at which cpu profiles can be taken if above qps threshold",
	1*time.Minute, settings.PositiveDuration,
)

const CpuProfTimeFormat = "2006-01-02T15_04_05.000"
const CpuProfFileNamePrefix = "cpuprof."

type testingKnobs struct {
	dontWrite bool
}

type CpuProfiler struct {
	lastProfileTime time.Time
	currentTime     func() time.Time
	store           *dumpstore.DumpStore
	st              *cluster.Settings
	knobs           testingKnobs
}

// NewCpuProfiler creates a new CpuProfiler. dir indicates the directory which
// dumps are stored.
func NewCpuProfiler(ctx context.Context, dir string, st *cluster.Settings) (*CpuProfiler, error) {
	if dir == "" {
		return nil, errors.New("directory to store dumps could not be determined")
	}

	log.Infof(ctx, "writing cpu profile dumps to %s", log.SafeManaged(dir))

	cp := &CpuProfiler{
		currentTime: timeutil.Now,
		store:       dumpstore.NewStore(dir, MaxCombinedCPUProfFileSize, st),
		st:          st,
		knobs:       testingKnobs{},
	}
	return cp, nil
}

// PreFilter is part of the dumpstore.Dumper interface.
func (cp *CpuProfiler) PreFilter(
	ctx context.Context, files []os.FileInfo, _ func(fileName string) error,
) (preserved map[int]bool, _ error) {
	preserved = make(map[int]bool)
	// Always keep at least the last profile.
	for i := len(files) - 1; i >= 0; i-- {
		if cp.CheckOwnsFile(ctx, files[i]) {
			preserved[i] = true
			break
		}
	}
	return
}

// CheckOwnsFile is part of the dumpstore.Dumper interface.
func (cp *CpuProfiler) CheckOwnsFile(_ context.Context, fi os.FileInfo) bool {
	return strings.HasPrefix(fi.Name(), CpuProfFileNamePrefix)
}

// MaybeTakeProfile takes a cpu profile and writes to a file provided the
// threshold is met and the time between the last profile is greater than the
// cpuProfileInterval setting.
func (cp *CpuProfiler) MaybeTakeProfile(ctx context.Context, cpuUsage float64) {
	// Exit early if threshold is not met.
	if cpuUsageCombined.Get(&cp.st.SV) > cpuUsage {
		return
	}

	// Exit early if a profile has already been taken within the interval period.
	lastProfileTimeDiff := timeutil.Since(cp.lastProfileTime)
	if cpuProfileInterval.Get(&cp.st.SV).Seconds() > lastProfileTimeDiff.Seconds() {
		return
	}

	if err := debug.CPUProfileDo(cp.st, cluster.CPUProfileDefault, func() error {
		var buf bytes.Buffer
		var now time.Time
		// Only create the profile if not testing.
		if !cp.knobs.dontWrite {
			// Set profiling rate to 10hz samples/s to reduce sample rate and limit cpu overhead.
			runtime.SetCPUProfileRate(10)
			// Start the new profile.
			if err := pprof.StartCPUProfile(&buf); err != nil {
				return err
			}
			defer pprof.StopCPUProfile()
			now = cp.currentTime()
			name := CpuProfFileNamePrefix + now.Format(CpuProfTimeFormat)
			path := cp.store.GetFullPath(name)
			f, err := os.Create(path)
			if err != nil {
				log.Errorf(ctx, "error creating cpu profile %s: %v", path, err)
				return err
			}
			defer f.Close()
			if _, err := f.Write(buf.Bytes()); err != nil {
				return err
			}
			cp.store.GC(ctx, now, cp)
		}
		cp.lastProfileTime = now
		return nil
	}); err != nil {
		// Only log the errors, since errors can occur due to cpu profiles being taken
		// elsewhere.
		log.Infof(ctx, "error during CPU profile: %s", err)
	}
}
