// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package heapprofiler

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
	"github.com/cockroachdb/errors"
)

var MaxCombinedCPUProfFileSize = settings.RegisterByteSizeSetting(
	settings.TenantWritable,
	"server.cpu_profile.total_dump_size_limit",
	"maximum combined disk size of preserved CPU profiles",
	128<<20, // 128MiB
)

var cpuUsageCombined = settings.RegisterIntSetting(
	settings.TenantWritable,
	"server.cpu_profile.cpu_usage_combined_threshold",
	"a threshold beyond which if the combined cpu usage is above, "+
		"then a cpu profile can be triggered",
	80,
)

var cpuProfileInterval = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"server.cpu_profile.interval",
	"rate at which cpu profiles can be taken if above cpu usage threshold",
	1*time.Minute, settings.PositiveDuration,
)

const CpuProfTimeFormat = "2006-01-02T15_04_05.000"
const CpuProfFileNamePrefix = "cpuprof."

type CpuProfiler struct {
	profiler profiler
	st       *cluster.Settings
}

// NewCPUProfiler creates a new CPUProfiler. dir indicates the directory which
// dumps are stored.
func NewCPUProfiler(ctx context.Context, dir string, st *cluster.Settings) (*CpuProfiler, error) {
	if dir == "" {
		return nil, errors.New("directory to store dumps could not be determined")
	}

	log.Infof(ctx, "writing cpu profile dumps to %s", log.SafeManaged(dir))
	dumpStore := dumpstore.NewStore(dir, MaxCombinedCPUProfFileSize, st)
	cp := &CpuProfiler{
		profiler: profiler{
			store:              newProfileStore(dumpStore, CpuProfFileNamePrefix, HeapFileNameSuffix, st),
			highwaterMarkBytes: cpuUsageCombined.Get(&st.SV),
			customInterval:     cpuProfileInterval.Get(&st.SV),
		},
		st: st,
	}
	return cp, nil
}

// MaybeTakeProfile takes a cpu profile if cpu usage is high enough.
func (cp *CpuProfiler) MaybeTakeProfile(ctx context.Context, currentCpuUsage int64) {
	cp.profiler.maybeTakeProfile(ctx, currentCpuUsage, cp.takeCpuProfile)
}

func (cp *CpuProfiler) takeCpuProfile(ctx context.Context, path string) (success bool) {
	if err := debug.CPUProfileDo(cp.st, cluster.CPUProfileDefault, func() error {
		// Try writing a go heap profile.
		f, err := os.Create(path)
		if err != nil {
			log.Warningf(ctx, "error creating go heap profile %s: %v", path, err)
			return err
		}
		defer f.Close()
		// Start the new profile.
		if err := pprof.StartCPUProfile(f); err != nil {
			return err
		}
		defer pprof.StopCPUProfile()
		return nil
	}); err != nil {
		// Only log the errors, since errors can occur due to cpu profiles being taken
		// elsewhere.
		log.Infof(ctx, "error during CPU profile: %s", err)
		return false
	}
	return true
}
