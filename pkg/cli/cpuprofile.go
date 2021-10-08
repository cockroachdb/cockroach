// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/dumpstore"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var maxCombinedCPUProfFileSize = settings.RegisterByteSizeSetting(
	"server.cpu_profile.total_dump_size_limit",
	"maximum combined disk size of preserved CPU profiles",
	128<<20, // 128MiB
)

const cpuProfTimeFormat = "2006-01-02T15_04_05.000"
const cpuProfFileNamePrefix = "cpuprof."

type cpuProfiler struct{}

// PreFilter is part of the dumpstore.Dumper interface.
func (s cpuProfiler) PreFilter(
	ctx context.Context, files []os.FileInfo, cleanupFn func(fileName string) error,
) (preserved map[int]bool, _ error) {
	preserved = make(map[int]bool)
	// Always keep at least the last profile.
	for i := len(files) - 1; i >= 0; i-- {
		if s.CheckOwnsFile(ctx, files[i]) {
			preserved[i] = true
			break
		}
	}
	return
}

// CheckOwnsFile is part of the dumpstore.Dumper interface.
func (s cpuProfiler) CheckOwnsFile(_ context.Context, fi os.FileInfo) bool {
	return strings.HasPrefix(fi.Name(), cpuProfFileNamePrefix)
}

func initCPUProfile(ctx context.Context, dir string, st *cluster.Settings) {
	cpuProfileInterval := envutil.EnvOrDefaultDuration("COCKROACH_CPUPROF_INTERVAL", -1)
	if cpuProfileInterval <= 0 {
		return
	}

	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		// This is possible when running with only in-memory stores;
		// in that case the start-up code sets the output directory
		// to the current directory (.). If running the process
		// from a directory which is not writable, we won't
		// be able to create a sub-directory here.
		log.Warningf(ctx, "cannot create CPU profile dump dir -- CPU profiles will be disabled: %v", err)
		return
	}

	if min := time.Second; cpuProfileInterval < min {
		log.Infof(ctx, "fixing excessively short cpu profiling interval: %s -> %s",
			cpuProfileInterval, min)
		cpuProfileInterval = min
	}

	profilestore := dumpstore.NewStore(dir, maxCombinedCPUProfFileSize, st)
	profiler := dumpstore.Dumper(cpuProfiler{})

	// TODO(knz,tbg): The caller of initCPUProfile() also defines a stopper;
	// arguably this code would be better served by stopper.RunAsyncTask().
	go func() {
		defer logcrash.RecoverAndReportPanic(ctx, &serverCfg.Settings.SV)

		ctx := context.Background()

		t := time.NewTicker(cpuProfileInterval)
		defer t.Stop()

		var currentProfile *os.File
		defer func() {
			if currentProfile != nil {
				pprof.StopCPUProfile()
				currentProfile.Close()
			}
		}()

		for {
			// Grab a profile.
			if err := debug.CPUProfileDo(st, cluster.CPUProfileDefault, func() error {

				var buf bytes.Buffer
				// Start the new profile. Write to a buffer so we can name the file only
				// when we know the time at end of profile.
				if err := pprof.StartCPUProfile(&buf); err != nil {
					return err
				}

				<-t.C

				pprof.StopCPUProfile()

				now := timeutil.Now()
				name := cpuProfFileNamePrefix + now.Format(cpuProfTimeFormat)
				path := profilestore.GetFullPath(name)
				if err := ioutil.WriteFile(path, buf.Bytes(), 0644); err != nil {
					return err
				}
				profilestore.GC(ctx, now, profiler)
				return nil
			}); err != nil {
				// Log errors, but continue. There's always next time.
				log.Infof(ctx, "error during CPU profile: %s", err)
			}
		}
	}()
}
