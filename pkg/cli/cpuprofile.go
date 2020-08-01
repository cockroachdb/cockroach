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
	"path/filepath"
	"runtime/pprof"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func initCPUProfile(ctx context.Context, dir string, st *cluster.Settings) {
	const cpuprof = "cpuprof."
	gcProfiles(dir, cpuprof, maxSizePerProfile)

	cpuProfileInterval := envutil.EnvOrDefaultDuration("COCKROACH_CPUPROF_INTERVAL", -1)
	if cpuProfileInterval <= 0 {
		return
	}
	if min := time.Second; cpuProfileInterval < min {
		log.Infof(ctx, "fixing excessively short cpu profiling interval: %s -> %s",
			cpuProfileInterval, min)
		cpuProfileInterval = min
	}

	go func() {
		defer log.RecoverAndReportPanic(ctx, &serverCfg.Settings.SV)

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
				const format = "2006-01-02T15_04_05.999"

				var buf bytes.Buffer
				// Start the new profile. Write to a buffer so we can name the file only
				// when we know the time at end of profile.
				if err := pprof.StartCPUProfile(&buf); err != nil {
					return err
				}

				<-t.C

				pprof.StopCPUProfile()

				suffix := timeutil.Now().Format(format)
				if err := ioutil.WriteFile(filepath.Join(dir, cpuprof+suffix), buf.Bytes(), 0644); err != nil {
					return err
				}
				gcProfiles(dir, cpuprof, maxSizePerProfile)
				return nil
			}); err != nil {
				// Log errors, but continue. There's always next time.
				log.Infof(ctx, "error during CPU profile: %s", err)
			}
		}
	}()
}
