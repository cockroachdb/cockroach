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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server/dumpstore"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/assert"
)

var cpuProfilerDirName = "cockroach-data/logs/pprof_dump"

func TestCPUProfiler(t *testing.T) {
	ctx := context.Background()
	dumpStore := dumpstore.NewStore(cpuProfilerDirName, nil, nil)
	s := &cluster.Settings{}
	sv := &s.SV
	s.Version = clusterversion.MakeVersionHandle(sv)
	sv.Init(ctx, s.Version)
	pastTime := time.Date(2023, 1, 1, 1, 1, 1, 1, time.UTC)
	cases := []struct {
		name             string
		cpuUsage         int64
		lastProfileTime  time.Time
		expectNewProfile bool
	}{
		{
			name:             "take profile",
			cpuUsage:         90,
			lastProfileTime:  pastTime,
			expectNewProfile: true,
		},
		{
			name:             "no profile due to low cpu usage",
			cpuUsage:         50,
			lastProfileTime:  pastTime,
			expectNewProfile: false,
		},
		{
			name:             "no profile due to last profile being within the time interval",
			cpuUsage:         90,
			lastProfileTime:  timeutil.Now(),
			expectNewProfile: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cpuProfiler := CpuProfiler{
				profiler: profiler{
					store:              newProfileStore(dumpStore, CpuProfFileNamePrefix, HeapFileNameSuffix, s),
					lastProfileTime:    c.lastProfileTime,
					highwaterMarkBytes: cpuUsageCombined.Get(sv),
					resetInterval:      cpuProfileInterval.Get(sv),
					knobs: testingKnobs{
						dontWriteProfiles: true,
					},
				},
				st: s,
			}
			cpuProfiler.MaybeTakeProfile(ctx, c.cpuUsage)
			if c.expectNewProfile {
				assert.NotEqual(t, c.lastProfileTime, cpuProfiler.profiler.lastProfileTime)
			} else {
				assert.Equal(t, c.lastProfileTime, cpuProfiler.profiler.lastProfileTime)
			}
		})
	}
}
