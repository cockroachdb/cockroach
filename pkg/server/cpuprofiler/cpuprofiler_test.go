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
	s := &cluster.Settings{}
	sv := &s.SV
	s.Version = clusterversion.MakeVersionHandle(sv)
	sv.Init(ctx, s.Version)
	pastTime := time.Date(2023, 1, 1, 1, 1, 1, 1, time.UTC)
	cases := []struct {
		name             string
		params           []int64
		lastProfileTime  time.Time
		currentTime      func() time.Time
		expectNewProfile bool
	}{
		{
			name:             "take profile",
			params:           []int64{100, 100, 1},
			lastProfileTime:  pastTime,
			currentTime:      timeutil.Now,
			expectNewProfile: true,
		},
		{
			name:             "no profile due to low qps",
			params:           []int64{1, 1, 1},
			lastProfileTime:  pastTime,
			currentTime:      timeutil.Now,
			expectNewProfile: false,
		},
		{
			name:             "no profile due to last profile being within the time interval",
			params:           []int64{100, 100, 1},
			lastProfileTime:  timeutil.Now(),
			currentTime:      timeutil.Now,
			expectNewProfile: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			profiler := CpuProfiler{
				lastProfileTime: c.lastProfileTime,
				currentTime:     c.currentTime,
				store:           dumpstore.NewStore(cpuProfilerDirName, nil, nil),
				st:              s,
				knobs:           testingKnobs{dontWrite: true},
			}
			profiler.MaybeTakeProfile(ctx, c.params[0], c.params[1], c.params[2])
			if c.expectNewProfile {
				assert.NotEqual(t, c.lastProfileTime, profiler.lastProfileTime)
			} else {
				assert.Equal(t, c.lastProfileTime, profiler.lastProfileTime)
			}
		})
	}
}
