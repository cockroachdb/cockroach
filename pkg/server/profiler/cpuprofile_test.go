// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package profiler

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

func TestCPUProfiler(t *testing.T) {
	ctx := context.Background()
	dumpStore := dumpstore.NewStore(t.TempDir(), nil, nil)
	s := &cluster.Settings{}
	sv := &s.SV
	s.Version = clusterversion.MakeVersionHandle(sv)
	sv.Init(ctx, s.Version)
	cpuProfileInterval.Override(ctx, sv, time.Hour)
	cpuUsageCombined.Override(ctx, sv, 80)
	cpuProfileEnabled.Override(ctx, sv, true)
	pastTime := time.Date(2023, 1, 1, 1, 1, 1, 1, time.UTC)
	cases := []struct {
		name                 string
		cpuUsage             int64
		highWaterMark        int64
		lastProfileTime      time.Time
		expectNewProfile     bool
		highWaterMarkUpdated bool
	}{
		{
			name:                 "take profile, highWaterMark update",
			cpuUsage:             90,
			highWaterMark:        0,
			lastProfileTime:      pastTime,
			expectNewProfile:     true,
			highWaterMarkUpdated: true,
		},
		{
			name:                 "no profile, no highwaterMark update",
			cpuUsage:             50,
			highWaterMark:        80,
			lastProfileTime:      timeutil.Now(),
			expectNewProfile:     false,
			highWaterMarkUpdated: false,
		},
		{
			name:                 "no profile, highWaterMarkUpdate",
			cpuUsage:             70,
			highWaterMark:        70,
			lastProfileTime:      timeutil.Now(),
			expectNewProfile:     false,
			highWaterMarkUpdated: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := makeProfiler(
				newProfileStore(dumpStore, cpuProfFileNamePrefix, HeapFileNameSuffix, s),
				func() int64 { return cpuUsageCombined.Get(sv) },
				func() time.Duration { return cpuProfileInterval.Get(sv) },
			)
			p.knobs = testingKnobs{
				dontWriteProfiles: true,
			}
			p.lastProfileTime = c.lastProfileTime
			p.highWaterMark = c.highWaterMark

			cpuProfiler := CPUProfiler{
				profiler: p,
				st:       s,
			}
			cpuProfiler.MaybeTakeProfile(ctx, c.cpuUsage)
			if c.expectNewProfile {
				assert.NotEqual(t, c.lastProfileTime, cpuProfiler.profiler.lastProfileTime)
			} else {
				assert.Equal(t, c.lastProfileTime, cpuProfiler.profiler.lastProfileTime)
			}
			if c.highWaterMarkUpdated {
				assert.NotEqual(t, c.highWaterMark, cpuProfiler.profiler.highWaterMark)
			} else {
				assert.Equal(t, c.highWaterMark, cpuProfiler.profiler.highWaterMark)
			}
		})
	}
}
