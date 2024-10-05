// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package profiler

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/server/dumpstore"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

var heapProfilerDirName = "cockroach-data/logs/heap_profiler"

func TestNewActiveQueryProfiler(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		name     string
		wantErr  bool
		errMsg   string
		storeDir string
		profiler *ActiveQueryProfiler
		limitFn  func() (int64, string, error)
	}{
		{
			name:     "returns error when no access to cgroups",
			wantErr:  true,
			errMsg:   "failed to detect cgroup memory limit: cgroups not available",
			storeDir: heapProfilerDirName,
			limitFn:  cgroupFnWithReturn(0, "", errors.New("cgroups not available")),
		},
		{
			name:     "returns error when store directory empty",
			wantErr:  true,
			errMsg:   "need to specify dir for NewQueryProfiler",
			storeDir: "",
		},
		{
			name:     "constructs and returns ActiveQueryProfiler",
			wantErr:  false,
			storeDir: heapProfilerDirName,
			profiler: &ActiveQueryProfiler{
				profiler: makeProfiler(
					newProfileStore(
						dumpstore.NewStore(heapProfilerDirName, activeQueryCombinedFileSize, nil),
						QueryFileNamePrefix,
						QueryFileNameSuffix,
						nil),
					zeroFloor,
					envMemprofInterval,
				),
				cgroupMemLimit: mbToBytes(256),
			},
			limitFn: cgroupFnWithReturn(mbToBytes(256), "", nil),
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			memLimitFn = test.limitFn
			profiler, err := NewActiveQueryProfiler(ctx, test.storeDir, nil)
			if test.wantErr {
				require.EqualError(t, err, test.errMsg)
				return
			}
			require.Equal(t, test.profiler.highWaterMarkFloor(), profiler.highWaterMarkFloor())
			test.profiler.highWaterMarkFloor = nil
			profiler.highWaterMarkFloor = nil
			require.Equal(t, test.profiler.resetInterval(), profiler.resetInterval())
			test.profiler.resetInterval = nil
			profiler.resetInterval = nil
			require.Equal(t, test.profiler, profiler)
		})
	}
}

func TestShouldDump(t *testing.T) {
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	createSettingFn := func(settingEnabled bool) *cluster.Settings {
		s := cluster.MakeClusterSettings()
		ActiveQueryDumpsEnabled.Override(ctx, &s.SV, settingEnabled)
		return s
	}

	profiler := &ActiveQueryProfiler{
		profiler: profiler{
			store: newProfileStore(
				dumpstore.NewStore(heapProfilerDirName, activeQueryCombinedFileSize, nil),
				QueryFileNamePrefix,
				QueryFileNameSuffix,
				nil),
			knobs: testingKnobs{
				dontWriteProfiles: false,
			},
		},
		cgroupMemLimit: mbToBytes(256),
	}
	testCases := []struct {
		name                   string
		want                   bool
		usage                  int64
		prevUsage              int64
		dontWriteProfileKnob   bool
		memUsageFn             func() (int64, string, error)
		memInactiveFileUsageFn func() (int64, string, error)
		st                     *cluster.Settings
	}{
		{
			name:       "returns false when failing to read memory usage",
			want:       false,
			usage:      0,
			memUsageFn: cgroupFnWithReturn(0, "", errors.New("no cgroup access")),
			st:         createSettingFn(true),
		},
		{
			name:                   "returns false when cluster setting disabled",
			want:                   false,
			usage:                  0,
			prevUsage:              0,
			memUsageFn:             cgroupFnWithReturn(mbToBytes(185), "", nil),
			memInactiveFileUsageFn: cgroupFnWithReturn(mbToBytes(35), "", nil),
			st:                     createSettingFn(false),
		},
		{
			name:                   "returns false when failing to read inactive file usage",
			want:                   false,
			usage:                  0,
			memUsageFn:             cgroupFnWithReturn(mbToBytes(64), "", nil),
			memInactiveFileUsageFn: cgroupFnWithReturn(0, "", errors.New("no cgroup access")),
			st:                     createSettingFn(true),
		},
		{
			name:                   "returns false on first run where prevMemUsage is zero",
			want:                   false,
			usage:                  mbToBytes(128) - mbToBytes(32),
			memUsageFn:             cgroupFnWithReturn(mbToBytes(128), "", nil),
			memInactiveFileUsageFn: cgroupFnWithReturn(mbToBytes(32), "", nil),
			st:                     createSettingFn(true),
		},
		{
			name:                   "returns false if test knobs set to not write profiles",
			want:                   false,
			dontWriteProfileKnob:   true,
			usage:                  mbToBytes(128) - mbToBytes(32),
			prevUsage:              mbToBytes(64),
			memUsageFn:             cgroupFnWithReturn(mbToBytes(128), "", nil),
			memInactiveFileUsageFn: cgroupFnWithReturn(mbToBytes(32), "", nil),
			st:                     createSettingFn(true),
		},
		{
			name:                   "returns false if memory growth rate is acceptable",
			want:                   false,
			usage:                  mbToBytes(128) - mbToBytes(32),
			prevUsage:              mbToBytes(64),
			memUsageFn:             cgroupFnWithReturn(mbToBytes(128), "", nil),
			memInactiveFileUsageFn: cgroupFnWithReturn(mbToBytes(32), "", nil),
			st:                     createSettingFn(true),
		},
		{
			name:                   "returns true if memory growth rate indicates OOM",
			want:                   true,
			usage:                  mbToBytes(185) - mbToBytes(35),
			prevUsage:              mbToBytes(64),
			memUsageFn:             cgroupFnWithReturn(mbToBytes(185), "", nil),
			memInactiveFileUsageFn: cgroupFnWithReturn(mbToBytes(35), "", nil),
			st:                     createSettingFn(true),
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			profiler.knobs.dontWriteProfiles = test.dontWriteProfileKnob

			profiler.mu.Lock()
			profiler.mu.prevMemUsage = test.prevUsage
			profiler.mu.Unlock()

			memUsageFn = test.memUsageFn
			memInactiveFileUsageFn = test.memInactiveFileUsageFn

			shouldDump, usage := profiler.shouldDump(ctx, test.st)
			require.Equal(t, test.want, shouldDump)
			require.Equal(t, test.usage, usage)

			profiler.mu.Lock()
			defer profiler.mu.Unlock()
			require.Equal(t, profiler.mu.prevMemUsage, usage)
		})
	}
}

func TestMaybeDumpQueries_PanicHandler(t *testing.T) {
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	memLimitFn = cgroupFnWithReturn(mbToBytes(256), "", nil)
	s := &cluster.Settings{}

	profiler, err := NewActiveQueryProfiler(ctx, heapProfilerDirName, nil)
	require.NoError(t, err)

	require.NotPanics(t, func() {
		profiler.MaybeDumpQueries(ctx, nil, s)
	})
}

func cgroupFnWithReturn(value int64, warnings string, err error) func() (int64, string, error) {
	return func() (int64, string, error) {
		return value, warnings, err
	}
}

func mbToBytes(mb int64) int64 {
	return mb * 1024 * 1024
}
