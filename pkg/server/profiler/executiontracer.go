// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package profiler

import (
	"context"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/dumpstore"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/goexectrace"
	"github.com/cockroachdb/errors"
)

var executionTracerTotalDumpSizeLimit = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"server.execution_trace.total_dump_size_limit",
	"maximum combined disk size of preserved Go execution traces",
	// We set a large default value since this feature is off by default, and
	// situations in which it is turned on may require lots of execution traces
	// to be preserved.
	4<<30, // 4GiB
)

var executionTracerMaxFiles = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"server.execution_tracer.max_files",
	"maximum number of trace files to be kept",
	// The default retains a number of profiles that corresponds to
	// 24h coverage at the default 10s trace duration when taken
	// back to back. Note that executionTracerTotalDumpSizeLimit
	// applies.
	int64((24*time.Hour)/(10*time.Second)), // 8640
)

var ExecutionTracerInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"server.execution_tracer.interval",
	"duration between Go execution traces (zero disables)",
	0, // disabled
	settings.DurationWithMinimumOrZeroDisable(0),
)

var ExecutionTracerDuration = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"server.execution_tracer.duration",
	"the length of automatic Go execution traces",
	10*time.Second,
	settings.PositiveDuration,
)

func UniqueExecutionTracerFileName() string {
	return makeNewFileName(executionTraceFileNamePrefix, executionTraceFileNameSuffix, timeutil.Now(), 0)
}

const executionTraceFileNamePrefix = "executiontrace"
const executionTraceFileNameSuffix = ".bin"

type ExecutionTracer struct {
	dir      string
	st       *cluster.Settings
	profiler profiler
	// cfr isn't used to take the execution traces, but needs to be synchronized
	// with since only one execution trace can be taken at a time and the profiler
	// doesn't fill our need to control precisely the starting point of the
	// profile.
	cfr *goexectrace.CoalescedFlightRecorder
}

func NewExecutionTracer(
	st *cluster.Settings, cfr *goexectrace.CoalescedFlightRecorder,
) (*ExecutionTracer, error) {
	dir := cfr.Dir()
	if dir == "" {
		return nil, errors.Errorf("need to specify dir for NewExecutionTracer")
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	dumpStore := dumpstore.NewStore(dir, executionTracerTotalDumpSizeLimit, st)
	profStore := newProfileStore(dumpStore, executionTraceFileNamePrefix, executionTraceFileNameSuffix, st)
	profStore.maxProfilesOrNil = executionTracerMaxFiles
	prof := makeProfiler(
		profStore,
		func() int64 {
			// There is no obvious "importance" of a goroutine profile; we may care
			// about different aspects. As is, execution traces are taken whenever
			// the min interval allows for it.
			return 0
		},
		func() time.Duration { return ExecutionTracerInterval.Get(&st.SV) },
	)

	return &ExecutionTracer{
		st:       st,
		dir:      dir,
		profiler: prof,
		cfr:      cfr,
	}, nil
}

func (e *ExecutionTracer) MaybeRecordTrace(ctx context.Context) {
	if e.profiler.resetInterval() == 0 {
		return
	}
	e.profiler.maybeTakeProfile(ctx, e.profiler.highWaterMarkFloor()+1, e.maybeRecordTrace)
}

func (e *ExecutionTracer) maybeRecordTrace(ctx context.Context, _ string, _ ...interface{}) bool {
	hdl := e.cfr.New()
	defer hdl.Release()
	// TODO(obs): would be a better API to pass in the path and store the profile
	// there (and only there). We do want to tap into the existing profiler's
	// management of the traces, but as is, they're stored in whatever the flight
	// recorder uses as its directory. So the cleanup basically doesn't work
	// unless the flight recorder is configured to write to exactly the right
	// directory and in the exact right format.
	if _, err := hdl.RecordSyncTrace(ctx, ExecutionTracerDuration.Get(&e.st.SV), nil); err != nil {
		log.Infof(ctx, "error during Go execution trace: %s", err)
		return false
	}
	return true
}
