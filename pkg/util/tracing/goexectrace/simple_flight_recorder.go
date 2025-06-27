// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package goexectrace

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/dumpstore"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"golang.org/x/exp/trace"
)

var executionTracerTotalDumpSizeLimit = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"obs.execution_tracer.total_dump_size_limit",
	"maximum combined disk size of preserved Go execution traces",
	// We set a large default value since this feature is off by default, and
	// situations in which it is turned on may require lots of execution traces
	// to be preserved.
	4<<30, // 4GiB
)

var ExecutionTracerInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"obs.execution_tracer.interval",
	"duration between Go execution traces (zero disables)",
	0, // disabled
	settings.DurationWithMinimumOrZeroDisable(0),
)

var ExecutionTracerDuration = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"obs.execution_tracer.duration",
	"the length of automatic Go execution traces",
	10*time.Second,
	settings.PositiveDuration,
)

type SimpleFlightRecorder struct {
	fr *trace.FlightRecorder

	dumpStore *dumpstore.DumpStore

	sv                *settings.Values
	directory         string
	minSampleInterval time.Duration
}

var _ dumpstore.Dumper = &SimpleFlightRecorder{}

func NewFlightRecorder(
	st *cluster.Settings, minSampleInterval time.Duration, directory string,
) *SimpleFlightRecorder {
	fr := trace.NewFlightRecorder()
	if directory == "" {
		log.Warningf(context.Background(), "flight recorder directory argument is empty, will not record execution traces")
		return nil
	}
	if err := os.MkdirAll(directory, 0755); err != nil {
		log.Warningf(context.Background(), "cannot create execution trace directory, will not record execution traces: %v", err)
		return nil
	}

	return &SimpleFlightRecorder{
		fr: fr,

		dumpStore: dumpstore.NewStore(directory, executionTracerTotalDumpSizeLimit, st),

		sv:                &st.SV,
		directory:         directory,
		minSampleInterval: minSampleInterval,
	}
}

// Copied from profiler/profiler_common.go.
const timestampFormat = "2006-01-02T15_04_05.000"

func (sfr *SimpleFlightRecorder) TimestampedFilename() string {
	return filepath.Join(sfr.directory, fmt.Sprintf("executiontrace.%s.bin", timeutil.Now().Format(timestampFormat)))
}

var fileMatchRegexp = regexp.MustCompile("^executiontrace.*bin$")

func (sfr *SimpleFlightRecorder) CheckOwnsFile(ctx context.Context, fi os.FileInfo) bool {
	return fileMatchRegexp.MatchString(fi.Name())
}

func (sfr *SimpleFlightRecorder) PreFilter(
	ctx context.Context, files []os.FileInfo, cleanupFn func(fileName string) error,
) (preserved map[int]bool, err error) {
	return nil, nil
}

func (sfr *SimpleFlightRecorder) Start(ctx context.Context, stopper *stop.Stopper) error {
	if sfr == nil {
		log.Warningf(ctx, "flight recorder is not initialized, will not record execution traces")
		return nil
	}
	return stopper.RunAsyncTask(ctx, "simple-flight-recorder", func(ctx context.Context) {
		t := timeutil.Timer{}
		t.Reset(sfr.minSampleInterval)

		for {
			select {
			case <-t.C:
				startTime := timeutil.Now()
				filename := sfr.TimestampedFilename()
				interval := ExecutionTracerInterval.Get(sfr.sv)
				if interval == 0 {
					if sfr.fr.Enabled() {
						err := sfr.fr.Stop()
						if err != nil {
							log.Errorf(ctx, "error while stopping flight recorder: %v", err)
						}
						log.Warningf(ctx, "flight recorder stopped")
					}
					// Disabled. Check again soon.
					t.Reset(sfr.minSampleInterval)
					continue
				}
				if !sfr.fr.Enabled() {
					duration := ExecutionTracerDuration.Get(sfr.sv)
					sfr.fr.SetPeriod(duration)
					err := sfr.fr.Start()
					if err != nil {
						log.Errorf(ctx, "error while starting flight recorder, will try again: %v", err)
						t.Reset(interval - timeutil.Now().Sub(startTime))
						continue
					}
					log.Warningf(ctx, "flight recorder started")
				}

				destFile, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
				if err != nil {
					log.Errorf(ctx, "unable to open file %s to dump flight record, will try again: %v", filename, err)
					t.Reset(interval - timeutil.Now().Sub(startTime))
					continue
				}
				_, err = sfr.fr.WriteTo(destFile)
				if err != nil {
					log.Errorf(ctx, "error while writing flight record to %s, will try again: %v", filename, err)
					t.Reset(interval - timeutil.Now().Sub(startTime))
					continue
				}

				// Can run GC since we successfully wrote a new file.
				sfr.dumpStore.GC(ctx, timeutil.Now(), sfr)
				t.Reset(interval - timeutil.Now().Sub(startTime))
			case <-stopper.ShouldQuiesce():
				err := sfr.fr.Stop()
				if err != nil {
					log.Errorf(ctx, "error while stopping flight recorder: %v", err)
				}
				return
			case <-ctx.Done():
				err := sfr.fr.Stop()
				if err != nil {
					log.Errorf(ctx, "error while stopping flight recorder: %v", err)
				}
				return
			}
		}
	})
}
