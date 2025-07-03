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
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/dumpstore"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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

// SimpleFlightRecorder is a wrapper around `trace.FlightRecorder`
// that enables continuous trace capture over a configurable interval.
type SimpleFlightRecorder struct {
	fr *trace.FlightRecorder

	dumpStore *dumpstore.DumpStore

	sv        *settings.Values
	directory string

	// enabledCheckInterval configures the interval at which the async task
	// checks to see if the feature has been enabled if it's disabled.
	enabledCheckInterval time.Duration
	enabled              atomic.Bool
}

// A `Dumper` implementation is needed to run `DumpStore.GC` periodically. This
// enables `SimpleFlightRecorder` to identify its files for the automated
// cleanup process.
var _ dumpstore.Dumper = &SimpleFlightRecorder{}

func NewFlightRecorder(
	st *cluster.Settings, enabledCheckInterval time.Duration, directory string,
) (*SimpleFlightRecorder, error) {
	fr := trace.NewFlightRecorder()
	if directory == "" {
		return nil, errors.New("flight recorder directory argument is empty, will not record execution traces")
	}
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, errors.Wrap(err, "cannot create execution trace directory, will not record execution traces")
	}

	return &SimpleFlightRecorder{
		fr: fr,

		dumpStore: dumpstore.NewStore(directory, executionTracerTotalDumpSizeLimit, st),

		sv:                   &st.SV,
		directory:            directory,
		enabledCheckInterval: enabledCheckInterval,
	}, nil
}

// Copied from profiler/profiler_common.go.
const timestampFormat = "2006-01-02T15_04_05.000"

func (sfr *SimpleFlightRecorder) TimestampedFilename() string {
	return filepath.Join(sfr.directory, fmt.Sprintf("executiontrace.%s.out", timeutil.Now().Format(timestampFormat)))
}

var fileMatchRegexp = regexp.MustCompile("^executiontrace.*out$")

// CheckOwnsFile is part of the `Dumper` interface.
func (sfr *SimpleFlightRecorder) CheckOwnsFile(ctx context.Context, fi os.FileInfo) bool {
	return fileMatchRegexp.MatchString(fi.Name())
}

// PreFilter is part of the `Dumper` interface. In this case we do not mark any
// files for preservation.
func (sfr *SimpleFlightRecorder) PreFilter(
	ctx context.Context, files []os.FileInfo, cleanupFn func(fileName string) error,
) (preserved map[int]bool, err error) {
	return nil, nil
}

// enabledForTests is a helper function for tests to check if the flight recorder
// is enabled. It is necessary because we cannot call `sfr.fr.Enabled()`
// concurrently with `sfr.fr.Start()`.
func (sfr *SimpleFlightRecorder) enabledForTests() bool {
	return sfr.enabled.Load()
}

func (sfr *SimpleFlightRecorder) Start(ctx context.Context, stopper *stop.Stopper) error {
	if sfr == nil {
		return errors.New("flight recorder is not initialized, will not record execution traces")
	}
	return stopper.RunAsyncTask(ctx, "simple-flight-recorder", func(ctx context.Context) {
		t := timeutil.Timer{}
		t.Reset(sfr.enabledCheckInterval)

		defer func() {
			if sfr.fr.Enabled() {
				err := sfr.fr.Stop()
				if err != nil {
					log.Warningf(ctx, "error while stopping flight recorder: %v", err)
				}
				sfr.enabled.Store(false)
			}
		}()

		for {
			select {
			case <-t.C:
				startTime := timeutil.Now()
				interval := ExecutionTracerInterval.Get(sfr.sv)
				if interval == 0 {
					if sfr.fr.Enabled() {
						err := sfr.fr.Stop()
						if err != nil {
							log.Warningf(ctx, "error while stopping flight recorder: %v", err)
						}
						sfr.enabled.Store(false)
						log.Infof(ctx, "flight recorder stopped")
					}
					// Disabled. Check again soon.
					t.Reset(sfr.enabledCheckInterval)
					continue
				}
				if !sfr.fr.Enabled() {
					duration := ExecutionTracerDuration.Get(sfr.sv)
					sfr.fr.SetPeriod(duration)
					err := sfr.fr.Start()
					if err != nil {
						log.Warningf(ctx, "error while starting flight recorder, will try again: %v", err)
						t.Reset(max(interval-timeutil.Since(startTime), 0))
						continue
					}
					sfr.enabled.Store(true)
					log.Infof(ctx, "flight recorder started")
				}
				filename := sfr.TimestampedFilename()
				destFile, err := os.Create(filename)
				if err != nil {
					log.Warningf(ctx, "unable to open file %s to dump flight record, will try again: %v", filename, err)
					t.Reset(max(interval-timeutil.Since(startTime), 0))
					continue
				}
				_, err = sfr.fr.WriteTo(destFile)
				if err != nil {
					log.Warningf(ctx, "error while writing flight record to %s, will try again: %v", filename, err)
					t.Reset(max(interval-timeutil.Since(startTime), 0))
					continue
				}

				// Can run GC since we successfully wrote a new file.
				sfr.dumpStore.GC(ctx, timeutil.Now(), sfr)
				t.Reset(max(interval-timeutil.Since(startTime), 0))
			case <-stopper.ShouldQuiesce():
				return
			case <-ctx.Done():
				return
			}
		}
	})
}
