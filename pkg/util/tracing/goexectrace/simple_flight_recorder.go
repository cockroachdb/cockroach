// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package goexectrace

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime/trace"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/dumpstore"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// errDumpBusy is a sentinel error returned by doDump when the flight recorder
// is already handling a concurrent WriteTo call. Callers treat this as a
// non-fatal skip.
var errDumpBusy = errors.New("flight recorder snapshot already in progress")

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
	"duration between periodic Go execution trace dumps to disk (zero disables periodic dumps; "+
		"the flight recorder still runs if obs.execution_tracer.duration is positive)",
	0, // disabled
	settings.DurationWithMinimumOrZeroDisable(0),
)

var ExecutionTracerDuration = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"obs.execution_tracer.duration",
	"how much execution trace data the flight recorder keeps in its buffer "+
		"(zero disables the flight recorder entirely)",
	0, // disabled
	settings.DurationWithMinimumOrZeroDisable(0),
)

var executionTracerOnDemandMinInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"obs.execution_tracer.on_demand.min_interval",
	"minimum interval between on-demand execution trace dumps",
	10*time.Second,
	settings.DurationWithMinimum(0),
)

// SimpleFlightRecorder is a wrapper around trace.FlightRecorder that enables
// continuous trace capture. The flight recorder lifecycle is controlled by the
// obs.execution_tracer.duration setting: a positive value enables it, zero
// disables it. Periodic dumps to disk are independently controlled by
// obs.execution_tracer.interval.
type SimpleFlightRecorder struct {
	dumpStore *dumpstore.DumpStore

	sv        *settings.Values
	directory string

	// enabledCheckInterval configures the interval at which the async task
	// checks to see if the feature has been enabled if it's disabled.
	enabledCheckInterval time.Duration
	enabled              atomic.Bool

	// lastOnDemandDumpNanos stores the UnixNano timestamp of the last
	// successful on-demand dump, used for rate limiting.
	lastOnDemandDumpNanos atomic.Int64

	// frMu protects the fr pointer, which is reassigned in
	// ensureFRStarted when the configured duration changes. Without this
	// mutex, concurrent readers (doDump, WriteTraceTo) would race with
	// the Start loop replacing fr. The Start loop holds a write lock
	// during lifecycle changes; doDump and WriteTraceTo hold a read lock
	// when calling WriteTo.
	frMu struct {
		syncutil.RWMutex

		fr *trace.FlightRecorder
		// period is the MinAge used to construct the current fr instance.
		// It is used to avoid reconstructing the FlightRecorder when
		// the period hasn't changed.
		period time.Duration
	}
}

// A Dumper implementation is needed to run DumpStore.GC periodically. This
// enables SimpleFlightRecorder to identify its files for the automated
// cleanup process.
var _ dumpstore.Dumper = &SimpleFlightRecorder{}

func NewFlightRecorder(
	st *cluster.Settings, enabledCheckInterval time.Duration, directory string,
) (*SimpleFlightRecorder, error) {
	if directory == "" {
		return nil, errors.New("flight recorder directory argument is empty, will not record execution traces")
	}
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, errors.Wrap(err, "cannot create execution trace directory, will not record execution traces")
	}

	return &SimpleFlightRecorder{
		dumpStore:            dumpstore.NewStore(directory, executionTracerTotalDumpSizeLimit, st),
		sv:                   &st.SV,
		directory:            directory,
		enabledCheckInterval: enabledCheckInterval,
	}, nil
}

// Copied from profiler/profiler_common.go.
const timestampFormat = "2006-01-02T15_04_05.000"

func (sfr *SimpleFlightRecorder) timestampedFilename(tag string) string {
	if tag != "" {
		return filepath.Join(sfr.directory,
			fmt.Sprintf("executiontrace.%s.%s.out", timeutil.Now().Format(timestampFormat), tag))
	}
	return filepath.Join(sfr.directory,
		fmt.Sprintf("executiontrace.%s.out", timeutil.Now().Format(timestampFormat)))
}

var fileMatchRegexp = regexp.MustCompile("^executiontrace.*out$")

// CheckOwnsFile is part of the Dumper interface.
func (sfr *SimpleFlightRecorder) CheckOwnsFile(ctx context.Context, fi os.DirEntry) bool {
	return fileMatchRegexp.MatchString(fi.Name())
}

// PreFilter is part of the Dumper interface. In this case we do not mark any
// files for preservation.
func (sfr *SimpleFlightRecorder) PreFilter(
	ctx context.Context, files []os.DirEntry, cleanupFn func(fileName string) error,
) (preserved map[int]bool, err error) {
	return nil, nil
}

// Enabled returns whether the flight recorder is currently running.
func (sfr *SimpleFlightRecorder) Enabled() bool {
	return sfr.enabled.Load()
}

// isSnapshotInProgress reports whether err indicates a concurrent
// FlightRecorder.WriteTo call is already in progress. Go 1.25's
// runtime/trace does not export a sentinel error for this condition;
// it returns a plain fmt.Errorf. String matching is the only option.
// If the runtime changes this message, we'll lose the ability to
// distinguish busy from broken, which is acceptable: the worst case
// is that a concurrent dump returns an error instead of being silently
// skipped.
func isSnapshotInProgress(err error) bool {
	return err != nil && strings.Contains(err.Error(), "already in progress")
}

// isDisabledRecorder reports whether err indicates WriteTo was called
// on a stopped FlightRecorder. Same caveat as isSnapshotInProgress:
// the runtime uses a plain fmt.Errorf with no sentinel.
func isDisabledRecorder(err error) bool {
	return err != nil && strings.Contains(err.Error(), "disabled flight recorder")
}

// doDump performs a dump to a file with the given tag. If a concurrent WriteTo
// is already in progress, or if the FR was stopped between the caller's
// enabled check and the actual write, doDump returns errDumpBusy without
// writing a file.
//
// doDump writes to a temporary file and renames it to the final timestamped
// name on success. This prevents the error-path cleanup (os.Remove) of one
// caller from deleting a file that a concurrent caller wrote successfully,
// which can happen when two callers race within the same millisecond and
// resolve to the same timestamped filename.
//
// nolint:deferunlockcheck
func (sfr *SimpleFlightRecorder) doDump(tag string) (string, error) {
	tmpFile, err := os.CreateTemp(sfr.directory, "executiontrace.*.tmp")
	if err != nil {
		return "", errors.Wrap(err, "creating temp file for flight record dump")
	}
	tmpName := tmpFile.Name()

	// Hold frMu as a reader to prevent the Start loop from
	// stopping/replacing the FR mid-write.
	sfr.frMu.RLock()
	_, writeErr := sfr.frMu.fr.WriteTo(tmpFile)
	sfr.frMu.RUnlock()

	closeErr := tmpFile.Close()
	if writeErr != nil {
		_ = os.Remove(tmpName)
		if isSnapshotInProgress(writeErr) || isDisabledRecorder(writeErr) {
			return "", errDumpBusy
		}
		return "", errors.Wrapf(writeErr, "writing flight record")
	}
	if closeErr != nil {
		_ = os.Remove(tmpName)
		return "", errors.Wrapf(closeErr, "closing flight record temp file")
	}

	filename := sfr.timestampedFilename(tag)
	if err := os.Rename(tmpName, filename); err != nil {
		_ = os.Remove(tmpName)
		return "", errors.Wrapf(err, "renaming flight record to %s", filename)
	}
	return filename, nil
}

// WriteTraceTo writes the current flight recorder buffer to the given writer.
// This is used by the pprof trace handler to stream the trace data without
// saving to disk. If a concurrent WriteTo is in progress, it retries briefly.
func (sfr *SimpleFlightRecorder) WriteTraceTo(ctx context.Context, w io.Writer) (int64, error) {
	retryOpts := retry.Options{
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     time.Second,
		Multiplier:     2,
		MaxRetries:     5,
	}
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		var n int64
		var err error
		func() {
			sfr.frMu.RLock()
			defer sfr.frMu.RUnlock()
			n, err = sfr.frMu.fr.WriteTo(w)
		}()
		if err == nil || !isSnapshotInProgress(err) {
			return n, err
		}
	}
	return 0, errors.New("goexectrace: concurrent trace write could not complete after retries")
}

// validTag returns true if s contains only ASCII letters, digits, and
// underscores (or is empty). This prevents path-traversal via the tag.
func validTag(s string) bool {
	for _, r := range s {
		if r != '_' && !(r >= 'a' && r <= 'z') && !(r >= 'A' && r <= 'Z') && !(r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}

// DumpNow triggers an on-demand execution trace dump. The flight recorder must
// already be enabled via the obs.execution_tracer.duration cluster setting. The
// tag parameter is appended to the filename (e.g., "scheduling_latency") and
// must contain only alphanumeric characters and underscores.
//
// If a dump is already in progress, the call is skipped (returns "", nil).
// On-demand dumps are rate limited by obs.execution_tracer.on_demand.min_interval.
func (sfr *SimpleFlightRecorder) DumpNow(
	ctx context.Context, reason redact.RedactableString, tag string,
) (string, error) {
	if !sfr.enabled.Load() {
		return "", errors.New(
			"flight recorder is not enabled; set obs.execution_tracer.duration to enable")
	}
	if !validTag(tag) {
		return "", errors.Newf(
			"invalid tag %q: must contain only alphanumeric characters and underscores", tag)
	}

	// Rate limiting: check whether we're within the minimum interval since
	// the last successful dump. This is best-effort: two concurrent callers
	// can both pass the check before either updates the timestamp.
	minInterval := executionTracerOnDemandMinInterval.Get(sfr.sv)
	nowNanos := timeutil.Now().UnixNano()
	lastNanos := sfr.lastOnDemandDumpNanos.Load()
	if minInterval > 0 && nowNanos-lastNanos < minInterval.Nanoseconds() {
		log.Ops.VEventfDepth(
			ctx, 1, 1, "goexectrace: on-demand dump suppressed by rate limit: %s", reason)
		return "", nil
	}

	log.Ops.Infof(ctx, "goexectrace: taking on-demand dump: %s [tag=%s]", reason, tag)
	filename, err := sfr.doDump(tag)
	if errors.Is(err, errDumpBusy) {
		log.Dev.VEventf(ctx, 2, "goexectrace: on-demand dump skipped, snapshot already in progress")
		return "", nil
	}
	if err != nil {
		return "", err
	}

	sfr.lastOnDemandDumpNanos.Store(timeutil.Now().UnixNano())

	sfr.dumpStore.GC(ctx, timeutil.Now(), sfr)
	return filename, nil
}

// stopFR stops the flight recorder under frMu.
func (sfr *SimpleFlightRecorder) stopFR() {
	sfr.frMu.Lock()
	defer sfr.frMu.Unlock()
	if sfr.frMu.fr != nil && sfr.frMu.fr.Enabled() {
		sfr.frMu.fr.Stop()
		sfr.enabled.Store(false)
	}
}

// ensureFRStarted starts the flight recorder with the given period under
// frMu, if it is not already running with the same period. A new
// FlightRecorder is only created when the requested period differs from the
// current one, since FlightRecorderConfig is immutable after construction.
// Returns true if the FR is running after this call.
func (sfr *SimpleFlightRecorder) ensureFRStarted(ctx context.Context, period time.Duration) bool {
	sfr.frMu.Lock()
	defer sfr.frMu.Unlock()

	if sfr.frMu.period != period {
		log.Dev.Infof(ctx,
			"goexectrace: reconfiguring flight recorder period: %.1f sec -> %.1f sec",
			sfr.frMu.period.Seconds(), period.Seconds())
		if sfr.frMu.fr != nil && sfr.frMu.fr.Enabled() {
			sfr.frMu.fr.Stop()
		}
		sfr.frMu.fr = trace.NewFlightRecorder(trace.FlightRecorderConfig{
			MinAge: period,
		})
		sfr.frMu.period = period
	}

	if sfr.frMu.fr.Enabled() {
		return true
	}

	if err := sfr.frMu.fr.Start(); err != nil {
		log.Dev.Warningf(ctx,
			"goexectrace: error while starting flight recorder, will try again: %v", err)
		return false
	}
	sfr.enabled.Store(true)
	log.Dev.Infof(ctx, "goexectrace: flight recorder started with period: %.1f sec",
		period.Seconds())
	return true
}

func (sfr *SimpleFlightRecorder) Start(ctx context.Context, stopper *stop.Stopper) error {
	if sfr == nil {
		return errors.New("flight recorder is not initialized, will not record execution traces")
	}
	return stopper.RunAsyncTask(ctx, "simple-flight-recorder", func(ctx context.Context) {
		t := timeutil.Timer{}
		t.Reset(sfr.enabledCheckInterval)

		defer sfr.stopFR()

		for {
			select {
			case <-t.C:
				startTime := timeutil.Now()
				duration := ExecutionTracerDuration.Get(sfr.sv)

				// The flight recorder lifecycle is controlled by the duration
				// setting: positive enables, zero disables.
				if duration == 0 {
					wasEnabled := sfr.enabled.Load()
					sfr.stopFR()
					if wasEnabled {
						log.Dev.Infof(ctx, "goexectrace: flight recorder stopped")
					}
					t.Reset(sfr.enabledCheckInterval)
					continue
				}
				if !sfr.ensureFRStarted(ctx, duration) {
					t.Reset(sfr.enabledCheckInterval)
					continue
				}

				// Periodic dumps are optional; controlled by the interval
				// setting. If zero, the FR is running but we don't dump to
				// disk periodically.
				interval := ExecutionTracerInterval.Get(sfr.sv)
				if interval == 0 {
					t.Reset(sfr.enabledCheckInterval)
					continue
				}

				filename, err := sfr.doDump("" /* tag */)
				if errors.Is(err, errDumpBusy) {
					t.Reset(max(interval-timeutil.Since(startTime), 0))
					continue
				}
				if err != nil {
					log.Dev.Warningf(ctx,
						"goexectrace: error during periodic dump: %v", err)
					t.Reset(max(interval-timeutil.Since(startTime), 0))
					continue
				}

				sfr.dumpStore.GC(ctx, timeutil.Now(), sfr)
				_ = filename // logged by doDump internals if needed
				t.Reset(max(interval-timeutil.Since(startTime), 0))
			case <-stopper.ShouldQuiesce():
				return
			case <-ctx.Done():
				return
			}
		}
	})
}
