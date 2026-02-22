// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package goexectrace

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors/oserror"
	"github.com/stretchr/testify/require"
)

func TestSimpleFlightRecorder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir := t.TempDir()
	st := cluster.MakeTestingClusterSettings()

	fr, err := NewFlightRecorder(st, 1*time.Second, dir)
	require.NoError(t, err)

	stopper := stop.NewStopper()
	defer func() {
		<-stopper.IsStopped()
		if fr.Enabled() {
			t.Fatal("flight recorder is still enabled after stopper is stopped")
		}
	}()
	defer stopper.Stop(context.Background())

	err = fr.Start(context.Background(), stopper)
	require.NoError(t, err)

	// Tempdir is empty.
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Equal(t, len(files), 0)

	// Enable the FR via duration and enable periodic dumps via interval.
	ExecutionTracerDuration.Override(context.Background(), &st.SV, 10*time.Second)
	ExecutionTracerInterval.Override(context.Background(), &st.SV, 1*time.Millisecond)

	t.Run("writes a file when enabled", func(t *testing.T) {
		testutils.SucceedsSoon(t, func() error {
			if !fr.Enabled() {
				return errors.New("flight recorder is not enabled")
			}
			files, err := os.ReadDir(dir)
			if err != nil {
				return err
			}
			if len(files) == 0 {
				return errors.New("no files written")
			}
			fi, err := os.Stat(filepath.Join(dir, files[0].Name()))
			if err != nil {
				return err
			}
			if fi.Size() == 0 {
				return errors.New("file is empty")
			}
			if !fileMatchRegexp.MatchString(files[0].Name()) {
				return errors.New("file name does not match expected pattern")
			}
			return nil
		})
	})

	t.Run("stops the flight recorder when disabled", func(t *testing.T) {
		ExecutionTracerDuration.Override(context.Background(), &st.SV, 0)
		testutils.SucceedsSoon(t, func() error {
			if fr.Enabled() {
				return errors.New("flight recorder is still enabled")
			}
			return nil
		})
	})

	// Restart so we can test that it's stopped with the stopper.
	ExecutionTracerDuration.Override(context.Background(), &st.SV, 10*time.Second)
	testutils.SucceedsSoon(t, func() error {
		if !fr.Enabled() {
			return errors.New("flight recorder is not enabled")
		}
		return nil
	})
}

// TestSettingCombinations verifies the four combinations of duration and
// interval settings produce the correct behavior.
func TestSettingCombinations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir := t.TempDir()
	st := cluster.MakeTestingClusterSettings()

	fr, err := NewFlightRecorder(st, 100*time.Millisecond, dir)
	require.NoError(t, err)

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	ctx := context.Background()
	err = fr.Start(ctx, stopper)
	require.NoError(t, err)

	executionTracerOnDemandMinInterval.Override(ctx, &st.SV, 0)

	clearDir := func() error {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if err := os.Remove(filepath.Join(dir, e.Name())); err != nil && !oserror.IsNotExist(err) {
				return err
			}
		}
		return nil
	}

	tests := []struct {
		name             string
		duration         time.Duration
		interval         time.Duration
		wantEnabled      bool
		wantPeriodicDump bool
		wantDumpNow      bool
	}{{
		name:             "duration=0,interval=0",
		duration:         0,
		interval:         0,
		wantEnabled:      false,
		wantPeriodicDump: false,
		wantDumpNow:      false,
	}, {
		name:             "duration=0,interval>0",
		duration:         0,
		interval:         1 * time.Millisecond,
		wantEnabled:      false,
		wantPeriodicDump: false,
		wantDumpNow:      false,
	}, {
		name:             "duration>0,interval=0",
		duration:         10 * time.Second,
		interval:         0,
		wantEnabled:      true,
		wantPeriodicDump: false,
		wantDumpNow:      true,
	}, {
		name:             "duration>0,interval>0",
		duration:         10 * time.Second,
		interval:         1 * time.Millisecond,
		wantEnabled:      true,
		wantPeriodicDump: true,
		wantDumpNow:      true,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ExecutionTracerDuration.Override(ctx, &st.SV, tc.duration)
			ExecutionTracerInterval.Override(ctx, &st.SV, tc.interval)

			testutils.SucceedsSoon(t, func() error {
				if fr.Enabled() != tc.wantEnabled {
					return errors.New("waiting for FR enabled state to match expected")
				}
				return nil
			})

			require.NoError(t, clearDir())

			if tc.wantPeriodicDump {
				testutils.SucceedsSoon(t, func() error {
					files, err := os.ReadDir(dir)
					if err != nil {
						return err
					}
					if len(files) == 0 {
						return errors.New("expected periodic dump files")
					}
					return nil
				})
			} else {
				files, err := os.ReadDir(dir)
				require.NoError(t, err)
				require.Empty(t, files, "expected no periodic dump files")
			}

			if tc.wantDumpNow {
				var filename string
				testutils.SucceedsSoon(t, func() error {
					f, err := fr.DumpNow(ctx, "test dump", "test")
					if err != nil {
						return err
					}
					if f == "" {
						return errors.New("dump skipped due to concurrent snapshot")
					}
					filename = f
					return nil
				})
				fi, err := os.Stat(filename)
				require.NoError(t, err)
				require.Greater(t, fi.Size(), int64(0))
			} else {
				_, err := fr.DumpNow(ctx, "should fail", "test")
				require.Error(t, err)
				require.Contains(t, err.Error(), "not enabled")
			}
		})
	}

	t.Run("disable_periodic_fr_stays_on", func(t *testing.T) {
		ExecutionTracerDuration.Override(ctx, &st.SV, 10*time.Second)
		ExecutionTracerInterval.Override(ctx, &st.SV, 1*time.Millisecond)
		testutils.SucceedsSoon(t, func() error {
			if !fr.Enabled() {
				return errors.New("expected FR to be enabled")
			}
			return nil
		})

		ExecutionTracerInterval.Override(ctx, &st.SV, 0)

		// Wait for the async loop to observe interval=0 and stop
		// dumping. Multiple consecutive empty observations confirm
		// periodic dumps have actually stopped.
		emptyChecks := 0
		testutils.SucceedsSoon(t, func() error {
			files, err := os.ReadDir(dir)
			if err != nil {
				return err
			}
			for _, f := range files {
				if err := os.Remove(filepath.Join(dir, f.Name())); err != nil && !oserror.IsNotExist(err) {
					return err
				}
			}
			if len(files) > 0 {
				emptyChecks = 0
				return errors.New("periodic dump files still appearing")
			}
			emptyChecks++
			if emptyChecks < 5 {
				return errors.New("waiting for stable empty directory")
			}
			return nil
		})

		require.True(t, fr.Enabled())

		var filename string
		testutils.SucceedsSoon(t, func() error {
			f, err := fr.DumpNow(ctx, "still works", "after_disable")
			if err != nil {
				return err
			}
			if f == "" {
				return errors.New("dump skipped due to concurrent snapshot")
			}
			filename = f
			return nil
		})
		require.NotEmpty(t, filename)
	})

	t.Run("change_duration_while_running", func(t *testing.T) {
		ExecutionTracerDuration.Override(ctx, &st.SV, 5*time.Second)
		ExecutionTracerInterval.Override(ctx, &st.SV, 0)
		testutils.SucceedsSoon(t, func() error {
			if !fr.Enabled() {
				return errors.New("expected FR to be enabled")
			}
			return nil
		})

		ExecutionTracerDuration.Override(ctx, &st.SV, 20*time.Second)
		testutils.SucceedsSoon(t, func() error {
			if !fr.Enabled() {
				return errors.New("expected FR to remain enabled after period change")
			}
			return nil
		})

		var filename string
		testutils.SucceedsSoon(t, func() error {
			f, err := fr.DumpNow(ctx, "after period change", "period_change")
			if err != nil {
				return err
			}
			if f == "" {
				return errors.New("dump skipped due to concurrent snapshot")
			}
			filename = f
			return nil
		})
		fi, err := os.Stat(filename)
		require.NoError(t, err)
		require.Greater(t, fi.Size(), int64(0))

		testutils.SucceedsSoon(t, func() error {
			fr.frMu.RLock()
			currentPeriod := fr.frMu.period
			fr.frMu.RUnlock()
			if currentPeriod != 20*time.Second {
				return errors.New("waiting for period to update to 20s")
			}
			return nil
		})
	})
}

func TestDumpNow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir := t.TempDir()
	st := cluster.MakeTestingClusterSettings()

	fr, err := NewFlightRecorder(st, 100*time.Millisecond, dir)
	require.NoError(t, err)

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	err = fr.Start(context.Background(), stopper)
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("fails when not enabled", func(t *testing.T) {
		_, err := fr.DumpNow(ctx, "test reason", "test_tag")
		require.Error(t, err)
		require.Contains(t, err.Error(), "not enabled")
	})

	ExecutionTracerDuration.Override(ctx, &st.SV, 10*time.Second)
	executionTracerOnDemandMinInterval.Override(ctx, &st.SV, 0)

	testutils.SucceedsSoon(t, func() error {
		if !fr.Enabled() {
			return errors.New("flight recorder is not enabled")
		}
		return nil
	})

	t.Run("produces a file with tag", func(t *testing.T) {
		filename, err := fr.DumpNow(ctx, "scheduling latency", "scheduling_latency")
		require.NoError(t, err)
		require.NotEmpty(t, filename)

		fi, err := os.Stat(filename)
		require.NoError(t, err)
		require.Greater(t, fi.Size(), int64(0))

		base := filepath.Base(filename)
		require.Contains(t, base, "scheduling_latency")
		require.True(t, fileMatchRegexp.MatchString(base))
	})

	t.Run("produces a file without tag", func(t *testing.T) {
		filename, err := fr.DumpNow(ctx, "generic dump", "")
		require.NoError(t, err)
		require.NotEmpty(t, filename)

		fi, err := os.Stat(filename)
		require.NoError(t, err)
		require.Greater(t, fi.Size(), int64(0))

		base := filepath.Base(filename)
		require.True(t, fileMatchRegexp.MatchString(base))
	})
}

func TestDumpNowRateLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir := t.TempDir()
	st := cluster.MakeTestingClusterSettings()

	fr, err := NewFlightRecorder(st, 100*time.Millisecond, dir)
	require.NoError(t, err)

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	err = fr.Start(context.Background(), stopper)
	require.NoError(t, err)

	ctx := context.Background()

	ExecutionTracerDuration.Override(ctx, &st.SV, 10*time.Second)
	executionTracerOnDemandMinInterval.Override(ctx, &st.SV, 10*time.Second)

	testutils.SucceedsSoon(t, func() error {
		if !fr.Enabled() {
			return errors.New("flight recorder is not enabled")
		}
		return nil
	})

	filename, err := fr.DumpNow(ctx, "first dump", "first")
	require.NoError(t, err)
	require.NotEmpty(t, filename)

	filename2, err := fr.DumpNow(ctx, "second dump", "second")
	require.NoError(t, err)
	require.Empty(t, filename2)
}

func TestDumpNowConcurrent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir := t.TempDir()
	st := cluster.MakeTestingClusterSettings()

	fr, err := NewFlightRecorder(st, 100*time.Millisecond, dir)
	require.NoError(t, err)

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	err = fr.Start(context.Background(), stopper)
	require.NoError(t, err)

	ctx := context.Background()

	ExecutionTracerDuration.Override(ctx, &st.SV, 10*time.Second)
	executionTracerOnDemandMinInterval.Override(ctx, &st.SV, 0)

	testutils.SucceedsSoon(t, func() error {
		if !fr.Enabled() {
			return errors.New("flight recorder is not enabled")
		}
		return nil
	})

	const numGoroutines = 5
	var wg sync.WaitGroup
	filenames := make([]string, numGoroutines)
	errs := make([]error, numGoroutines)

	wg.Add(numGoroutines)
	for i := range numGoroutines {
		go func(idx int) {
			defer wg.Done()
			filenames[idx], errs[idx] = fr.DumpNow(ctx, "concurrent dump", "concurrent_test")
		}(i)
	}
	wg.Wait()

	var gotFilename string
	for i := range numGoroutines {
		require.NoError(t, errs[i])
		if filenames[i] != "" {
			if gotFilename == "" {
				gotFilename = filenames[i]
			}
		}
	}
	require.NotEmpty(t, gotFilename, "expected at least one successful dump")

	fi, err := os.Stat(gotFilename)
	require.NoError(t, err)
	require.Greater(t, fi.Size(), int64(0))
}

func TestWriteTraceTo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir := t.TempDir()
	st := cluster.MakeTestingClusterSettings()

	fr, err := NewFlightRecorder(st, 100*time.Millisecond, dir)
	require.NoError(t, err)

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	err = fr.Start(context.Background(), stopper)
	require.NoError(t, err)

	ctx := context.Background()

	ExecutionTracerDuration.Override(ctx, &st.SV, 10*time.Second)

	testutils.SucceedsSoon(t, func() error {
		if !fr.Enabled() {
			return errors.New("flight recorder is not enabled")
		}
		return nil
	})

	var buf bytes.Buffer
	n, err := fr.WriteTraceTo(ctx, &buf)
	require.NoError(t, err)
	require.Greater(t, n, int64(0))
	require.Equal(t, n, int64(buf.Len()))
}

func TestTaggedFilenameMatchesGCRegex(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dir := t.TempDir()
	st := cluster.MakeTestingClusterSettings()

	fr, err := NewFlightRecorder(st, time.Second, dir)
	require.NoError(t, err)

	tags := []string{"", "scheduling_latency", "stmt_diag", "lease_expiry"}
	for _, tag := range tags {
		filename := fr.timestampedFilename(tag)
		base := filepath.Base(filename)
		require.True(t, fileMatchRegexp.MatchString(base),
			"filename %q does not match GC regex", base)
	}
}

func TestDumpNowInvalidTag(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir := t.TempDir()
	st := cluster.MakeTestingClusterSettings()

	fr, err := NewFlightRecorder(st, 100*time.Millisecond, dir)
	require.NoError(t, err)

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	err = fr.Start(context.Background(), stopper)
	require.NoError(t, err)

	ctx := context.Background()
	ExecutionTracerDuration.Override(ctx, &st.SV, 10*time.Second)
	executionTracerOnDemandMinInterval.Override(ctx, &st.SV, 0)

	testutils.SucceedsSoon(t, func() error {
		if !fr.Enabled() {
			return errors.New("flight recorder is not enabled")
		}
		return nil
	})

	tests := []struct {
		tag     string
		wantErr bool
	}{
		{"valid_tag", false},
		{"ValidTag123", false},
		{"", false},
		{"foo/bar", true},
		{"../etc/passwd", true},
		{"tag with spaces", true},
		{"tag;rm", true},
		{"foo\\bar", true},
	}

	for _, tc := range tests {
		t.Run("tag="+tc.tag, func(t *testing.T) {
			_, err := fr.DumpNow(ctx, "test", tc.tag)
			if tc.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "invalid tag")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestConcurrentDumpAndShutdown exercises the race between DumpNow (which
// calls WriteTo) and the Start loop shutting down the flight recorder. This
// test is primarily useful under the race detector (-race) to verify that the
// frMu locking correctly prevents data races between concurrent WriteTo calls
// and FR lifecycle changes.
func TestConcurrentDumpAndShutdown(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir := t.TempDir()
	st := cluster.MakeTestingClusterSettings()

	fr, err := NewFlightRecorder(st, 50*time.Millisecond, dir)
	require.NoError(t, err)

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	ctx := context.Background()
	err = fr.Start(ctx, stopper)
	require.NoError(t, err)

	executionTracerOnDemandMinInterval.Override(ctx, &st.SV, 0)
	ExecutionTracerDuration.Override(ctx, &st.SV, 10*time.Second)

	testutils.SucceedsSoon(t, func() error {
		if !fr.Enabled() {
			return errors.New("flight recorder is not enabled")
		}
		return nil
	})

	var wg sync.WaitGroup
	const iterations = 50

	wg.Add(2)
	go func() {
		defer wg.Done()
		for range iterations {
			_, _ = fr.DumpNow(ctx, "race test", "race")
		}
	}()
	go func() {
		defer wg.Done()
		var buf bytes.Buffer
		for range iterations {
			buf.Reset()
			if fr.Enabled() {
				_, _ = fr.WriteTraceTo(ctx, &buf)
			}
		}
	}()

	for range iterations {
		ExecutionTracerDuration.Override(ctx, &st.SV, 0)
		ExecutionTracerDuration.Override(ctx, &st.SV, 10*time.Second)
	}

	wg.Wait()
}

func TestValidTag(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		tag  string
		want bool
	}{
		{"", true},
		{"abc", true},
		{"ABC", true},
		{"abc123", true},
		{"foo_bar", true},
		{"foo/bar", false},
		{"foo..bar", false},
		{"foo bar", false},
		{"foo;bar", false},
		{"../etc", false},
	}
	for _, tc := range tests {
		t.Run("tag="+tc.tag, func(t *testing.T) {
			require.Equal(t, tc.want, validTag(tc.tag))
		})
	}
}
