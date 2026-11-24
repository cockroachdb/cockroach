// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package goexectrace

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	rttrace "runtime/trace"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
	exptrace "golang.org/x/exp/trace"
)

func TestSimpleFlightRecorder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir := t.TempDir()
	st := cluster.MakeTestingClusterSettings()

	// The logMetadata callback emits a runtime/trace.Log event with a known
	// category. We verify below that this event actually appears in the
	// flight recorder's snapshot, confirming that the flight recorder
	// captures runtime/trace.Log events.
	const metaCategory = "crdb.exectrace.meta.test"
	const metaMessage = "test-metadata-payload"
	logMetadata := func(ctx context.Context, _ string) {
		rttrace.Log(ctx, metaCategory, metaMessage)
	}

	fr, err := NewFlightRecorder(st, 1*time.Second, dir, logMetadata)
	require.NoError(t, err)

	stopper := stop.NewStopper()
	defer func() {
		<-stopper.IsStopped()
		if fr.enabledForTests() {
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

	ExecutionTracerInterval.Override(context.Background(), &st.SV, 1*time.Millisecond)

	t.Run("writes a file when enabled", func(t *testing.T) {
		testutils.SucceedsSoon(t, func() error {
			if !fr.enabledForTests() {
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

	t.Run("metadata log event appears in trace snapshot", func(t *testing.T) {
		// Parse trace files and verify that the runtime/trace.Log event
		// emitted by logMetadata is present. We retry because the most
		// recent file may have been created (os.Create) but not yet
		// written to (WriteTo) by the flight recorder goroutine.
		testutils.SucceedsSoon(t, func() error {
			files, err := os.ReadDir(dir)
			if err != nil {
				return err
			}
			if len(files) == 0 {
				return errors.New("no trace files found")
			}

			// Use the last file. os.ReadDir returns entries sorted by name,
			// and filenames contain timestamps, so the last entry is the
			// most recent.
			latestFile := filepath.Join(dir, files[len(files)-1].Name())
			f, err := os.Open(latestFile)
			if err != nil {
				return err
			}
			defer f.Close()

			reader, err := exptrace.NewReader(f)
			if err != nil {
				return err
			}

			for {
				ev, err := reader.ReadEvent()
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				if ev.Kind() == exptrace.EventLog {
					logEv := ev.Log()
					if logEv.Category == metaCategory && logEv.Message == metaMessage {
						return nil
					}
				}
			}
			return fmt.Errorf(
				"no trace.Log event with category=%q found in the flight recorder snapshot %s",
				metaCategory, latestFile,
			)
		})
	})

	t.Run("stops the flight recorder when disabled", func(t *testing.T) {
		ExecutionTracerInterval.Override(context.Background(), &st.SV, 0)
		testutils.SucceedsSoon(t, func() error {
			if fr.enabledForTests() {
				return errors.New("flight recorder is still enabled")
			}
			return nil
		})
	})

	// Restart so we can test that it's stopped with the stopper.
	ExecutionTracerInterval.Override(context.Background(), &st.SV, 10*time.Second)
	testutils.SucceedsSoon(t, func() error {
		if !fr.enabledForTests() {
			return errors.New("flight recorder is not enabled")
		}
		return nil
	})
}
