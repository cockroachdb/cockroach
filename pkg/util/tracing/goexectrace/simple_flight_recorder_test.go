// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package goexectrace

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

func TestSimpleFlightRecorder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir := t.TempDir()
	st := cluster.MakeTestingClusterSettings()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	fr, err := NewFlightRecorder(st, 1*time.Second, dir)
	require.NoError(t, err)
	err = fr.Start(context.Background(), stopper)
	require.NoError(t, err)

	// Tempdir is empty.
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Equal(t, len(files), 0)

	ExecutionTracerInterval.Override(context.Background(), &st.SV, 1*time.Millisecond)

	t.Run("writes a file when enabled", func(t *testing.T) {
		testutils.SucceedsSoon(t, func() error {
			if !fr.fr.Enabled() {
				return errors.New("flight recorder is not enabled")
			}
			files, err := os.ReadDir(dir)
			require.NoError(t, err)
			require.Greater(t, len(files), 0)
			require.Regexp(t, fileMatchRegexp, files[0].Name())
			return nil
		})
	})

	t.Run("stops the flight recorder when disabled", func(t *testing.T) {
		ExecutionTracerInterval.Override(context.Background(), &st.SV, 0)
		testutils.SucceedsSoon(t, func() error {
			if fr.fr.Enabled() {
				return errors.New("flight recorder is still enabled")
			}
			return nil
		})
	})
}
