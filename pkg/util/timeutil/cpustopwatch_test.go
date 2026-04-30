// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package timeutil_test

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestCPUStopWatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("stop without start", func(t *testing.T) {
		// Stop without Start should return 0.
		var w timeutil.CPUStopWatch
		require.Zero(t, w.Stop())
	})

	if !grunning.Supported {
		skip.IgnoreLint(t, "grunning not supported on this platform")
	}

	// burnCPU does enough work to register a nonzero grunning delta on most
	// platforms.
	burnCPU := func() {
		sum := 0
		for i := range 100000 {
			sum += i
		}
		_ = sum
	}

	t.Run("single cycle", func(t *testing.T) {
		var w timeutil.CPUStopWatch
		w.Start()
		burnCPU()
		delta := w.Stop()
		require.Greater(t, delta, time.Duration(0))
	})

	t.Run("multiple cycles", func(t *testing.T) {
		// Sum deltas across multiple Start/Stop cycles.
		var w timeutil.CPUStopWatch
		var total time.Duration
		for range 3 {
			w.Start()
			burnCPU()
			total += w.Stop()
		}
		require.Greater(t, total, time.Duration(0),
			"expected nonzero accumulated CPU after three cycles")
	})

	t.Run("idempotent stop", func(t *testing.T) {
		// Second Stop should return 0.
		var w timeutil.CPUStopWatch
		w.Start()
		burnCPU()
		first := w.Stop()
		require.Greater(t, first, time.Duration(0))

		second := w.Stop()
		require.Zero(t, second, "second Stop should return 0")
	})

	t.Run("restart after stop", func(t *testing.T) {
		// A new Start/Stop cycle after a previous one should produce a
		// nonzero value.
		var w timeutil.CPUStopWatch
		w.Start()
		burnCPU()
		_ = w.Stop()

		w.Start()
		burnCPU()
		delta := w.Stop()
		require.Greater(t, delta, time.Duration(0),
			"restart should produce nonzero delta")
	})
}
