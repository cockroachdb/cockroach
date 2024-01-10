// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logmetrics

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestIncrementCounter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	l := newLogMetricsRegistry()
	metrics := l.counters
	for _, m := range metrics {
		require.Zero(t, m.Count())
	}
	for i := range metrics {
		l.IncrementCounter(log.Metric(i), 1)
		l.IncrementCounter(log.Metric(i), 2)
	}
	for _, m := range l.counters {
		require.Equal(t, int64(3), m.Count())
	}
}

func TestNewRegistry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("panics when logMetricsReg is nil", func(t *testing.T) {
		logMetricsReg = nil
		require.PanicsWithErrorf(t,
			"logMetricsRegistry was not initialized",
			func() {
				_ = NewRegistry()
			}, "expected NewRegistry() to panic with nil logMetricsReg package-level var")
	})
}
