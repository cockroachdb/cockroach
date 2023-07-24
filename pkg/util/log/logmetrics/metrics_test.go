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
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/stretchr/testify/require"
)

func TestIncrementCounter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("panics when log.MetricName not registered", func(t *testing.T) {
		l := &LogMetricsRegistry{}
		l.mu.counters = map[log.MetricName]*metric.Counter{}
		require.PanicsWithErrorf(t,
			`MetricName not registered in LogMetricsRegistry: "unregistered"`,
			func() {
				l.IncrementCounter("unregistered", 1)
			}, "expected IncrementCounter to panic for unregistered metric")
	})

	t.Run("increments counter", func(t *testing.T) {
		l := newLogMetricsRegistry()
		func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			require.Zero(t, l.mu.metricsStruct.FluentSinkConnErrors.Count())
		}()
		l.IncrementCounter(log.FluentSinkConnectionError, 1)
		l.IncrementCounter(log.FluentSinkConnectionError, 2)
		func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			require.Equal(t, int64(3), l.mu.metricsStruct.FluentSinkConnErrors.Count())
		}()
	})
}

func TestNewRegistry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("panics when logMetricsReg is nil", func(t *testing.T) {
		logMetricsReg = nil
		require.PanicsWithErrorf(t,
			"LogMetricsRegistry was not initialized",
			func() {
				_ = NewRegistry()
			}, "expected NewRegistry() to panic with nil logMetricsReg package-level var")
	})
}

type fakeLogMetrics struct{}

func (*fakeLogMetrics) IncrementCounter(_ log.MetricName, _ int64) {}

var _ log.LogMetrics = (*fakeLogMetrics)(nil)
