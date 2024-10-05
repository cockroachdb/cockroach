// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

	l := newLogMetricsRegistry()

	metrics := map[log.Metric]*metric.Counter{
		log.FluentSinkConnectionAttempt: l.metricsStruct.FluentSinkConnAttempts,
		log.FluentSinkConnectionError:   l.metricsStruct.FluentSinkConnErrors,
		log.FluentSinkWriteAttempt:      l.metricsStruct.FluentSinkWriteAttempts,
		log.FluentSinkWriteError:        l.metricsStruct.FluentSinkWriteErrors,
		log.BufferedSinkMessagesDropped: l.metricsStruct.BufferedSinkMessagesDropped,
		log.LogMessageCount:             l.metricsStruct.LogMessageCount,
	}
	func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		for _, m := range metrics {
			require.Zero(t, m.Count())
		}
	}()
	for m := range metrics {
		l.IncrementCounter(m, 1)
		l.IncrementCounter(m, 2)
	}
	func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		for _, m := range metrics {
			require.Equal(t, int64(3), m.Count())
		}
	}()
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

func (*fakeLogMetrics) IncrementCounter(_ log.Metric, _ int64) {}

var _ log.LogMetrics = (*fakeLogMetrics)(nil)
