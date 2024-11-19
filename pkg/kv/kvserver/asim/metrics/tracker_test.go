// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metrics_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/scheduled"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
	"github.com/stretchr/testify/require"
)

type mockListener struct {
	history [][]metrics.StoreMetrics
}

func (ml *mockListener) Listen(ctx context.Context, sms []metrics.StoreMetrics) {
	ml.history = append(ml.history, sms)
}

// TestTracker asserts that the Tracker calls Listen on each registered
// listener with identical arguments.
func TestTracker(t *testing.T) {
	ctx := context.Background()
	settings := config.DefaultSimulationSettings()
	duration := 200 * time.Second
	rwg := []workload.Generator{
		workload.TestCreateWorkloadGenerator(settings.Seed, settings.StartTime, 10, 10000),
	}
	s := state.LoadConfig(state.ComplexConfig, state.SingleRangeConfig, settings)
	l1 := &mockListener{history: [][]metrics.StoreMetrics{}}
	l2 := &mockListener{history: [][]metrics.StoreMetrics{}}
	tracker := metrics.NewTracker(testingMetricsInterval, l1, l2)

	sim := asim.NewSimulator(duration, rwg, s, settings, tracker, scheduled.NewExecutorWithNoEvents())
	sim.RunSim(ctx)

	require.Equal(t, l1.history, l2.history)
}
