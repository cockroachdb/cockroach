// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metrics

import (
	"context"
	"time"

	mtasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	tasksrepo "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/errors"
)

// collector.go handles periodic collection and updating of Prometheus metrics for the tasks service.

const (
	// DefaultStatisticsUpdateInterval is the default value for how often the
	// tasks statistics are updated.
	DefaultStatisticsUpdateInterval = 10 * time.Second
)

// MetricsCollector defines the interface for updating metrics.
// This allows the service to provide metrics update logic while keeping
// the collection scheduling isolated.
type MetricsCollector interface {
	// UpdateMetrics fetches statistics from the repository and updates Prometheus gauges
	UpdateMetrics(ctx context.Context, l *logger.Logger) error
}

// StartMetricsCollection routinely updates the metrics with the tasks statistics.
// The onComplete callback is called when the goroutine exits (for WaitGroup.Done()).
func StartMetricsCollection(
	ctx context.Context,
	l *logger.Logger,
	errChan chan<- error,
	updateInterval time.Duration,
	collector MetricsCollector,
	onComplete func(),
) error {
	l.Debug("Starting tasks statistics routine")

	go func() {
		defer func() {
			l.Debug("Tasks statistics routine stopped")
			if onComplete != nil {
				onComplete()
			}
		}()

		tickerStats := time.NewTicker(updateInterval)
		defer tickerStats.Stop()

		for {
			select {
			case <-ctx.Done():
				l.Debug("Stopping tasks statistics routine")
				return

			case <-tickerStats.C:
				l.Debug("Getting tasks statistics from the repository")
				err := collector.UpdateMetrics(ctx, l)
				if err != nil {
					errChan <- errors.Wrap(err, "unable to get tasks statistics")
				}
			}
		}
	}()

	return nil
}

// UpdateTaskMetrics updates the Prometheus metrics with the tasks statistics.
// This is a helper function that can be called by the service implementing MetricsCollector.
func UpdateTaskMetrics(
	ctx context.Context,
	l *logger.Logger,
	repository tasksrepo.ITasksRepository,
	metricsGauges MetricsGauges,
	knownTaskTypes []string,
) error {
	stats, err := repository.GetStatistics(ctx, l)
	if err != nil {
		return errors.Wrap(err, "unable to get tasks statistics")
	}

	// All possible task states
	allStates := []mtasks.TaskState{
		mtasks.TaskStatePending,
		mtasks.TaskStateRunning,
		mtasks.TaskStateDone,
		mtasks.TaskStateFailed,
	}

	// First, set ALL combinations to 0 (prevents stale data)
	for _, state := range allStates {
		for _, taskType := range knownTaskTypes {
			metricsGauges.SetTaskCount(state, taskType, 0)
		}
	}

	// Then update with actual values from DB
	for state, taskTypes := range stats {
		for taskType, count := range taskTypes {
			metricsGauges.SetTaskCount(state, taskType, float64(count))
		}
	}

	return nil
}

// MetricsGauges defines the interface for Prometheus gauges.
type MetricsGauges interface {
	// SetTaskCount sets the count for a specific state and task type
	SetTaskCount(state mtasks.TaskState, taskType string, count float64)
}
