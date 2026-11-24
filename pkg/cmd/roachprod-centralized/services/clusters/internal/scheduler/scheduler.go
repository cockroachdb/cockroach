// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/errors"
)

// PeriodicRefreshScheduler defines the interface for scheduling periodic refresh tasks.
// This allows the service to provide task creation logic while keeping
// the scheduling logic isolated.
type PeriodicRefreshScheduler interface {
	// ScheduleSyncTaskIfNeeded creates a sync task if one hasn't been scheduled recently
	ScheduleSyncTaskIfNeeded(ctx context.Context, l *logger.Logger) (tasks.ITask, error)
}

// StartPeriodicRefresh starts a background goroutine that periodically schedules sync tasks.
// This uses the task scheduling system to ensure only one instance performs syncing,
// even when multiple instances are running.
// The onComplete callback is called when the goroutine exits (for WaitGroup.Done()).
func StartPeriodicRefresh(
	ctx context.Context,
	l *logger.Logger,
	errChan chan<- error,
	refreshInterval time.Duration,
	scheduler PeriodicRefreshScheduler,
	onComplete func(),
) {
	refreshLogger := l.With(
		slog.String("service", "clusters"),
		slog.String("routine", "periodicRefresh"),
	)

	refreshLogger.Info(
		"starting periodic refresh routine",
		slog.String("interval", refreshInterval.String()),
	)

	go func() {
		defer func() {
			refreshLogger.Debug("Periodic refresh routine stopped")
			if onComplete != nil {
				onComplete()
			}
		}()

		ticker := time.NewTicker(refreshInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				refreshLogger.Debug("Stopping periodic refresh routine")
				return

			case <-ticker.C:
				refreshLogger.Debug("Periodic refresh routine triggered")

				// Create sync task using helper that prevents duplicates across instances
				task, err := scheduler.ScheduleSyncTaskIfNeeded(ctx, refreshLogger)
				if err != nil {
					errChan <- errors.Wrap(err, "error in periodic refresh routine")
					continue
				}

				if task != nil {
					refreshLogger.Info(
						"Task created from periodic refresh routine",
						slog.String("task_id", task.GetID().String()),
					)
				}
			}
		}
	}()
}
