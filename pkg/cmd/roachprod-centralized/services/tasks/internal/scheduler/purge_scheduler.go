// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scheduler

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/errors"
)

// purge_scheduler.go handles periodic scheduling of purge tasks to clean up old completed/failed tasks.

const (
	// DefaultPurgeDoneTaskOlderThan is the default duration after which tasks
	// in done state are purged from the repository.
	DefaultPurgeDoneTaskOlderThan = 2 * time.Hour
	// DefaultPurgeFailedTaskOlderThan is the default duration after which tasks
	// in failed state are purged from the repository.
	DefaultPurgeFailedTaskOlderThan = 24 * time.Hour
	// DefaultPurgeTasksInterval is the default value for how often tasks are
	// purged from the repository.
	DefaultPurgeTasksInterval = 10 * time.Minute
)

// PurgeScheduler defines the interface for scheduling purge tasks.
// This allows the service to provide task creation logic while keeping
// the scheduling logic isolated.
type PurgeScheduler interface {
	// SchedulePurgeTask creates a purge task if one hasn't been scheduled recently
	SchedulePurgeTask(ctx context.Context, l *logger.Logger, interval time.Duration) error
}

// StartPurgeScheduling periodically schedules tasks to purge old completed/failed tasks.
// This uses the task scheduling system to ensure only one instance performs purging,
// even when multiple instances are running.
// The onComplete callback is called when the goroutine exits (for WaitGroup.Done()).
func StartPurgeScheduling(
	ctx context.Context,
	l *logger.Logger,
	errChan chan<- error,
	purgeInterval time.Duration,
	scheduler PurgeScheduler,
	onComplete func(),
) error {
	l.Debug("Starting purge task scheduling routine")

	go func() {
		defer func() {
			l.Debug("Purge task scheduling routine stopped")
			if onComplete != nil {
				onComplete()
			}
		}()

		ticker := time.NewTicker(purgeInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				l.Debug("Stopping purge task scheduling routine")
				return

			case <-ticker.C:
				l.Debug("Scheduling purge task")
				err := scheduler.SchedulePurgeTask(ctx, l, purgeInterval)
				if err != nil {
					errChan <- errors.Wrap(err, "unable to schedule purge task")
				}
			}
		}
	}()

	return nil
}
