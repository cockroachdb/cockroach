// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package processor

import (
	"context"
	"log/slog"
	"time"

	mtasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	tasksrepo "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// processor.go manages the task processing queue and worker coordination.

// StartProcessing begins the task processing routine with the specified number of workers.
// It starts worker goroutines that consume tasks from the repository and process them.
// Returns error if workers are disabled (count == 0).
func StartProcessing(
	ctx context.Context,
	l *logger.Logger,
	errChan chan<- error,
	workers int,
	instanceID string,
	repository tasksrepo.ITasksRepository,
	executor TaskExecutor,
) error {
	// Skip task processing if no workers are configured (API-only mode)
	if workers == 0 {
		l.Info("Task workers disabled (Workers=0), skipping task processing routine")
		return nil
	}

	taskRoutineLogger := l.With(
		slog.String("service", "tasks"),
		slog.String("routine", "processTask"),
	)

	taskRoutineLogger.Debug("Starting tasks processing routine")

	taskChan := make(chan mtasks.ITask)

	// Start the workers that handle the tasks
	for range workers {
		go func() {
			for {
				select {
				case <-ctx.Done():
					l.Debug("Stopping task processing routine")
					return

				case task := <-taskChan:
					// Log the received task with its ID, type, state, creation and update timestamps
					// but not the full payload because it is not human-readable yet (base64-encoded JSON).
					taskRoutineLogger.Debug(
						"Received task to process",
						slog.Any("task", struct {
							ID        uuid.UUID
							Type      string
							State     mtasks.TaskState
							CreatedAt time.Time
							UpdatedAt time.Time
						}{
							ID:        task.GetID(),
							Type:      task.GetType(),
							State:     task.GetState(),
							CreatedAt: task.GetCreationDatetime(),
							UpdatedAt: task.GetUpdateDatetime(),
						},
						),
					)

					// Track active workers
					if executor.GetMetricsEnabled() {
						executor.IncrementActiveWorkers()
					}

					err := ExecuteTask(ctx, taskRoutineLogger, task, executor)
					if err != nil {
						errChan <- errors.Wrap(err, "unable to process task")
					}

					// Decrement after task completes
					if executor.GetMetricsEnabled() {
						executor.DecrementActiveWorkers()
					}
				}
			}
		}()
	}

	// Get tasks for processing from repository
	go func() {
		defer func() {
			taskRoutineLogger.Debug("Tasks retrieval routine stopped")
		}()
		err := repository.GetTasksForProcessing(
			ctx,
			taskRoutineLogger,
			taskChan,
			instanceID,
		)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				// An error occurring while getting tasks for processing
				// is considered critical and will stop the service
				errChan <- utils.NewCriticalError(
					errors.Wrap(err, "unable to get tasks for processing"),
				)
			}
		}
	}()

	return nil
}
