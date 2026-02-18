// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package processor

import (
	"context"
	"log/slog"

	mtasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// executor.go handles the execution of individual tasks with error handling and state transitions.

// TaskExecutor defines the interface for task execution operations.
// This interface allows the parent service to provide necessary methods
// while keeping the executor logic self-contained.
type TaskExecutor interface {
	// HydrateTask converts a repository task to its concrete type
	HydrateTask(base mtasks.ITask) (types.ITask, error)
	// MarkTaskAs updates the task state in the repository
	MarkTaskAs(ctx context.Context, l *logger.Logger, id uuid.UUID, status mtasks.TaskState) error
	// UpdateError stores an error message for a task
	UpdateError(ctx context.Context, l *logger.Logger, id uuid.UUID, errMsg string) error
	// GetManagedTask checks if a task type is managed
	GetManagedTask(taskType string) types.ITask
	// GetMetricsEnabled returns whether metrics collection is enabled
	GetMetricsEnabled() bool
	// RecordTaskCompletion records task completion status and duration by type
	RecordTaskCompletion(taskType string, success bool, duration float64)
	// IncrementActiveWorkers increments the active workers counter
	IncrementActiveWorkers()
	// DecrementActiveWorkers decrements the active workers counter
	DecrementActiveWorkers()
	// RecordQueueAge records the time a task spent waiting in the queue
	RecordQueueAge(taskType string, ageSeconds float64)
	// IncrementTimeouts increments the timeout counter for a task type
	IncrementTimeouts(taskType string)
	// GetDefaultTimeout returns the default task timeout
	GetDefaultTimeout() types.TimeoutGetter
	// NewLogSink creates a log sink for the given task.
	// Returns nil if log streaming is not configured.
	NewLogSink(taskID uuid.UUID) logger.LogSink
}

// ExecuteTask processes a task and updates its status in the repository.
// This is the main entry point for task execution.
func ExecuteTask(
	ctx context.Context, l *logger.Logger, baseTask mtasks.ITask, executor TaskExecutor,
) error {
	taskLogger := l.With(
		slog.String("routine", "processTask"),
		slog.String("task_id", baseTask.GetID().String()),
		slog.String("task_type", baseTask.GetType()),
	)

	// Hydrate base task to concrete type with service references
	hydratedTask, err := executor.HydrateTask(baseTask)
	if err != nil {
		taskLogger.Error(
			"Failed to hydrate task",
			slog.Any("task_id", baseTask.GetID()),
			slog.Any("error", err),
		)
		// Store error and mark as failed
		errMsg := err.Error()
		if errUpdateError := executor.UpdateError(ctx, l, baseTask.GetID(), errMsg); errUpdateError != nil {
			l.Error(
				"Failed to update task error message",
				slog.Any("task_id", baseTask.GetID()),
				slog.Any("error", errUpdateError),
			)
		}
		if errFailedStatus := executor.MarkTaskAs(ctx, l, baseTask.GetID(), mtasks.TaskStateFailed); errFailedStatus != nil {
			l.Error(
				"Failed to update task status",
				slog.Any("task_id", baseTask.GetID()),
				slog.String("status", string(mtasks.TaskStateFailed)),
				slog.Any("error", errFailedStatus),
			)
		}
		return err
	}

	// Create log sink for this task execution.
	sink := executor.NewLogSink(hydratedTask.GetID())
	if sink != nil {
		taskLogger = taskLogger.WithSink(sink)
		defer func() {
			if closeErr := sink.Close(); closeErr != nil {
				l.Warn("failed to close log sink",
					slog.String("task_id", hydratedTask.GetID().String()),
					slog.Any("error", closeErr),
				)
			}
		}()
	}

	// Log the hydrated task with deserialized options (not base64 payload)
	taskLogger.LogAttrs(ctx, slog.LevelDebug, "Processing task", hydratedTask.AsLogAttributes()...)

	// Start timing for metrics (measure total execution time including state updates)
	var errTaskProcess error
	start := timeutil.Now()
	defer func() {
		duration := timeutil.Since(start).Seconds()
		// Record metrics for both success and failure cases
		if executor.GetMetricsEnabled() {
			executor.RecordTaskCompletion(hydratedTask.GetType(), errTaskProcess == nil, duration)
		}
	}()

	errStatus := executor.MarkTaskAs(ctx, l, hydratedTask.GetID(), mtasks.TaskStateRunning)
	if errStatus != nil {
		l.Error(
			"Failed to update task status",
			slog.Any("task_id", hydratedTask.GetID()),
			slog.String("status", string(mtasks.TaskStateRunning)),
			slog.Any("error", errStatus),
		)
	}

	// Record queue age (time from task creation to processing start)
	if executor.GetMetricsEnabled() {
		queueAge := timeutil.Since(hydratedTask.GetCreationDatetime()).Seconds()
		executor.RecordQueueAge(hydratedTask.GetType(), queueAge)
	}

	errTaskProcess = executeTaskWithTimeout(ctx, taskLogger, hydratedTask, executor)
	if errTaskProcess != nil {
		taskLogger.Error(
			"Unable to process task",
			slog.Any("task_id", hydratedTask.GetID()),
			slog.Any("error", errTaskProcess),
		)

		// Store the error message in the database
		errMsg := errTaskProcess.Error()
		if errUpdateError := executor.UpdateError(ctx, l, hydratedTask.GetID(), errMsg); errUpdateError != nil {
			l.Error(
				"Failed to update task error message",
				slog.Any("task_id", hydratedTask.GetID()),
				slog.Any("error", errUpdateError),
			)
		}

		errFailedStatus := executor.MarkTaskAs(ctx, l, hydratedTask.GetID(), mtasks.TaskStateFailed)
		if errFailedStatus != nil {
			l.Error(
				"Failed to update task status",
				slog.Any("task_id", hydratedTask.GetID()),
				slog.String("status", string(mtasks.TaskStateFailed)),
				slog.Any("error", errFailedStatus),
			)
		}
		return errTaskProcess
	}

	taskLogger.Info(
		"Task processed successfully",
		slog.Any("task_id", hydratedTask.GetID()),
	)

	errDoneStatus := executor.MarkTaskAs(ctx, l, hydratedTask.GetID(), mtasks.TaskStateDone)
	if errDoneStatus != nil {
		l.Error(
			"Failed to update task status",
			slog.Any("task_id", hydratedTask.GetID()),
			slog.String("status", string(mtasks.TaskStateDone)),
			slog.Any("error", errDoneStatus),
		)
	}

	return nil
}

// executeTaskWithTimeout processes a task and handles timeouts.
// Expects a fully hydrated task (types.ITask) with service references and deserialized options.
// Since types.ITask embeds tasks.ITask, the task has both data methods and Process().
func executeTaskWithTimeout(
	ctx context.Context, l *logger.Logger, task types.ITask, executor TaskExecutor,
) error {
	tType := task.GetType()

	if executor.GetManagedTask(tType) == nil {
		return types.ErrTaskTypeNotManaged
	}

	taskTimeout := executor.GetDefaultTimeout().GetTimeout()
	if t, ok := task.(types.ITaskWithTimeout); ok {
		taskTimeout = t.GetTimeout()
	}

	// Create a new cancellable context for the task
	taskCtx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	resultCh := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				var panicErr error
				switch val := r.(type) {
				case error:
					panicErr = errors.Wrap(val, "task panicked")
				default:
					panicErr = errors.Newf("task panicked: %v", val)
				}
				resultCh <- panicErr
			}
		}()
		resultCh <- task.Process(taskCtx, l)
	}()

	var taskErr error
	deadlineExceeded := false

	select {
	case taskErr = <-resultCh:
	case <-taskCtx.Done():
		deadlineExceeded = errors.Is(taskCtx.Err(), context.DeadlineExceeded)
		taskErr = <-resultCh
	}

	// If the context deadline fired, ensure we surface a timeout even if the task
	// returned nil or a different error after finishing its work.
	if deadlineExceeded || errors.Is(taskCtx.Err(), context.DeadlineExceeded) {
		if executor.GetMetricsEnabled() {
			executor.IncrementTimeouts(tType)
		}
		l.Error(
			"Task processing timed out",
			slog.Any("task_id", task.GetID()),
			slog.String("task_type", tType),
		)

		// Preserve additional context if the task surfaced its own error.
		if taskErr != nil && !errors.Is(taskErr, context.DeadlineExceeded) && !errors.Is(taskErr, types.ErrTaskTimeout) {
			return errors.CombineErrors(types.ErrTaskTimeout, taskErr)
		}
		return types.ErrTaskTimeout
	}

	// Propagate cancellation from the parent context when it was not a timeout.
	if ctxErr := ctx.Err(); ctxErr != nil {
		if taskErr != nil {
			return taskErr
		}
		return ctxErr
	}

	return taskErr
}
