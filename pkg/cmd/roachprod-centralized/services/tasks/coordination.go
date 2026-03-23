// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"context"
	"log/slog"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// coordination.go contains helper methods used by other services to coordinate task scheduling.
// These methods prevent duplicate task creation and handle task waiting patterns.

// CreateTaskIfNotAlreadyPlanned creates a new task in the repository
// if one of the same type is not already planned (in pending state).
// This is primarily used for ad-hoc tasks triggered via API/controllers.
func (s *Service) CreateTaskIfNotAlreadyPlanned(
	ctx context.Context, l *logger.Logger, task tasks.ITask,
) (tasks.ITask, error) {
	// Create filters to check for existing pending tasks of the same type
	filters := filters.NewFilterSet().
		AddFilter("Type", filtertypes.OpEqual, task.GetType()).
		AddFilter("State", filtertypes.OpEqual, string(tasks.TaskStatePending))

	storedTasks, _, err := s.store.GetTasks(ctx, l, *filters)
	if err != nil {
		return nil, err
	}

	// If no task of the same type is already planned, create a new one
	if len(storedTasks) == 0 {
		return s.CreateTask(ctx, l, task)
	}

	// If a task of the same type is already planned, return it
	return storedTasks[0], nil
}

// CreateTaskIfNotRecentlyScheduled creates a task if no recent task of the same type exists.
// It checks for tasks (in any non-failed state) created within the specified recencyWindow.
// This prevents duplicate scheduled tasks when multiple instances are running periodic jobs.
// This is primarily used for periodic maintenance tasks.
func (s *Service) CreateTaskIfNotRecentlyScheduled(
	ctx context.Context, l *logger.Logger, task tasks.ITask, recencyWindow time.Duration,
) (tasks.ITask, error) {

	// We remove one second to the recencyWindow to avoid issues with sub-second precision
	// in database timestamp comparisons.
	fetchRecencyWindow := recencyWindow - time.Second
	if fetchRecencyWindow <= 0 {
		fetchRecencyWindow = time.Second
	}

	// Check for tasks of the same type created within the recency window
	// We exclude failed tasks to allow retry, but include pending/running/done
	taskFilters := filters.NewFilterSet().
		AddFilter("Type", filtertypes.OpEqual, task.GetType()).
		AddFilter("State", filtertypes.OpNotEqual, string(tasks.TaskStateFailed)).
		AddFilter("CreationDatetime", filtertypes.OpGreater, timeutil.Now().Add(-fetchRecencyWindow))

	recentTasks, _, err := s.store.GetTasks(ctx, l, *taskFilters)
	if err != nil {
		return nil, errors.Wrap(err, "failed to check for recent tasks")
	}

	// If a recent task exists, return it instead of creating a new one
	if len(recentTasks) > 0 {
		l.Debug("skipping task creation - recent task exists",
			slog.String("task_type", task.GetType()),
			slog.String("recency_window", recencyWindow.String()),
			slog.Int("recent_tasks_found", len(recentTasks)),
			slog.String(
				"most_recent_task_creation_time",
				recentTasks[len(recentTasks)-1].GetCreationDatetime().String(),
			),
		)
		return recentTasks[len(recentTasks)-1], nil
	}

	// No recent task found, create a new one
	l.Debug("creating new task - no recent task found",
		slog.String("task_type", task.GetType()),
		slog.String("recency_window", recencyWindow.String()),
	)
	return s.CreateTask(ctx, l, task)
}

// GetMostRecentCompletedTaskOfType returns the most recently completed task of the given type.
func (s *Service) GetMostRecentCompletedTaskOfType(
	ctx context.Context, l *logger.Logger, taskType string,
) (tasks.ITask, error) {
	return s.store.GetMostRecentCompletedTaskOfType(ctx, l, taskType)
}

// WaitForTaskCompletion blocks until the task reaches a final state (done/failed) or timeout.
func (s *Service) WaitForTaskCompletion(
	ctx context.Context, l *logger.Logger, taskID uuid.UUID, timeout time.Duration,
) error {
	deadline := timeutil.Now().Add(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if timeutil.Now().After(deadline) {
				return errors.Newf("timeout waiting for task %s to complete", taskID)
			}

			task, err := s.store.GetTask(ctx, l, taskID)
			if err != nil {
				return errors.Wrap(err, "failed to get task")
			}

			state := task.GetState()
			if state == tasks.TaskStateDone {
				l.Debug("task completed successfully", slog.String("task_id", taskID.String()))
				return nil
			}
			if state == tasks.TaskStateFailed {
				return errors.Newf("task %s failed", taskID)
			}
		}
	}
}
