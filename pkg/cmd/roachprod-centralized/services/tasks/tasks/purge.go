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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
)

// TasksTaskType represents the type of tasks-related tasks.
type TasksTaskType string

const (
	// TasksPurge is the task type for purging old completed/failed tasks.
	TasksPurge TasksTaskType = types.TaskServiceName + "_purge"
)

// ITasksService defines the interface that task purge needs from the tasks service.
type ITasksService interface {
	PurgeTasks(ctx context.Context, l *logger.Logger) (int, int, error)
}

// TaskPurge represents a task that purges old completed and failed tasks.
type TaskPurge struct {
	tasks.Task
	Service ITasksService
}

// NewTaskPurge creates a new TaskPurge instance.
func NewTaskPurge() *TaskPurge {
	return &TaskPurge{
		Task: tasks.Task{
			Type: string(TasksPurge),
		},
	}
}

// Process executes the purge task.
func (t *TaskPurge) Process(ctx context.Context, l *logger.Logger) error {
	taskLogger := l.With(slog.String("task", "tasks_purge"))
	taskLogger.Info("starting task purge")

	purgedDone, purgedFailed, err := t.Service.PurgeTasks(ctx, l)
	if err != nil {
		taskLogger.Error("failed to purge tasks", slog.Any("error", err))
		return err
	}

	if purgedDone > 0 || purgedFailed > 0 {
		taskLogger.Info("purged tasks from database",
			slog.Int("state_done", purgedDone),
			slog.Int("state_failed", purgedFailed))
	} else {
		taskLogger.Debug("no old tasks to purge")
	}

	return nil
}

// GetTimeout returns the timeout for the purge task.
func (t *TaskPurge) GetTimeout() time.Duration {
	return 1 * time.Minute
}
