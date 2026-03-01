// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"context"
	"log/slog"
	"time"

	mtasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/health/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
)

// HealthTaskType represents the type of health-related tasks.
type HealthTaskType string

const (
	// HealthTaskCleanup is the task type for cleaning up dead instances.
	HealthTaskCleanup HealthTaskType = types.TaskServiceName + "_cleanup"
)

// TaskCleanupOptions contains configuration for the cleanup task.
type TaskCleanupOptions struct {
	// InstanceTimeout is how long an instance can be unhealthy before being considered dead.
	InstanceTimeout time.Duration `json:"instance_timeout"`
	// CleanupRetention is how long to keep deleted instance records.
	CleanupRetention time.Duration `json:"cleanup_retention"`
}

// TaskCleanup represents a task that cleans up dead instances.
type TaskCleanup struct {
	mtasks.TaskWithOptions[TaskCleanupOptions]
	Service types.IHealthService
}

// NewTaskCleanup creates a new TaskCleanup instance with the given options.
func NewTaskCleanup(instanceTimeout, cleanupRetention time.Duration) (*TaskCleanup, error) {
	task := &TaskCleanup{}
	task.Type = string(HealthTaskCleanup)
	if err := task.SetOptions(TaskCleanupOptions{
		InstanceTimeout:  instanceTimeout,
		CleanupRetention: cleanupRetention,
	}); err != nil {
		return nil, err
	}
	return task, nil
}

// Process executes the cleanup task.
func (t *TaskCleanup) Process(ctx context.Context, l *logger.Logger) error {
	taskLogger := l.With(slog.String("task", "health_cleanup"))
	opts := t.GetOptions()
	taskLogger.Info("starting health cleanup task",
		slog.String("instance_timeout", opts.InstanceTimeout.String()),
		slog.String("cleanup_retention", opts.CleanupRetention.String()),
	)

	deletedCount, err := t.Service.CleanupDeadInstances(ctx, l, opts.InstanceTimeout, opts.CleanupRetention)
	if err != nil {
		taskLogger.Error("failed to cleanup dead instances", slog.Any("error", err))
		return err
	}

	if deletedCount > 0 {
		taskLogger.Info("cleaned up dead instances", slog.Int("count", deletedCount))
	} else {
		taskLogger.Debug("no dead instances to cleanup")
	}

	return nil
}

// GetTimeout returns the timeout for the cleanup task.
func (t *TaskCleanup) GetTimeout() time.Duration {
	return 1 * time.Minute
}
