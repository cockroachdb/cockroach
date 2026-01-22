// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

var (
	// ErrTaskNotFound is returned when a task is not found.
	ErrTaskNotFound = fmt.Errorf("task not found")
)

// ITasksRepository defines the interface for task data persistence operations.
// This repository handles CRUD operations for background tasks, manages task state transitions,
// and provides querying capabilities with filtering support for task management systems.
type ITasksRepository interface {
	// GetTasks retrieves multiple tasks based on the provided filter criteria.
	// Returns tasks, total count (for pagination), and error.
	GetTasks(context.Context, *logger.Logger, filtertypes.FilterSet) ([]tasks.ITask, int, error)
	// GetTask retrieves a single task by its unique identifier.
	GetTask(context.Context, *logger.Logger, uuid.UUID) (tasks.ITask, error)
	// CreateTask persists a new task to the repository.
	CreateTask(context.Context, *logger.Logger, tasks.ITask) error
	// UpdateState changes the state of an existing task (pending -> running -> done/failed).
	UpdateState(context.Context, *logger.Logger, uuid.UUID, tasks.TaskState) error
	// UpdateError sets the error message for a task.
	UpdateError(context.Context, *logger.Logger, uuid.UUID, string) error
	// GetStatistics returns aggregated counts of tasks grouped by their current state.
	GetStatistics(context.Context, *logger.Logger) (Statistics, error)
	// PurgeTasks removes tasks in the specified state that are older than the given duration.
	// Returns the number of tasks that were purged.
	PurgeTasks(context.Context, *logger.Logger, time.Duration, tasks.TaskState) (int, error)
	// GetTasksForProcessing retrieves pending tasks and sends them to the provided channel
	// for worker processing. Uses the instance ID for distributed coordination.
	GetTasksForProcessing(context.Context, *logger.Logger, chan<- tasks.ITask, string) error
	// GetMostRecentCompletedTaskOfType returns the most recently completed task of the given type.
	// Returns nil if no completed task of that type exists.
	GetMostRecentCompletedTaskOfType(context.Context, *logger.Logger, string) (tasks.ITask, error)
}

// Statistics represents aggregated task counts grouped by task state and task type.
// The outer map key is the task state, the inner map key is the task type,
// and the value is the count of tasks matching both state and type.
// This provides detailed visibility into the task system's workload composition.
// Example: Statistics[TaskStatePending]["cluster_sync"] = 10
type Statistics map[tasks.TaskState]map[string]int
