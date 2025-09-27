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
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

var (
	// ErrTaskNotFound is returned when a task is not found.
	ErrTaskNotFound = fmt.Errorf("task not found")
)

type ITasksRepository interface {
	GetTasks(context.Context, InputGetTasksFilters) ([]tasks.ITask, error)
	GetTask(context.Context, uuid.UUID) (tasks.ITask, error)
	CreateTask(context.Context, tasks.ITask) error
	UpdateState(context.Context, uuid.UUID, tasks.TaskState) error
	GetStatistics(context.Context) (Statistics, error)
	PurgeTasks(context.Context, time.Duration, tasks.TaskState) (int, error)
	GetTasksForProcessing(context.Context, chan<- tasks.ITask, uuid.UUID) error
}

// TasksStatistics represents the statistics of the tasks
type Statistics map[tasks.TaskState]int

type InputGetTasksFilters struct {
	State tasks.TaskState
	Type  string
}
