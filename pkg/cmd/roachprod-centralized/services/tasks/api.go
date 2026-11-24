// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"context"

	pkgauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	tasksrepo "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// api.go contains the public CRUD API methods for managing tasks.
// These methods are exposed via the types.IService interface and used by controllers.

// GetTasks returns all tasks from the repository with total count for pagination.
func (s *Service) GetTasks(
	ctx context.Context, l *logger.Logger, _ *pkgauth.Principal, input types.InputGetAllTasksDTO,
) ([]tasks.ITask, int, error) {
	// Validate filters if present
	if !input.Filters.IsEmpty() {
		if err := input.Filters.Validate(); err != nil {
			return nil, 0, errors.Wrap(err, "invalid filters")
		}
	}

	tasks, totalCount, err := s.store.GetTasks(ctx, l, input.Filters)
	if err != nil {
		return nil, 0, err
	}

	return tasks, totalCount, nil
}

// GetTask returns a task from the repository.
func (s *Service) GetTask(
	ctx context.Context, l *logger.Logger, _ *pkgauth.Principal, input types.InputGetTaskDTO,
) (tasks.ITask, error) {
	task, err := s.store.GetTask(ctx, l, input.ID)
	if err != nil {
		if errors.Is(err, tasksrepo.ErrTaskNotFound) {
			return nil, types.ErrTaskNotFound
		}
		return nil, err
	}

	return task, nil
}

// CreateTask creates a new task in the repository.
func (s *Service) CreateTask(
	ctx context.Context, l *logger.Logger, input tasks.ITask,
) (tasks.ITask, error) {
	newTask := input

	// Generate an ID
	newTask.SetID(uuid.MakeV4())

	// Set creation and update datetime
	ctime := timeutil.Now()
	newTask.SetCreationDatetime(ctime)
	newTask.SetUpdateDatetime(ctime)

	// Set state
	newTask.SetState(tasks.TaskStatePending)

	// Save the task
	err := s.store.CreateTask(ctx, l, newTask)
	if err != nil {
		return nil, err
	}

	return newTask, nil
}
