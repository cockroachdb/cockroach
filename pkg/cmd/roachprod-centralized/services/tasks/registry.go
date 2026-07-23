// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/types"
	"github.com/cockroachdb/errors"
)

// registry.go contains the task registration system that allows services to register
// their task types and enables the task processor to hydrate tasks with proper service references.

// RegisterTasksService registers a tasks service and the tasks it handles.
func (s *Service) RegisterTasksService(tasksService types.ITasksService) {
	// Register the tasks managed by the service
	s.managedPkgs[tasksService.GetTaskServiceName()] = true
	for taskName, task := range tasksService.GetHandledTasks() {
		s.managedTasks[taskName] = task
		s.taskServices[taskName] = tasksService
		// Update pre-computed task types slice for metrics
		s.managedTaskTypes = append(s.managedTaskTypes, taskName)
		// Track the highest timeout across all registered tasks
		if t, ok := task.(types.ITaskWithTimeout); ok {
			if timeout := t.GetTimeout(); timeout > s.maxTaskTimeout {
				s.maxTaskTimeout = timeout
			}
		}
	}
}

// hydrateTask upgrades a base task to its concrete type with service references.
// Repository returns base tasks.Task instances; this method creates the proper
// concrete type (e.g., TaskSync, TaskPurge) with service dependencies injected.
// Returns (nil, error) if task type is unknown or hydration fails.
func (s *Service) hydrateTask(base tasks.ITask) (types.ITask, error) {
	taskType := base.GetType()

	// Check if we have a service that handles this task type
	taskService, exists := s.taskServices[taskType]
	if !exists {
		return nil, types.ErrUnknownTaskType
	}

	// Create concrete task instance (with service references)
	concreteTask, err := taskService.CreateTaskInstance(taskType)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create instance for task type %s", taskType)
	}

	// Copy all fields from base task to concrete task
	concreteTask.SetID(base.GetID())
	concreteTask.SetState(base.GetState())
	concreteTask.SetCreationDatetime(base.GetCreationDatetime())
	concreteTask.SetUpdateDatetime(base.GetUpdateDatetime())
	concreteTask.SetError(base.GetError())

	// Deserialize payload into concrete type's options
	if len(base.GetPayload()) > 0 {
		if err := concreteTask.SetPayload(base.GetPayload()); err != nil {
			return nil, errors.Wrapf(err, "failed to deserialize payload for task %s", base.GetID())
		}
	}

	// Type-assert to types.ITask (which has Process method)
	processableTask, ok := concreteTask.(types.ITask)
	if !ok {
		return nil, errors.Newf("task type %s does not implement Process method", taskType)
	}

	return processableTask, nil
}
