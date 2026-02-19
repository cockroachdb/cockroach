// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package provisionings

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	ptasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/tasks"
	stasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/types"
)

// GetTaskServiceName returns the name of this service for task registration.
func (s *Service) GetTaskServiceName() string {
	return ptasks.TaskServiceName
}

// GetHandledTasks returns a map of task types to task implementations handled
// by this service.
func (s *Service) GetHandledTasks() map[string]stasks.ITask {
	return map[string]stasks.ITask{
		string(ptasks.ProvisioningsTaskProvision): &ptasks.TaskProvision{Service: s},
		string(ptasks.ProvisioningsTaskDestroy):   &ptasks.TaskDestroy{Service: s},
		string(ptasks.ProvisioningsTaskGC):        &ptasks.TaskGC{Service: s},
	}
}

// CreateTaskInstance creates a new instance of a task of the given type.
func (s *Service) CreateTaskInstance(taskType string) (tasks.ITask, error) {
	switch taskType {
	case string(ptasks.ProvisioningsTaskProvision):
		return &ptasks.TaskProvision{
			Task:    tasks.Task{Type: taskType},
			Service: s,
		}, nil
	case string(ptasks.ProvisioningsTaskDestroy):
		return &ptasks.TaskDestroy{
			Task:    tasks.Task{Type: taskType},
			Service: s,
		}, nil
	case string(ptasks.ProvisioningsTaskGC):
		return &ptasks.TaskGC{
			Task:    tasks.Task{Type: taskType},
			Service: s,
		}, nil
	default:
		return nil, stasks.ErrUnknownTaskType
	}
}
