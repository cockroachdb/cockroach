// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	authtasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/tasks"
	stasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/types"
)

// GetHandledTasks returns a map of task types to task implementations that are
// handled by this service.
func (s *Service) GetHandledTasks() map[string]stasks.ITask {
	return map[string]stasks.ITask{
		string(authtasks.CleanupExpiredTokensTask): &authtasks.TaskExpiredTokensCleanup{
			Service: s,
		},
	}
}

// CreateTaskInstance creates a new instance of a task of the given type.
func (s *Service) CreateTaskInstance(taskType string) (tasks.ITask, error) {
	switch taskType {
	case string(authtasks.CleanupExpiredTokensTask):
		task := &authtasks.TaskExpiredTokensCleanup{Service: s}
		task.Type = taskType
		// Options will be deserialized from payload by the task service
		return task, nil
	default:
		return nil, stasks.ErrUnknownTaskType
	}
}
