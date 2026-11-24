// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusters

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	sclustertasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters/tasks"
	stasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/types"
)

// GetHandledTasks returns a map of task types to task implementations that are
// handled by this service.
func (s *Service) GetHandledTasks() map[string]stasks.ITask {
	return map[string]stasks.ITask{
		string(sclustertasks.ClustersTaskSync): &sclustertasks.TaskSync{
			Service: s,
		},
	}
}

// CreateTaskInstance creates a new instance of a task of the given type.
func (s *Service) CreateTaskInstance(taskType string) (tasks.ITask, error) {
	switch taskType {
	case string(sclustertasks.ClustersTaskSync):
		return &sclustertasks.TaskSync{
			Task:    tasks.Task{Type: taskType},
			Service: s,
		}, nil
	default:
		return nil, stasks.ErrUnknownTaskType
	}
}
