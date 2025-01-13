// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusters

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	stasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
)

// CustersTaskType is the type of the task.
type CustersTaskType string

const (
	// ClustersTaskSync is the task type for synchronizing the clusters.
	ClustersTaskSync CustersTaskType = "clusters_sync"
)

// GetTaskServiceName returns the name of the task service.
func (s *Service) GetTaskServiceName() string {
	return "clusters"
}

// GetHandledTasks returns a map of task types to task implementations that are
// handled by this service.
func (s *Service) GetHandledTasks() map[string]stasks.ITask {
	return map[string]stasks.ITask{
		string(ClustersTaskSync): &TaskSync{
			service: s,
		},
	}
}

// TaskSync is the task that synchronizes the clusters.
type TaskSync struct {
	tasks.Task
	service *Service
}

// Process will synchronize the clusters.
func (t *TaskSync) Process(ctx context.Context, l *utils.Logger, resChan chan<- error) {
	_, err := t.service.sync(ctx, l)
	if err != nil {
		resChan <- err
		return
	}
	resChan <- nil
}
