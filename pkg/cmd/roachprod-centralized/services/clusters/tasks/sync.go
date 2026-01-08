// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
)

// CustersTaskType is the type of the task.
type ClustersTaskType string

const (
	// ClustersTaskSync is the task type for synchronizing the clusters.
	ClustersTaskSync ClustersTaskType = types.TaskServiceName + "_sync"
)

// TaskSync is the task that synchronizes the clusters.
type TaskSync struct {
	tasks.Task
	Service types.IService
}

// NewTaskSync creates a new TaskSync instance.
func NewTaskSync() *TaskSync {
	return &TaskSync{
		Task: tasks.Task{
			Type: string(ClustersTaskSync),
		},
	}
}

// Process will synchronize the clusters.
func (t *TaskSync) Process(ctx context.Context, l *logger.Logger) error {
	_, err := t.Service.Sync(ctx, l)
	return err
}

func (t *TaskSync) GetTimeout() time.Duration {
	// Set a generous timeout of 2.5 minutes for cluster syncs.
	// When there are many clusters (e.g. nightly roachtests), this can take a while.
	return time.Second * 150
}
