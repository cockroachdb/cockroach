// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
)

// TaskGC is the task that scans for expired provisionings and schedules
// individual destroy tasks for each. Running GC as a task ensures that
// only one instance performs the scan at a time, preventing duplicate
// destroy task creation across multiple replicas.
type TaskGC struct {
	tasks.Task
	Service types.IProvisioningTaskHandler
}

// NewTaskGC creates a new TaskGC instance.
func NewTaskGC() *TaskGC {
	return &TaskGC{
		Task: tasks.Task{
			Type: string(ProvisioningsTaskGC),
		},
	}
}

// GetTimeout returns the maximum duration for a GC task. The scan is a
// lightweight DB query followed by a few task creations, so 5 minutes
// is generous.
func (t *TaskGC) GetTimeout() time.Duration {
	return 5 * time.Minute
}

// Process executes the GC scan.
func (t *TaskGC) Process(ctx context.Context, l *logger.Logger) error {
	return t.Service.HandleGC(ctx, l)
}
