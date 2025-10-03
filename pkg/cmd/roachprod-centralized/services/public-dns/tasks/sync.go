// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/public-dns/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
)

const (
	// PublicDNSTaskSync is the task type for synchronizing the public DNS.
	PublicDNSTaskSync types.PublicDNSTaskType = types.TaskServiceName + "_sync"
)

// TaskSync is the task that synchronizes the DNS.
type TaskSync struct {
	tasks.Task
	Service types.IService
}

// NewTaskSync creates a new TaskSync instance.
func NewTaskSync() *TaskSync {
	return &TaskSync{
		Task: tasks.Task{
			Type: string(PublicDNSTaskSync),
		},
	}
}

// Process will synchronize the DNS.
func (t *TaskSync) Process(ctx context.Context, l *logger.Logger) error {
	return t.Service.Sync(ctx, l)
}

func (t *TaskSync) GetTimeout() time.Duration {
	return time.Second * 90
}
