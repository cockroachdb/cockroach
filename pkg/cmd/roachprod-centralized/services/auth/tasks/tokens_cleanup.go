// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"context"
	"time"

	mtasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
)

// TaskType is the type of the task.
type TaskType string

const (
	// CleanupExpiredTokensTask is the task type for cleaning up expired tokens.
	CleanupExpiredTokensTask TaskType = types.TaskServiceName + "_cleanup_expired_tokens"
)

// TaskTokensCleanupOptions contains configuration for the expired tokens cleanup task.
type TaskExpiredTokensCleanupOptions struct {
	// ExpiredTokensRetention is how long to keep deleted instance records.
	ExpiredTokensRetention time.Duration `json:"expired_tokens_retention"`
}

// TaskExpiredTokensCleanup represents a task that cleans up dead instances.
type TaskExpiredTokensCleanup struct {
	mtasks.TaskWithOptions[TaskExpiredTokensCleanupOptions]
	Service types.IService
}

// NewTaskExpiredTokensCleanup creates a new TaskExpiredTokensCleanup instance.
func NewTaskExpiredTokensCleanup(
	expiredTokensRetention time.Duration,
) (*TaskExpiredTokensCleanup, error) {
	task := &TaskExpiredTokensCleanup{}
	task.Type = string(CleanupExpiredTokensTask)
	if err := task.SetOptions(TaskExpiredTokensCleanupOptions{
		ExpiredTokensRetention: expiredTokensRetention,
	}); err != nil {
		return nil, err
	}
	return task, nil
}

// Process will clean up expired tokens.
func (t *TaskExpiredTokensCleanup) Process(ctx context.Context, l *logger.Logger) error {
	_, err := t.Service.CleanupRevokedAndExpiredTokens(ctx, l, t.GetOptions().ExpiredTokensRetention)
	return err
}

func (t *TaskExpiredTokensCleanup) GetTimeout() time.Duration {
	return time.Second * 30
}
