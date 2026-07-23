// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/internal/metrics"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/internal/processor"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/internal/scheduler"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/mock"
)

// Test helpers - these expose internal methods for testing purposes only.

func (s *Service) schedulePurgeTaskRoutine(
	ctx context.Context, l *logger.Logger, errChan chan<- error,
) error {
	return scheduler.StartPurgeScheduling(ctx, l, errChan, s.options.PurgeTasksInterval, s, nil)
}

func (s *Service) processTasksUpdateStatisticsRoutine(
	ctx context.Context, l *logger.Logger, errChan chan<- error,
) error {
	if !s.options.CollectMetrics {
		return types.ErrMetricsCollectionDisabled
	}
	return metrics.StartMetricsCollection(ctx, l, errChan, s.options.StatisticsUpdateInterval, s, nil)
}

func (s *Service) processTaskRoutine(
	ctx context.Context, l *logger.Logger, errChan chan<- error,
) error {
	return processor.StartProcessing(ctx, l, errChan, s.options.Workers, s.instanceID, s.store, s, func() {})
}

func (s *Service) processTask(ctx context.Context, l *logger.Logger, baseTask tasks.ITask) error {
	return processor.ExecuteTask(ctx, l, baseTask, s)
}

func (s *Service) markTaskAs(
	ctx context.Context, l *logger.Logger, id uuid.UUID, status tasks.TaskState,
) error {
	return s.store.UpdateState(ctx, l, id, status)
}

// purgeTasksInState is already defined in operations.go, no need to redeclare

func (s *Service) updateMetrics(ctx context.Context, l *logger.Logger) error {
	return s.UpdateMetrics(ctx, l)
}

// MockTask is a minimal mock for tasks.ITask used by these tests
type MockTask struct {
	tasks.Task
	mock.Mock
}

// Process provides a mock function with given fields: ctx, l
func (m *MockTask) Process(ctx context.Context, l *logger.Logger) error {
	args := m.Called(ctx, l)
	if len(args) == 0 {
		return nil
	}
	if err, ok := args.Get(0).(error); ok {
		return err
	}
	return nil
}

// MockITasksService is a minimal mock for ITasksService
type MockITasksService struct {
	mock.Mock
}

func (m *MockITasksService) GetTaskServiceName() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockITasksService) GetHandledTasks() map[string]types.ITask {
	args := m.Called()
	return args.Get(0).(map[string]types.ITask)
}

func (m *MockITasksService) CreateTaskInstance(taskType string) (tasks.ITask, error) {
	args := m.Called(taskType)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(tasks.ITask), args.Error(1)
}
