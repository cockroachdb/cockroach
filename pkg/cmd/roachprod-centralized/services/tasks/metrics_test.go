// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	tasksrepo "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks"
	tasksrepomock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks/mocks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestProcessTasksUpdateStatisticsRoutine(t *testing.T) {

	mockRepo := &tasksrepomock.ITasksRepository{}
	taskServiceOpts := types.Options{
		CollectMetrics:           false, // Disable auto-creation
		StatisticsUpdateInterval: 1 * time.Millisecond,
	}
	taskService := NewService(mockRepo, "test-instance-stats", taskServiceOpts)
	// Manually create metrics with isolated registry for testing
	taskService.metrics = taskService.newMetrics(prometheus.NewRegistry())
	// Enable metrics flag after manual creation
	taskService.options.CollectMetrics = true

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockRepo.On(
		"GetStatistics", mock.Anything, mock.Anything,
	).Return(tasksrepo.Statistics{}, nil)

	errChan := make(chan error)
	err := taskService.processTasksUpdateStatisticsRoutine(ctx, logger.DefaultLogger, errChan)
	assert.Nil(t, err)
	select {
	case <-time.After(50 * time.Millisecond):
		break
	case err := <-errChan:
		t.Fatalf("unexpected error received: %s", err)
	}
	mockRepo.AssertExpectations(t)
}

func TestProcessTasksUpdateStatisticsRoutine_Error(t *testing.T) {

	mockRepo := &tasksrepomock.ITasksRepository{}
	taskServiceOpts := types.Options{
		CollectMetrics:           false, // Disable auto-creation
		StatisticsUpdateInterval: 1 * time.Millisecond,
	}
	taskService := NewService(mockRepo, "test-instance-stats-error", taskServiceOpts)
	// Manually create metrics with isolated registry for testing
	taskService.metrics = taskService.newMetrics(prometheus.NewRegistry())
	// Enable metrics flag after manual creation
	taskService.options.CollectMetrics = true

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	expectedError := errors.New("some error")

	mockRepo.On(
		"GetStatistics", mock.Anything, mock.Anything,
	).Return(nil, expectedError)

	errChan := make(chan error)
	err := taskService.processTasksUpdateStatisticsRoutine(ctx, logger.DefaultLogger, errChan)
	assert.Nil(t, err)
	select {
	case <-time.After(50 * time.Millisecond):
		t.Fatalf("timeout while waiting for expected error")
	case err := <-errChan:
		assert.ErrorIs(t, err, expectedError)
	}
	mockRepo.AssertExpectations(t)
}

func TestProcessTasksUpdateStatisticsRoutine_NoMetrics(t *testing.T) {

	mockRepo := &tasksrepomock.ITasksRepository{}
	taskServiceOpts := types.Options{
		StatisticsUpdateInterval: 1 * time.Millisecond,
	}
	taskService := NewService(mockRepo, "test-instance", taskServiceOpts)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errChan := make(chan error)
	err := taskService.processTasksUpdateStatisticsRoutine(ctx, logger.DefaultLogger, errChan)
	assert.ErrorIs(t, err, types.ErrMetricsCollectionDisabled)

	mockRepo.AssertExpectations(t)
}

func TestUpdateMetrics(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance-metrics", types.Options{
		CollectMetrics: false, // Disable auto-creation
	})
	// Manually create metrics with isolated registry for testing
	taskService.metrics = taskService.newMetrics(prometheus.NewRegistry())
	// Enable metrics flag after manual creation
	taskService.options.CollectMetrics = true

	mockRepo.
		On("GetStatistics", mock.Anything, mock.Anything).
		Return(tasksrepo.Statistics{
			tasks.TaskStatePending: {
				"test_task_type": 5,
			},
			tasks.TaskStateRunning: {
				"test_task_type": 2,
			},
			tasks.TaskStateDone: {
				"test_task_type": 10,
			},
			tasks.TaskStateFailed: {
				"test_task_type": 1,
			},
		}, nil).
		Once()

	err := taskService.updateMetrics(context.Background(), logger.DefaultLogger)
	assert.NoError(t, err)
	mockRepo.AssertExpectations(t)
}

func TestUpdateMetrics_NoMetrics(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{})

	err := taskService.updateMetrics(context.Background(), logger.DefaultLogger)
	assert.ErrorIs(t, err, types.ErrMetricsCollectionDisabled)
	mockRepo.AssertExpectations(t)
}

func TestUpdateMetrics_Error(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance-metrics-error", types.Options{
		CollectMetrics: false, // Disable auto-creation
	})
	// Manually create metrics with isolated registry for testing
	taskService.metrics = taskService.newMetrics(prometheus.NewRegistry())
	// Enable metrics flag after manual creation
	taskService.options.CollectMetrics = true

	mockRepo.
		On("GetStatistics", mock.Anything, mock.Anything).
		Return(nil, errors.New("some error")).
		Once()

	err := taskService.updateMetrics(context.Background(), logger.DefaultLogger)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unable to get tasks statistics")
	mockRepo.AssertExpectations(t)
}
