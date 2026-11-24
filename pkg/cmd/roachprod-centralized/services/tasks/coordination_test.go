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
	tasksrepomock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks/mocks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestCreateTaskIfNotAlreadyPlanned_ErrorGet(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{})
	ctx := context.Background()

	expectedError := errors.New("db error")

	taskType := "fake_type"
	pendingTask := &MockTask{
		Task: tasks.Task{
			ID:    uuid.MakeV4(),
			Type:  taskType,
			State: tasks.TaskStatePending,
		},
	}
	filters := filters.NewFilterSet().
		AddFilter("Type", filtertypes.OpEqual, taskType).
		AddFilter("State", filtertypes.OpEqual, string(tasks.TaskStatePending))
	mockRepo.On("GetTasks", ctx, mock.Anything, *filters).Return(
		[]tasks.ITask{pendingTask}, 1, expectedError,
	)

	newTask := &MockTask{
		Task: tasks.Task{
			ID:   uuid.MakeV4(),
			Type: taskType,
		},
	}

	task, err := taskService.CreateTaskIfNotAlreadyPlanned(ctx, logger.DefaultLogger, newTask)
	assert.Nil(t, task)
	assert.ErrorIs(t, err, expectedError)
	mockRepo.AssertExpectations(t)
}

func TestCreateTaskIfNotAlreadyPlanned_CreatesNew(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{})
	ctx := context.Background()

	filters := filters.NewFilterSet().
		AddFilter("Type", filtertypes.OpEqual, "fake_type").
		AddFilter("State", filtertypes.OpEqual, string(tasks.TaskStatePending))
	mockRepo.On("GetTasks", ctx, mock.Anything, *filters).Return([]tasks.ITask{}, 0, nil)
	fakeTask := &MockTask{
		Task: tasks.Task{
			ID:   uuid.MakeV4(),
			Type: "fake_type",
		},
	}
	mockRepo.On("CreateTask", mock.Anything, mock.Anything, fakeTask).Return(nil)

	task, err := taskService.CreateTaskIfNotAlreadyPlanned(ctx, logger.DefaultLogger, fakeTask)
	assert.NotNil(t, task)
	assert.Nil(t, err)
	mockRepo.AssertExpectations(t)
}

func TestCreateTaskIfNotAlreadyPlanned_CreatesNew_FailedExists(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{})
	ctx := context.Background()

	taskType := "fake_type"
	pendingTask := &MockTask{
		Task: tasks.Task{
			ID:    uuid.MakeV4(),
			Type:  taskType,
			State: tasks.TaskStatePending,
		},
	}
	tasksList := []tasks.ITask{
		&MockTask{
			Task: tasks.Task{
				ID:    uuid.MakeV4(),
				Type:  taskType,
				State: tasks.TaskStateFailed,
			},
		},
		pendingTask,
	}
	filters := filters.NewFilterSet().
		AddFilter("Type", filtertypes.OpEqual, taskType).
		AddFilter("State", filtertypes.OpEqual, string(tasks.TaskStatePending))
	mockRepo.On("GetTasks", ctx, mock.Anything, *filters).Return(tasksList, len(tasksList), nil)

	newTask := &MockTask{
		Task: tasks.Task{
			ID:   uuid.MakeV4(),
			Type: taskType,
		},
	}

	task, err := taskService.CreateTaskIfNotAlreadyPlanned(ctx, logger.DefaultLogger, newTask)
	assert.NotNil(t, task)
	assert.Nil(t, err)
	mockRepo.AssertExpectations(t)
}

func TestCreateTaskIfNotAlreadyPlanned_ReturnsExisting(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{})
	ctx := context.Background()

	taskType := "fake_type"
	pendingTask := &MockTask{
		Task: tasks.Task{
			ID:    uuid.MakeV4(),
			Type:  taskType,
			State: tasks.TaskStatePending,
		},
	}
	filters := filters.NewFilterSet().
		AddFilter("Type", filtertypes.OpEqual, taskType).
		AddFilter("State", filtertypes.OpEqual, string(tasks.TaskStatePending))
	mockRepo.On("GetTasks", ctx, mock.Anything, *filters).Return([]tasks.ITask{
		pendingTask,
	}, 1, nil).Once()

	newTask := &MockTask{
		Task: tasks.Task{
			ID:   uuid.MakeV4(),
			Type: taskType,
		},
	}

	task, err := taskService.CreateTaskIfNotAlreadyPlanned(ctx, logger.DefaultLogger, newTask)
	assert.NotNil(t, task)
	assert.Nil(t, err)
	assert.Equal(t, task.GetID(), pendingTask.GetID())
	mockRepo.AssertExpectations(t)
}

func TestCreateTaskIfNotRecentlyScheduled_RecencyWindowAdjustment(t *testing.T) {
	// This test verifies the recency window adjustment logic that subtracts 1 second
	// to handle sub-second timing precision issues.
	//
	// Scenario:
	// - Task created at: 2025-10-09T03:24:01.199060Z
	// - 10 minutes later we check: 2025-10-09T03:34:01.188060Z
	// - Actual elapsed time: 9m59.989s (slightly less than 10 minutes)
	//
	// Without adjustment: Query looks for tasks created after 03:24:01.188060Z
	//                     The existing task (03:24:01.199060Z) would NOT match
	//                     A new task would be incorrectly created
	//
	// With adjustment: Query looks for tasks created after 03:24:02.188060Z (10min - 1s)
	//                  The existing task (03:24:01.199060Z) DOES match
	//                  Returns existing task (correct behavior)

	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{})
	ctx := context.Background()

	// Task was created 10 minutes ago (minus 11ms to simulate sub-second drift)
	taskCreationTime := timeutil.Now().Add(-10*time.Minute + 11*time.Millisecond)

	existingTask := &MockTask{
		Task: tasks.Task{
			ID:               uuid.MakeV4(),
			Type:             "periodic_sync",
			State:            tasks.TaskStateDone,
			CreationDatetime: taskCreationTime,
		},
	}

	// The query should use recencyWindow - 1s (10min - 1s = 9min59s)
	// This ensures we catch the task created at 10min - 11ms ago
	expectedCutoffTime := timeutil.Now().Add(-10*time.Minute + time.Second)

	// Mock expects a filter checking for tasks created within the last 9min59s
	// The existing task (created 10min - 11ms ago) should match this filter
	mockRepo.On("GetTasks", ctx, mock.Anything, mock.MatchedBy(func(f interface{}) bool {
		filterSet, ok := f.(filtertypes.FilterSet)
		if !ok {
			return false
		}
		// Verify the filter has:
		// - Type = "periodic_sync"
		// - State != "failed"
		// - creation_datetime > (now - 9min59s)
		var creationFilter *filtertypes.FieldFilter
		for i := range filterSet.Filters {
			if filterSet.Filters[i].Field == "CreationDatetime" {
				creationFilter = &filterSet.Filters[i]
				break
			}
		}

		if creationFilter == nil {
			return false
		}

		// Check that creation time filter is approximately 9min59s ago (accounting for test execution time)
		cutoffTime, ok := creationFilter.Value.(time.Time)
		if !ok {
			return false
		}

		// Allow 100ms tolerance for test execution time
		timeDiff := cutoffTime.Sub(expectedCutoffTime).Abs()
		return timeDiff < 100*time.Millisecond
	})).Return([]tasks.ITask{existingTask}, 1, nil)

	newTask := &MockTask{
		Task: tasks.Task{
			Type: "periodic_sync",
		},
	}

	// Should return existing task instead of creating new one
	task, err := taskService.CreateTaskIfNotRecentlyScheduled(
		ctx,
		logger.DefaultLogger,
		newTask,
		10*time.Minute,
	)

	assert.NoError(t, err)
	assert.NotNil(t, task)
	assert.Equal(t, existingTask.GetID(), task.GetID(), "should return existing task, not create new one")
	mockRepo.AssertExpectations(t)
}

func TestCreateTaskIfNotRecentlyScheduled_MinimumRecencyWindow(t *testing.T) {
	// Test that when recencyWindow is <= 1 second, it uses 1 second as minimum
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{})
	ctx := context.Background()

	// Mock expects query with CreationDatetime filter at least 1 second ago
	// (even though we pass 500ms, it should adjust to 1 second minimum)
	mockRepo.On("GetTasks", ctx, mock.Anything, mock.MatchedBy(func(f interface{}) bool {
		filterSet, ok := f.(filtertypes.FilterSet)
		if !ok {
			return false
		}
		var creationFilter *filtertypes.FieldFilter
		for i := range filterSet.Filters {
			if filterSet.Filters[i].Field == "CreationDatetime" {
				creationFilter = &filterSet.Filters[i]
				break
			}
		}
		if creationFilter == nil {
			return false
		}

		cutoffTime, ok := creationFilter.Value.(time.Time)
		if !ok {
			return false
		}

		// Should be approximately 1 second ago, not 500ms
		timeSinceCutoff := timeutil.Since(cutoffTime)
		return timeSinceCutoff >= 900*time.Millisecond && timeSinceCutoff <= 1100*time.Millisecond
	})).Return([]tasks.ITask{}, 0, nil)

	newTask := &MockTask{Task: tasks.Task{Type: "test_task"}}
	mockRepo.On("CreateTask", mock.Anything, mock.Anything, newTask).Return(nil)

	// Pass 500ms recency window
	task, err := taskService.CreateTaskIfNotRecentlyScheduled(
		ctx,
		logger.DefaultLogger,
		newTask,
		500*time.Millisecond, // Less than 1 second
	)

	assert.NoError(t, err)
	assert.NotNil(t, task)
	mockRepo.AssertExpectations(t)
}
