// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	tasksrepo "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks"
	tasksrepomock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks/mocks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestGetTasks(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{})
	ctx := context.Background()

	expectedTasks := []tasks.ITask{&MockTask{}}

	filters := filters.NewFilterSet()

	mockRepo.On("GetTasks", ctx, mock.Anything, *filters).Return(expectedTasks, len(expectedTasks), nil)

	tasks, _, err := taskService.GetTasks(ctx, logger.DefaultLogger, nil, types.InputGetAllTasksDTO{
		Filters: *filters,
	})
	assert.Nil(t, err)
	assert.Equal(t, expectedTasks, tasks)
	mockRepo.AssertExpectations(t)
}

func TestGetTasksFiltered(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{})
	ctx := context.Background()

	expectedTasks := []tasks.ITask{&MockTask{}}

	expectedFilters := filters.NewFilterSet().
		AddFilter("Type", filtertypes.OpEqual, "test").
		AddFilter("State", filtertypes.OpEqual, string(tasks.TaskStateRunning))

	mockRepo.On("GetTasks", ctx, mock.Anything, *expectedFilters).Return(expectedTasks, len(expectedTasks), nil)

	filters := filters.NewFilterSet().
		AddFilter("Type", filtertypes.OpEqual, "test").
		AddFilter("State", filtertypes.OpEqual, string(tasks.TaskStateRunning))

	tasks, _, err := taskService.GetTasks(ctx, logger.DefaultLogger, nil, types.InputGetAllTasksDTO{
		Filters: *filters,
	})
	assert.Nil(t, err)
	assert.Equal(t, expectedTasks, tasks)
	mockRepo.AssertExpectations(t)
}

func TestGetTasks_Error(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{})
	ctx := context.Background()

	expectedError := errors.New("db error")

	mockRepo.On("GetTasks", ctx, mock.Anything, *filters.NewFilterSet()).Return(nil, 0, expectedError)

	tasks, _, err := taskService.GetTasks(ctx, logger.DefaultLogger, nil, types.NewInputGetAllTasksDTO())
	assert.Nil(t, tasks)
	assert.ErrorIs(t, err, expectedError)
	mockRepo.AssertExpectations(t)
}

func TestGetTask(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{})
	ctx := context.Background()
	fakeID := uuid.MakeV4()
	fakeTask := &MockTask{
		Task: tasks.Task{
			ID: fakeID,
		},
	}

	mockRepo.On("GetTask", mock.Anything, mock.Anything, fakeID).Return(fakeTask, nil)

	task, err := taskService.GetTask(ctx, logger.DefaultLogger, nil, types.InputGetTaskDTO{ID: fakeID})
	assert.Equal(t, fakeTask, task)
	assert.Nil(t, err)
	mockRepo.AssertExpectations(t)
}

func TestGetTask_NotFound(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{})
	ctx := context.Background()
	fakeID := uuid.MakeV4()

	mockRepo.On("GetTask", mock.Anything, mock.Anything, fakeID).Return(nil, tasksrepo.ErrTaskNotFound)

	task, err := taskService.GetTask(ctx, logger.DefaultLogger, nil, types.InputGetTaskDTO{ID: fakeID})
	assert.Nil(t, task)
	assert.ErrorAs(t, err, &types.ErrTaskNotFound)
	assert.IsType(t, &utils.PublicError{}, err)
	mockRepo.AssertExpectations(t)
}

func TestGetTask_PrivateError(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{})
	ctx := context.Background()
	fakeID := uuid.MakeV4()

	expectedError := errors.New("db error")

	mockRepo.On("GetTask", mock.Anything, mock.Anything, fakeID).Return(nil, expectedError)

	task, err := taskService.GetTask(ctx, logger.DefaultLogger, nil, types.InputGetTaskDTO{ID: fakeID})
	assert.Nil(t, task)
	assert.ErrorIs(t, err, expectedError)
	mockRepo.AssertExpectations(t)
}

func TestCreateTask(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{})
	ctx := context.Background()

	fakeTask := &MockTask{}
	mockRepo.On("CreateTask", mock.Anything, mock.Anything, fakeTask).Return(nil)

	task, err := taskService.CreateTask(ctx, logger.DefaultLogger, fakeTask)
	assert.Nil(t, err)
	assert.NotNil(t, task)
	mockRepo.AssertExpectations(t)
}

func TestCreateTask_Error(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{})
	ctx := context.Background()

	fakeTask := &MockTask{}
	mockRepo.On("CreateTask", mock.Anything, mock.Anything, fakeTask).Return(errors.New("db error"))

	task, err := taskService.CreateTask(ctx, logger.DefaultLogger, fakeTask)
	assert.Nil(t, task)
	assert.NotNil(t, err)
	mockRepo.AssertExpectations(t)
}
