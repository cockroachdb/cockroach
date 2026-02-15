// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks/memory"
	tasksrepomock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks/mocks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestProcessTaskRoutine_Success(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{Workers: 1})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Mock channel push
	mockRepo.On(
		"GetTasksForProcessing", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return(nil).Once()

	errChan := make(chan error)

	err := taskService.processTaskRoutine(ctx, logger.DefaultLogger, errChan)
	assert.Nil(t, err)
	select {
	case <-time.After(2 * time.Millisecond):
		break
	case err := <-errChan:
		t.Fatalf("unexpected error received: %s", err)
	}
	mockRepo.AssertExpectations(t)
}

func TestProcessTaskRoutine_GetTasksError(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{Workers: 1})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockErr := fmt.Errorf("some error")
	mockRepo.On(
		"GetTasksForProcessing", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return(mockErr).Once()

	errChan := make(chan error)

	err := taskService.processTaskRoutine(ctx, logger.DefaultLogger, errChan)
	assert.Nil(t, err)
	select {
	case <-time.After(50 * time.Millisecond):
		t.Fatalf("timeout while waiting for expected error")
	case err := <-errChan:
		assert.ErrorAs(t, err, &mockErr)
	}
	mockRepo.AssertExpectations(t)
}

func TestProcessTaskRoutine_GetTask(t *testing.T) {
	repo := memory.NewTasksRepository()
	taskService := NewService(repo, "test-instance", types.Options{Workers: 1})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockErr := fmt.Errorf("some expected error")
	service := &MockITasksService{}
	fakeTask := &MockTask{
		Task: tasks.Task{
			ID:   uuid.MakeV4(),
			Type: "fake_type",
		},
	}
	service.On("GetTaskServiceName").Return("test_type").Once()
	service.On("GetHandledTasks").Return(map[string]types.ITask{"fake_type": fakeTask}).Once()
	fakeTask.On("Process", mock.Anything, mock.Anything).Return(mockErr).Once()
	service.On("CreateTaskInstance", mock.Anything).Return(fakeTask, nil).Maybe()

	errChan := make(chan error)
	_, err := taskService.CreateTaskIfNotAlreadyPlanned(ctx, logger.DefaultLogger, fakeTask)
	assert.Nil(t, err)

	taskService.RegisterTasksService(service)
	err = taskService.processTaskRoutine(ctx, logger.DefaultLogger, errChan)
	assert.Nil(t, err)
	select {
	case <-time.After(50 * time.Millisecond):
		t.Fatalf("timeout while waiting for expected error")
	case err := <-errChan:
		assert.ErrorAs(t, err, &mockErr)
	}
}

func TestProcessTask(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{
		DefaultTasksTimeout: 50 * time.Millisecond,
	})

	service := &MockITasksService{}
	task := &MockTask{
		Task: tasks.Task{ID: uuid.MakeV4(), Type: "test_type"},
	}
	mockRepo.On("UpdateState", mock.Anything, mock.Anything, task.GetID(), tasks.TaskStateRunning).Return(nil).Once()
	service.On("GetTaskServiceName").Return("test_type").Once()
	service.On("GetHandledTasks").Return(map[string]types.ITask{"test_type": task}).Once()
	task.On("GetType").Return("test_type").Once()
	service.On("CreateTaskInstance", mock.Anything).Return(task, nil).Maybe()
	task.On("Process", mock.Anything, mock.Anything).Return(nil).Once()
	mockRepo.On("UpdateState", mock.Anything, mock.Anything, task.GetID(), tasks.TaskStateDone).Return(nil).Once()

	taskService.RegisterTasksService(service)
	err := taskService.processTask(context.Background(), logger.DefaultLogger, task)
	assert.Nil(t, err)
}

func TestProcessTask_Error(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{
		DefaultTasksTimeout: 50 * time.Millisecond,
	})

	service := &MockITasksService{}
	task := &MockTask{
		Task: tasks.Task{ID: uuid.MakeV4(), Type: "test_type"},
	}

	expectedErr := errors.New("some error")

	mockRepo.On("UpdateState", mock.Anything, mock.Anything, task.GetID(), tasks.TaskStateRunning).Return(nil).Once()
	service.On("GetTaskServiceName").Return("test_type").Once()
	service.On("GetHandledTasks").Return(map[string]types.ITask{"test_type": task}).Once()
	task.On("GetType").Return("test_type").Once()
	service.On("CreateTaskInstance", mock.Anything).Return(task, nil).Maybe()
	task.On("Process", mock.Anything, mock.Anything).Return(expectedErr).Once()
	mockRepo.On("UpdateError", mock.Anything, mock.Anything, task.GetID(), expectedErr.Error()).Return(nil).Once()
	mockRepo.On("UpdateState", mock.Anything, mock.Anything, task.GetID(), tasks.TaskStateFailed).Return(nil).Once()

	taskService.RegisterTasksService(service)
	err := taskService.processTask(context.Background(), logger.DefaultLogger, task)
	assert.ErrorIs(t, err, expectedErr)
}

func TestProcessTask_Timeout(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{
		DefaultTasksTimeout: 1 * time.Microsecond,
	})

	service := &MockITasksService{}
	task := &MockTask{
		Task: tasks.Task{ID: uuid.MakeV4(), Type: "test_type"},
	}

	mockRepo.On("UpdateState", mock.Anything, mock.Anything, task.GetID(), tasks.TaskStateRunning).Return(nil).Once()
	service.On("GetTaskServiceName").Return("test_type").Once()
	service.On("GetHandledTasks").Return(map[string]types.ITask{"test_type": task}).Once()
	task.On("GetType").Return("test_type").Once()
	service.On("CreateTaskInstance", mock.Anything).Return(task, nil).Maybe()
	task.On("Process", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		time.Sleep(5 * time.Millisecond)
	}).Once()
	mockRepo.On("UpdateError", mock.Anything, mock.Anything, task.GetID(), mock.Anything).Return(nil).Once()
	mockRepo.On("UpdateState", mock.Anything, mock.Anything, task.GetID(), tasks.TaskStateFailed).Return(nil).Once()

	taskService.RegisterTasksService(service)
	err := taskService.processTask(context.Background(), logger.DefaultLogger, task)
	assert.ErrorIs(t, err, types.ErrTaskTimeout)
}

func TestProcessTask_Unmanaged(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{})

	task := &MockTask{
		Task: tasks.Task{
			ID:   uuid.MakeV4(),
			Type: "test_type",
		},
	}

	// Expect hydration to fail for unmanaged task type
	mockRepo.On("UpdateError", mock.Anything, mock.Anything, task.GetID(), mock.Anything).Return(nil).Once()
	mockRepo.On("UpdateState", mock.Anything, mock.Anything, task.GetID(), tasks.TaskStateFailed).Return(nil).Once()

	err := taskService.processTask(context.Background(), logger.DefaultLogger, task)
	assert.ErrorIs(t, err, types.ErrUnknownTaskType)
	mockRepo.AssertExpectations(t)
}

func TestProcessTask_MarkAsRunningFailed(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{
		DefaultTasksTimeout: 50 * time.Millisecond,
	})

	service := &MockITasksService{}
	task := &MockTask{
		Task: tasks.Task{ID: uuid.MakeV4(), Type: "test_type"},
	}
	mockRepo.On(
		"UpdateState", mock.Anything, mock.Anything, task.GetID(), tasks.TaskStateRunning,
	).Return(errors.New("some error")).Once()
	service.On("GetTaskServiceName").Return("test_type").Once()
	service.On("GetHandledTasks").Return(map[string]types.ITask{"test_type": task}).Once()
	task.On("GetType").Return("test_type").Once()
	service.On("CreateTaskInstance", mock.Anything).Return(task, nil).Maybe()
	task.On("Process", mock.Anything, mock.Anything).Return(nil).Once()
	mockRepo.On(
		"UpdateState", mock.Anything, mock.Anything, task.GetID(), tasks.TaskStateDone,
	).Return(errors.New("some error")).Once()

	taskService.RegisterTasksService(service)
	err := taskService.processTask(context.Background(), logger.DefaultLogger, task)
	assert.Nil(t, err)
}

func TestProcessTask_Failed_MarkAsRunningFailed(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{
		DefaultTasksTimeout: 50 * time.Millisecond,
	})

	service := &MockITasksService{}
	task := &MockTask{
		Task: tasks.Task{ID: uuid.MakeV4(), Type: "test_type"},
	}

	expectedErr := errors.New("some error")

	mockRepo.On(
		"UpdateState", mock.Anything, mock.Anything, task.GetID(), tasks.TaskStateRunning,
	).Return(nil).Once()
	service.On("GetTaskServiceName").Return("test_type").Once()
	service.On("GetHandledTasks").Return(map[string]types.ITask{"test_type": task}).Once()
	task.On("GetType").Return("test_type").Once()
	service.On("CreateTaskInstance", mock.Anything).Return(task, nil).Maybe()
	task.On("Process", mock.Anything, mock.Anything).Return(expectedErr).Once()
	mockRepo.On(
		"UpdateError", mock.Anything, mock.Anything, task.GetID(), expectedErr.Error(),
	).Return(nil).Once()
	mockRepo.On(
		"UpdateState", mock.Anything, mock.Anything, task.GetID(), tasks.TaskStateFailed,
	).Return(errors.New("some error")).Once()

	taskService.RegisterTasksService(service)
	err := taskService.processTask(context.Background(), logger.DefaultLogger, task)
	assert.ErrorIs(t, err, expectedErr)
}

func TestMarkTaskAs(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{})

	taskID := uuid.MakeV4()
	mockRepo.
		On("UpdateState", mock.Anything, mock.Anything, taskID, tasks.TaskStateRunning).
		Return(nil).
		Once()

	err := taskService.markTaskAs(context.Background(), logger.DefaultLogger, taskID, tasks.TaskStateRunning)
	assert.NoError(t, err)
	mockRepo.AssertExpectations(t)
}
