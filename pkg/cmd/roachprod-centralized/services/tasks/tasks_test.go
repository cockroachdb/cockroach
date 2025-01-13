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
	rtasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks/memory"
	tasksrepomock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks/mocks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockTask is a minimal mock for tasks.ITask used by these tests
type MockTask struct {
	tasks.Task
	mock.Mock
}

// Process provides a mock function with given fields: _a0, _a1, _a2
func (m *MockTask) Process(_a0 context.Context, _a1 *utils.Logger, _a2 chan<- error) {
	args := m.Called(_a0, _a1, _a2)
	if args.Get(0) != nil {
		_a2 <- args.Get(0).(error)
	}
	_a2 <- nil
}

// MockITasksService is a minimal mock for ITasksService
type MockITasksService struct {
	mock.Mock
}

func (m *MockITasksService) GetTaskServiceName() string {
	args := m.Called()
	return args.String(0)
}
func (m *MockITasksService) GetHandledTasks() map[string]ITask {
	args := m.Called()
	return args.Get(0).(map[string]ITask)
}

func TestGetTasks(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{})
	ctx := context.Background()

	expectedTasks := []tasks.ITask{&MockTask{}}

	mockRepo.On("GetTasks", ctx, rtasks.InputGetTasksFilters{}).Return(expectedTasks, nil)

	tasks, err := taskService.GetTasks(ctx, utils.DefaultLogger, InputGetAllTasksDTO{})
	assert.Nil(t, err)
	assert.Equal(t, expectedTasks, tasks)
	mockRepo.AssertExpectations(t)
}

func TestGetTasksFiltered(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{})
	ctx := context.Background()

	expectedTasks := []tasks.ITask{&MockTask{}}

	mockRepo.On("GetTasks", ctx, rtasks.InputGetTasksFilters{
		Type:  "test",
		State: tasks.TaskStateRunning,
	}).Return(expectedTasks, nil)

	tasks, err := taskService.GetTasks(ctx, utils.DefaultLogger, InputGetAllTasksDTO{
		Type:  "test",
		State: tasks.TaskStateRunning,
	})
	assert.Nil(t, err)
	assert.Equal(t, expectedTasks, tasks)
	mockRepo.AssertExpectations(t)
}

func TestGetTasks_Error(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{})
	ctx := context.Background()

	expectedError := errors.New("db error")

	mockRepo.On("GetTasks", ctx, rtasks.InputGetTasksFilters{}).Return(nil, expectedError)

	tasks, err := taskService.GetTasks(ctx, utils.DefaultLogger, InputGetAllTasksDTO{})
	assert.Nil(t, tasks)
	assert.ErrorIs(t, err, expectedError)
	mockRepo.AssertExpectations(t)
}

func TestGetTask(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{})
	ctx := context.Background()
	fakeID := uuid.MakeV4()
	fakeTask := &MockTask{
		Task: tasks.Task{
			ID: fakeID,
		},
	}

	mockRepo.On("GetTask", mock.Anything, fakeID).Return(fakeTask, nil)

	task, err := taskService.GetTask(ctx, utils.DefaultLogger, InputGetTaskDTO{ID: fakeID})
	assert.Equal(t, fakeTask, task)
	assert.Nil(t, err)
	mockRepo.AssertExpectations(t)
}

func TestGetTask_NotFound(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{})
	ctx := context.Background()
	fakeID := uuid.MakeV4()

	mockRepo.On("GetTask", mock.Anything, fakeID).Return(nil, rtasks.ErrTaskNotFound)

	task, err := taskService.GetTask(ctx, utils.DefaultLogger, InputGetTaskDTO{ID: fakeID})
	assert.Nil(t, task)
	assert.ErrorAs(t, err, &ErrTaskNotFound)
	assert.IsType(t, &utils.PublicError{}, err)
	mockRepo.AssertExpectations(t)
}

func TestGetTask_PrivateError(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{})
	ctx := context.Background()
	fakeID := uuid.MakeV4()

	expectedError := errors.New("db error")

	mockRepo.On("GetTask", mock.Anything, fakeID).Return(nil, expectedError)

	task, err := taskService.GetTask(ctx, utils.DefaultLogger, InputGetTaskDTO{ID: fakeID})
	assert.Nil(t, task)
	assert.ErrorIs(t, err, expectedError)
	mockRepo.AssertExpectations(t)
}

func TestCreateTask(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{})
	ctx := context.Background()

	fakeTask := &MockTask{}
	mockRepo.On("CreateTask", mock.Anything, fakeTask).Return(nil)

	task, err := taskService.CreateTask(ctx, utils.DefaultLogger, fakeTask)
	assert.Nil(t, err)
	assert.NotNil(t, task)
	mockRepo.AssertExpectations(t)
}

func TestCreateTask_Error(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{})
	ctx := context.Background()

	fakeTask := &MockTask{}
	mockRepo.On("CreateTask", mock.Anything, fakeTask).Return(errors.New("db error"))

	task, err := taskService.CreateTask(ctx, utils.DefaultLogger, fakeTask)
	assert.Nil(t, task)
	assert.NotNil(t, err)
	mockRepo.AssertExpectations(t)
}

func TestCreateTaskIfNotAlreadyPlanned_ErrorGet(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{})
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
	mockRepo.On("GetTasks", ctx, rtasks.InputGetTasksFilters{
		Type:  taskType,
		State: tasks.TaskStatePending,
	}).Return(
		[]tasks.ITask{pendingTask}, expectedError,
	)

	newTask := &MockTask{
		Task: tasks.Task{
			ID:   uuid.MakeV4(),
			Type: taskType,
		},
	}

	task, err := taskService.CreateTaskIfNotAlreadyPlanned(ctx, utils.DefaultLogger, newTask)
	assert.Nil(t, task)
	assert.ErrorIs(t, err, expectedError)
	mockRepo.AssertExpectations(t)
}

func TestCreateTaskIfNotAlreadyPlanned_CreatesNew(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{})
	ctx := context.Background()

	mockRepo.On("GetTasks", ctx, rtasks.InputGetTasksFilters{
		Type:  "fake_type",
		State: tasks.TaskStatePending,
	}).Return([]tasks.ITask{}, nil)
	fakeTask := &MockTask{
		Task: tasks.Task{
			ID:   uuid.MakeV4(),
			Type: "fake_type",
		},
	}
	mockRepo.On("CreateTask", mock.Anything, fakeTask).Return(nil)

	task, err := taskService.CreateTaskIfNotAlreadyPlanned(ctx, utils.DefaultLogger, fakeTask)
	assert.NotNil(t, task)
	assert.Nil(t, err)
	mockRepo.AssertExpectations(t)
}

func TestCreateTaskIfNotAlreadyPlanned_CreatesNew_FailedExists(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{})
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
	mockRepo.On("GetTasks", ctx, rtasks.InputGetTasksFilters{
		Type:  taskType,
		State: tasks.TaskStatePending,
	}).Return(tasksList, nil)

	newTask := &MockTask{
		Task: tasks.Task{
			ID:   uuid.MakeV4(),
			Type: taskType,
		},
	}

	task, err := taskService.CreateTaskIfNotAlreadyPlanned(ctx, utils.DefaultLogger, newTask)
	assert.NotNil(t, task)
	assert.Nil(t, err)
	mockRepo.AssertExpectations(t)
}

func TestCreateTaskIfNotAlreadyPlanned_ReturnsExisting(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{})
	ctx := context.Background()

	taskType := "fake_type"
	pendingTask := &MockTask{
		Task: tasks.Task{
			ID:    uuid.MakeV4(),
			Type:  taskType,
			State: tasks.TaskStatePending,
		},
	}
	mockRepo.On("GetTasks", ctx, rtasks.InputGetTasksFilters{
		Type:  taskType,
		State: tasks.TaskStatePending,
	}).Return([]tasks.ITask{
		pendingTask,
	}, nil).Once()

	newTask := &MockTask{
		Task: tasks.Task{
			ID:   uuid.MakeV4(),
			Type: taskType,
		},
	}

	task, err := taskService.CreateTaskIfNotAlreadyPlanned(ctx, utils.DefaultLogger, newTask)
	assert.NotNil(t, task)
	assert.Nil(t, err)
	assert.Equal(t, task.GetID(), pendingTask.GetID())
	mockRepo.AssertExpectations(t)
}

func TestRegisterTasksService(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{})
	mockTasksService := &MockITasksService{}
	mockTasksService.On("GetTaskServiceName").Return("myService")
	mockTasksService.On("GetHandledTasks").Return(map[string]ITask{"demo": nil})

	taskService.RegisterTasksService(mockTasksService)
	assert.Equal(t, taskService.managedPkgs, map[string]bool{"myService": true})
	assert.Equal(t, taskService.managedTasks, map[string]ITask{"demo": nil})
	mockRepo.AssertExpectations(t)
	mockTasksService.AssertExpectations(t)
}

func TestProcessTasksMaintenanceRoutine(t *testing.T) {

	mockRepo := &tasksrepomock.ITasksRepository{}
	taskServiceOpts := Options{
		PurgeDoneTaskOlderThan:   10 * time.Hour,
		PurgeFailedTaskOlderThan: 20 * time.Hour,
		PurgeTasksInterval:       50 * time.Millisecond,
	}
	taskService := NewService(mockRepo, taskServiceOpts)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockRepo.On(
		"PurgeTasks", mock.Anything, taskServiceOpts.PurgeDoneTaskOlderThan, tasks.TaskStateDone,
	).Return(1, nil)
	mockRepo.On(
		"PurgeTasks", mock.Anything, taskServiceOpts.PurgeFailedTaskOlderThan, tasks.TaskStateFailed,
	).Return(0, nil)

	errChan := make(chan error)
	err := taskService.processTasksMaintenanceRoutine(ctx, utils.DefaultLogger, errChan)
	assert.Nil(t, err)
	select {
	case <-time.After(time.Second):
		break
	case err := <-errChan:
		t.Fatalf("unexpected error received: %s", err)
	}
	mockRepo.AssertExpectations(t)
}

func TestProcessTasksMaintenanceRoutine_Error(t *testing.T) {

	mockRepo := &tasksrepomock.ITasksRepository{}
	taskServiceOpts := Options{
		PurgeDoneTaskOlderThan:   10 * time.Hour,
		PurgeFailedTaskOlderThan: 20 * time.Hour,
		PurgeTasksInterval:       50 * time.Millisecond,
	}
	taskService := NewService(mockRepo, taskServiceOpts)

	expectedError := errors.New("some error")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockRepo.On(
		"PurgeTasks", mock.Anything, taskServiceOpts.PurgeDoneTaskOlderThan, tasks.TaskStateDone,
	).Return(0, expectedError).Once()

	errChan := make(chan error)
	err := taskService.processTasksMaintenanceRoutine(ctx, utils.DefaultLogger, errChan)
	assert.Nil(t, err)
	select {
	case <-time.After(time.Second):
		t.Fatalf("timeout while waiting for expected error")
	case err := <-errChan:
		assert.ErrorIs(t, err, expectedError)
	}
	mockRepo.AssertExpectations(t)
}

func TestProcessTasksUpdateStatisticsRoutine(t *testing.T) {

	mockRepo := &tasksrepomock.ITasksRepository{}
	taskServiceOpts := Options{
		CollectMetrics:           true,
		StatisticsUpdateInterval: 350 * time.Millisecond,
	}
	taskService := NewService(mockRepo, taskServiceOpts)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockRepo.On(
		"GetStatistics", mock.Anything,
	).Return(rtasks.Statistics{}, nil).Once()

	errChan := make(chan error)
	err := taskService.processTasksUpdateStatisticsRoutine(ctx, utils.DefaultLogger, errChan)
	assert.Nil(t, err)
	select {
	case <-time.After(500 * time.Millisecond):
		break
	case err := <-errChan:
		t.Fatalf("unexpected error received: %s", err)
	}
	mockRepo.AssertExpectations(t)
}

func TestProcessTasksUpdateStatisticsRoutine_Error(t *testing.T) {

	mockRepo := &tasksrepomock.ITasksRepository{}
	taskServiceOpts := Options{
		CollectMetrics:           true,
		StatisticsUpdateInterval: 350 * time.Millisecond,
	}
	taskService := NewService(mockRepo, taskServiceOpts)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	expectedError := errors.New("some error")

	mockRepo.On(
		"GetStatistics", mock.Anything,
	).Return(nil, expectedError).Once()

	errChan := make(chan error)
	err := taskService.processTasksUpdateStatisticsRoutine(ctx, utils.DefaultLogger, errChan)
	assert.Nil(t, err)
	select {
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timeout while waiting for expected error")
	case err := <-errChan:
		assert.ErrorIs(t, err, expectedError)
	}
	mockRepo.AssertExpectations(t)
}

func TestProcessTasksUpdateStatisticsRoutine_NoMetrics(t *testing.T) {

	mockRepo := &tasksrepomock.ITasksRepository{}
	taskServiceOpts := Options{
		StatisticsUpdateInterval: 350 * time.Millisecond,
	}
	taskService := NewService(mockRepo, taskServiceOpts)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errChan := make(chan error)
	err := taskService.processTasksUpdateStatisticsRoutine(ctx, utils.DefaultLogger, errChan)
	assert.ErrorIs(t, err, ErrMetricsCollectionDisabled)

	mockRepo.AssertExpectations(t)
}

func TestProcessTaskRoutine_Success(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{Workers: 1})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Mock channel push
	mockRepo.On(
		"GetTasksForProcessing", mock.Anything, mock.Anything, mock.Anything,
	).Return(nil).Once()

	errChan := make(chan error)

	err := taskService.processTaskRoutine(ctx, utils.DefaultLogger, errChan)
	assert.Nil(t, err)
	select {
	case <-time.After(time.Second):
		break
	case err := <-errChan:
		t.Fatalf("unexpected error received: %s", err)
	}
	mockRepo.AssertExpectations(t)
}

func TestProcessTaskRoutine_GetTasksError(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{Workers: 1})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockErr := fmt.Errorf("some error")
	mockRepo.On(
		"GetTasksForProcessing", mock.Anything, mock.Anything, mock.Anything,
	).Return(mockErr).Once()

	errChan := make(chan error)

	err := taskService.processTaskRoutine(ctx, utils.DefaultLogger, errChan)
	assert.Nil(t, err)
	select {
	case <-time.After(time.Second):
		t.Fatalf("timeout while waiting for expected error")
	case err := <-errChan:
		assert.ErrorAs(t, err, &mockErr)
	}
	mockRepo.AssertExpectations(t)
}

func TestProcessTaskRoutine_GetTask(t *testing.T) {
	repo := memory.NewTasksRepository()
	taskService := NewService(repo, Options{Workers: 1})

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
	service.On("GetHandledTasks").Return(map[string]ITask{"fake_type": fakeTask}).Once()
	fakeTask.On("Process", mock.Anything, mock.Anything, mock.Anything).Return(mockErr).Once()

	errChan := make(chan error)
	_, err := taskService.CreateTaskIfNotAlreadyPlanned(ctx, utils.DefaultLogger, fakeTask)
	assert.Nil(t, err)

	taskService.RegisterTasksService(service)
	err = taskService.processTaskRoutine(ctx, utils.DefaultLogger, errChan)
	assert.Nil(t, err)
	select {
	case <-time.After(time.Second):
		t.Fatalf("timeout while waiting for expected error")
	case err := <-errChan:
		assert.ErrorAs(t, err, &mockErr)
	}
}

func TestProcessTask(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{
		TasksTimeout: 50 * time.Millisecond,
	})

	service := &MockITasksService{}
	task := &MockTask{
		Task: tasks.Task{ID: uuid.MakeV4(), Type: "test_type"},
	}
	mockRepo.On("UpdateState", mock.Anything, task.GetID(), tasks.TaskStateRunning).Return(nil).Once()
	service.On("GetTaskServiceName").Return("test_type").Once()
	service.On("GetHandledTasks").Return(map[string]ITask{"test_type": task}).Once()
	task.On("GetType").Return("test_type").Once()
	task.On("Process", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	mockRepo.On("UpdateState", mock.Anything, task.GetID(), tasks.TaskStateDone).Return(nil).Once()

	taskService.RegisterTasksService(service)
	err := taskService.processTask(context.Background(), utils.DefaultLogger, task)
	assert.Nil(t, err)
}

func TestProcessTask_Error(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{
		TasksTimeout: 50 * time.Millisecond,
	})

	service := &MockITasksService{}
	task := &MockTask{
		Task: tasks.Task{ID: uuid.MakeV4(), Type: "test_type"},
	}

	expectedErr := errors.New("some error")

	mockRepo.On("UpdateState", mock.Anything, task.GetID(), tasks.TaskStateRunning).Return(nil).Once()
	service.On("GetTaskServiceName").Return("test_type").Once()
	service.On("GetHandledTasks").Return(map[string]ITask{"test_type": task}).Once()
	task.On("GetType").Return("test_type").Once()
	task.On("Process", mock.Anything, mock.Anything, mock.Anything).Return(expectedErr).Once()
	mockRepo.On("UpdateState", mock.Anything, task.GetID(), tasks.TaskStateFailed).Return(nil).Once()

	taskService.RegisterTasksService(service)
	err := taskService._processTask(context.Background(), utils.DefaultLogger, task)
	assert.ErrorIs(t, err, expectedErr)
}

func TestProcessTask_Timeout(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{
		TasksTimeout: 50 * time.Millisecond,
	})

	service := &MockITasksService{}
	task := &MockTask{
		Task: tasks.Task{ID: uuid.MakeV4(), Type: "test_type"},
	}

	mockRepo.On("UpdateState", mock.Anything, task.GetID(), tasks.TaskStateRunning).Return(nil).Once()
	service.On("GetTaskServiceName").Return("test_type").Once()
	service.On("GetHandledTasks").Return(map[string]ITask{"test_type": task}).Once()
	task.On("GetType").Return("test_type").Once()
	task.On("Process", mock.Anything, mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		time.Sleep(100 * time.Millisecond)
	}).Once()
	mockRepo.On("UpdateState", mock.Anything, task.GetID(), tasks.TaskStateFailed).Return(nil).Once()

	taskService.RegisterTasksService(service)
	err := taskService.processTask(context.Background(), utils.DefaultLogger, task)
	assert.ErrorIs(t, err, ErrTaskTimeout)
}

func TestProcessTask_Unmanaged(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{})

	task := &MockTask{
		Task: tasks.Task{
			ID:   uuid.MakeV4(),
			Type: "test_type",
		},
	}
	task.On("GetType").Return("test_type")

	err := taskService._processTask(context.Background(), utils.DefaultLogger, task)
	assert.ErrorIs(t, err, ErrTaskTypeNotManaged)
}

func TestProcessTask_MarkAsRunningFailed(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{
		TasksTimeout: 50 * time.Millisecond,
	})

	service := &MockITasksService{}
	task := &MockTask{
		Task: tasks.Task{ID: uuid.MakeV4(), Type: "test_type"},
	}
	mockRepo.On(
		"UpdateState", mock.Anything, task.GetID(), tasks.TaskStateRunning,
	).Return(errors.New("some error")).Once()
	service.On("GetTaskServiceName").Return("test_type").Once()
	service.On("GetHandledTasks").Return(map[string]ITask{"test_type": task}).Once()
	task.On("GetType").Return("test_type").Once()
	task.On("Process", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	mockRepo.On(
		"UpdateState", mock.Anything, task.GetID(), tasks.TaskStateDone,
	).Return(errors.New("some error")).Once()

	taskService.RegisterTasksService(service)
	err := taskService.processTask(context.Background(), utils.DefaultLogger, task)
	assert.Nil(t, err)
}

func TestProcessTask_Failed_MarkAsRunningFailed(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{
		TasksTimeout: 50 * time.Millisecond,
	})

	service := &MockITasksService{}
	task := &MockTask{
		Task: tasks.Task{ID: uuid.MakeV4(), Type: "test_type"},
	}

	expectedErr := errors.New("some error")

	mockRepo.On(
		"UpdateState", mock.Anything, task.GetID(), tasks.TaskStateRunning,
	).Return(nil).Once()
	service.On("GetTaskServiceName").Return("test_type").Once()
	service.On("GetHandledTasks").Return(map[string]ITask{"test_type": task}).Once()
	task.On("GetType").Return("test_type").Once()
	task.On("Process", mock.Anything, mock.Anything, mock.Anything).Return(expectedErr).Once()
	mockRepo.On(
		"UpdateState", mock.Anything, task.GetID(), tasks.TaskStateFailed,
	).Return(errors.New("some error")).Once()

	taskService.RegisterTasksService(service)
	err := taskService.processTask(context.Background(), utils.DefaultLogger, task)
	assert.ErrorIs(t, err, expectedErr)
}

func TestMarkTaskAs(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{})

	taskID := uuid.MakeV4()
	mockRepo.
		On("UpdateState", mock.Anything, taskID, tasks.TaskStateRunning).
		Return(nil).
		Once()

	err := taskService.markTaskAs(context.Background(), taskID, tasks.TaskStateRunning)
	assert.NoError(t, err)
	mockRepo.AssertExpectations(t)
}

func TestPurgeTasks(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{
		PurgeDoneTaskOlderThan:   time.Second,
		PurgeFailedTaskOlderThan: time.Minute,
	})

	mockRepo.
		On("PurgeTasks", mock.Anything, time.Second, tasks.TaskStateDone).
		Return(2, nil).
		Once()
	mockRepo.
		On("PurgeTasks", mock.Anything, time.Minute, tasks.TaskStateFailed).
		Return(1, nil).
		Once()

	done, failed, err := taskService.purgeTasks(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 2, done)
	assert.Equal(t, 1, failed)
	mockRepo.AssertExpectations(t)
}

func TestPurgeTasks_ErrorOnDonePurge(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{
		PurgeDoneTaskOlderThan: time.Second,
	})

	mockRepo.
		On("PurgeTasks", mock.Anything, time.Second, tasks.TaskStateDone).
		Return(0, errors.New("some error")).
		Once()

	done, failed, err := taskService.purgeTasks(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unable to purge tasks in done state")
	assert.Equal(t, 0, done)
	assert.Equal(t, 0, failed)
	mockRepo.AssertExpectations(t)
}

func TestPurgeTasks_ErrorOnFailedPurge(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{
		PurgeDoneTaskOlderThan:   time.Second,
		PurgeFailedTaskOlderThan: time.Minute,
	})
	mockRepo.
		On("PurgeTasks", mock.Anything, time.Second, tasks.TaskStateDone).
		Return(2, nil).
		Once()

	mockRepo.
		On("PurgeTasks", mock.Anything, time.Minute, tasks.TaskStateFailed).
		Return(0, errors.New("some error")).
		Once()

	done, failed, err := taskService.purgeTasks(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unable to purge tasks in failed state")
	assert.Equal(t, 2, done)
	assert.Equal(t, 0, failed)
	mockRepo.AssertExpectations(t)
}

func TestPurgeTasksInState_Error(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{})

	mockRepo.
		On("PurgeTasks", mock.Anything, time.Minute, tasks.TaskStatePending).
		Return(0, errors.New("some error")).
		Once()

	count, err := taskService.purgeTasksInState(context.Background(), time.Minute, tasks.TaskStatePending)
	assert.Error(t, err)
	assert.Equal(t, 0, count)
	mockRepo.AssertExpectations(t)
}

func TestPurgeTasksInState(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{})

	mockRepo.
		On("PurgeTasks", mock.Anything, time.Minute, tasks.TaskStatePending).
		Return(3, nil).
		Once()

	count, err := taskService.purgeTasksInState(context.Background(), time.Minute, tasks.TaskStatePending)
	assert.NoError(t, err)
	assert.Equal(t, 3, count)
	mockRepo.AssertExpectations(t)
}

func TestUpdateMetrics(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{
		CollectMetrics: true,
	})

	mockRepo.
		On("GetStatistics", mock.Anything).
		Return(rtasks.Statistics{
			tasks.TaskStatePending: 5,
			tasks.TaskStateRunning: 2,
			tasks.TaskStateDone:    10,
			tasks.TaskStateFailed:  1,
		}, nil).
		Once()

	err := taskService.updateMetrics(context.Background())
	assert.NoError(t, err)
	mockRepo.AssertExpectations(t)
}

func TestUpdateMetrics_NoMetrics(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{})

	err := taskService.updateMetrics(context.Background())
	assert.ErrorIs(t, err, ErrMetricsCollectionDisabled)
	mockRepo.AssertExpectations(t)
}

func TestUpdateMetrics_Error(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, Options{
		CollectMetrics: true,
	})

	mockRepo.
		On("GetStatistics", mock.Anything).
		Return(nil, errors.New("some error")).
		Once()

	err := taskService.updateMetrics(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unable to get tasks statistics")
	mockRepo.AssertExpectations(t)
}
