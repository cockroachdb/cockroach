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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestPurgeTasks(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{
		PurgeDoneTaskOlderThan:   time.Second,
		PurgeFailedTaskOlderThan: time.Minute,
	})

	mockRepo.
		On("PurgeTasks", mock.Anything, mock.Anything, time.Second, tasks.TaskStateDone).
		Return(2, nil).
		Once()
	mockRepo.
		On("PurgeTasks", mock.Anything, mock.Anything, time.Minute, tasks.TaskStateFailed).
		Return(1, nil).
		Once()

	done, failed, err := taskService.PurgeTasks(context.Background(), logger.DefaultLogger)
	assert.NoError(t, err)
	assert.Equal(t, 2, done)
	assert.Equal(t, 1, failed)
	mockRepo.AssertExpectations(t)
}

func TestPurgeTasks_ErrorOnDonePurge(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{
		PurgeDoneTaskOlderThan: time.Second,
	})

	mockRepo.
		On("PurgeTasks", mock.Anything, mock.Anything, time.Second, tasks.TaskStateDone).
		Return(0, errors.New("some error")).
		Once()

	done, failed, err := taskService.PurgeTasks(context.Background(), logger.DefaultLogger)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unable to purge tasks in done state")
	assert.Equal(t, 0, done)
	assert.Equal(t, 0, failed)
	mockRepo.AssertExpectations(t)
}

func TestPurgeTasks_ErrorOnFailedPurge(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{
		PurgeDoneTaskOlderThan:   time.Second,
		PurgeFailedTaskOlderThan: time.Minute,
	})
	mockRepo.
		On("PurgeTasks", mock.Anything, mock.Anything, time.Second, tasks.TaskStateDone).
		Return(2, nil).
		Once()

	mockRepo.
		On("PurgeTasks", mock.Anything, mock.Anything, time.Minute, tasks.TaskStateFailed).
		Return(0, errors.New("some error")).
		Once()

	done, failed, err := taskService.PurgeTasks(context.Background(), logger.DefaultLogger)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unable to purge tasks in failed state")
	assert.Equal(t, 2, done)
	assert.Equal(t, 0, failed)
	mockRepo.AssertExpectations(t)
}

func TestPurgeTasksInState_Error(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{})

	mockRepo.
		On("PurgeTasks", mock.Anything, mock.Anything, time.Minute, tasks.TaskStatePending).
		Return(0, errors.New("some error")).
		Once()

	count, err := taskService.purgeTasksInState(context.Background(), logger.DefaultLogger, time.Minute, tasks.TaskStatePending)
	assert.Error(t, err)
	assert.Equal(t, 0, count)
	mockRepo.AssertExpectations(t)
}

func TestPurgeTasksInState(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{})

	mockRepo.
		On("PurgeTasks", mock.Anything, mock.Anything, time.Minute, tasks.TaskStatePending).
		Return(3, nil).
		Once()

	count, err := taskService.purgeTasksInState(context.Background(), logger.DefaultLogger, time.Minute, tasks.TaskStatePending)
	assert.NoError(t, err)
	assert.Equal(t, 3, count)
	mockRepo.AssertExpectations(t)
}
