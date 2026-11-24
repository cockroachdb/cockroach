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

func TestSchedulePurgeTaskRoutine(t *testing.T) {

	mockRepo := &tasksrepomock.ITasksRepository{}
	taskServiceOpts := types.Options{
		PurgeDoneTaskOlderThan:   10 * time.Hour,
		PurgeFailedTaskOlderThan: 20 * time.Hour,
		PurgeTasksInterval:       1 * time.Millisecond,
	}
	taskService := NewService(mockRepo, "test-instance", taskServiceOpts)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Expect GetTasks to check for recent purge tasks
	mockRepo.On(
		"GetTasks", mock.Anything, mock.Anything, mock.Anything,
	).Return([]tasks.ITask{}, 0, nil)

	// Expect CreateTask to be called to schedule the purge task
	mockRepo.On(
		"CreateTask", mock.Anything, mock.Anything, mock.Anything,
	).Return(nil)

	errChan := make(chan error)
	err := taskService.schedulePurgeTaskRoutine(ctx, logger.DefaultLogger, errChan)
	assert.Nil(t, err)
	select {
	case <-time.After(50 * time.Millisecond):
		break
	case err := <-errChan:
		t.Fatalf("unexpected error received: %s", err)
	}
	mockRepo.AssertExpectations(t)
}

func TestSchedulePurgeTaskRoutine_Error(t *testing.T) {

	mockRepo := &tasksrepomock.ITasksRepository{}
	taskServiceOpts := types.Options{
		PurgeDoneTaskOlderThan:   10 * time.Hour,
		PurgeFailedTaskOlderThan: 20 * time.Hour,
		PurgeTasksInterval:       1 * time.Millisecond,
	}
	taskService := NewService(mockRepo, "test-instance", taskServiceOpts)

	expectedError := errors.New("some error")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockRepo.On(
		"GetTasks", mock.Anything, mock.Anything, mock.Anything,
	).Return(nil, 0, expectedError)

	errChan := make(chan error)
	err := taskService.schedulePurgeTaskRoutine(ctx, logger.DefaultLogger, errChan)
	assert.Nil(t, err)
	select {
	case <-time.After(50 * time.Millisecond):
		t.Fatalf("timeout while waiting for expected error")
	case err := <-errChan:
		assert.ErrorIs(t, err, expectedError)
	}
	mockRepo.AssertExpectations(t)
}
