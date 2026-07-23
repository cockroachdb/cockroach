// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"testing"

	tasksrepomock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks/mocks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/types"
	"github.com/stretchr/testify/assert"
)

func TestRegisterTasksService(t *testing.T) {
	mockRepo := &tasksrepomock.ITasksRepository{}
	taskService := NewService(mockRepo, "test-instance", types.Options{})
	mockTasksService := &MockITasksService{}
	mockTasksService.On("GetTaskServiceName").Return("myService")
	mockTasksService.On("GetHandledTasks").Return(map[string]types.ITask{"demo": nil})

	taskService.RegisterTasksService(mockTasksService)
	assert.Equal(t, taskService.managedPkgs, map[string]bool{"myService": true})
	assert.Equal(t, taskService.managedTasks, map[string]types.ITask{"demo": nil})
	mockRepo.AssertExpectations(t)
	mockTasksService.AssertExpectations(t)
}
