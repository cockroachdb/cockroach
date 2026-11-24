// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusters

import (
	"testing"

	sclustertasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters/tasks"
	stasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestService_GetTaskServiceName(t *testing.T) {
	service, store, _, healthService := createTestService(t, Options{})

	name := service.GetTaskServiceName()
	assert.Equal(t, "clusters", name)

	store.AssertExpectations(t)
	healthService.AssertExpectations(t)
}

func TestService_GetHandledTasks(t *testing.T) {
	service, store, _, healthService := createTestService(t, Options{})

	handledTasks := service.GetHandledTasks()

	assert.NotNil(t, handledTasks)
	assert.Len(t, handledTasks, 1)
	assert.Contains(t, handledTasks, string(sclustertasks.ClustersTaskSync))

	// Verify the task is properly configured
	task, ok := handledTasks[string(sclustertasks.ClustersTaskSync)]
	assert.True(t, ok)
	assert.NotNil(t, task)

	// Verify it's a TaskSync with the service set
	syncTask, ok := task.(*sclustertasks.TaskSync)
	assert.True(t, ok, "expected *TaskSync")
	assert.NotNil(t, syncTask.Service, "Service should be set on TaskSync")

	store.AssertExpectations(t)
	healthService.AssertExpectations(t)
}

func TestService_CreateTaskInstance(t *testing.T) {
	service, store, _, healthService := createTestService(t, Options{})

	tests := []struct {
		name      string
		taskType  string
		wantErr   error
		checkTask func(*testing.T, interface{})
	}{
		{
			name:     "ClustersTaskSync",
			taskType: string(sclustertasks.ClustersTaskSync),
			checkTask: func(t *testing.T, task interface{}) {
				syncTask, ok := task.(*sclustertasks.TaskSync)
				require.True(t, ok, "expected *TaskSync")
				assert.Equal(t, string(sclustertasks.ClustersTaskSync), syncTask.Type)
				assert.NotNil(t, syncTask.Service, "Service should be set")
			},
		},
		{
			name:     "unknown task type",
			taskType: "unknown-task",
			wantErr:  stasks.ErrUnknownTaskType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task, err := service.CreateTaskInstance(tt.taskType)

			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				assert.Nil(t, task)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, task)

			if tt.checkTask != nil {
				tt.checkTask(t, task)
			}
		})
	}

	store.AssertExpectations(t)
	healthService.AssertExpectations(t)
}

func TestService_TaskRegistration_Integration(t *testing.T) {
	// This test verifies that the task registration methods work together correctly
	service, store, _, healthService := createTestService(t, Options{})

	// Get service name
	serviceName := service.GetTaskServiceName()
	assert.Equal(t, "clusters", serviceName)

	// Get handled tasks
	handledTasks := service.GetHandledTasks()
	assert.Len(t, handledTasks, 1)

	// Create a task instance for each handled task type
	for taskType := range handledTasks {
		task, err := service.CreateTaskInstance(taskType)
		require.NoError(t, err, "should be able to create instance for handled task type %s", taskType)
		require.NotNil(t, task)

		// Verify the task type matches
		syncTask, ok := task.(*sclustertasks.TaskSync)
		require.True(t, ok)
		assert.Equal(t, taskType, syncTask.Type)
	}

	store.AssertExpectations(t)
	healthService.AssertExpectations(t)
}
