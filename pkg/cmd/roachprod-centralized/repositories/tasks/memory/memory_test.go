// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memory

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	rtasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/assert"
)

func newMockTask(id uuid.UUID, state tasks.TaskState) tasks.ITask {
	now := time.Now()
	task := &tasks.Task{
		ID:               id,
		CreationDatetime: now,
		UpdateDatetime:   now,
		Type:             "MOCK",
		State:            state,
	}
	return task
}

func TestGetTasks(t *testing.T) {
	repo := NewTasksRepository()
	tasksList, err := repo.GetTasks(context.Background(), rtasks.InputGetTasksFilters{})
	assert.NoError(t, err)
	assert.Empty(t, tasksList, "expected non-empty tasks list")
}

func TestGetTask(t *testing.T) {
	repo := NewTasksRepository()
	mockTask := newMockTask(
		uuid.MakeV4(),
		tasks.TaskStatePending,
	)

	err := repo.CreateTask(context.Background(), mockTask)
	assert.NoError(t, err)

	task, err := repo.GetTask(context.Background(), mockTask.GetID())
	assert.NoError(t, err)
	assert.Equal(
		t, mockTask.GetID(), task.GetID(),
		"expected task with ID %v, got %v", mockTask.GetID(), task.GetID(),
	)
}

func TestCreateTask(t *testing.T) {
	repo := NewTasksRepository()
	task := newMockTask(uuid.MakeV4(), tasks.TaskStatePending)
	task.SetState(tasks.TaskStatePending)
	err := repo.CreateTask(context.Background(), task)
	assert.NoError(t, err)
}

func TestUpdateState(t *testing.T) {
	repo := NewTasksRepository()
	id := uuid.MakeV4()
	task := newMockTask(id, tasks.TaskStatePending)
	err := repo.CreateTask(context.Background(), task)
	assert.NoError(t, err)
	err = repo.UpdateState(context.Background(), id, tasks.TaskStateRunning)
	assert.NoError(t, err)
	got, err := repo.GetTask(context.Background(), id)
	assert.NoError(t, err)
	assert.Equal(
		t,
		tasks.TaskStateRunning, got.GetState(),
		"expected state %v, got %v", tasks.TaskStateRunning, got.GetState(),
	)
}

func TestGetStatistics(t *testing.T) {
	repo := NewTasksRepository()
	id := uuid.MakeV4()
	taskState := tasks.TaskStatePending
	task := newMockTask(id, taskState)
	err := repo.CreateTask(context.Background(), task)
	assert.NoError(t, err)
	stats, err := repo.GetStatistics(context.Background())
	assert.NoError(t, err)
	assert.Equal(
		t,
		1, stats[taskState],
		"expected 1 task in state %v, got %v", taskState, stats[taskState],
	)
}

func TestPurgeTasks(t *testing.T) {
	repo := NewTasksRepository()
	id := uuid.MakeV4()
	task := newMockTask(id, tasks.TaskStatePending)
	err := repo.CreateTask(context.Background(), task)
	assert.NoError(t, err)
	time.Sleep(time.Millisecond)
	deleted, err := repo.PurgeTasks(context.Background(), time.Microsecond, tasks.TaskStatePending)
	assert.NoError(t, err)
	assert.Equal(t, 1, deleted, "expected 1 deleted task, got %d", deleted)
}

func TestGetTasksForProcessing(t *testing.T) {
	repo := NewTasksRepository()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	taskChan := make(chan tasks.ITask)
	go func() {
		err := repo.GetTasksForProcessing(ctx, taskChan, uuid.MakeV4())
		assert.NoError(t, err)
	}()

	id := uuid.MakeV4()
	task := newMockTask(id, tasks.TaskStatePending)
	err := repo.CreateTask(ctx, task)
	assert.NoError(t, err)

	select {
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for task %s", id)
	case tsk := <-taskChan:
		t.Logf("Received task with ID %v", tsk.GetID())
		assert.Equal(t, id, tsk.GetID(), "expected task with ID %v, got %v", id, tsk.GetID())
	}
}

func TestGetTasksForProcessingEnqueuesPreviousTasks(t *testing.T) {
	repo := NewTasksRepository()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	id := uuid.MakeV4()
	task := newMockTask(id, tasks.TaskStatePending)
	err := repo.CreateTask(ctx, task)
	assert.NoError(t, err)

	taskChan := make(chan tasks.ITask)
	go func() {
		err := repo.GetTasksForProcessing(ctx, taskChan, uuid.MakeV4())
		assert.NoError(t, err)
	}()

	select {
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for task %s", id)
	case tsk := <-taskChan:
		t.Logf("Received task with ID %v", tsk.GetID())
		assert.Equal(t, id, tsk.GetID(), "expected task with ID %v, got %v", id, tsk.GetID())
	}
}

func TestCreateTaskNonPendingState(t *testing.T) {
	repo := NewTasksRepository()
	id := uuid.MakeV4()
	task := newMockTask(id, tasks.TaskStateRunning)
	if err := repo.CreateTask(context.Background(), task); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case <-repo._tasksQueuedForProcessing:
		t.Fatalf("task with non-pending state should not be enqueued")
	case <-time.After(500 * time.Millisecond):
		// No task should be received
	}
}

func TestGetTaskNotFound(t *testing.T) {
	repo := NewTasksRepository()
	id := uuid.MakeV4()
	_, err := repo.GetTask(context.Background(), id)
	assert.ErrorIs(t, err, rtasks.ErrTaskNotFound)
}

func TestUpdateStateTaskNotFound(t *testing.T) {
	repo := NewTasksRepository()
	id := uuid.MakeV4()
	err := repo.UpdateState(context.Background(), id, tasks.TaskStateRunning)
	assert.ErrorIs(t, err, rtasks.ErrTaskNotFound)
}

func TestPurgeTasksNoMatch(t *testing.T) {
	repo := NewTasksRepository()
	id := uuid.MakeV4()
	task := newMockTask(id, tasks.TaskStatePending)
	err := repo.CreateTask(context.Background(), task)
	assert.NoError(t, err)
	deleted, err := repo.PurgeTasks(context.Background(), time.Microsecond, tasks.TaskStateRunning)
	assert.NoError(t, err)
	assert.Equal(t, 0, deleted, "expected 0 deleted tasks, got %d", deleted)
}

func TestGetTasksForProcessingNonPendingTask(t *testing.T) {
	repo := NewTasksRepository()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	taskChan := make(chan tasks.ITask)
	go func() {
		err := repo.GetTasksForProcessing(ctx, taskChan, uuid.MakeV4())
		assert.NoError(t, err)
	}()

	id := uuid.MakeV4()
	task := newMockTask(id, tasks.TaskStateRunning)
	err := repo.CreateTask(ctx, task)
	assert.NoError(t, err)

	select {
	case tsk := <-taskChan:
		t.Fatalf("expected no task, but got task with ID %v", tsk.GetID())
	case <-time.After(500 * time.Millisecond):
		// No task should be received
	}
}
