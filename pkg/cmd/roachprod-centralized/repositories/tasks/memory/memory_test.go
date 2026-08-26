// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	rtasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/assert"
)

func newMockTask(id uuid.UUID, state tasks.TaskState) tasks.ITask {
	now := timeutil.Now()
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
	tasksList, totalCount, err := repo.GetTasks(context.Background(), logger.DefaultLogger, *filters.NewFilterSet())
	assert.NoError(t, err)
	assert.Equal(t, 0, totalCount, "expected total count to be 0")
	assert.Empty(t, tasksList, "expected empty tasks list")
}

func TestGetTask(t *testing.T) {
	repo := NewTasksRepository()
	mockTask := newMockTask(
		uuid.MakeV4(),
		tasks.TaskStatePending,
	)

	err := repo.CreateTask(context.Background(), logger.DefaultLogger, mockTask)
	assert.NoError(t, err)

	task, err := repo.GetTask(context.Background(), logger.DefaultLogger, mockTask.GetID())
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
	err := repo.CreateTask(context.Background(), logger.DefaultLogger, task)
	assert.NoError(t, err)
}

func TestUpdateState(t *testing.T) {
	repo := NewTasksRepository()
	id := uuid.MakeV4()
	task := newMockTask(id, tasks.TaskStatePending)
	err := repo.CreateTask(context.Background(), logger.DefaultLogger, task)
	assert.NoError(t, err)
	err = repo.UpdateState(context.Background(), logger.DefaultLogger, id, tasks.TaskStateRunning)
	assert.NoError(t, err)
	got, err := repo.GetTask(context.Background(), logger.DefaultLogger, id)
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
	err := repo.CreateTask(context.Background(), logger.DefaultLogger, task)
	assert.NoError(t, err)
	stats, err := repo.GetStatistics(context.Background(), logger.DefaultLogger)
	assert.NoError(t, err)
	assert.NotNil(t, stats[taskState], "expected stats for state %v", taskState)
	assert.Equal(
		t,
		1, stats[taskState]["MOCK"],
		"expected 1 task of type MOCK in state %v, got %v", taskState, stats[taskState]["MOCK"],
	)
}

func TestPurgeTasks(t *testing.T) {
	repo := NewTasksRepository()
	id := uuid.MakeV4()
	task := newMockTask(id, tasks.TaskStatePending)
	err := repo.CreateTask(context.Background(), logger.DefaultLogger, task)
	assert.NoError(t, err)
	// No sleep needed - we're testing in-memory state
	deleted, err := repo.PurgeTasks(context.Background(), logger.DefaultLogger, 0, tasks.TaskStatePending)
	assert.NoError(t, err)
	assert.Equal(t, 1, deleted, "expected 1 deleted task, got %d", deleted)
}

func TestGetTasksForProcessing(t *testing.T) {
	repo := NewTasksRepository()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	taskChan := make(chan tasks.ITask)
	consumerDone := make(chan error, 1)
	go func() {
		consumerDone <- repo.GetTasksForProcessing(
			ctx, logger.DefaultLogger, taskChan, "test-instance",
		)
	}()

	id := uuid.MakeV4()
	task := newMockTask(id, tasks.TaskStatePending)
	err := repo.CreateTask(ctx, logger.DefaultLogger, task)
	assert.NoError(t, err)

	tsk := receiveTask(t, taskChan, id)
	assert.Equal(t, id, tsk.GetID(), "expected task with ID %v, got %v", id, tsk.GetID())
	cancel()
	requireTaskConsumerStopped(t, consumerDone)
}

func TestGetTasksForProcessingEnqueuesPreviousTasks(t *testing.T) {
	repo := NewTasksRepository()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	id := uuid.MakeV4()
	task := newMockTask(id, tasks.TaskStatePending)
	err := repo.CreateTask(ctx, logger.DefaultLogger, task)
	assert.NoError(t, err)

	taskChan := make(chan tasks.ITask)
	consumerDone := make(chan error, 1)
	go func() {
		consumerDone <- repo.GetTasksForProcessing(
			ctx, logger.DefaultLogger, taskChan, "test-instance",
		)
	}()

	tsk := receiveTask(t, taskChan, id)
	assert.Equal(t, id, tsk.GetID(), "expected task with ID %v, got %v", id, tsk.GetID())
	cancel()
	requireTaskConsumerStopped(t, consumerDone)
}

func TestGetTasksForProcessingConcurrentCreates(t *testing.T) {
	const taskCount = 100

	repo := NewTasksRepository()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Buffer every expected task so CreateTask can finish before this test
	// starts draining the channel.
	taskChan := make(chan tasks.ITask, taskCount)
	consumerDone := make(chan error, 1)
	go func() {
		consumerDone <- repo.GetTasksForProcessing(
			ctx, logger.DefaultLogger, taskChan, "test-instance",
		)
	}()
	requireTaskConsumerStarted(t, repo)

	ids := make(map[uuid.UUID]struct{}, taskCount)
	start := make(chan struct{})
	createErrs := make(chan error, taskCount)
	var createWG sync.WaitGroup
	for i := 0; i < taskCount; i++ {
		id := uuid.MakeV4()
		ids[id] = struct{}{}
		createWG.Add(1)
		go func() {
			defer createWG.Done()
			<-start
			createErrs <- repo.CreateTask(
				ctx, logger.DefaultLogger, newMockTask(id, tasks.TaskStatePending),
			)
		}()
	}
	close(start)
	createWG.Wait()
	close(createErrs)
	for err := range createErrs {
		assert.NoError(t, err)
	}

	for i := 0; i < taskCount; i++ {
		task := receiveTask(t, taskChan, uuid.Nil)
		_, expected := ids[task.GetID()]
		assert.True(t, expected, "unexpected or duplicate task %s", task.GetID())
		delete(ids, task.GetID())
	}
	assert.Empty(t, ids)

	cancel()
	requireTaskConsumerStopped(t, consumerDone)
}

func TestGetTasksForProcessingBackpressure(t *testing.T) {
	repo := NewTasksRepository()
	for i := 0; i < 3; i++ {
		task := newMockTask(uuid.MakeV4(), tasks.TaskStatePending)
		if err := repo.CreateTask(
			context.Background(), logger.DefaultLogger, task,
		); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Production also uses an unbuffered task channel.
	taskChan := make(chan tasks.ITask)
	consumerDone := make(chan error, 1)
	go func() {
		consumerDone <- repo.GetTasksForProcessing(
			ctx, logger.DefaultLogger, taskChan, "test",
		)
	}()

	// Model the first action of a single worker. The repository must not hold
	// its lock while it waits for the worker to accept later tasks.
	workerDone := make(chan error, 1)
	go func() {
		task := <-taskChan
		workerDone <- repo.UpdateState(
			ctx, logger.DefaultLogger, task.GetID(), tasks.TaskStateRunning,
		)
	}()

	select {
	case err := <-workerDone:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("worker deadlocked updating the first task")
	}
	cancel()
	requireTaskConsumerStopped(t, consumerDone)
}

func TestCreateTaskNonPendingState(t *testing.T) {
	repo := NewTasksRepository()
	id := uuid.MakeV4()
	task := newMockTask(id, tasks.TaskStateRunning)
	if err := repo.CreateTask(context.Background(), logger.DefaultLogger, task); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case <-repo._tasksQueuedForProcessing:
		t.Fatalf("task with non-pending state should not be enqueued")
	case <-time.After(10 * time.Millisecond):
		// No task should be received
	}
}

func TestGetTaskNotFound(t *testing.T) {
	repo := NewTasksRepository()
	id := uuid.MakeV4()
	_, err := repo.GetTask(context.Background(), logger.DefaultLogger, id)
	assert.ErrorIs(t, err, rtasks.ErrTaskNotFound)
}

func TestUpdateStateTaskNotFound(t *testing.T) {
	repo := NewTasksRepository()
	id := uuid.MakeV4()
	err := repo.UpdateState(context.Background(), logger.DefaultLogger, id, tasks.TaskStateRunning)
	assert.ErrorIs(t, err, rtasks.ErrTaskNotFound)
}

func TestPurgeTasksNoMatch(t *testing.T) {
	repo := NewTasksRepository()
	id := uuid.MakeV4()
	task := newMockTask(id, tasks.TaskStatePending)
	err := repo.CreateTask(context.Background(), logger.DefaultLogger, task)
	assert.NoError(t, err)
	deleted, err := repo.PurgeTasks(context.Background(), logger.DefaultLogger, time.Microsecond, tasks.TaskStateRunning)
	assert.NoError(t, err)
	assert.Equal(t, 0, deleted, "expected 0 deleted tasks, got %d", deleted)
}

func TestGetTasksForProcessingNonPendingTask(t *testing.T) {
	repo := NewTasksRepository()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	taskChan := make(chan tasks.ITask)
	consumerDone := make(chan error, 1)
	go func() {
		consumerDone <- repo.GetTasksForProcessing(
			ctx, logger.DefaultLogger, taskChan, "test-instance",
		)
	}()

	id := uuid.MakeV4()
	task := newMockTask(id, tasks.TaskStateRunning)
	err := repo.CreateTask(ctx, logger.DefaultLogger, task)
	assert.NoError(t, err)

	select {
	case tsk := <-taskChan:
		t.Fatalf("expected no task, but got task with ID %v", tsk.GetID())
	case <-time.After(10 * time.Millisecond):
		// No task should be received
	}
	cancel()
	requireTaskConsumerStopped(t, consumerDone)
}

func receiveTask(t *testing.T, taskChan <-chan tasks.ITask, id uuid.UUID) tasks.ITask {
	t.Helper()
	timer := time.NewTimer(testutils.SucceedsSoonDuration())
	defer timer.Stop()
	select {
	case <-timer.C:
		t.Fatalf("timed out waiting for task %s", id)
		return nil
	case task := <-taskChan:
		t.Logf("received task with ID %v", task.GetID())
		return task
	}
}

func requireTaskConsumerStopped(t *testing.T, consumerDone <-chan error) {
	t.Helper()
	timer := time.NewTimer(testutils.SucceedsSoonDuration())
	defer timer.Stop()
	select {
	case <-timer.C:
		t.Fatal("timed out waiting for task consumer to stop")
	case err := <-consumerDone:
		assert.NoError(t, err)
	}
}

func requireTaskConsumerStarted(t *testing.T, repo *MemTasksRepo) {
	t.Helper()
	testutils.SucceedsSoon(t, func() error {
		if !repo._fireEvents.Load() {
			return errors.New("task consumer has not started")
		}
		return nil
	})
}
