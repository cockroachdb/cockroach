// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memory

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	rtasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"golang.org/x/exp/maps"
)

// MemTasksRepo is an in-memory implementation of the tasks repository.
// The task distribution is handled by a channel, so this is only compatible
// with a single instance of the API.
type MemTasksRepo struct {
	tasks map[uuid.UUID]tasks.ITask
	lock  syncutil.Mutex

	_fireEvents               atomic.Bool
	_tasksQueuedForProcessing chan uuid.UUID
}

// NewTasksRepository creates a new in-memory tasks repository.
func NewTasksRepository() *MemTasksRepo {
	return &MemTasksRepo{
		tasks:                     make(map[uuid.UUID]tasks.ITask),
		_tasksQueuedForProcessing: make(chan uuid.UUID),
	}
}

// GetTasks returns all tasks in from the in-memory tasks map.
func (s *MemTasksRepo) GetTasks(
	ctx context.Context, filters rtasks.InputGetTasksFilters,
) ([]tasks.ITask, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// If no filters are specified, return all tasks.
	if filters.State == "" && filters.Type == "" {
		return maps.Values(s.tasks), nil
	}

	var filteredTasks []tasks.ITask
	for _, task := range s.tasks {
		if filters.State != "" && task.GetState() != filters.State {
			continue
		}
		if filters.Type != "" && task.GetType() != filters.Type {
			continue
		}
		filteredTasks = append(filteredTasks, task)
	}

	return filteredTasks, nil
}

// GetTask returns a task from the in-memory tasks map.
func (s *MemTasksRepo) GetTask(ctx context.Context, taskID uuid.UUID) (tasks.ITask, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if t, ok := s.tasks[taskID]; !ok {
		return nil, rtasks.ErrTaskNotFound
	} else {
		return t, nil
	}
}

// CreateTask creates a task in the in-memory tasks map.
// If the task is created in state pending, it is also queued for processing
// in an unbuffered channel. As this is an in-memory implementation the data is
// not persisted and the unbuffered channel is not a concern.
func (s *MemTasksRepo) CreateTask(ctx context.Context, task tasks.ITask) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.tasks[task.GetID()] = task

	s.maybeEnqueueTaskForProcessing(task)

	return nil
}

// UpdateState updates the state of a task in the in-memory tasks map.
func (s *MemTasksRepo) UpdateState(
	ctx context.Context, taskID uuid.UUID, state tasks.TaskState,
) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.tasks[taskID]; !ok {
		return rtasks.ErrTaskNotFound
	}
	s.tasks[taskID].SetState(state)
	return nil
}

// GetStatistics returns the statistics of the tasks in the in-memory tasks map.
func (s *MemTasksRepo) GetStatistics(ctx context.Context) (rtasks.Statistics, error) {

	s.lock.Lock()
	defer s.lock.Unlock()

	stats := make(rtasks.Statistics)
	for _, task := range s.tasks {
		stats[task.GetState()]++
	}

	return stats, nil

}

// PurgeTasks deletes tasks from the in-memory tasks map that are
// from the specified state and older than the specified duration.
func (s *MemTasksRepo) PurgeTasks(
	ctx context.Context, olderThan time.Duration, state tasks.TaskState,
) (int, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	deleted := 0
	for _, task := range s.tasks {

		// We only delete tasks that have been processed
		if task.GetState() != state {
			continue
		}

		if timeutil.Since(task.GetUpdateDatetime()) > olderThan {
			delete(s.tasks, task.GetID())
			deleted++
		}
	}

	return deleted, nil
}

// GetTasksForProcessing returns tasks that are pending. The tasks are sent
// to the specified taskChan channel. This function is blocking and will wait
// until the context is done.
func (s *MemTasksRepo) GetTasksForProcessing(
	ctx context.Context, taskChan chan<- tasks.ITask, consumerID uuid.UUID,
) error {

	s._fireEvents.Store(true)

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		for {
			select {
			case <-ctx.Done():
				wg.Done()
				return
			case taskID := <-s._tasksQueuedForProcessing:
				taskChan <- s.tasks[taskID]
			}
		}
	}()

	s.enqueueExistingTasksForProcessing()

	wg.Wait()
	return nil
}

// enqueueExistingTasksForProcessing queues all pending tasks for processing.
func (s *MemTasksRepo) enqueueExistingTasksForProcessing() {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, task := range s.tasks {
		s.maybeEnqueueTaskForProcessing(task)
	}
}

// maybeEnqueueTaskForProcessing queues a task for processing if it is pending
// and the _fireEvents flag is set (aka GetTasksForProcessing() was called).
func (s *MemTasksRepo) maybeEnqueueTaskForProcessing(task tasks.ITask) {
	if !s._fireEvents.Load() {
		return
	}
	if task.GetState() != tasks.TaskStatePending {
		return
	}
	s._tasksQueuedForProcessing <- task.GetID()
}
