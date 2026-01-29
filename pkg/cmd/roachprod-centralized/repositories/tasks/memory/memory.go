// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memory

import (
	"context"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	rtasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks"
	filters "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	memoryfilters "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/memory"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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

// GetTasks returns all tasks from the in-memory tasks map, filtered and paginated, with total count.
func (s *MemTasksRepo) GetTasks(
	ctx context.Context, l *logger.Logger, filterSet filtertypes.FilterSet,
) ([]tasks.ITask, int, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	//  1. Apply filters
	evaluator := filters.NewMemoryFilterEvaluatorWithTypeHint(
		reflect.TypeOf(tasks.Task{}),
	)
	var filteredTasks []tasks.ITask

	for _, task := range s.tasks {
		// If no filters, include all tasks
		if filterSet.IsEmpty() {
			filteredTasks = append(filteredTasks, task)
			continue
		}

		matches, err := evaluator.Evaluate(task, &filterSet)
		if err != nil {
			l.Error(
				"Error filtering task, continuing with other tasks",
				slog.String("task_id", task.GetID().String()),
				slog.Any("error", err),
			)
			continue
		}
		if matches {
			filteredTasks = append(filteredTasks, task)
		}
	}

	totalCount := len(filteredTasks)

	// 2. Apply sorting (with creation_datetime as default)
	_ = memoryfilters.SortByField(filteredTasks, filterSet.Sort, "creation_datetime")

	// 3. Apply pagination
	filteredTasks = memoryfilters.ApplyPagination(filteredTasks, filterSet.Pagination)

	return filteredTasks, totalCount, nil
}

// GetTask returns a task from the in-memory tasks map.
func (s *MemTasksRepo) GetTask(
	ctx context.Context, l *logger.Logger, taskID uuid.UUID,
) (tasks.ITask, error) {
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
func (s *MemTasksRepo) CreateTask(ctx context.Context, l *logger.Logger, task tasks.ITask) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.tasks[task.GetID()] = task

	s.maybeEnqueueTaskForProcessing(task)

	return nil
}

// UpdateState updates the state of a task in the in-memory tasks map.
func (s *MemTasksRepo) UpdateState(
	ctx context.Context, l *logger.Logger, taskID uuid.UUID, state tasks.TaskState,
) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.tasks[taskID]; !ok {
		return rtasks.ErrTaskNotFound
	}
	s.tasks[taskID].SetState(state)
	return nil
}

// UpdateError sets the error message for a task.
func (s *MemTasksRepo) UpdateError(
	ctx context.Context, l *logger.Logger, taskID uuid.UUID, errorMsg string,
) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.tasks[taskID]; !ok {
		return rtasks.ErrTaskNotFound
	}
	s.tasks[taskID].SetError(errorMsg)
	return nil
}

// GetStatistics returns the statistics of the tasks in the in-memory tasks map.
func (s *MemTasksRepo) GetStatistics(
	ctx context.Context, l *logger.Logger,
) (rtasks.Statistics, error) {

	s.lock.Lock()
	defer s.lock.Unlock()

	stats := make(rtasks.Statistics)
	for _, task := range s.tasks {
		state := task.GetState()
		taskType := task.GetType()

		// Initialize nested map if needed
		if stats[state] == nil {
			stats[state] = make(map[string]int)
		}

		stats[state][taskType]++
	}

	return stats, nil

}

// PurgeTasks deletes tasks from the in-memory tasks map that are
// from the specified state and older than the specified duration.
func (s *MemTasksRepo) PurgeTasks(
	ctx context.Context, l *logger.Logger, olderThan time.Duration, state tasks.TaskState,
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
	ctx context.Context, l *logger.Logger, taskChan chan<- tasks.ITask, instanceID string,
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

// GetMostRecentCompletedTaskOfType returns the most recently completed task of the given type.
func (s *MemTasksRepo) GetMostRecentCompletedTaskOfType(
	ctx context.Context, l *logger.Logger, taskType string,
) (tasks.ITask, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	var mostRecent tasks.ITask
	var mostRecentTime time.Time

	for _, task := range s.tasks {
		if task.GetType() == taskType && task.GetState() == tasks.TaskStateDone {
			if mostRecent == nil || task.GetUpdateDatetime().After(mostRecentTime) {
				mostRecent = task
				mostRecentTime = task.GetUpdateDatetime()
			}
		}
	}

	return mostRecent, nil
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
