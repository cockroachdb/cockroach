// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	tasksrepo "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	PROMETHEUS_NAMESPACE = "roachprod"
)

var (
	// ErrTaskNotFound is the error returned when a task is not found.
	ErrTaskNotFound = utils.NewPublicError(fmt.Errorf("task not found"))
	// ErrTaskTypeNotManaged is the error returned for unmanaged task types.
	ErrTaskTypeNotManaged = fmt.Errorf("task type not managed")
	// ErrTaskTimeout is the error returned when a task processing times out.
	ErrTaskTimeout = fmt.Errorf("task processing timeout")
	// ErrShutdownTimeout is the error returned when the service shutdown times out.
	ErrShutdownTimeout = fmt.Errorf("service shutdown timeout")

	// DefaultTasksTimeout is the default timeout for tasks.
	DefaultTasksTimeout = 30 * time.Second
	// DefaultTaksWorkers is the default number of workers processing tasks.
	DefaultTaksWorkers = 1
)

// IService is the interface for the tasks service.
type IService interface {
	GetTasks(context.Context, *utils.Logger, InputGetAllTasksDTO) ([]tasks.ITask, error)
	GetTask(context.Context, *utils.Logger, InputGetTaskDTO) (tasks.ITask, error)
	CreateTask(context.Context, *utils.Logger, tasks.ITask) (tasks.ITask, error)
	CreateTaskIfNotAlreadyPlanned(context.Context, *utils.Logger, tasks.ITask) (tasks.ITask, error)
	RegisterTasksService(ITasksService)
}

// Service is the implementation of the tasks service.
type Service struct {
	options Options

	consumerID uuid.UUID
	store      tasksrepo.ITasksRepository
	metrics    *metrics

	managedPkgs  map[string]bool
	managedTasks map[string]ITask

	backgroundJobsCancelFunc context.CancelFunc
	backgroundJobsWg         *sync.WaitGroup
}

// Options are the options for the tasks service.
type Options struct {
	// Number of workers that process the tasks
	Workers int

	// CollectMetrics is a flag to enable metrics collection.
	CollectMetrics bool

	// TasksTimeout is the timeout for tasks.
	TasksTimeout time.Duration

	// PurgeDoneTaskOlderThan is the duration after which tasks in done state
	// are purged from the repository.
	PurgeDoneTaskOlderThan time.Duration
	// PurgeFailedTaskOlderThan is the duration after which tasks in failed
	// state are purged from the repository.
	PurgeFailedTaskOlderThan time.Duration
	// PurgeTaskOlderThan is the value for how often tasks are purged from the
	// repository.
	PurgeTasksInterval time.Duration
	// StatisticsUpdateInterval is the value for how often the tasks statistics
	// are updated.
	StatisticsUpdateInterval time.Duration
}

// metrics are the metrics for the tasks service.
type metrics struct {
	processedTasksProcessed prometheus.Counter
	totalTasksPending       prometheus.Gauge
	totalTasksRunning       prometheus.Gauge
	totalTasksDone          prometheus.Gauge
	totalTasksFailed        prometheus.Gauge
}

// NewService creates a new tasks service.
func NewService(store tasksrepo.ITasksRepository, options Options) *Service {

	consumerID := uuid.MakeV4()

	if options.Workers == 0 {
		options.Workers = DefaultTaksWorkers
	}
	if options.TasksTimeout == 0 {
		options.TasksTimeout = DefaultTasksTimeout
	}
	if options.PurgeDoneTaskOlderThan == 0 {
		options.PurgeDoneTaskOlderThan = DefaultPurgeDoneTaskOlderThan
	}
	if options.PurgeFailedTaskOlderThan == 0 {
		options.PurgeFailedTaskOlderThan = DefaultPurgeFailedTaskOlderThan
	}
	if options.PurgeTasksInterval == 0 {
		options.PurgeTasksInterval = DefaultPurgeTasksInterval
	}
	if options.StatisticsUpdateInterval == 0 {
		options.StatisticsUpdateInterval = DefaultStatisticsUpdateInterval
	}

	s := &Service{
		options:          options,
		consumerID:       consumerID,
		store:            store,
		managedPkgs:      make(map[string]bool),
		managedTasks:     make(map[string]ITask),
		backgroundJobsWg: &sync.WaitGroup{},
	}

	if options.CollectMetrics {
		s.metrics = newMetrics(consumerID)
	}

	return s
}

// newMetrics creates a new Prometheus metrics instance.
func newMetrics(consumerID uuid.UUID) *metrics {

	promGlobalLabels := prometheus.Labels{
		"consumer": consumerID.String(),
	}

	return &metrics{
		// Prometheus counter of tasks processed
		processedTasksProcessed: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "processed_total",
			Help:        "The total number of processed tasks",
			Namespace:   PROMETHEUS_NAMESPACE,
			ConstLabels: promGlobalLabels,
		}),
		// Prometheus gauges of pending tasks in the database
		totalTasksPending: promauto.NewGauge(prometheus.GaugeOpts{
			Name:        string(tasks.TaskStatePending),
			Help:        "The number of tasks to be processed in the database",
			Namespace:   PROMETHEUS_NAMESPACE,
			ConstLabels: promGlobalLabels,
		}),
		// Prometheus gauges of running tasks in the database
		totalTasksRunning: promauto.NewGauge(prometheus.GaugeOpts{
			Name:        string(tasks.TaskStateRunning),
			Help:        "The number of tasks being processed in the database",
			Namespace:   PROMETHEUS_NAMESPACE,
			ConstLabels: promGlobalLabels,
		}),
		// Prometheus gauges of done tasks in the database
		totalTasksDone: promauto.NewGauge(prometheus.GaugeOpts{
			Name:        string(tasks.TaskStateDone),
			Help:        "The number of tasks processed successfully in the database",
			Namespace:   PROMETHEUS_NAMESPACE,
			ConstLabels: promGlobalLabels,
		}),
		// Prometheus gauges of failed tasks in the database
		totalTasksFailed: promauto.NewGauge(prometheus.GaugeOpts{
			Name:        string(tasks.TaskStateFailed),
			Help:        "The number of tasks that failed to be processed in the database",
			Namespace:   PROMETHEUS_NAMESPACE,
			ConstLabels: promGlobalLabels,
		}),
	}
}

// Init initializes the tasks service; it starts:
// - the task processing routine
// - the task maintenance routine
// - the task statistics update routine if metrics collection is enabled
func (s *Service) StartBackgroundWork(
	ctx context.Context, l *utils.Logger, errChan chan<- error,
) error {

	// Create a new context without cancel because we prefer to properly
	// handle the cancellation of the context in the Shutdown method.
	ctx, s.backgroundJobsCancelFunc = context.WithCancel(context.WithoutCancel(ctx))

	err := s.processTaskRoutine(ctx, l, errChan)
	if err != nil {
		return err
	}

	err = s.processTasksMaintenanceRoutine(ctx, l, errChan)
	if err != nil {
		return err
	}

	if s.options.CollectMetrics {
		return s.processTasksUpdateStatisticsRoutine(ctx, l, errChan)
	}

	return nil
}

// Shutdown shuts down the tasks service.
// Cancel was called on the context, so the workers should all stop processing
// tasks on their own. We just wait for the ones that are still processing.
func (s *Service) Shutdown(ctx context.Context) error {

	done := make(chan struct{})

	// Start a goroutine to wait for the workers to finish
	go func() {
		s.backgroundJobsWg.Wait()
		close(done)
	}()

	// Cancel the context to signal the workers to stop processing tasks
	s.backgroundJobsCancelFunc()

	// Wait for either the workers to finish or the context to be cancelled
	select {
	case <-ctx.Done():
		// If context is done (e.g., due to timeout), return the error
		return ErrShutdownTimeout
	case <-done:
		// If all workers finish successfully, return nil
		return nil
	}
}

// RegisterTasksService registers a tasks service and the tasks it handles.
func (s *Service) RegisterTasksService(tasksService ITasksService) {
	// Register the tasks managed by the service
	s.managedPkgs[tasksService.GetTaskServiceName()] = true
	for taskName, task := range tasksService.GetHandledTasks() {
		s.managedTasks[taskName] = task
	}
}

// InputGetAllDTO is the data transfer object to get all tasks.
type InputGetAllTasksDTO struct {
	Type  string          `json:"type" binding:"omitempty,alphanum"`
	State tasks.TaskState `json:"state" binding:"omitempty,oneof=pending running done failed"`
}

// GetTasks returns all tasks from the repository.
func (s *Service) GetTasks(
	ctx context.Context, l *utils.Logger, input InputGetAllTasksDTO,
) ([]tasks.ITask, error) {
	tasks, err := s.store.GetTasks(ctx, tasksrepo.InputGetTasksFilters{
		Type:  input.Type,
		State: input.State,
	})
	if err != nil {
		return nil, err
	}

	return tasks, nil
}

// InputGetTaskDTO is the data transfer object to get a task.
type InputGetTaskDTO struct {
	ID uuid.UUID `json:"id" binding:"required"`
}

// GetTask returns a task from the repository.
func (s *Service) GetTask(
	ctx context.Context, l *utils.Logger, input InputGetTaskDTO,
) (tasks.ITask, error) {
	task, err := s.store.GetTask(ctx, input.ID)
	if err != nil {
		if errors.Is(err, tasksrepo.ErrTaskNotFound) {
			return nil, ErrTaskNotFound
		}
		return nil, err
	}

	return task, nil
}

// CreateTask creates a new task in the repository.
func (s *Service) CreateTask(
	ctx context.Context, l *utils.Logger, input tasks.ITask,
) (tasks.ITask, error) {
	newTask := input

	// Generate an ID
	newTask.SetID(uuid.MakeV4())

	// Set creation and update datetime
	ctime := timeutil.Now()
	newTask.SetCreationDatetime(ctime)
	newTask.SetUpdateDatetime(ctime)

	// Set state
	newTask.SetState(tasks.TaskStatePending)

	// Save the task
	err := s.store.CreateTask(ctx, newTask)
	if err != nil {
		return nil, err
	}

	return newTask, nil
}

// CreateTaskIfNotAlreadyPlanned creates a new task in the repository
// if one of the same type is not already planned.
func (s *Service) CreateTaskIfNotAlreadyPlanned(
	ctx context.Context, l *utils.Logger, task tasks.ITask,
) (tasks.ITask, error) {
	storedTasks, err := s.store.GetTasks(ctx, tasksrepo.InputGetTasksFilters{
		Type:  task.GetType(),
		State: tasks.TaskStatePending,
	})
	if err != nil {
		return nil, err
	}

	// If no task of the same type is already planned, create a new one
	if len(storedTasks) == 0 {
		return s.CreateTask(ctx, l, task)
	}

	// If a task of the same type is already planned, return it
	return storedTasks[0], nil
}
