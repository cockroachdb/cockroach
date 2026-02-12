// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"time"

	configtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/config/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	tasksrepo "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/internal/metrics"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/internal/processor"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/internal/scheduler"
	ttasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// service.go contains the main Service struct, lifecycle management, and orchestration
// of all task service components (processor, scheduler, metrics, registry).

// shutdownTimeoutMargin is the extra time added on top of the maximum task
// timeout to allow for cleanup work during shutdown.
const shutdownTimeoutMargin = 10 * time.Second

var _ services.IServiceWithShutdownTimeout = (*Service)(nil)

// Service implements the tasks service interface and manages background task processing.
// It coordinates between task producers (other services) and task consumers (workers)
// to ensure efficient and reliable task execution across the system.
type Service struct {
	options types.Options

	instanceID string
	store      tasksrepo.ITasksRepository
	metrics    *taskMetrics

	// Task registry fields
	managedPkgs  map[string]bool
	managedTasks map[string]types.ITask
	// managedTaskTypes is a pre-computed slice of task type names for metrics
	managedTaskTypes []string
	// taskServices maps task type names to their managing service
	taskServices map[string]types.ITasksService

	// maxTaskTimeout tracks the highest timeout across all registered tasks,
	// updated at registration time. Used to derive the service shutdown timeout.
	maxTaskTimeout time.Duration

	backgroundJobsCancelFunc context.CancelFunc
	backgroundJobsWg         *sync.WaitGroup
}

// taskMetrics contains Prometheus metrics for monitoring tasks service performance and health.
// These metrics provide visibility into task processing rates, queue depths, and system status.
type taskMetrics struct {
	taskCompletionStatus   *prometheus.CounterVec
	taskProcessingDuration *prometheus.HistogramVec
	taskQueueAge           *prometheus.HistogramVec
	taskTimeouts           *prometheus.CounterVec
	totalTasks             *prometheus.GaugeVec
	maxWorkers             prometheus.Gauge
	activeWorkers          prometheus.Gauge
}

// NewService creates a new tasks service.
func NewService(
	store tasksrepo.ITasksRepository, instanceID string, options types.Options,
) *Service {

	// Workers < 0 means explicitly disabled (API-only mode)
	// Workers == 0 means not set, use default
	// Workers > 0 means explicitly set
	if options.Workers == 0 {
		options.Workers = types.DefaultTasksWorkers
	} else if options.Workers < 0 {
		options.Workers = 0
	}
	if options.DefaultTasksTimeout == 0 {
		options.DefaultTasksTimeout = types.DefaultTasksTimeout
	}
	if options.PurgeDoneTaskOlderThan == 0 {
		options.PurgeDoneTaskOlderThan = scheduler.DefaultPurgeDoneTaskOlderThan
	}
	if options.PurgeFailedTaskOlderThan == 0 {
		options.PurgeFailedTaskOlderThan = scheduler.DefaultPurgeFailedTaskOlderThan
	}
	if options.PurgeTasksInterval == 0 {
		options.PurgeTasksInterval = scheduler.DefaultPurgeTasksInterval
	}
	if options.StatisticsUpdateInterval == 0 {
		options.StatisticsUpdateInterval = metrics.DefaultStatisticsUpdateInterval
	}

	s := &Service{
		options:          options,
		instanceID:       instanceID,
		store:            store,
		managedPkgs:      make(map[string]bool),
		managedTasks:     make(map[string]types.ITask),
		taskServices:     make(map[string]types.ITasksService),
		backgroundJobsWg: &sync.WaitGroup{},
	}

	if options.CollectMetrics {
		s.metrics = s.newMetrics(prometheus.DefaultRegisterer)
		s.metrics.maxWorkers.Set(float64(options.Workers))
	}

	return s
}

// newMetrics creates a new Prometheus metrics instance.
// registerer allows using custom registries for testing.
func (s *Service) newMetrics(registerer prometheus.Registerer) *taskMetrics {

	promGlobalLabels := prometheus.Labels{}
	instanceLabels := prometheus.Labels{
		"instance": s.instanceID,
	}

	combinedLabels := maps.Clone(promGlobalLabels)
	maps.Copy(combinedLabels, instanceLabels)

	factory := promauto.With(registerer)

	return &taskMetrics{
		// Prometheus counter vector of completed tasks by type and status
		taskCompletionStatus: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name:        fmt.Sprintf("%s_completion_status_total", types.TaskServiceName),
				Help:        "The total number of completed tasks by type and status",
				Namespace:   configtypes.MetricsNamespace,
				ConstLabels: combinedLabels,
			},
			[]string{"task_type", "status"},
		),
		// Prometheus histogram of task processing duration by type
		taskProcessingDuration: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:        fmt.Sprintf("%s_processing_duration_seconds", types.TaskServiceName),
				Help:        "Time spent processing tasks by type",
				Namespace:   configtypes.MetricsNamespace,
				ConstLabels: promGlobalLabels,
				Buckets:     prometheus.ExponentialBuckets(0.1, 2, 10), // 0.1s to ~100s
			},
			[]string{"task_type"},
		),
		// Prometheus histogram of task queue age (time from creation to processing start)
		taskQueueAge: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:        fmt.Sprintf("%s_queue_age_seconds", types.TaskServiceName),
				Help:        "Time tasks spend in pending state before processing starts",
				Namespace:   configtypes.MetricsNamespace,
				ConstLabels: promGlobalLabels,
				Buckets:     prometheus.ExponentialBuckets(1, 2, 12), // 1s to ~1hr
			},
			[]string{"task_type"},
		),
		// Prometheus counter of task timeouts by type
		taskTimeouts: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name:        fmt.Sprintf("%s_timeouts_total", types.TaskServiceName),
				Help:        "Total number of task timeouts by type",
				Namespace:   configtypes.MetricsNamespace,
				ConstLabels: promGlobalLabels,
			},
			[]string{"task_type"},
		),
		// Prometheus gauge vector of tasks in the database by state and type
		totalTasks: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name:        fmt.Sprintf("%s_total", types.TaskServiceName),
				Help:        "The total number of tasks in the database by state and type",
				Namespace:   configtypes.MetricsNamespace,
				ConstLabels: promGlobalLabels,
			},
			[]string{"state", "task_type"},
		),
		// Prometheus gauge of max workers available for processing tasks
		maxWorkers: factory.NewGauge(
			prometheus.GaugeOpts{
				Name:        fmt.Sprintf("%s_max_workers", types.TaskServiceName),
				Help:        "Maximum number of workers configured for task processing",
				Namespace:   configtypes.MetricsNamespace,
				ConstLabels: combinedLabels,
			},
		),
		// Prometheus gauge of active workers currently processing tasks
		activeWorkers: factory.NewGauge(
			prometheus.GaugeOpts{
				Name:        fmt.Sprintf("%s_active_workers", types.TaskServiceName),
				Help:        "Number of workers currently processing tasks",
				Namespace:   configtypes.MetricsNamespace,
				ConstLabels: combinedLabels,
			},
		),
	}
}

// RegisterTasks registers the tasks with the tasks service.
func (s *Service) RegisterTasks(ctx context.Context) error {
	// Register the tasks tasks with the tasks service.
	s.RegisterTasksService(s)
	return nil
}

// StartService is a nil-op for this service as it does not require initialization.
func (s *Service) StartService(ctx context.Context, l *logger.Logger) error {
	return nil
}

// StartBackgroundWork initializes the tasks service; it starts:
// - the task processing routine
// - the task purge scheduling routine (only when workers are enabled)
// - the task statistics update routine if metrics collection is enabled
func (s *Service) StartBackgroundWork(
	ctx context.Context, l *logger.Logger, errChan chan<- error,
) error {

	// Create a new context without the parent cancel because we prefer
	// to properly handle the context cancellation in the Shutdown method
	// that is called by our parent when the app is stopping.
	ctx, s.backgroundJobsCancelFunc = context.WithCancel(context.WithoutCancel(ctx))

	// Start task processor
	s.backgroundJobsWg.Add(1)
	err := processor.StartProcessing(
		ctx,
		l,
		errChan,
		s.options.Workers,
		s.instanceID,
		s.store,
		s,                       // Service implements processor.TaskExecutor interface
		s.backgroundJobsWg.Done, // Call Done() when all processor goroutines exit
	)
	if err != nil {
		s.backgroundJobsWg.Done()
		return err
	}

	// Only schedule purge tasks when workers are enabled
	// API-only instances should not schedule purge to avoid multiple instances
	// competing to purge tasks
	if s.options.WorkersEnabled {
		s.backgroundJobsWg.Add(1)
		err = scheduler.StartPurgeScheduling(
			ctx,
			l,
			errChan,
			s.options.PurgeTasksInterval,
			s,                       // Service implements scheduler.PurgeScheduler interface
			s.backgroundJobsWg.Done, // Call Done() when goroutine exits
		)
		if err != nil {
			s.backgroundJobsWg.Done()
			return err
		}
	} else {
		l.Info("task service: skipping purge task scheduling (workers disabled)")
	}

	// Start metrics collection if enabled
	if s.options.CollectMetrics {
		// Perform initial metrics collection to populate gauges immediately
		l.Debug("performing initial metrics collection")
		if err := s.UpdateMetrics(ctx, l); err != nil {
			// Don't fail startup if initial metrics collection fails
			l.Warn("failed to perform initial metrics collection", "error", err)
		}

		s.backgroundJobsWg.Add(1)
		err = metrics.StartMetricsCollection(
			ctx,
			l,
			errChan,
			s.options.StatisticsUpdateInterval,
			s,                       // Service implements metrics.MetricsCollector interface
			s.backgroundJobsWg.Done, // Call Done() when goroutine exits
		)
		if err != nil {
			s.backgroundJobsWg.Done()
			return err
		}
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
		return types.ErrShutdownTimeout
	case <-done:
		// If all workers finish successfully, return nil
		return nil
	}
}

// GetShutdownTimeout returns a shutdown timeout derived from the maximum
// timeout across all registered tasks, plus a margin for cleanup.
func (s *Service) GetShutdownTimeout() time.Duration {
	maxTimeout := s.options.DefaultTasksTimeout
	if s.maxTaskTimeout > maxTimeout {
		maxTimeout = s.maxTaskTimeout
	}
	return maxTimeout + shutdownTimeoutMargin
}

// GetTaskServiceName returns the name of the task service.
func (s *Service) GetTaskServiceName() string {
	return types.TaskServiceName
}

// GetHandledTasks returns a map of task types to task implementations that are
// handled by this service.
func (s *Service) GetHandledTasks() map[string]types.ITask {
	return map[string]types.ITask{
		string(ttasks.TasksPurge): &ttasks.TaskPurge{
			Service: s,
		},
	}
}

// CreateTaskInstance creates a new instance of a task of the given type.
func (s *Service) CreateTaskInstance(taskType string) (tasks.ITask, error) {
	switch taskType {
	case string(ttasks.TasksPurge):
		return &ttasks.TaskPurge{
			Task:    tasks.Task{Type: taskType},
			Service: s,
		}, nil
	default:
		return nil, types.ErrUnknownTaskType
	}
}

// Implementations of internal package interfaces below

// processor.TaskExecutor interface implementation

func (s *Service) HydrateTask(base tasks.ITask) (types.ITask, error) {
	return s.hydrateTask(base)
}

func (s *Service) MarkTaskAs(
	ctx context.Context, l *logger.Logger, id uuid.UUID, status tasks.TaskState,
) error {
	return s.store.UpdateState(ctx, l, id, status)
}

func (s *Service) UpdateError(
	ctx context.Context, l *logger.Logger, id uuid.UUID, errMsg string,
) error {
	return s.store.UpdateError(ctx, l, id, errMsg)
}

func (s *Service) GetManagedTask(taskType string) types.ITask {
	return s.managedTasks[taskType]
}

func (s *Service) GetMetricsEnabled() bool {
	return s.options.CollectMetrics
}

func (s *Service) RecordTaskCompletion(taskType string, success bool, duration float64) {
	if s.metrics != nil {
		status := "success"
		if !success {
			status = "failure"
		}
		s.metrics.taskCompletionStatus.WithLabelValues(taskType, status).Inc()
		s.metrics.taskProcessingDuration.WithLabelValues(taskType).Observe(duration)
	}
}

func (s *Service) IncrementActiveWorkers() {
	if s.metrics != nil {
		s.metrics.activeWorkers.Inc()
	}
}

func (s *Service) DecrementActiveWorkers() {
	if s.metrics != nil {
		s.metrics.activeWorkers.Dec()
	}
}

func (s *Service) RecordQueueAge(taskType string, ageSeconds float64) {
	if s.metrics != nil {
		s.metrics.taskQueueAge.WithLabelValues(taskType).Observe(ageSeconds)
	}
}

func (s *Service) IncrementTimeouts(taskType string) {
	if s.metrics != nil {
		s.metrics.taskTimeouts.WithLabelValues(taskType).Inc()
	}
}

func (s *Service) GetDefaultTimeout() types.TimeoutGetter {
	return &timeoutGetter{timeout: s.options.DefaultTasksTimeout}
}

// timeoutGetter implements types.TimeoutGetter
type timeoutGetter struct {
	timeout time.Duration
}

func (t *timeoutGetter) GetTimeout() time.Duration {
	return t.timeout
}

// scheduler.PurgeScheduler interface implementation

func (s *Service) SchedulePurgeTask(
	ctx context.Context, l *logger.Logger, interval time.Duration,
) error {
	_, err := s.CreateTaskIfNotRecentlyScheduled(
		ctx, l,
		ttasks.NewTaskPurge(),
		interval,
	)
	return err
}

// metrics.MetricsCollector interface implementation

func (s *Service) UpdateMetrics(ctx context.Context, l *logger.Logger) error {
	if !s.options.CollectMetrics {
		return types.ErrMetricsCollectionDisabled
	}
	return metrics.UpdateTaskMetrics(ctx, l, s.store, s.metrics, s.managedTaskTypes)
}

// metrics.MetricsGauges interface implementation (used by metrics package)

func (m *taskMetrics) SetTaskCount(state tasks.TaskState, taskType string, count float64) {
	m.totalTasks.WithLabelValues(string(state), taskType).Set(count)
}
