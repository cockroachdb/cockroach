// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"context"
	"log/slog"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

const (
	// DefaultPurgeDoneTaskOlderThan is the default duration after which tasks
	// in done state are purged from the repository.
	DefaultPurgeDoneTaskOlderThan = 2 * time.Hour
	// DefaultPurgeFailedTaskOlderThan is the default duration after which tasks
	// in failed state are purged from the repository.
	DefaultPurgeFailedTaskOlderThan = 24 * time.Hour
	// DefaultPurgeTaskOlderThan is the default value for how often tasks are
	// purged from the repository.
	DefaultPurgeTasksInterval = 10 * time.Minute
	// DefaultStatisticsUpdateInterval is the default value for how often the
	// tasks statistics are updated.
	DefaultStatisticsUpdateInterval = 30 * time.Second
)

var (
	// ErrMetricsCollectionDisabled is returned when metrics collection is disabled
	// and a metrics-related operation is attempted.
	ErrMetricsCollectionDisabled = errors.New("metrics collection is disabled")
)

type ITasksService interface {
	GetTaskServiceName() string
	GetHandledTasks() (tasks map[string]ITask)
}

type ITask interface {
	Process(context.Context, *utils.Logger, chan<- error)
}

// processTasksMaintenanceRoutine will routinely purge old tasks and update metrics
func (s *Service) processTasksMaintenanceRoutine(
	ctx context.Context, l *utils.Logger, errChan chan<- error,
) error {

	l.Debug("Starting tasks maintenance routine")

	s.backgroundJobsWg.Add(1)
	go func() {
		defer s.backgroundJobsWg.Done()
		tickerPurge := time.NewTicker(s.options.PurgeTasksInterval)
		defer tickerPurge.Stop()

		for {
			select {
			case <-ctx.Done():
				l.Debug("Stopping tasks maintenance routine")
				return

			case <-tickerPurge.C:

				l.Debug("Purging tasks from the repository")
				purgedDone, purgedFailed, err := s.purgeTasks(ctx)
				if err != nil {
					errChan <- errors.Wrap(err, "unable to purge tasks")
				}

				if purgedDone > 0 || purgedFailed > 0 {
					l.Info(
						"Purged tasks from the database",
						slog.Int("state_done", purgedDone),
						slog.Int("deleted_failed", purgedFailed),
					)
				}

			}
		}
	}()

	return nil
}

// processTasksStatisticsRoutine will routinely update the metrics
// with the tasks statistics
func (s *Service) processTasksUpdateStatisticsRoutine(
	ctx context.Context, l *utils.Logger, errChan chan<- error,
) error {

	if !s.options.CollectMetrics {
		return ErrMetricsCollectionDisabled
	}

	l.Debug("Starting tasks statistics routine")

	s.backgroundJobsWg.Add(1)
	go func() {
		defer s.backgroundJobsWg.Done()
		tickerStats := time.NewTicker(s.options.StatisticsUpdateInterval)
		defer tickerStats.Stop()

		for {
			select {
			case <-ctx.Done():
				l.Debug("Stopping tasks statistics routine")
				return

			case <-tickerStats.C:

				l.Debug("Getting tasks statistics from the repository")
				err := s.updateMetrics(ctx)
				if err != nil {
					errChan <- errors.Wrap(err, "unable to get tasks statistics")
				}
			}
		}
	}()

	return nil
}

// processTaskRoutine will listen for tasks to process and start the workers
func (s *Service) processTaskRoutine(
	ctx context.Context, l *utils.Logger, errChan chan<- error,
) error {

	l.Debug("Starting tasks processing routine")

	taskChan := make(chan tasks.ITask)

	// Start the workers that handle the tasks
	for i := 0; i < s.options.Workers; i++ {

		s.backgroundJobsWg.Add(1)
		go func() {
			defer s.backgroundJobsWg.Done()

			for {
				select {
				case <-ctx.Done():
					l.Debug("Stopping task processing routine")
					return

				case task := <-taskChan:
					l.Debug("Received task to process", slog.Any("task", task))
					err := s.processTask(ctx, l, task)
					if err != nil {
						errChan <- errors.Wrap(err, "unable to process task")
					}
				}
			}
		}()
	}

	// Get tasks for processing from repository
	s.backgroundJobsWg.Add(1)
	go func() {
		defer s.backgroundJobsWg.Done()
		err := s.store.GetTasksForProcessing(
			ctx,
			taskChan,
			s.consumerID,
		)
		if err != nil {
			// An error occurring while getting tasks for processing
			// is considered critical and will stop the service
			errChan <- utils.NewCriticalError(
				errors.Wrap(err, "unable to get tasks for processing"),
			)
		}
	}()

	return nil
}

// processTask processes a task and updates its status in the repository.
func (s *Service) processTask(ctx context.Context, l *utils.Logger, task tasks.ITask) error {

	taskLogger := &utils.Logger{
		Logger: l.With(slog.String("task_id", task.GetID().String())),
	}

	errStatus := s.markTaskAs(ctx, task.GetID(), tasks.TaskStateRunning)
	if errStatus != nil {
		l.Error(
			"Failed to update task status",
			slog.Any("task_id", task.GetID()),
			slog.String("status", string(tasks.TaskStateRunning)),
			slog.Any("error", errStatus),
		)
	}

	err := s._processTask(
		ctx,
		taskLogger,
		task,
	)
	if err != nil {
		taskLogger.Error(
			"Unable to process task",
			slog.Any("task_id", task.GetID()),
			slog.Any("error", err),
		)

		errFailedStatus := s.markTaskAs(ctx, task.GetID(), tasks.TaskStateFailed)
		if errFailedStatus != nil {
			l.Error(
				"Failed to update task status",
				slog.Any("task_id", task.GetID()),
				slog.String("status", string(tasks.TaskStateFailed)),
				slog.Any("error", errFailedStatus),
			)
		}
		return err
	}

	taskLogger.Info(
		"Task processed successfully",
		slog.Any("task_id", task.GetID()),
	)

	errDoneStatus := s.markTaskAs(ctx, task.GetID(), tasks.TaskStateDone)
	if errDoneStatus != nil {
		l.Error(
			"Failed to update task status",
			slog.Any("task_id", task.GetID()),
			slog.String("status", string(tasks.TaskStateDone)),
			slog.Any("error", errDoneStatus),
		)
	}

	return nil
}

// _processTask processes a task and handles timeouts.
func (s *Service) _processTask(ctx context.Context, l *utils.Logger, task tasks.ITask) error {

	tType := task.GetType()

	if s.managedTasks[tType] == nil {
		return ErrTaskTypeNotManaged
	}

	// Increment the counter of tasks processed once processed
	if s.options.CollectMetrics {
		defer s.metrics.processedTasksProcessed.Inc()
	}

	// Create a new cancellable context for the task
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Create a channel to receive errors from the task processing
	errChan := make(chan error, 1)

	go s.managedTasks[tType].Process(ctx, l, errChan)

	// Wait for the task to finish or timeout
	select {
	case err := <-errChan:
		return err
	case <-time.After(s.options.TasksTimeout):
		return ErrTaskTimeout
	}
}

// markTaskAs updates the status of a task in the repository.
func (s *Service) markTaskAs(
	ctx context.Context, id uuid.UUID, status tasks.TaskState,
) (err error) {
	return s.store.UpdateState(ctx, id, status)
}

// purgeTasks purges tasks in done and failed states that are older
// than a given interval.
func (s *Service) purgeTasks(ctx context.Context) (int, int, error) {
	delDone, err := s.purgeTasksInState(ctx, s.options.PurgeDoneTaskOlderThan, tasks.TaskStateDone)
	if err != nil {
		return 0, 0, errors.Wrapf(
			err, "unable to purge tasks in %s state", string(tasks.TaskStateDone),
		)
	}

	delFailed, err := s.purgeTasksInState(
		ctx, s.options.PurgeFailedTaskOlderThan, tasks.TaskStateFailed,
	)
	if err != nil {
		return delDone, 0, errors.Wrapf(
			err, "unable to purge tasks in %s state", string(tasks.TaskStateFailed),
		)
	}

	return delDone, delFailed, nil
}

// purgeTasksInState purges tasks in a given state that are older
// than a given interval.
func (s *Service) purgeTasksInState(
	ctx context.Context, interval time.Duration, state tasks.TaskState,
) (int, error) {
	del, err := s.store.PurgeTasks(ctx, interval, state)
	if err != nil {
		return 0, err
	}
	return del, nil
}

// updateMetrics updates the Prometheus metrics with the tasks statistics.
func (s *Service) updateMetrics(ctx context.Context) error {

	if !s.options.CollectMetrics {
		return ErrMetricsCollectionDisabled
	}

	stats, err := s.store.GetStatistics(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to get tasks statistics")
	}

	// Pening
	s.metrics.totalTasksPending.Set(
		float64(stats[tasks.TaskStatePending]),
	)

	// Running
	s.metrics.totalTasksRunning.Set(
		float64(stats[tasks.TaskStateRunning]),
	)

	// Done
	s.metrics.totalTasksDone.Set(
		float64(stats[tasks.TaskStateDone]),
	)

	// Failed
	s.metrics.totalTasksFailed.Set(
		float64(stats[tasks.TaskStateFailed]),
	)

	return nil
}
