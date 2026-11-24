// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package health

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/health"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	rhealth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/health"
	htasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/health/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/health/types"
	stasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// Service implements the health service.
type Service struct {
	options    types.Options
	instanceID string
	hostname   string

	_repository  rhealth.IHealthRepository
	_taskService stasks.IService

	// Background work management
	_backgroundJobsWg        *sync.WaitGroup
	backgroundJobsCtx        context.Context
	backgroundJobsCancelFunc context.CancelFunc
	shutdownOnce             sync.Once
}

// NewService creates a new health service.
func NewService(
	repository rhealth.IHealthRepository,
	taskService stasks.IService,
	instanceID string,
	opts types.Options,
) (*Service, error) {
	// Set defaults
	if opts.HeartbeatInterval == 0 {
		opts.HeartbeatInterval = types.DefaultHeartbeatInterval
	}
	if opts.InstanceTimeout == 0 {
		opts.InstanceTimeout = types.DefaultInstanceTimeout
	}
	if opts.CleanupInterval == 0 {
		opts.CleanupInterval = types.DefaultCleanupInterval
	}
	if opts.CleanupRetention == 0 {
		opts.CleanupRetention = types.DefaultCleanupRetention
	}

	hostname, _ := os.Hostname()

	return &Service{
		options:           opts,
		instanceID:        instanceID,
		hostname:          hostname,
		_repository:       repository,
		_taskService:      taskService,
		_backgroundJobsWg: &sync.WaitGroup{},
	}, nil
}

// GenerateInstanceID creates a unique instance ID.
func GenerateInstanceID() string {
	hostname, _ := os.Hostname()
	id := uuid.MakeV4()
	return fmt.Sprintf("%s-%s", hostname, id.Short())
}

// RegisterTasks registers the cluster tasks with the tasks service.
func (s *Service) RegisterTasks(ctx context.Context) error {
	// Register the cluster tasks with the tasks service.
	if s._taskService != nil {
		s._taskService.RegisterTasksService(s)
	}
	return nil
}

func (s *Service) StartService(ctx context.Context, l *logger.Logger) error {
	healthLogger := l.With(
		slog.String("service", "health"),
		slog.String("instance_id", s.instanceID),
	)

	healthLogger.Info("starting health service")

	// Register this instance
	instance := health.InstanceInfo{
		InstanceID:    s.instanceID,
		Hostname:      s.hostname,
		Mode:          s.options.Mode,
		StartedAt:     timeutil.Now(),
		LastHeartbeat: timeutil.Now(),
		Metadata:      make(map[string]string),
	}

	return s._repository.RegisterInstance(ctx, l, instance)
}

func (s *Service) StartBackgroundWork(
	ctx context.Context, l *logger.Logger, errChan chan<- error,
) error {
	s.backgroundJobsCtx, s.backgroundJobsCancelFunc = context.WithCancel(ctx)

	// Start heartbeat routine
	s.startHeartbeatRoutine(l)

	// Start cleanup task scheduling routine if workers are enabled
	if s.options.WorkersEnabled {
		s.startCleanupScheduler(l)
	}

	return nil
}

func (s *Service) Shutdown(ctx context.Context) error {
	var shutdownErr error
	s.shutdownOnce.Do(func() {
		// Cancel background work
		if s.backgroundJobsCancelFunc != nil {
			s.backgroundJobsCancelFunc()
		}

		// Wait for background work to finish with timeout
		done := make(chan struct{})
		go func() {
			s._backgroundJobsWg.Wait()
			close(done)
		}()

		select {
		case <-ctx.Done():
			shutdownErr = fmt.Errorf("shutdown timeout")
		case <-done:
			// Clean shutdown
		}
	})

	return shutdownErr
}

// Implements ITasksService interface

func (s *Service) GetTaskServiceName() string {
	return types.TaskServiceName
}

func (s *Service) GetHandledTasks() map[string]stasks.ITask {
	return map[string]stasks.ITask{
		string(htasks.HealthTaskCleanup): &htasks.TaskCleanup{
			Service: s,
		},
	}
}

// CreateTaskInstance creates a new instance of a task of the given type.
func (s *Service) CreateTaskInstance(taskType string) (tasks.ITask, error) {
	switch taskType {
	case string(htasks.HealthTaskCleanup):
		task := &htasks.TaskCleanup{Service: s}
		task.Type = taskType
		// Options will be deserialized from payload by the task service
		return task, nil
	default:
		return nil, stasks.ErrUnknownTaskType
	}
}

// Background work routines
func (s *Service) startHeartbeatRoutine(l *logger.Logger) {
	heartbeatLogger := l.With(
		slog.String("service", "health"),
		slog.String("routine", "heartbeat"),
	)
	heartbeatLogger.Info(
		"starting heartbeat routine",
		slog.String("interval", s.options.HeartbeatInterval.String()),
	)

	s._backgroundJobsWg.Add(1)
	go func() {
		defer s._backgroundJobsWg.Done()

		ticker := time.NewTicker(s.options.HeartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-s.backgroundJobsCtx.Done():
				heartbeatLogger.Info("stopping heartbeat routine")
				return

			case <-ticker.C:
				if err := s._repository.UpdateHeartbeat(
					s.backgroundJobsCtx, heartbeatLogger, s.instanceID,
				); err != nil {
					heartbeatLogger.Error(
						"failed to send heartbeat",
						slog.Any("error", err),
						slog.String("instance_id", s.instanceID),
						slog.String("interval", s.options.HeartbeatInterval.String()),
					)
				}
			}
		}
	}()
}

func (s *Service) startCleanupScheduler(l *logger.Logger) {
	cleanupLogger := l.With(
		slog.String("service", "health"),
		slog.String("routine", "cleanup_scheduler"),
	)
	cleanupLogger.Info(
		"starting cleanup scheduler",
		slog.String("interval", s.options.CleanupInterval.String()),
	)

	s._backgroundJobsWg.Add(1)
	go func() {
		defer s._backgroundJobsWg.Done()

		ticker := time.NewTicker(s.options.CleanupInterval)
		defer ticker.Stop()

		for {
			select {
			case <-s.backgroundJobsCtx.Done():
				cleanupLogger.Info("stopping cleanup scheduler")
				return

			case <-ticker.C:
				if err := s.scheduleCleanupTaskIfNeeded(s.backgroundJobsCtx, cleanupLogger); err != nil {
					cleanupLogger.Error(
						"failed to schedule cleanup task",
						slog.Any("error", err),
						slog.String("instance_id", s.instanceID),
						slog.String("cleanup_interval", s.options.CleanupInterval.String()),
					)
				}
			}
		}
	}()
}

func (s *Service) scheduleCleanupTaskIfNeeded(ctx context.Context, l *logger.Logger) error {
	if s._taskService == nil {
		return nil
	}

	// Create cleanup task with configured options
	task, err := htasks.NewTaskCleanup(s.options.InstanceTimeout, s.options.CleanupRetention)
	if err != nil {
		return errors.Wrap(err, "failed to create cleanup task")
	}

	_, err = s._taskService.CreateTaskIfNotRecentlyScheduled(
		ctx, l,
		task, s.options.CleanupInterval,
	)
	return err
}

// Implements IHealthService interface
func (s *Service) RegisterInstance(
	ctx context.Context, l *logger.Logger, instanceID, hostname string,
) error {
	instance := health.InstanceInfo{
		InstanceID:    instanceID,
		Hostname:      hostname,
		StartedAt:     timeutil.Now(),
		LastHeartbeat: timeutil.Now(),
		Metadata:      make(map[string]string),
	}

	return s._repository.RegisterInstance(ctx, l, instance)
}

func (s *Service) IsInstanceHealthy(
	ctx context.Context, l *logger.Logger, instanceID string,
) (bool, error) {
	return s._repository.IsInstanceHealthy(ctx, l, instanceID, s.options.InstanceTimeout)
}

func (s *Service) GetHealthyInstances(
	ctx context.Context, l *logger.Logger,
) ([]health.InstanceInfo, error) {
	return s._repository.GetHealthyInstances(ctx, l, s.options.InstanceTimeout)
}

func (s *Service) GetInstanceID() string {
	return s.instanceID
}

// GetInstanceTimeout returns the configured instance timeout duration.
func (s *Service) GetInstanceTimeout() time.Duration {
	return s.options.InstanceTimeout
}

func (s *Service) CleanupDeadInstances(
	ctx context.Context, l *logger.Logger, instanceTimeout, cleanupRetention time.Duration,
) (int, error) {
	return s._repository.CleanupDeadInstances(
		ctx, l,
		instanceTimeout, cleanupRetention,
	)
}
