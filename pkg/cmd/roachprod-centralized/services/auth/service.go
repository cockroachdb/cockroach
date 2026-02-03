// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"context"
	"log/slog"
	"sync"
	"time"

	rauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/auth"
	authmetrics "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/internal/metrics"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	stasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// Service implements the authentication service.
type Service struct {
	repo           rauth.IAuthRepository
	taskService    *stasks.Service
	tokenValidator func(ctx context.Context, token string) (string, string, error) // Returns oktaUserID, email, error
	instanceID     string
	metrics        authmetrics.IRecorder

	backgroundJobsWg         *sync.WaitGroup
	backgroundJobsCtx        context.Context
	backgroundJobsCancelFunc context.CancelFunc
	options                  types.Options
}

// NewService creates a new authentication service.
// IMPORTANT: You must call WithTokenValidator to configure Okta token validation before using the service.
func NewService(
	repo rauth.IAuthRepository, taskService *stasks.Service, instanceID string, opts types.Options,
) *Service {

	// Set defaults
	if opts.CleanupInterval == 0 {
		opts.CleanupInterval = 24 * time.Hour
	}
	if opts.ExpiredTokensRetention == 0 {
		opts.ExpiredTokensRetention = 24 * time.Hour
	}
	if opts.StatisticsUpdateInterval == 0 {
		opts.StatisticsUpdateInterval = 30 * time.Second
	}

	// Initialize metrics recorder
	var metricsRecorder authmetrics.IRecorder
	if opts.CollectMetrics {
		metricsRecorder = authmetrics.NewRecorder(prometheus.DefaultRegisterer, instanceID)
	} else {
		metricsRecorder = &authmetrics.NoOpRecorder{}
	}

	return &Service{
		repo:        repo,
		taskService: taskService,
		instanceID:  instanceID,
		metrics:     metricsRecorder,

		// Validator must be configured via WithTokenValidator before calling ExchangeOktaToken
		tokenValidator: nil,

		backgroundJobsWg:         &sync.WaitGroup{},
		backgroundJobsCtx:        context.Background(),
		backgroundJobsCancelFunc: nil,
		options:                  opts,
	}
}

// WithTokenValidator sets the token validator for the service.
func (s *Service) WithTokenValidator(
	validator func(context.Context, string) (string, string, error),
) *Service {
	s.tokenValidator = validator
	return s
}

// RegisterTasks registers any background tasks that this service needs to process.
func (s *Service) RegisterTasks(ctx context.Context) error {
	if s.taskService != nil {
		s.taskService.RegisterTasksService(s)
	}
	return nil
}

// GetTaskServiceName returns the unique name of the service.
func (s *Service) GetTaskServiceName() string {
	return "auth"
}

// StartService initializes the service and prepares it for operation.
func (s *Service) StartService(ctx context.Context, l *logger.Logger) error {
	return nil
}

// StartBackgroundWork starts any background processing routines for this service.
func (s *Service) StartBackgroundWork(
	ctx context.Context, l *logger.Logger, errChan chan<- error,
) error {

	if s.taskService == nil {
		l.Debug("task service not configured, skipping background work for auth service")
		return nil
	}

	s.backgroundJobsCtx, s.backgroundJobsCancelFunc = context.WithCancel(ctx)

	// Start the cleanup scheduler
	s.startCleanupScheduler(l)

	// Start metrics collection if enabled
	if s.options.CollectMetrics {
		// Perform initial collection
		if err := s.UpdateMetrics(ctx, l); err != nil {
			l.Warn("failed to perform initial metrics collection", "error", err)
		}

		s.backgroundJobsWg.Add(1)
		err := authmetrics.StartMetricsCollection(
			s.backgroundJobsCtx,
			l,
			errChan,
			s.options.StatisticsUpdateInterval,
			s, // Service implements ICollector
			s.backgroundJobsWg.Done,
		)
		if err != nil {
			s.backgroundJobsWg.Done()
			return err
		}
	}

	return nil
}

// Shutdown gracefully terminates the service and cleans up any resources.
func (s *Service) Shutdown(ctx context.Context) error {

	done := make(chan struct{})

	// Start a goroutine to wait for the workers to finish
	go func() {
		s.backgroundJobsWg.Wait()
		close(done)
	}()

	// Cancel the background context to stop the periodic refresh.
	if s.backgroundJobsCancelFunc != nil {
		s.backgroundJobsCancelFunc()
	}

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

func (s *Service) startCleanupScheduler(l *logger.Logger) {
	cleanupLogger := l.With(
		slog.String("service", "auth"),
		slog.String("routine", "cleanup_scheduler"),
	)
	cleanupLogger.Info(
		"starting cleanup scheduler",
		slog.String("interval", s.options.CleanupInterval.String()),
	)

	// Create an initial cleanup task immediately if needed
	if err := s.scheduleCleanupTaskIfNeeded(s.backgroundJobsCtx, cleanupLogger); err != nil {
		cleanupLogger.Error(
			"failed to schedule cleanup task",
			slog.Any("error", err),
		)
	}

	s.backgroundJobsWg.Add(1)
	go func() {
		defer s.backgroundJobsWg.Done()

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
					)
				}
			}
		}
	}()
}

func (s *Service) scheduleCleanupTaskIfNeeded(ctx context.Context, l *logger.Logger) error {
	if s.taskService == nil {
		return nil
	}

	task, err := tasks.NewTaskExpiredTokensCleanup(s.options.ExpiredTokensRetention)
	if err != nil {
		return errors.Wrap(err, "failed to create expired tokens cleanup task")
	}

	// Create cleanup task if not recently scheduled
	_, err = s.taskService.CreateTaskIfNotRecentlyScheduled(
		ctx, l,
		task,
		s.options.CleanupInterval,
	)

	return err
}

// UpdateMetrics implements metrics.ICollector for periodic gauge updates.
func (s *Service) UpdateMetrics(ctx context.Context, l *logger.Logger) error {
	stats, err := s.repo.GetStatistics(ctx, l)
	if err != nil {
		return err
	}

	s.metrics.SetUsersTotal(stats.UsersActive, stats.UsersInactive)
	s.metrics.SetGroupsTotal(stats.Groups)
	s.metrics.SetServiceAccountsTotal(stats.ServiceAccountsEnabled, stats.ServiceAccountsDisabled)

	// Reset and update token gauges
	s.metrics.ResetTokensTotal()
	for tokenType, statuses := range stats.TokensByTypeAndStatus {
		for status, count := range statuses {
			s.metrics.SetTokensTotal(tokenType, status, count)
		}
	}

	return nil
}

// RecordAuthentication records an authentication attempt for metrics.
func (s *Service) RecordAuthentication(result, authMethod string, latency time.Duration) {
	s.metrics.RecordAuthentication(result, authMethod, latency)
}

// RecordAuthzDecision records an authorization decision for metrics.
func (s *Service) RecordAuthzDecision(result, reason, endpoint, provider string) {
	s.metrics.RecordAuthzDecision(result, reason, endpoint, provider)
}

// RecordAuthzLatency records authorization latency for metrics.
func (s *Service) RecordAuthzLatency(endpoint string, latency time.Duration) {
	s.metrics.RecordAuthzLatency(endpoint, latency)
}
