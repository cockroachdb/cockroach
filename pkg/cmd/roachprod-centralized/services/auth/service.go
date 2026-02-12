// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	rauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/auth"
	authmetrics "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/internal/metrics"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	stasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
	tokenLastUsedCh          chan uuid.UUID // buffered channel for async last-used updates
}

// NewService creates a new authentication service.
// IMPORTANT: You must call WithTokenValidator to configure Okta token validation before using the service.
func NewService(
	repo rauth.IAuthRepository, taskService *stasks.Service, instanceID string, opts types.Options,
) *Service {

	// Set defaults
	if opts.CleanupInterval == 0 {
		opts.CleanupInterval = types.DefaultCleanupInterval
	}
	if opts.ExpiredTokensRetention == 0 {
		opts.ExpiredTokensRetention = types.DefaultExpiredTokensRetention
	}
	if opts.StatisticsUpdateInterval == 0 {
		opts.StatisticsUpdateInterval = types.DefaultStatisticsUpdateInterval
	}
	if opts.TokenLastUsedBufferSize == 0 {
		opts.TokenLastUsedBufferSize = types.DefaultTokenLastUsedBufferSize
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
		tokenLastUsedCh:          make(chan uuid.UUID, opts.TokenLastUsedBufferSize),
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
	// Bootstrap SCIM service account if configured and no service accounts exist
	if s.options.BootstrapSCIMToken != "" {
		if err := s.bootstrapSCIMServiceAccount(ctx, l); err != nil {
			return errors.Wrap(err, "failed to bootstrap SCIM service account")
		}
	}
	return nil
}

// bootstrapSCIMServiceAccount creates a bootstrap SCIM service account if none exist.
// This is a one-time operation that runs on first startup when configured.
func (s *Service) bootstrapSCIMServiceAccount(ctx context.Context, l *logger.Logger) error {
	bootstrapLogger := l.With(
		slog.String("service", "auth"),
		slog.String("operation", "bootstrap"),
	)

	// Check if any service accounts already exist
	existingSAs, _, err := s.repo.ListServiceAccounts(ctx, l, *filters.NewFilterSet())
	if err != nil {
		return errors.Wrap(err, "failed to check for existing service accounts")
	}

	if len(existingSAs) > 0 {
		bootstrapLogger.Debug("service accounts already exist, skipping bootstrap")
		return nil
	}

	bootstrapLogger.Info("no service accounts found, creating bootstrap SCIM service account")

	// Ensure the bootstrap token meets the format and entropy requirements
	expectedTokenPrefix := fmt.Sprintf(
		"%s$%s$%s$",
		types.TokenPrefix,
		types.TokenTypeSA,
		types.TokenVersion,
	)
	if !strings.HasPrefix(s.options.BootstrapSCIMToken, expectedTokenPrefix) {
		return errors.Newf("bootstrap SCIM token must start with prefix %q", expectedTokenPrefix)
	}
	if len(s.options.BootstrapSCIMToken) < len(expectedTokenPrefix)+types.TokenEntropyLength {
		return errors.Newf(
			"bootstrap SCIM token must have at least %d characters of entropy after the prefix",
			types.TokenEntropyLength)
	}

	// Create the service account
	now := timeutil.Now()
	sa := &auth.ServiceAccount{
		ID:          uuid.MakeV4(),
		Name:        "bootstrap-scim",
		Description: "Bootstrap service account for SCIM provisioning (created automatically on first startup)",
		Enabled:     true,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	if err := s.repo.CreateServiceAccount(ctx, l, sa); err != nil {
		return errors.Wrap(err, "failed to create bootstrap service account")
	}

	// Add set of admin permissions to the service account
	for _, perm := range []string{
		types.PermissionScimManageUser,          // Can manage users (via SCIM or API)
		types.PermissionServiceAccountCreate,    // Can create service accounts
		types.PermissionServiceAccountViewAll,   // Can view any service account
		types.PermissionServiceAccountUpdateAll, // Can update any service account
		types.PermissionServiceAccountDeleteAll, // Can delete any service account
		types.PermissionServiceAccountMintAll,   // Can mint tokens for any service account
		types.PermissionTokensViewAll,           // Can view all tokens
		types.PermissionTokensRevokeOwn,         // Can revoke own tokens
	} {
		permission := &auth.ServiceAccountPermission{
			ID:               uuid.MakeV4(),
			ServiceAccountID: sa.ID,
			Permission:       perm,
			CreatedAt:        now,
		}

		if err := s.repo.AddServiceAccountPermission(ctx, l, permission); err != nil {
			return errors.Wrapf(err, "failed to add permission %s to bootstrap service account", perm)
		}
	}

	// Create the token using the provided bootstrap token
	tokenHash := hashToken(s.options.BootstrapSCIMToken)
	tokenSuffix := computeTokenSuffix(s.options.BootstrapSCIMToken, types.TokenTypeSA)

	token := &auth.ApiToken{
		ID:               uuid.MakeV4(),
		TokenHash:        tokenHash,
		TokenSuffix:      tokenSuffix,
		TokenType:        auth.TokenTypeServiceAccount,
		ServiceAccountID: &sa.ID,
		Status:           auth.TokenStatusValid,
		CreatedAt:        now,
		UpdatedAt:        now,
		ExpiresAt:        now.Add(types.TokenDefaultTTLBootstrapServiceAccount),
	}

	if err := s.repo.CreateToken(ctx, l, token); err != nil {
		return errors.Wrap(err, "failed to create bootstrap token")
	}

	bootstrapLogger.Info(
		"bootstrap SCIM service account created successfully",
		slog.String("service_account_id", sa.ID.String()),
		slog.String("token_suffix", tokenSuffix),
		slog.String("expires_at", token.ExpiresAt.Format(time.RFC3339)),
	)

	// Audit the bootstrap operation
	s.auditEvent(ctx, l, nil, AuditSACreated, "success", map[string]interface{}{
		"service_account_id":   sa.ID.String(),
		"service_account_name": sa.Name,
		"bootstrap":            true,
	})

	return nil
}

// StartBackgroundWork starts any background processing routines for this service.
func (s *Service) StartBackgroundWork(
	ctx context.Context, l *logger.Logger, errChan chan<- error,
) error {

	s.backgroundJobsCtx, s.backgroundJobsCancelFunc = context.WithCancel(ctx)

	// Always start the token last-used worker (doesn't require taskService)
	s.startTokenLastUsedWorker(l)

	if s.taskService == nil {
		l.Debug("task service not configured, skipping cleanup and metrics background work")
		return nil
	}

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

// startTokenLastUsedWorker starts a background goroutine that processes
// async token last-used timestamp updates from the buffered channel.
// Updates are best-effort: failures are logged but do not affect callers.
// On shutdown, the worker drains any remaining buffered updates before exiting.
func (s *Service) startTokenLastUsedWorker(l *logger.Logger) {
	workerLogger := l.With(
		slog.String("service", "auth"),
		slog.String("routine", "token_last_used"),
	)
	workerLogger.Info(
		"starting token last-used update worker",
		slog.Int("buffer_size", s.options.TokenLastUsedBufferSize),
	)

	s.backgroundJobsWg.Add(1)
	go func() {
		defer s.backgroundJobsWg.Done()

		for {
			select {
			case tokenID := <-s.tokenLastUsedCh:
				if err := s.repo.UpdateTokenLastUsed(
					context.Background(), workerLogger, tokenID,
				); err != nil {
					workerLogger.Warn(
						"failed to update token last_used_at",
						slog.String("token_id", tokenID.String()),
						slog.Any("error", err),
					)
				}
			case <-s.backgroundJobsCtx.Done():
				// Drain remaining buffered updates before exiting.
				for {
					select {
					case tokenID := <-s.tokenLastUsedCh:
						if err := s.repo.UpdateTokenLastUsed(
							context.Background(), workerLogger, tokenID,
						); err != nil {
							workerLogger.Warn(
								"failed to update token last_used_at (drain)",
								slog.String("token_id", tokenID.String()),
								slog.Any("error", err),
							)
						}
					default:
						workerLogger.Info("token last-used update worker stopped")
						return
					}
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
