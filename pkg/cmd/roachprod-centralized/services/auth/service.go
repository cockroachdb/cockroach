// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"context"

	rauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	stasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
)

// Service implements the authentication service.
type Service struct {
	repo           rauth.IAuthRepository
	taskService    *stasks.Service
	tokenValidator func(ctx context.Context, token string) (string, string, error) // Returns oktaUserID, email, error
	instanceID     string

	options types.Options
}

// NewService creates a new authentication service.
// IMPORTANT: You must call WithTokenValidator to configure Okta token validation before using the service.
func NewService(
	repo rauth.IAuthRepository, taskService *stasks.Service, instanceID string, opts types.Options,
) *Service {

	return &Service{
		repo:        repo,
		taskService: taskService,
		instanceID:  instanceID,

		// Validator must be configured via WithTokenValidator before calling ExchangeOktaToken
		tokenValidator: nil,

		options: opts,
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
	return nil
}

// Shutdown gracefully terminates the service and cleans up any resources.
func (s *Service) Shutdown(ctx context.Context) error {
	return nil
}
