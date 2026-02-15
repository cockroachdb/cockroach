// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bearer

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// BearerAuthenticator implements IAuthenticator using opaque bearer tokens.
// This authenticator validates tokens against the database and enforces
// role-based permissions and IP allowlisting.
type BearerAuthenticator struct {
	config      auth.AuthConfig
	authService authtypes.IService
	metrics     authtypes.IAuthMetricsRecorder
	logger      *logger.Logger
}

// NewBearerAuthenticator creates a new bearer token authenticator.
func NewBearerAuthenticator(
	config auth.AuthConfig,
	authService authtypes.IService,
	metrics authtypes.IAuthMetricsRecorder,
	logger *logger.Logger,
) *BearerAuthenticator {
	return &BearerAuthenticator{
		config:      config,
		authService: authService,
		metrics:     metrics,
		logger:      logger,
	}
}

// Authenticate validates a bearer token and returns the authenticated principal.
// This is a thin wrapper that delegates to the auth service's AuthenticateToken method.
// Service errors are returned directly for the controller layer to map to HTTP status codes.
func (a *BearerAuthenticator) Authenticate(
	ctx context.Context, tokenString string, clientIP string,
) (*auth.Principal, error) {
	start := timeutil.Now()

	if tokenString == "" {
		a.metrics.RecordAuthentication("error", "none", timeutil.Since(start))
		return nil, authtypes.ErrNotAuthenticated
	}

	principal, err := a.authService.AuthenticateToken(ctx, a.logger, tokenString, clientIP)
	if err != nil {
		a.metrics.RecordAuthentication("error", "bearer", timeutil.Since(start))
		return nil, err
	}

	a.metrics.RecordAuthentication("success", principal.GetAuthMethod(), timeutil.Since(start))
	return principal, nil
}

// Authorize checks if the principal has the required permissions for an endpoint.
// Returns nil on success, or authtypes.ErrForbidden if authorization fails.
// This method also records authorization metrics.
func (a *BearerAuthenticator) Authorize(
	ctx context.Context,
	principal *auth.Principal,
	requirement *auth.AuthorizationRequirement,
	endpoint string,
) error {
	start := timeutil.Now()
	authMethod := principal.GetAuthMethod()

	// Check AnyOf permissions (OR logic)
	if len(requirement.AnyOf) > 0 && !principal.HasAnyPermission(requirement.AnyOf) {
		a.metrics.RecordAuthzDecision("deny", "missing_permission", endpoint, authMethod)
		a.metrics.RecordAuthzLatency(endpoint, timeutil.Since(start))
		return authtypes.ErrForbidden
	}

	// Check required permissions (AND logic)
	if len(requirement.RequiredPermissions) > 0 && !principal.HasAllPermissions(requirement.RequiredPermissions) {
		a.metrics.RecordAuthzDecision("deny", "missing_permission", endpoint, authMethod)
		a.metrics.RecordAuthzLatency(endpoint, timeutil.Since(start))
		return authtypes.ErrForbidden
	}

	a.metrics.RecordAuthzDecision("allow", "", endpoint, authMethod)
	a.metrics.RecordAuthzLatency(endpoint, timeutil.Since(start))
	return nil
}
