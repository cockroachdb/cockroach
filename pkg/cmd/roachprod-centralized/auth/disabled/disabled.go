// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package disabled

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	authmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// DisabledAuthenticator is an authenticator that bypasses authentication entirely.
// This is useful for local development and testing.
// It always returns a successful authentication with a default principal.
type DisabledAuthenticator struct {
	// No configuration needed
}

// NewDisabledAuthenticator creates a new disabled authenticator.
func NewDisabledAuthenticator() *DisabledAuthenticator {
	return &DisabledAuthenticator{}
}

// Authenticate always succeeds and returns a default principal with admin permissions.
// The token and clientIP parameters are ignored.
func (a *DisabledAuthenticator) Authenticate(
	ctx context.Context, _ string, _ string,
) (*auth.Principal, error) {
	// Create a default test principal with wildcard permissions
	// This bypasses all authorization checks during development and testing
	principal := &auth.Principal{
		Token: auth.TokenInfo{
			ID:   uuid.MakeV4(),
			Type: authmodels.TokenTypeUser,
		},
		UserID: &uuid.UUID{},
		User: &authmodels.User{
			ID:          uuid.UUID{},
			OktaUserID:  "dev-user",
			Email:       "dev@localhost",
			FullName:    "Development User",
			SlackHandle: "dev",
			Active:      true,
		},
		// Grant wildcard permission - matches any permission check
		Permissions: []authmodels.Permission{
			&authmodels.UserPermission{
				ID:         uuid.UUID{},
				UserID:     uuid.UUID{},
				Scope:      "*",
				Permission: "*", // Wildcard permission grants access to everything
			},
		},
		Claims: map[string]interface{}{
			"sub":   "dev-user",
			"email": "dev@localhost",
		},
	}

	return principal, nil
}

// Authorize is a no-op for disabled authentication - always allows access.
// No metrics are recorded in development mode.
func (a *DisabledAuthenticator) Authorize(
	ctx context.Context,
	principal *auth.Principal,
	requirement *auth.AuthorizationRequirement,
	endpoint string,
) error {
	return nil // Always allow in disabled mode
}
