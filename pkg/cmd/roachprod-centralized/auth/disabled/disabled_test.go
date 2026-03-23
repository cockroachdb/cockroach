// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package disabled

import (
	"context"
	"testing"

	authmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDisabledAuthenticator_Authenticate_AlwaysSucceeds(t *testing.T) {
	authenticator := NewDisabledAuthenticator()

	// Test with various token inputs - all should succeed
	testCases := []string{
		"",
		"any-token",
		"Bearer token",
		"invalid",
	}

	for _, token := range testCases {
		principal, err := authenticator.Authenticate(context.Background(), token, "127.0.0.1")
		require.NoError(t, err, "disabled auth should always succeed for token: %q", token)
		require.NotNil(t, principal, "principal should not be nil for token: %q", token)

		// Verify TokenInfo structure
		assert.NotEqual(t, uuid.UUID{}, principal.Token.ID, "token ID should be generated")
		assert.Equal(t, authmodels.TokenTypeUser, principal.Token.Type)
		assert.Nil(t, principal.Token.CreatedAt, "disabled auth doesn't populate timestamps")
		assert.Nil(t, principal.Token.ExpiresAt, "disabled auth doesn't populate timestamps")

		// Verify user info
		assert.NotNil(t, principal.User)
		assert.Equal(t, "dev@localhost", principal.User.Email)
		assert.Equal(t, "Development User", principal.User.FullName)
		assert.True(t, principal.User.Active)

		// Verify wildcard permissions
		assert.Len(t, principal.Permissions, 1)
		assert.Equal(t, "*", principal.Permissions[0].GetScope())
		assert.Equal(t, "*", principal.Permissions[0].GetPermission())
	}
}

func TestDisabledAuthenticator_WildcardPermissions(t *testing.T) {
	authenticator := NewDisabledAuthenticator()

	principal, err := authenticator.Authenticate(context.Background(), "test-token", "127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, principal)

	// Verify wildcard permissions grant access to everything
	assert.True(t, principal.HasPermission("clusters:create"))
	assert.True(t, principal.HasPermission("clusters:read"))
	assert.True(t, principal.HasPermission("clusters:write"))
	assert.True(t, principal.HasPermission("clusters:delete"))
	assert.True(t, principal.HasPermission("any:permission"))

	// Verify scoped permission checks also work
	assert.True(t, principal.HasPermissionScoped("clusters:create", "gcp-engineering"))
	assert.True(t, principal.HasPermissionScoped("clusters:read", "aws-staging"))
	assert.True(t, principal.HasPermissionScoped("any:permission", "any-scope"))
}

func TestDisabledAuthenticator_Claims(t *testing.T) {
	authenticator := NewDisabledAuthenticator()

	principal, err := authenticator.Authenticate(context.Background(), "test-token", "127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, principal)

	// Verify claims are populated
	assert.NotNil(t, principal.Claims)
	assert.Equal(t, "dev-user", principal.Claims["sub"])
	assert.Equal(t, "dev@localhost", principal.Claims["email"])
}
