// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package disabled

import (
	"context"
	"testing"

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
		assert.Nil(t, principal.Token.CreatedAt, "disabled auth doesn't populate timestamps")
		assert.Nil(t, principal.Token.ExpiresAt, "disabled auth doesn't populate timestamps")
	}
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
