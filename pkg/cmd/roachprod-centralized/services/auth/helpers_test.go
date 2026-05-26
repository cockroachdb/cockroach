// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"testing"

	pkgauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetPrincipalIdentifier tests the getPrincipalIdentifier function
func TestGetPrincipalIdentifier(t *testing.T) {
	userID := uuid.MakeV4()
	saID := uuid.MakeV4()

	tests := []struct {
		name      string
		principal *pkgauth.Principal
		expected  string
	}{
		{
			name:      "nil principal returns system",
			principal: nil,
			expected:  "system",
		},
		{
			name: "user principal returns user:uuid",
			principal: &pkgauth.Principal{
				UserID: &userID,
			},
			expected: "user:" + userID.String(),
		},
		{
			name: "service account principal returns sa:uuid",
			principal: &pkgauth.Principal{
				ServiceAccountID: &saID,
			},
			expected: "sa:" + saID.String(),
		},
		{
			name: "principal with both IDs prefers user",
			principal: &pkgauth.Principal{
				UserID:           &userID,
				ServiceAccountID: &saID,
			},
			expected: "user:" + userID.String(),
		},
		{
			name:      "principal with no IDs returns unknown",
			principal: &pkgauth.Principal{},
			expected:  "unknown",
		},
		{
			name: "service account principal with delegated from user",
			principal: &pkgauth.Principal{
				ServiceAccountID: &saID,
				DelegatedFrom:    &userID,
			},
			expected: "sa:" + saID.String() + "(delegatedFrom user:" + userID.String() + ")",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getPrincipalIdentifier(tt.principal)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestIsOwnedByPrincipal tests the isOwnedByPrincipal function
func TestIsOwnedByPrincipal(t *testing.T) {
	userID := uuid.MakeV4()
	otherUserID := uuid.MakeV4()
	saID := uuid.MakeV4()
	otherSAID := uuid.MakeV4()

	tests := []struct {
		name      string
		sa        *auth.ServiceAccount
		principal *pkgauth.Principal
		expected  bool
	}{
		{
			name: "user owns SA they created",
			sa: &auth.ServiceAccount{
				ID:        uuid.MakeV4(),
				Name:      "my-sa",
				CreatedBy: &userID,
			},
			principal: &pkgauth.Principal{
				UserID: &userID,
			},
			expected: true,
		},
		{
			name: "user does not own SA created by another",
			sa: &auth.ServiceAccount{
				ID:        uuid.MakeV4(),
				Name:      "their-sa",
				CreatedBy: &otherUserID,
			},
			principal: &pkgauth.Principal{
				UserID: &userID,
			},
			expected: false,
		},
		{
			name: "SA owns SA it created",
			sa: &auth.ServiceAccount{
				ID:        uuid.MakeV4(),
				Name:      "child-sa",
				CreatedBy: &saID,
			},
			principal: &pkgauth.Principal{
				ServiceAccountID: &saID,
			},
			expected: true,
		},
		{
			name: "SA does not own SA created by another SA",
			sa: &auth.ServiceAccount{
				ID:        uuid.MakeV4(),
				Name:      "other-child-sa",
				CreatedBy: &otherSAID,
			},
			principal: &pkgauth.Principal{
				ServiceAccountID: &saID,
			},
			expected: false,
		},
		{
			name: "SA with nil CreatedBy is not owned",
			sa: &auth.ServiceAccount{
				ID:        uuid.MakeV4(),
				Name:      "orphan-sa",
				CreatedBy: nil,
			},
			principal: &pkgauth.Principal{
				UserID: &userID,
			},
			expected: false,
		},
		{
			name: "principal with no IDs does not own any SA",
			sa: &auth.ServiceAccount{
				ID:        uuid.MakeV4(),
				Name:      "any-sa",
				CreatedBy: &userID,
			},
			principal: &pkgauth.Principal{},
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isOwnedByPrincipal(tt.sa, tt.principal)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestComputeChangedFieldsSA tests the computeChangedFieldsSA function
func TestComputeChangedFieldsSA(t *testing.T) {
	tests := []struct {
		name     string
		old      *auth.ServiceAccount
		new      *auth.ServiceAccount
		expected []string
	}{
		{
			name: "no changes",
			old: &auth.ServiceAccount{
				ID:          uuid.MakeV4(),
				Name:        "ci-bot",
				Description: "CI automation",
				Enabled:     true,
			},
			new: &auth.ServiceAccount{
				ID:          uuid.MakeV4(),
				Name:        "ci-bot",
				Description: "CI automation",
				Enabled:     true,
			},
			expected: nil,
		},
		{
			name: "name changed",
			old: &auth.ServiceAccount{
				Name:        "old-name",
				Description: "Description",
				Enabled:     true,
			},
			new: &auth.ServiceAccount{
				Name:        "new-name",
				Description: "Description",
				Enabled:     true,
			},
			expected: []string{"name"},
		},
		{
			name: "description changed",
			old: &auth.ServiceAccount{
				Name:        "ci-bot",
				Description: "Old description",
				Enabled:     true,
			},
			new: &auth.ServiceAccount{
				Name:        "ci-bot",
				Description: "New description",
				Enabled:     true,
			},
			expected: []string{"description"},
		},
		{
			name: "enabled changed",
			old: &auth.ServiceAccount{
				Name:        "ci-bot",
				Description: "Description",
				Enabled:     true,
			},
			new: &auth.ServiceAccount{
				Name:        "ci-bot",
				Description: "Description",
				Enabled:     false,
			},
			expected: []string{"enabled"},
		},
		{
			name: "all fields changed",
			old: &auth.ServiceAccount{
				Name:        "old-name",
				Description: "Old description",
				Enabled:     true,
			},
			new: &auth.ServiceAccount{
				Name:        "new-name",
				Description: "New description",
				Enabled:     false,
			},
			expected: []string{"name", "description", "enabled"},
		},
		{
			name: "description added (empty to non-empty)",
			old: &auth.ServiceAccount{
				Name:        "ci-bot",
				Description: "",
				Enabled:     true,
			},
			new: &auth.ServiceAccount{
				Name:        "ci-bot",
				Description: "Now has a description",
				Enabled:     true,
			},
			expected: []string{"description"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := computeChangedFieldsSA(tt.old, tt.new)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestComputeTokenSuffix tests the computeTokenSuffix function
func TestComputeTokenSuffix(t *testing.T) {
	tests := []struct {
		name      string
		token     string
		tokenType string
		expected  string
	}{
		{
			name:      "user token",
			token:     "rp$user$1$ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij12345678",
			tokenType: authtypes.TokenTypeUser,
			expected:  authtypes.TokenPrefix + "$user$1$****12345678",
		},
		{
			name:      "service account token",
			token:     "rp$sa$1$ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij87654321",
			tokenType: authtypes.TokenTypeSA,
			expected:  authtypes.TokenPrefix + "$sa$1$****87654321",
		},
		{
			name:      "token with exactly 8 chars",
			token:     "12345678",
			tokenType: authtypes.TokenTypeUser,
			expected:  authtypes.TokenPrefix + "$user$1$****12345678",
		},
		{
			name:      "short token (less than 8 chars)",
			token:     "short",
			tokenType: authtypes.TokenTypeUser,
			expected:  "short", // Returns token as-is if too short
		},
		{
			name:      "empty token",
			token:     "",
			tokenType: authtypes.TokenTypeUser,
			expected:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := computeTokenSuffix(tt.token, tt.tokenType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestHashToken tests the hashToken function
func TestHashToken(t *testing.T) {
	t.Run("consistent hashing", func(t *testing.T) {
		token := "rp$user$1$ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij12345678"
		hash1 := hashToken(token)
		hash2 := hashToken(token)

		assert.Equal(t, hash1, hash2, "same token should produce same hash")
	})

	t.Run("different tokens produce different hashes", func(t *testing.T) {
		token1 := "rp$user$1$ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij12345678"
		token2 := "rp$user$1$ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij12345679"
		hash1 := hashToken(token1)
		hash2 := hashToken(token2)

		assert.NotEqual(t, hash1, hash2, "different tokens should produce different hashes")
	})

	t.Run("hash is hex encoded SHA256", func(t *testing.T) {
		token := "test-token"
		hash := hashToken(token)

		// SHA256 produces 32 bytes, hex encoded = 64 characters
		assert.Len(t, hash, 64)

		// Should only contain hex characters
		for _, c := range hash {
			assert.True(t, (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'),
				"hash should only contain hex characters, got: %c", c)
		}
	})

	t.Run("empty token produces valid hash", func(t *testing.T) {
		hash := hashToken("")
		assert.Len(t, hash, 64)
	})
}

// TestService_GenerateToken tests the generateToken method
func TestService_GenerateToken(t *testing.T) {
	// Create a minimal service - generateToken doesn't use the repo
	service := NewService(nil, nil, "test-instance", authtypes.Options{})

	t.Run("generates valid user token", func(t *testing.T) {
		tokenString, tokenHash, tokenSuffix, err := service.generateToken(auth.TokenTypeUser)

		require.NoError(t, err)

		// Token format: rp$user$1$<random>
		assert.True(t, len(tokenString) > 0)
		assert.Contains(t, tokenString, authtypes.TokenPrefix+"$user$1$")

		// Hash should be 64 hex chars (SHA256)
		assert.Len(t, tokenHash, 64)

		// Suffix format: rp$user$1$****<last8>
		assert.Contains(t, tokenSuffix, authtypes.TokenPrefix+"$user$1$****")
		assert.Len(t, tokenSuffix, len(authtypes.TokenPrefix+"$user$1$****")+8)
	})

	t.Run("generates valid service account token", func(t *testing.T) {
		tokenString, tokenHash, tokenSuffix, err := service.generateToken(auth.TokenTypeServiceAccount)

		require.NoError(t, err)

		// Token format: rp$sa$1$<random>
		assert.Contains(t, tokenString, authtypes.TokenPrefix+"$sa$1$")

		// Hash should be 64 hex chars
		assert.Len(t, tokenHash, 64)

		// Suffix format: rp$sa$1$****<last8>
		assert.Contains(t, tokenSuffix, authtypes.TokenPrefix+"$sa$1$****")
	})

	t.Run("generates unique tokens", func(t *testing.T) {
		token1, hash1, _, err1 := service.generateToken(auth.TokenTypeUser)
		token2, hash2, _, err2 := service.generateToken(auth.TokenTypeUser)

		require.NoError(t, err1)
		require.NoError(t, err2)

		assert.NotEqual(t, token1, token2, "tokens should be unique")
		assert.NotEqual(t, hash1, hash2, "hashes should be unique")
	})

	t.Run("token has correct entropy length", func(t *testing.T) {
		tokenString, _, _, err := service.generateToken(auth.TokenTypeUser)
		require.NoError(t, err)

		// Token format: rp$user$1$<random>
		// The random portion should be TokenEntropyLength chars
		prefix := authtypes.TokenPrefix + "$user$1$"
		assert.True(t, len(tokenString) > len(prefix))

		randomPortion := tokenString[len(prefix):]
		assert.Len(t, randomPortion, authtypes.TokenEntropyLength)

		// Random portion should only contain base62 characters
		for _, c := range randomPortion {
			isBase62 := (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
			assert.True(t, isBase62, "random portion should only contain base62 chars, got: %c", c)
		}
	})

	t.Run("hash matches token", func(t *testing.T) {
		tokenString, tokenHash, _, err := service.generateToken(auth.TokenTypeUser)
		require.NoError(t, err)

		// Verify the hash matches what hashToken would produce
		expectedHash := hashToken(tokenString)
		assert.Equal(t, expectedHash, tokenHash)
	})

	t.Run("suffix matches token ending", func(t *testing.T) {
		tokenString, _, tokenSuffix, err := service.generateToken(auth.TokenTypeUser)
		require.NoError(t, err)

		// Last 8 chars of token should appear in suffix
		last8 := tokenString[len(tokenString)-8:]
		assert.True(t, len(tokenSuffix) >= 8)
		assert.Equal(t, last8, tokenSuffix[len(tokenSuffix)-8:])
	})
}
