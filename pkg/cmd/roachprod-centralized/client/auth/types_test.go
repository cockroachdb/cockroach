// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/assert"
)

func TestStoredToken_IsExpired(t *testing.T) {
	tests := []struct {
		name      string
		token     StoredToken
		wantExpir bool
	}{
		{
			name: "not expired",
			token: StoredToken{
				Token:     "test-token",
				ExpiresAt: timeutil.Now().Add(1 * time.Hour),
			},
			wantExpir: false,
		},
		{
			name: "expired",
			token: StoredToken{
				Token:     "test-token",
				ExpiresAt: timeutil.Now().Add(-1 * time.Hour),
			},
			wantExpir: true,
		},
		{
			name: "no expiration set",
			token: StoredToken{
				Token: "test-token",
			},
			wantExpir: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.token.IsExpired()
			assert.Equal(t, tt.wantExpir, got)
		})
	}
}

func TestStoredToken_ExpiresWithin(t *testing.T) {
	tests := []struct {
		name     string
		token    StoredToken
		duration time.Duration
		want     bool
	}{
		{
			name: "expires within duration",
			token: StoredToken{
				Token:     "test-token",
				ExpiresAt: timeutil.Now().Add(1 * time.Hour),
			},
			duration: 2 * time.Hour,
			want:     true,
		},
		{
			name: "does not expire within duration",
			token: StoredToken{
				Token:     "test-token",
				ExpiresAt: timeutil.Now().Add(3 * time.Hour),
			},
			duration: 2 * time.Hour,
			want:     false,
		},
		{
			name: "no expiration set",
			token: StoredToken{
				Token: "test-token",
			},
			duration: 2 * time.Hour,
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.token.ExpiresWithin(tt.duration)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLoadOktaConfigFromEnv(t *testing.T) {
	// Save and restore original getEnv
	originalGetEnv := getEnv
	defer func() { getEnv = originalGetEnv }()

	t.Run("default values", func(t *testing.T) {
		getEnv = func(key string) string { return "" }

		cfg := LoadOktaConfigFromEnv()
		assert.Equal(t, DefaultOktaIssuer, cfg.Issuer)
		assert.Equal(t, "", cfg.ClientID)
	})

	t.Run("custom values from env", func(t *testing.T) {
		envVars := map[string]string{
			EnvOktaIssuer:   "https://custom.okta.com",
			EnvOktaClientID: "custom-client-id",
		}
		getEnv = func(key string) string { return envVars[key] }

		cfg := LoadOktaConfigFromEnv()
		assert.Equal(t, "https://custom.okta.com", cfg.Issuer)
		assert.Equal(t, "custom-client-id", cfg.ClientID)
	})
}
