// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package auth provides authentication utilities for the roachprod-centralized client.
package auth

import (
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Constants for keyring configuration.
const (
	// KeyringServiceName is the service name used in the OS keyring.
	KeyringServiceName = "roachprod"
	// KeyringTokenKey is the key used to store the API token in the keyring.
	KeyringTokenKey = "api_token"
)

// envPrefix is the common prefix for all roachprod API env vars.
// This mirrors client.EnvPrefix; both are verified by a test.
const envPrefix = "ROACHPROD_API_"

// Environment variable names for authentication.
const (
	// EnvAPIToken is the environment variable for the API token.
	// When set, it takes precedence over the keyring.
	EnvAPIToken = envPrefix + "TOKEN"
	// EnvOktaIssuer is the environment variable for the Okta issuer URL.
	EnvOktaIssuer = envPrefix + "OKTA_ISSUER"
	// EnvOktaClientID is the environment variable for the Okta OAuth client ID.
	EnvOktaClientID = envPrefix + "OKTA_CLIENT_ID"
)

// Default values.
const (
	// DefaultOktaIssuer is the default Okta tenant URL.
	DefaultOktaIssuer = "https://cockroachlabs.okta.com"
	// ExpirationWarningThreshold is the duration before expiration to start warning.
	ExpirationWarningThreshold = 48 * time.Hour
)

// Common errors.
var (
	// ErrNoToken is returned when no token is available.
	ErrNoToken = errors.New("no authentication token available; run 'roachprod login' to authenticate")
	// ErrTokenExpired is returned when the token has expired.
	ErrTokenExpired = errors.New("authentication token has expired; run 'roachprod login' to refresh")
	// ErrAuthorizationPending is returned during device flow when user hasn't completed auth yet.
	ErrAuthorizationPending = errors.New("authorization pending")
	// ErrSlowDown is returned when polling too frequently during device flow.
	ErrSlowDown = errors.New("slow down")
	// ErrAccessDenied is returned when user denies authorization.
	ErrAccessDenied = errors.New("access denied by user")
	// ErrDeviceCodeExpired is returned when the device code expires before user completes auth.
	ErrDeviceCodeExpired = errors.New("device code expired; please try again")
)

// StoredToken represents a token stored in the keyring.
type StoredToken struct {
	// Token is the opaque bearer token.
	Token string `json:"token"`
	// ExpiresAt is the token expiration time (optional, may be zero).
	ExpiresAt time.Time `json:"expires_at,omitempty"`
}

// IsExpired returns true if the token has expired.
func (t *StoredToken) IsExpired() bool {
	if t.ExpiresAt.IsZero() {
		return false
	}
	return timeutil.Now().After(t.ExpiresAt)
}

// ExpiresWithin returns true if the token expires within the given duration.
func (t *StoredToken) ExpiresWithin(d time.Duration) bool {
	if t.ExpiresAt.IsZero() {
		return false
	}
	return timeutil.Now().Add(d).After(t.ExpiresAt)
}

// TokenInfo contains information about the current authentication token.
type TokenInfo struct {
	// Token is the opaque bearer token (may be masked for display).
	Token string
	// ExpiresAt is the token expiration time.
	ExpiresAt *time.Time
	// Principal is the email or service account name.
	Principal string
	// TokenType is "user" or "service_account".
	TokenType string
	// Permissions is the list of permissions for the principal.
	Permissions []string
}

// OktaConfig holds configuration for Okta authentication.
type OktaConfig struct {
	// Issuer is the Okta tenant URL (e.g., https://cockroachlabs.okta.com).
	Issuer string
	// ClientID is the OAuth client ID for the roachprod application.
	ClientID string
}

// LoadOktaConfigFromEnv loads Okta configuration from environment variables.
func LoadOktaConfigFromEnv() OktaConfig {
	cfg := OktaConfig{
		Issuer: DefaultOktaIssuer,
	}
	if issuer := getEnv(EnvOktaIssuer); issuer != "" {
		cfg.Issuer = issuer
	}
	if clientID := getEnv(EnvOktaClientID); clientID != "" {
		cfg.ClientID = clientID
	}
	return cfg
}

// getEnv is a helper to get environment variables.
// This is extracted to allow for testing.
var getEnv = os.Getenv

// GetEnvToken returns the token from the environment variable, or empty string if not set.
func GetEnvToken() string {
	return getEnv(EnvAPIToken)
}
