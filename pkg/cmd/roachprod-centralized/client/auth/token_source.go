// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"fmt"
	"net/http"
	"time"

	"github.com/cockroachdb/errors"
)

// BearerTokenSource provides bearer tokens for API authentication.
// It checks for tokens in the following order:
// 1. ROACHPROD_API_TOKEN environment variable
// 2. OS keyring
type BearerTokenSource struct {
	keyringStore *KeyringStore
}

// NewBearerTokenSource creates a new bearer token source.
func NewBearerTokenSource() (*BearerTokenSource, error) {
	store, err := NewKeyringStore()
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize keyring store")
	}
	return &BearerTokenSource{keyringStore: store}, nil
}

// GetToken returns the bearer token from environment variable or keyring.
// Returns ErrNoToken if no token is available.
// Returns ErrTokenExpired if the token has expired.
func (b *BearerTokenSource) GetToken() (string, error) {
	// First, check environment variable
	if token := getEnv(EnvAPIToken); token != "" {
		return token, nil
	}

	// Fall back to keyring
	storedToken, err := b.keyringStore.GetToken()
	if err != nil {
		return "", err
	}

	// Check if token has expired
	if storedToken.IsExpired() {
		return "", ErrTokenExpired
	}

	return storedToken.Token, nil
}

// GetStoredToken returns the full stored token information from the keyring.
// This is useful for checking expiration without returning the token itself.
func (b *BearerTokenSource) GetStoredToken() (*StoredToken, error) {
	// Environment variable tokens don't have expiration info
	if token := getEnv(EnvAPIToken); token != "" {
		return &StoredToken{Token: token}, nil
	}
	return b.keyringStore.GetToken()
}

// StoreToken stores a token in the keyring.
func (b *BearerTokenSource) StoreToken(token StoredToken) error {
	return b.keyringStore.StoreToken(token)
}

// ClearToken removes the stored token from the keyring.
func (b *BearerTokenSource) ClearToken() error {
	return b.keyringStore.ClearToken()
}

// GetHTTPClient returns an HTTP client configured with bearer token authentication.
func (b *BearerTokenSource) GetHTTPClient() (*http.Client, error) {
	// Verify a token exists now (fail fast), but don't capture it.
	if _, err := b.GetToken(); err != nil {
		return nil, err
	}
	return &http.Client{
		Transport: &bearerAuthTransport{
			source: b,
			base:   http.DefaultTransport,
		},
	}, nil
}

// CheckExpiration checks if the token will expire soon and returns a warning message.
// Returns an empty string if no warning is needed.
func (b *BearerTokenSource) CheckExpiration() (string, error) {
	// Environment variable tokens don't have expiration info
	if getEnv(EnvAPIToken) != "" {
		return "", nil
	}

	storedToken, err := b.keyringStore.GetToken()
	if err != nil {
		if errors.Is(err, ErrNoToken) {
			return "", nil
		}
		return "", err
	}

	if storedToken.IsExpired() {
		return "Your token has expired. Run 'roachprod login' to refresh.", nil
	}

	if storedToken.ExpiresWithin(ExpirationWarningThreshold) {
		remaining := time.Until(storedToken.ExpiresAt)
		return fmt.Sprintf("Your token expires in %s. Run 'roachprod login' to refresh.", formatDuration(remaining)), nil
	}

	return "", nil
}

// HasToken returns true if a token is available (from env var or keyring).
func (b *BearerTokenSource) HasToken() bool {
	if getEnv(EnvAPIToken) != "" {
		return true
	}
	return b.keyringStore.HasToken()
}

// bearerAuthTransport adds a bearer token to every outgoing request.
// It calls source.GetToken() per-request so the token is always fresh
// (e.g. if the user runs `roachprod login` in another terminal).
type bearerAuthTransport struct {
	source *BearerTokenSource
	base   http.RoundTripper
}

func (t *bearerAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	token, err := t.source.GetToken()
	if err != nil {
		return nil, errors.Wrap(err, "bearer token")
	}
	reqClone := req.Clone(req.Context())
	reqClone.Header.Set("Authorization", "Bearer "+token)
	return t.base.RoundTrip(reqClone)
}

// formatDuration formats a duration in a human-readable way.
func formatDuration(d time.Duration) string {
	if d < 0 {
		return "expired"
	}

	days := int(d.Hours() / 24)
	hours := int(d.Hours()) % 24

	if days > 0 {
		if hours > 0 {
			return fmt.Sprintf("%d day(s) %d hour(s)", days, hours)
		}
		return fmt.Sprintf("%d day(s)", days)
	}

	if hours > 0 {
		return fmt.Sprintf("%d hour(s)", hours)
	}

	minutes := int(d.Minutes())
	if minutes > 0 {
		return fmt.Sprintf("%d minute(s)", minutes)
	}

	return "less than a minute"
}
