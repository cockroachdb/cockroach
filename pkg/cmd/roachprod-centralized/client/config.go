// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package client

import (
	"os"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/roachprodutil"
	"github.com/cockroachdb/errors"
)

// Config contains configuration for the centralized API client.
type Config struct {
	// Enabled controls whether the client should make API calls
	Enabled bool
	// BaseURL is the base URL of the centralized API (e.g., "https://api.example.com")
	// Also used as the OAuth2 audience for authentication
	BaseURL string
	// ForceFetchCreds forces fetching fresh credentials from the service account
	ForceFetchCreds bool
	// Timeout is the request timeout (default: 10s)
	Timeout time.Duration
	// RetryAttempts is the number of retry attempts on failure (default: 2)
	RetryAttempts int
	// RetryDelay is the delay between retries (default: 1s)
	RetryDelay time.Duration
	// SilentFailures when true, API failures won't be logged as errors (default: true)
	SilentFailures bool
	// IAPServiceAccountEmail is the service account email used for IAP authentication.
	IAPServiceAccountEmail string
}

// DefaultConfig returns a default configuration for the client.
func DefaultConfig() Config {
	return Config{
		Enabled:                DefaultEnabled,
		BaseURL:                DefaultBaseURL,
		Timeout:                DefaultTimeout,
		RetryAttempts:          DefaultRetryAttempts,
		RetryDelay:             DefaultRetryDelay,
		IAPServiceAccountEmail: DefaultServiceAccountEmail,
		ForceFetchCreds:        false,
	}
}

// LoadConfigFromEnv loads the client configuration from environment variables.
// This allows users to configure the centralized API client without code changes.
func LoadConfigFromEnv() Config {
	config := DefaultConfig()

	// Check if centralized API is enabled
	if enabled := os.Getenv("ROACHPROD_CENTRALIZED_API_ENABLED"); enabled != "" {
		config.Enabled = enabled == "true" || enabled == "1"
	}

	// Base URL (required if enabled)
	if baseURL := os.Getenv("ROACHPROD_CENTRALIZED_API_BASE_URL"); baseURL != "" {
		config.BaseURL = baseURL
	}

	// Force fetch credentials
	if forceFetch := os.Getenv("ROACHPROD_CENTRALIZED_API_FORCE_FETCH_CREDS"); forceFetch != "" {
		config.ForceFetchCreds = forceFetch == "true" || forceFetch == "1"
	}

	// Service Account Email
	if saEmail := os.Getenv("ROACHPROD_CENTRALIZED_API_IAP_SERVICE_ACCOUNT_EMAIL"); saEmail != "" {
		config.IAPServiceAccountEmail = saEmail
	}

	// Timeout
	if timeoutStr := os.Getenv("ROACHPROD_CENTRALIZED_API_TIMEOUT"); timeoutStr != "" {
		if timeout, err := time.ParseDuration(timeoutStr); err == nil {
			config.Timeout = timeout
		}
	}

	// Retry attempts
	if retryStr := os.Getenv("ROACHPROD_CENTRALIZED_API_RETRY_ATTEMPTS"); retryStr != "" {
		if retry, err := strconv.Atoi(retryStr); err == nil && retry >= 0 {
			config.RetryAttempts = retry
		}
	}

	// Retry delay
	if delayStr := os.Getenv("ROACHPROD_CENTRALIZED_API_RETRY_DELAY"); delayStr != "" {
		if delay, err := time.ParseDuration(delayStr); err == nil {
			config.RetryDelay = delay
		}
	}

	// Silent failures
	if silentStr := os.Getenv("ROACHPROD_CENTRALIZED_API_SILENT_FAILURES"); silentStr != "" {
		config.SilentFailures = silentStr == "true" || silentStr == "1"
	}

	return config
}

// Validate checks if the configuration is valid.
func (c Config) Validate() error {
	if c.Enabled && c.BaseURL == "" {
		return errors.New("base URL is required when centralized API is enabled")
	}
	if c.Timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	if c.RetryAttempts < 0 {
		return errors.New("retry attempts must be non-negative")
	}
	if c.RetryDelay < 0 {
		return errors.New("retry delay must be non-negative")
	}
	return nil
}

// Option is a functional option for configuring the Client.
type Option interface {
	apply(*Client)
}

// OptionFunc is a function that implements the Option interface.
type OptionFunc func(*Client)

func (o OptionFunc) apply(c *Client) { o(c) }

// WithConfig applies a complete configuration to the client.
func WithConfig(config Config) OptionFunc {
	return func(c *Client) {
		c.config = config
	}
}

// WithEnabled sets whether the client is enabled.
func WithEnabled(enabled bool) OptionFunc {
	return func(c *Client) {
		c.config.Enabled = enabled
	}
}

// WithBaseURL sets the base URL for the centralized API.
func WithBaseURL(baseURL string) OptionFunc {
	return func(c *Client) {
		c.config.BaseURL = baseURL
	}
}

// WithTimeout sets the request timeout.
func WithTimeout(timeout time.Duration) OptionFunc {
	return func(c *Client) {
		c.config.Timeout = timeout
	}
}

// WithRetryAttempts sets the number of retry attempts.
func WithRetryAttempts(attempts int) OptionFunc {
	return func(c *Client) {
		c.config.RetryAttempts = attempts
	}
}

// WithRetryDelay sets the delay between retries.
func WithRetryDelay(delay time.Duration) OptionFunc {
	return func(c *Client) {
		c.config.RetryDelay = delay
	}
}

// WithForceFetchCreds sets whether to force fetching fresh credentials.
func WithForceFetchCreds(force bool) OptionFunc {
	return func(c *Client) {
		c.config.ForceFetchCreds = force
	}
}

// WithSilentFailures sets whether API failures should be logged silently.
func WithSilentFailures(silent bool) OptionFunc {
	return func(c *Client) {
		c.config.SilentFailures = silent
	}
}

// WithIAPTokenSource allows setting a custom IAP token source for authentication.
func WithIAPTokenSource(tokenSource roachprodutil.IAPTokenSource) OptionFunc {
	return func(c *Client) {
		c.httpClient = tokenSource.GetHTTPClient()
	}
}
