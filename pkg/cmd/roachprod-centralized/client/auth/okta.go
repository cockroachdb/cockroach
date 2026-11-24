// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
)

// OktaDeviceFlow handles the OAuth 2.0 Device Authorization Grant (RFC 8628).
type OktaDeviceFlow struct {
	// Issuer is the Okta tenant URL (e.g., https://cockroachlabs.okta.com).
	Issuer string
	// ClientID is the OAuth client ID for the roachprod application.
	ClientID string
	// HTTPClient is the HTTP client to use for requests (optional, defaults to http.DefaultClient).
	HTTPClient *http.Client
}

// DeviceCodeResponse is the response from Okta's device authorization endpoint.
type DeviceCodeResponse struct {
	// DeviceCode is the device verification code.
	DeviceCode string `json:"device_code"`
	// UserCode is the end-user verification code.
	UserCode string `json:"user_code"`
	// VerificationURI is the end-user verification URI on the authorization server.
	VerificationURI string `json:"verification_uri"`
	// VerificationURIComplete is the verification URI that includes the user_code.
	VerificationURIComplete string `json:"verification_uri_complete"`
	// ExpiresIn is the lifetime in seconds of the device_code and user_code.
	ExpiresIn int `json:"expires_in"`
	// Interval is the minimum number of seconds the client should wait between polling requests.
	Interval int `json:"interval"`
}

// TokenResponse is the response from Okta's token endpoint.
type TokenResponse struct {
	// AccessToken is the access token issued by the authorization server.
	AccessToken string `json:"access_token"`
	// IDToken is the ID token (JWT) containing user identity claims.
	IDToken string `json:"id_token"`
	// TokenType is the type of token (typically "Bearer").
	TokenType string `json:"token_type"`
	// ExpiresIn is the lifetime in seconds of the access token.
	ExpiresIn int `json:"expires_in"`
	// Scope is the scope of the access token.
	Scope string `json:"scope"`
}

// tokenErrorResponse is the error response from Okta's token endpoint.
type tokenErrorResponse struct {
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

// NewOktaDeviceFlow creates a new OktaDeviceFlow instance.
func NewOktaDeviceFlow(issuer, clientID string) *OktaDeviceFlow {
	return &OktaDeviceFlow{
		Issuer:     issuer,
		ClientID:   clientID,
		HTTPClient: http.DefaultClient,
	}
}

// StartDeviceAuthorization initiates the device authorization flow.
// It requests a device code from Okta that the user will use to complete authentication.
func (o *OktaDeviceFlow) StartDeviceAuthorization(
	ctx context.Context,
) (*DeviceCodeResponse, error) {
	if o.ClientID == "" {
		return nil, errors.Newf("Okta client ID is required; set %s", EnvOktaClientID)
	}

	// Build the device authorization endpoint URL
	deviceAuthURL := strings.TrimSuffix(o.Issuer, "/") + "/oauth2/v1/device/authorize"

	// Build request body
	data := url.Values{}
	data.Set("client_id", o.ClientID)
	data.Set("scope", "openid email profile")

	req, err := http.NewRequestWithContext(ctx, "POST", deviceAuthURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create device authorization request")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := o.httpClient().Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to send device authorization request")
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read device authorization response")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Newf("device authorization failed: %s (status %d)", string(body), resp.StatusCode)
	}

	var deviceCodeResp DeviceCodeResponse
	if err := json.Unmarshal(body, &deviceCodeResp); err != nil {
		return nil, errors.Wrap(err, "failed to parse device authorization response")
	}

	// Set default interval if not provided
	if deviceCodeResp.Interval == 0 {
		deviceCodeResp.Interval = 5
	}

	return &deviceCodeResp, nil
}

// PollForToken polls Okta's token endpoint until the user completes authentication,
// the device code expires, or the context is cancelled.
// Returns the token response containing the ID token for exchange.
func (o *OktaDeviceFlow) PollForToken(
	ctx context.Context, deviceCode string, interval int,
) (*TokenResponse, error) {
	tokenURL := strings.TrimSuffix(o.Issuer, "/") + "/oauth2/v1/token"

	// Build request body
	data := url.Values{}
	data.Set("client_id", o.ClientID)
	data.Set("device_code", deviceCode)
	data.Set("grant_type", "urn:ietf:params:oauth:grant-type:device_code")

	pollInterval := time.Duration(interval) * time.Second
	if pollInterval < time.Second {
		pollInterval = 5 * time.Second
	}

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			tokenResp, err := o.pollOnce(ctx, tokenURL, data)
			if err != nil {
				// Check for specific OAuth errors that require action
				if errors.Is(err, ErrAuthorizationPending) {
					// User hasn't completed auth yet, keep polling
					continue
				}
				if errors.Is(err, ErrSlowDown) {
					// Increase polling interval
					pollInterval += 5 * time.Second
					ticker.Reset(pollInterval)
					continue
				}
				// Other errors are terminal
				return nil, err
			}
			return tokenResp, nil
		}
	}
}

// pollOnce makes a single token request.
func (o *OktaDeviceFlow) pollOnce(
	ctx context.Context, tokenURL string, data url.Values,
) (*TokenResponse, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", tokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create token request")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := o.httpClient().Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to send token request")
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read token response")
	}

	// Check for OAuth error responses (typically 400 status)
	if resp.StatusCode == http.StatusBadRequest {
		var errResp tokenErrorResponse
		if err := json.Unmarshal(body, &errResp); err == nil {
			switch errResp.Error {
			case "authorization_pending":
				return nil, ErrAuthorizationPending
			case "slow_down":
				return nil, ErrSlowDown
			case "access_denied":
				return nil, ErrAccessDenied
			case "expired_token":
				return nil, ErrDeviceCodeExpired
			default:
				return nil, errors.Newf("token request failed: %s - %s", errResp.Error, errResp.ErrorDescription)
			}
		}
		return nil, errors.Newf("token request failed: %s", string(body))
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Newf("token request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp TokenResponse
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return nil, errors.Wrap(err, "failed to parse token response")
	}

	return &tokenResp, nil
}

// httpClient returns the HTTP client to use for requests.
func (o *OktaDeviceFlow) httpClient() *http.Client {
	if o.HTTPClient != nil {
		return o.HTTPClient
	}
	return http.DefaultClient
}
