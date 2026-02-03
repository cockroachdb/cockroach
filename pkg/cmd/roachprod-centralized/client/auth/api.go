// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/cockroachdb/errors"
)

// API response types that mirror server controller types.

// ExchangeTokenRequest is the request body for exchanging an Okta token.
type ExchangeTokenRequest struct {
	OktaAccessToken string `json:"okta_access_token"`
}

// ExchangeTokenResponse is the response for successful token exchange.
type ExchangeTokenResponse struct {
	Token     string `json:"token"`
	ExpiresAt string `json:"expires_at"`
}

// WhoAmIResponse contains information about the current authenticated principal.
type WhoAmIResponse struct {
	User           *UserInfo           `json:"user,omitempty"`
	ServiceAccount *ServiceAccountInfo `json:"service_account,omitempty"`
	Permissions    []PermissionInfo    `json:"permissions"`
	Token          TokenInfoResponse   `json:"token"`
}

// UserInfo contains user details.
type UserInfo struct {
	ID     string `json:"id"`
	Email  string `json:"email"`
	Name   string `json:"name"`
	Active bool   `json:"active"`
}

// ServiceAccountInfo contains service account details.
type ServiceAccountInfo struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Enabled     bool   `json:"enabled"`
}

// PermissionInfo contains permission details.
type PermissionInfo struct {
	Provider   string `json:"provider"`
	Account    string `json:"account"`
	Permission string `json:"permission"`
}

// TokenInfoResponse contains metadata about the authentication token.
type TokenInfoResponse struct {
	ID         string `json:"id"`
	Token      string `json:"token"`
	Type       string `json:"type"`
	Status     string `json:"status,omitempty"`
	CreatedAt  string `json:"created_at,omitempty"`
	ExpiresAt  string `json:"expires_at,omitempty"`
	LastUsedAt string `json:"last_used_at,omitempty"`
}

// RevokeTokenResponse is the response for successful token revocation.
type RevokeTokenResponse struct {
	Message string `json:"message"`
}

// APIResponse wraps the standard API response format.
type APIResponse[T any] struct {
	Data  T      `json:"data,omitempty"`
	Error string `json:"error,omitempty"`
}

// APIClient provides methods to call the roachprod-centralized API.
type APIClient struct {
	baseURL    string
	httpClient *http.Client
	token      string
}

// NewAPIClient creates a new API client.
func NewAPIClient(baseURL string) *APIClient {
	return &APIClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// WithToken sets the bearer token for authenticated requests.
func (c *APIClient) WithToken(token string) *APIClient {
	c.token = token
	return c
}

// ExchangeOktaToken exchanges an Okta ID token for an opaque bearer token.
// POST /v1/auth/okta/exchange
func (c *APIClient) ExchangeOktaToken(
	ctx context.Context, oktaIDToken string,
) (*ExchangeTokenResponse, error) {
	reqBody := ExchangeTokenRequest{
		OktaAccessToken: oktaIDToken,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal request")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/v1/auth/okta/exchange", bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create request")
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to send request")
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read response")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Newf("token exchange failed: %s (status %d)", string(body), resp.StatusCode)
	}

	var apiResp APIResponse[ExchangeTokenResponse]
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, errors.Wrap(err, "failed to parse response")
	}

	if apiResp.Error != "" {
		return nil, errors.Newf("token exchange failed: %s", apiResp.Error)
	}

	return &apiResp.Data, nil
}

// WhoAmI returns information about the current authenticated principal.
// GET /v1/auth/whoami
func (c *APIClient) WhoAmI(ctx context.Context) (*WhoAmIResponse, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+"/v1/auth/whoami", nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create request")
	}

	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to send request")
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read response")
	}

	if resp.StatusCode == http.StatusUnauthorized {
		return nil, errors.New("not authenticated or token expired")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Newf("whoami request failed: %s (status %d)", string(body), resp.StatusCode)
	}

	var apiResp APIResponse[WhoAmIResponse]
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, errors.Wrap(err, "failed to parse response")
	}

	if apiResp.Error != "" {
		return nil, errors.Newf("whoami request failed: %s", apiResp.Error)
	}

	return &apiResp.Data, nil
}

// RevokeToken revokes a specific token by ID.
// DELETE /v1/auth/tokens/:id
func (c *APIClient) RevokeToken(ctx context.Context, tokenID string) error {
	req, err := http.NewRequestWithContext(ctx, "DELETE", fmt.Sprintf("%s/v1/auth/tokens/%s", c.baseURL, tokenID), nil)
	if err != nil {
		return errors.Wrap(err, "failed to create request")
	}

	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to send request")
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrap(err, "failed to read response")
	}

	if resp.StatusCode == http.StatusUnauthorized {
		return errors.New("not authenticated or token expired")
	}

	if resp.StatusCode != http.StatusOK {
		return errors.Newf("token revocation failed: %s (status %d)", string(body), resp.StatusCode)
	}

	return nil
}
