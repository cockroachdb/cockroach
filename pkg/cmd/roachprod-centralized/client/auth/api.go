// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/cockroachdb/errors"
)

// API response/request types that mirror server controller types.

// ExchangeTokenRequest is the request body for exchanging an Okta token.
type ExchangeTokenRequest struct {
	OktaIDToken string `json:"okta_id_token"`
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

// ExchangeOktaToken exchanges an Okta ID token for a roachprod opaque
// bearer token. This is an unauthenticated call used during login.
// POST /v1/auth/okta/exchange
func ExchangeOktaToken(
	ctx context.Context, baseURL string, oktaIDToken string,
) (*ExchangeTokenResponse, error) {
	reqBody := ExchangeTokenRequest{
		OktaIDToken: oktaIDToken,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, errors.Wrap(err, "marshal request")
	}

	req, err := http.NewRequestWithContext(
		ctx, "POST", baseURL+"/v1/auth/okta/exchange", bytes.NewReader(bodyBytes),
	)
	if err != nil {
		return nil, errors.Wrap(err, "create request")
	}
	req.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send request")
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Newf(
			"token exchange failed: %s (status %d)", string(body), resp.StatusCode,
		)
	}

	var apiResp APIResponse[ExchangeTokenResponse]
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, errors.Wrap(err, "parse response")
	}

	if apiResp.Error != "" {
		return nil, errors.Newf("token exchange failed: %s", apiResp.Error)
	}

	return &apiResp.Data, nil
}
