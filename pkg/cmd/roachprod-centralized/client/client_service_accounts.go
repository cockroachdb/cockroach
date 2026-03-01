// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package client

import (
	"context"
	"fmt"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/client/auth"
	satypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/service-accounts/types"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

// CreateServiceAccount creates a new service account.
// POST /v1/service-accounts
func (c *Client) CreateServiceAccount(
	ctx context.Context, l *logger.Logger, req satypes.CreateServiceAccountRequest,
) (*satypes.ServiceAccountResponse, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}

	endpoint := fmt.Sprintf("%s%s", c.config.BaseURL, satypes.ControllerPath)
	var response auth.APIResponse[satypes.ServiceAccountResponse]
	if err := c.makeRequest(
		ctx, l, "POST", endpoint, req, &response, "create service account",
	); err != nil {
		return nil, err
	}

	return &response.Data, nil
}

// ListServiceAccounts lists all service accounts.
// GET /v1/service-accounts
func (c *Client) ListServiceAccounts(
	ctx context.Context, l *logger.Logger,
) ([]satypes.ServiceAccountResponse, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}

	endpoint := fmt.Sprintf("%s%s", c.config.BaseURL, satypes.ControllerPath)
	var response auth.APIResponse[[]satypes.ServiceAccountResponse]
	if err := c.makeRequest(
		ctx, l, "GET", endpoint, nil, &response, "list service accounts",
	); err != nil {
		return nil, err
	}

	return response.Data, nil
}

// DeleteServiceAccount deletes a service account by ID.
// DELETE /v1/service-accounts/{id}
func (c *Client) DeleteServiceAccount(ctx context.Context, l *logger.Logger, id string) error {
	if !c.IsEnabled() {
		return ErrDisabled
	}

	endpoint := fmt.Sprintf(
		"%s%s/%s", c.config.BaseURL, satypes.ControllerPath, url.PathEscape(id),
	)
	return c.makeRequest(
		ctx, l, "DELETE", endpoint, nil, nil, "delete service account",
	)
}

// ListServiceAccountTokens lists all tokens for a service account.
// GET /v1/service-accounts/{saID}/tokens
func (c *Client) ListServiceAccountTokens(
	ctx context.Context, l *logger.Logger, saID string,
) ([]satypes.TokenListResponse, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}

	endpoint := fmt.Sprintf(
		"%s%s/%s/tokens",
		c.config.BaseURL, satypes.ControllerPath, url.PathEscape(saID),
	)
	var response auth.APIResponse[[]satypes.TokenListResponse]
	if err := c.makeRequest(
		ctx, l, "GET", endpoint, nil, &response, "list service account tokens",
	); err != nil {
		return nil, err
	}

	return response.Data, nil
}

// MintServiceAccountToken mints a new token for a service account.
// POST /v1/service-accounts/{saID}/tokens
func (c *Client) MintServiceAccountToken(
	ctx context.Context, l *logger.Logger, saID string, ttlDays int,
) (*satypes.MintTokenResponse, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}

	endpoint := fmt.Sprintf(
		"%s%s/%s/tokens",
		c.config.BaseURL, satypes.ControllerPath, url.PathEscape(saID),
	)
	var response auth.APIResponse[satypes.MintTokenResponse]
	if err := c.makeRequest(
		ctx, l, "POST", endpoint,
		satypes.MintTokenRequest{TTLDays: ttlDays},
		&response,
		"mint service account token",
	); err != nil {
		return nil, err
	}

	return &response.Data, nil
}

// RevokeServiceAccountToken revokes a specific token for a service account.
// DELETE /v1/service-accounts/{saID}/tokens/{tokenID}
func (c *Client) RevokeServiceAccountToken(
	ctx context.Context, l *logger.Logger, saID string, tokenID string,
) error {
	if !c.IsEnabled() {
		return ErrDisabled
	}

	endpoint := fmt.Sprintf(
		"%s%s/%s/tokens/%s",
		c.config.BaseURL, satypes.ControllerPath,
		url.PathEscape(saID), url.PathEscape(tokenID),
	)
	return c.makeRequest(
		ctx, l, "DELETE", endpoint, nil, nil, "revoke service account token",
	)
}

// ListServiceAccountOrigins lists all origin restrictions for a service account.
// GET /v1/service-accounts/{saID}/origins
func (c *Client) ListServiceAccountOrigins(
	ctx context.Context, l *logger.Logger, saID string,
) ([]satypes.OriginResponse, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}

	endpoint := fmt.Sprintf(
		"%s%s/%s/origins",
		c.config.BaseURL, satypes.ControllerPath, url.PathEscape(saID),
	)
	var response auth.APIResponse[[]satypes.OriginResponse]
	if err := c.makeRequest(
		ctx, l, "GET", endpoint, nil, &response, "list service account origins",
	); err != nil {
		return nil, err
	}

	return response.Data, nil
}

// CreateServiceAccountOrigin adds an origin restriction to a service account.
// POST /v1/service-accounts/{saID}/origins
func (c *Client) CreateServiceAccountOrigin(
	ctx context.Context, l *logger.Logger, saID string, cidr string, description string,
) (*satypes.OriginResponse, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}

	endpoint := fmt.Sprintf(
		"%s%s/%s/origins",
		c.config.BaseURL, satypes.ControllerPath, url.PathEscape(saID),
	)
	var response auth.APIResponse[satypes.OriginResponse]
	if err := c.makeRequest(
		ctx, l, "POST", endpoint,
		satypes.AddOriginRequest{CIDR: cidr, Description: description},
		&response,
		"create service account origin",
	); err != nil {
		return nil, err
	}

	return &response.Data, nil
}

// DeleteServiceAccountOrigin deletes an origin restriction from a service account.
// DELETE /v1/service-accounts/{saID}/origins/{originID}
func (c *Client) DeleteServiceAccountOrigin(
	ctx context.Context, l *logger.Logger, saID string, originID string,
) error {
	if !c.IsEnabled() {
		return ErrDisabled
	}

	endpoint := fmt.Sprintf(
		"%s%s/%s/origins/%s",
		c.config.BaseURL, satypes.ControllerPath,
		url.PathEscape(saID), url.PathEscape(originID),
	)
	return c.makeRequest(
		ctx, l, "DELETE", endpoint, nil, nil, "delete service account origin",
	)
}

// ListServiceAccountPermissions lists all permissions for a service account.
// GET /v1/service-accounts/{saID}/permissions
func (c *Client) ListServiceAccountPermissions(
	ctx context.Context, l *logger.Logger, saID string,
) ([]satypes.PermissionResponse, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}

	endpoint := fmt.Sprintf(
		"%s%s/%s/permissions",
		c.config.BaseURL, satypes.ControllerPath, url.PathEscape(saID),
	)
	var response auth.APIResponse[[]satypes.PermissionResponse]
	if err := c.makeRequest(
		ctx, l, "GET", endpoint, nil, &response, "list service account permissions",
	); err != nil {
		return nil, err
	}

	return response.Data, nil
}

// UpdateServiceAccountPermissions replaces all permissions for a service account.
// PUT /v1/service-accounts/{saID}/permissions
func (c *Client) UpdateServiceAccountPermissions(
	ctx context.Context, l *logger.Logger, saID string, permissions []satypes.AddPermissionRequest,
) error {
	if !c.IsEnabled() {
		return ErrDisabled
	}

	endpoint := fmt.Sprintf(
		"%s%s/%s/permissions",
		c.config.BaseURL, satypes.ControllerPath, url.PathEscape(saID),
	)
	return c.makeRequest(
		ctx, l, "PUT", endpoint,
		satypes.ReplacePermissionsRequest{Permissions: permissions},
		nil,
		"update service account permissions",
	)
}
