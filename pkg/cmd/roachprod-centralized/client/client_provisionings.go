// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	provtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/provisionings/types"
	provmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

// ProvisioningResponse captures both the provisioning data and the optional
// task_id returned by create/destroy operations.
type ProvisioningResponse struct {
	Data   *provmodels.Provisioning `json:"data,omitempty"`
	TaskID string                   `json:"task_id,omitempty"`
	Error  string                   `json:"error,omitempty"`
}

// ListProvisioningsOptions contains optional query filters.
type ListProvisioningsOptions struct {
	State       string
	Environment string
	Owner       string
}

// ListTemplates lists all available provisioning templates.
// GET /v1/provisionings/templates
func (c *Client) ListTemplates(
	ctx context.Context, l *logger.Logger,
) ([]provmodels.Template, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}
	endpoint := fmt.Sprintf("%s%s", c.config.BaseURL, provtypes.TemplatesPath)
	var response apiResponse[[]provmodels.Template]
	if err := c.makeRequest(
		ctx, l, "GET", endpoint, nil, &response, "list templates",
	); err != nil {
		return nil, err
	}
	return response.Data, nil
}

// GetTemplate gets a specific template by name.
// GET /v1/provisionings/templates/:name
func (c *Client) GetTemplate(
	ctx context.Context, l *logger.Logger, name string,
) (*provmodels.Template, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}
	endpoint := fmt.Sprintf(
		"%s%s/%s", c.config.BaseURL, provtypes.TemplatesPath, url.PathEscape(name),
	)
	var response apiResponse[provmodels.Template]
	if err := c.makeRequest(
		ctx, l, "GET", endpoint, nil, &response, "get template",
	); err != nil {
		return nil, err
	}
	return &response.Data, nil
}

// CreateProvisioning creates a new provisioning.
// POST /v1/provisionings
func (c *Client) CreateProvisioning(
	ctx context.Context, l *logger.Logger, req provtypes.InputCreateProvisioningDTO,
) (*ProvisioningResponse, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}
	endpoint := fmt.Sprintf("%s%s", c.config.BaseURL, provtypes.ControllerPath)
	var response ProvisioningResponse
	if err := c.makeRequest(
		ctx, l, "POST", endpoint, req, &response, "create provisioning",
	); err != nil {
		return nil, err
	}
	return &response, nil
}

// ListProvisionings lists provisionings with optional filters.
// GET /v1/provisionings?...
func (c *Client) ListProvisionings(
	ctx context.Context, l *logger.Logger, opts ListProvisioningsOptions,
) ([]provmodels.Provisioning, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}
	params := url.Values{}
	if opts.State != "" {
		params.Set("state", opts.State)
	}
	if opts.Environment != "" {
		params.Set("environment", opts.Environment)
	}
	if opts.Owner != "" {
		params.Set("owner", opts.Owner)
	}
	endpoint := fmt.Sprintf("%s%s", c.config.BaseURL, provtypes.ControllerPath)
	if encoded := params.Encode(); encoded != "" {
		endpoint += "?" + encoded
	}
	var response apiResponse[[]provmodels.Provisioning]
	if err := c.makeRequest(
		ctx, l, "GET", endpoint, nil, &response, "list provisionings",
	); err != nil {
		return nil, err
	}
	return response.Data, nil
}

// GetProvisioning gets a provisioning by ID.
// GET /v1/provisionings/:id
func (c *Client) GetProvisioning(
	ctx context.Context, l *logger.Logger, id string,
) (*provmodels.Provisioning, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}
	endpoint := fmt.Sprintf(
		"%s%s/%s", c.config.BaseURL, provtypes.ControllerPath, url.PathEscape(id),
	)
	var response apiResponse[provmodels.Provisioning]
	if err := c.makeRequest(
		ctx, l, "GET", endpoint, nil, &response, "get provisioning",
	); err != nil {
		return nil, err
	}
	return &response.Data, nil
}

// DestroyProvisioning triggers destruction of a provisioning.
// POST /v1/provisionings/:id/destroy
func (c *Client) DestroyProvisioning(
	ctx context.Context, l *logger.Logger, id string,
) (*ProvisioningResponse, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}
	endpoint := fmt.Sprintf(
		"%s%s/%s/destroy", c.config.BaseURL, provtypes.ControllerPath, url.PathEscape(id),
	)
	var response ProvisioningResponse
	if err := c.makeRequest(
		ctx, l, "POST", endpoint, nil, &response, "destroy provisioning",
	); err != nil {
		return nil, err
	}
	return &response, nil
}

// DeleteProvisioning deletes a provisioning record.
// DELETE /v1/provisionings/:id
func (c *Client) DeleteProvisioning(ctx context.Context, l *logger.Logger, id string) error {
	if !c.IsEnabled() {
		return ErrDisabled
	}
	endpoint := fmt.Sprintf(
		"%s%s/%s", c.config.BaseURL, provtypes.ControllerPath, url.PathEscape(id),
	)
	return c.makeRequest(
		ctx, l, "DELETE", endpoint, nil, nil, "delete provisioning",
	)
}

// GetProvisioningPlan gets the plan output for a provisioning.
// GET /v1/provisionings/:id/plan
func (c *Client) GetProvisioningPlan(
	ctx context.Context, l *logger.Logger, id string,
) (json.RawMessage, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}
	endpoint := fmt.Sprintf(
		"%s%s/%s/plan", c.config.BaseURL, provtypes.ControllerPath, url.PathEscape(id),
	)
	var response apiResponse[json.RawMessage]
	if err := c.makeRequest(
		ctx, l, "GET", endpoint, nil, &response, "get provisioning plan",
	); err != nil {
		return nil, err
	}
	return response.Data, nil
}

// GetProvisioningOutputs gets the outputs for a provisioning.
// GET /v1/provisionings/:id/outputs
func (c *Client) GetProvisioningOutputs(
	ctx context.Context, l *logger.Logger, id string,
) (map[string]interface{}, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}
	endpoint := fmt.Sprintf(
		"%s%s/%s/outputs", c.config.BaseURL, provtypes.ControllerPath, url.PathEscape(id),
	)
	var response apiResponse[map[string]interface{}]
	if err := c.makeRequest(
		ctx, l, "GET", endpoint, nil, &response, "get provisioning outputs",
	); err != nil {
		return nil, err
	}
	return response.Data, nil
}

// ExtendProvisioningLifetime extends the lifetime of a provisioning.
// PATCH /v1/provisionings/:id/lifetime
func (c *Client) ExtendProvisioningLifetime(
	ctx context.Context, l *logger.Logger, id string,
) (*provmodels.Provisioning, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}
	endpoint := fmt.Sprintf(
		"%s%s/%s/lifetime", c.config.BaseURL, provtypes.ControllerPath, url.PathEscape(id),
	)
	var response apiResponse[provmodels.Provisioning]
	if err := c.makeRequest(
		ctx, l, "PATCH", endpoint, nil, &response, "extend provisioning lifetime",
	); err != nil {
		return nil, err
	}
	return &response.Data, nil
}

// SetupSSHKeys triggers SSH key setup on a provisioning.
// POST /v1/provisionings/:id/hooks/ssh-keys-setup
func (c *Client) SetupSSHKeys(
	ctx context.Context, l *logger.Logger, id string,
) (*ProvisioningResponse, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}
	endpoint := fmt.Sprintf(
		"%s%s/%s/hooks/ssh-keys-setup",
		c.config.BaseURL, provtypes.ControllerPath, url.PathEscape(id),
	)
	var response ProvisioningResponse
	if err := c.makeRequest(
		ctx, l, "POST", endpoint, nil, &response, "setup ssh keys",
	); err != nil {
		return nil, err
	}
	return &response, nil
}
