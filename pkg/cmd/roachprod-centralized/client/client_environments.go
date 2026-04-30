// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package client

import (
	"context"
	"fmt"
	"net/url"

	envtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/environments/types"
	envmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/environments"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

// ListEnvironments lists all environments.
// GET /v1/environments
func (c *Client) ListEnvironments(
	ctx context.Context, l *logger.Logger,
) ([]envmodels.Environment, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}
	endpoint := fmt.Sprintf("%s%s", c.config.BaseURL, envtypes.ControllerPath)
	var response apiResponse[[]envmodels.Environment]
	if err := c.makeRequest(
		ctx, l, "GET", endpoint, nil, &response, "list environments",
	); err != nil {
		return nil, err
	}
	return response.Data, nil
}

// CreateEnvironment creates a new environment.
// POST /v1/environments
func (c *Client) CreateEnvironment(
	ctx context.Context, l *logger.Logger, req envtypes.InputCreateEnvironmentDTO,
) (*envmodels.Environment, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}
	endpoint := fmt.Sprintf("%s%s", c.config.BaseURL, envtypes.ControllerPath)
	var response apiResponse[envmodels.Environment]
	if err := c.makeRequest(
		ctx, l, "POST", endpoint, req, &response, "create environment",
	); err != nil {
		return nil, err
	}
	return &response.Data, nil
}

// GetEnvironment gets an environment by name.
// GET /v1/environments/:name
func (c *Client) GetEnvironment(
	ctx context.Context, l *logger.Logger, name string,
) (*envmodels.Environment, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}
	endpoint := fmt.Sprintf(
		"%s%s/%s", c.config.BaseURL, envtypes.ControllerPath, url.PathEscape(name),
	)
	var response apiResponse[envmodels.Environment]
	if err := c.makeRequest(
		ctx, l, "GET", endpoint, nil, &response, "get environment",
	); err != nil {
		return nil, err
	}
	return &response.Data, nil
}

// DeleteEnvironment deletes an environment by name.
// DELETE /v1/environments/:name
func (c *Client) DeleteEnvironment(ctx context.Context, l *logger.Logger, name string) error {
	if !c.IsEnabled() {
		return ErrDisabled
	}
	endpoint := fmt.Sprintf(
		"%s%s/%s", c.config.BaseURL, envtypes.ControllerPath, url.PathEscape(name),
	)
	return c.makeRequest(
		ctx, l, "DELETE", endpoint, nil, nil, "delete environment",
	)
}

// ListEnvironmentVariables lists all variables for an environment.
// GET /v1/environments/:name/variables
func (c *Client) ListEnvironmentVariables(
	ctx context.Context, l *logger.Logger, envName string,
) ([]envmodels.EnvironmentVariable, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}
	endpoint := fmt.Sprintf(
		"%s%s/%s/variables",
		c.config.BaseURL, envtypes.ControllerPath, url.PathEscape(envName),
	)
	var response apiResponse[[]envmodels.EnvironmentVariable]
	if err := c.makeRequest(
		ctx, l, "GET", endpoint, nil, &response, "list environment variables",
	); err != nil {
		return nil, err
	}
	return response.Data, nil
}

// GetEnvironmentVariable gets a specific variable.
// GET /v1/environments/:name/variables/:key
func (c *Client) GetEnvironmentVariable(
	ctx context.Context, l *logger.Logger, envName string, key string,
) (*envmodels.EnvironmentVariable, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}
	endpoint := fmt.Sprintf(
		"%s%s/%s/variables/%s",
		c.config.BaseURL, envtypes.ControllerPath,
		url.PathEscape(envName), url.PathEscape(key),
	)
	var response apiResponse[envmodels.EnvironmentVariable]
	if err := c.makeRequest(
		ctx, l, "GET", endpoint, nil, &response, "get environment variable",
	); err != nil {
		return nil, err
	}
	return &response.Data, nil
}

// CreateEnvironmentVariable creates a new variable in an environment.
// POST /v1/environments/:name/variables
func (c *Client) CreateEnvironmentVariable(
	ctx context.Context, l *logger.Logger, envName string, req envtypes.InputCreateVariableDTO,
) (*envmodels.EnvironmentVariable, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}
	endpoint := fmt.Sprintf(
		"%s%s/%s/variables",
		c.config.BaseURL, envtypes.ControllerPath, url.PathEscape(envName),
	)
	var response apiResponse[envmodels.EnvironmentVariable]
	if err := c.makeRequest(
		ctx, l, "POST", endpoint, req, &response, "create environment variable",
	); err != nil {
		return nil, err
	}
	return &response.Data, nil
}

// UpdateEnvironmentVariable updates an existing variable.
// PUT /v1/environments/:name/variables/:key
func (c *Client) UpdateEnvironmentVariable(
	ctx context.Context,
	l *logger.Logger,
	envName string,
	key string,
	req envtypes.InputUpdateVariableDTO,
) (*envmodels.EnvironmentVariable, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}
	endpoint := fmt.Sprintf(
		"%s%s/%s/variables/%s",
		c.config.BaseURL, envtypes.ControllerPath,
		url.PathEscape(envName), url.PathEscape(key),
	)
	var response apiResponse[envmodels.EnvironmentVariable]
	if err := c.makeRequest(
		ctx, l, "PUT", endpoint, req, &response, "update environment variable",
	); err != nil {
		return nil, err
	}
	return &response.Data, nil
}

// DeleteEnvironmentVariable deletes a variable from an environment.
// DELETE /v1/environments/:name/variables/:key
func (c *Client) DeleteEnvironmentVariable(
	ctx context.Context, l *logger.Logger, envName string, key string,
) error {
	if !c.IsEnabled() {
		return ErrDisabled
	}
	endpoint := fmt.Sprintf(
		"%s%s/%s/variables/%s",
		c.config.BaseURL, envtypes.ControllerPath,
		url.PathEscape(envName), url.PathEscape(key),
	)
	return c.makeRequest(
		ctx, l, "DELETE", endpoint, nil, nil, "delete environment variable",
	)
}
