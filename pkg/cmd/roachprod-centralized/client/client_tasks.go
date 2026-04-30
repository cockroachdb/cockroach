// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

// ListTasksOptions contains optional query filters for listing tasks.
type ListTasksOptions struct {
	Type  string
	State string
}

// TaskListItem is the JSON representation of a task returned by the list and
// get endpoints.
type TaskListItem struct {
	ID               string `json:"id"`
	Type             string `json:"type"`
	State            string `json:"state"`
	Error            string `json:"error,omitempty"`
	Reference        string `json:"reference,omitempty"`
	CreationDatetime string `json:"creation_datetime"`
	UpdateDatetime   string `json:"update_datetime"`
}

// ListTasks lists tasks with optional filters.
// GET /v1/tasks?...
func (c *Client) ListTasks(
	ctx context.Context, l *logger.Logger, opts ListTasksOptions,
) ([]TaskListItem, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}
	params := url.Values{}
	if opts.Type != "" {
		params.Set("type", opts.Type)
	}
	if opts.State != "" {
		params.Set("state", opts.State)
	}
	endpoint := fmt.Sprintf("%s/v1/tasks", c.config.BaseURL)
	if encoded := params.Encode(); encoded != "" {
		endpoint += "?" + encoded
	}
	var response apiResponse[[]TaskListItem]
	if err := c.makeRequest(
		ctx, l, "GET", endpoint, nil, &response, "list tasks",
	); err != nil {
		return nil, err
	}
	return response.Data, nil
}

// GetTask gets a task by ID.
// GET /v1/tasks/:id
func (c *Client) GetTask(ctx context.Context, l *logger.Logger, id string) (*TaskListItem, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}
	endpoint := fmt.Sprintf(
		"%s/v1/tasks/%s", c.config.BaseURL, url.PathEscape(id),
	)
	var response apiResponse[TaskListItem]
	if err := c.makeRequest(
		ctx, l, "GET", endpoint, nil, &response, "get task",
	); err != nil {
		return nil, err
	}
	return &response.Data, nil
}

// StreamTaskLogs connects to the task logs SSE endpoint and returns the raw
// HTTP response for streaming. The caller owns resp.Body and must close it.
// This bypasses makeRequest because SSE streams are long-lived and must not
// be subject to the default client timeout.
func (c *Client) StreamTaskLogs(
	ctx context.Context, id string, offset int,
) (*http.Response, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}
	if c.httpClient == nil {
		return nil, errHTTPClientNotConfigured
	}

	endpoint := fmt.Sprintf("%s/v1/tasks/%s/logs?offset=%d",
		c.config.BaseURL, url.PathEscape(id), offset)

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, errors.Wrap(err, "create SSE request")
	}
	req.Header.Set("User-Agent", CLIENT_USERAGENT)
	req.Header.Set("Accept", "text/event-stream")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "SSE connection")
	}

	if resp.StatusCode >= http.StatusBadRequest {
		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, &HTTPError{StatusCode: resp.StatusCode, Body: string(bodyBytes)}
	}

	return resp, nil
}
