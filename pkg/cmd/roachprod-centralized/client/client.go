// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package client provides a client for communicating with the roachprod-centralized API.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	clustertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/clusters/types"
	publicdns "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/public-dns/types"
	tasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/tasks/types"
	cloudcluster "github.com/cockroachdb/cockroach/pkg/roachprod/cloud/types"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/promhelperclient"
	"github.com/cockroachdb/cockroach/pkg/roachprod/roachprodutil"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
)

const (
	// DefaultEnabled controls whether the centralized API client is enabled by default.
	DefaultEnabled = false
	// DefaultTimeout is the default timeout for API requests.
	DefaultTimeout = 10 * time.Second
	// DefaultRetryDelay is the default delay between retry attempts.
	DefaultRetryDelay = 1 * time.Second
	// DefaultRetryAttempts is the default number of retry attempts on failure.
	DefaultRetryAttempts = 2
	// DefaultBaseURL is the default base URL for the centralized API.
	DefaultBaseURL = "https://api.rp.testeng.crdb.io"
)

var (
	// ErrDisabled is returned when an operation is attempted on a disabled client.
	ErrDisabled = errors.New("centralized API client is disabled")
)

// Client provides methods to interact with the roachprod-centralized API.
type Client struct {
	config     Config
	httpClient *http.Client
}

// ListClustersOptions contains options for listing clusters from the centralized API.
type ListClustersOptions struct {
	// Name filters clusters by name (optional)
	Name string
}

// ClustersResponse represents the response from the centralized API for listing clusters.
type ClustersResponse struct {
	Data cloudcluster.Clusters `json:"data"`
}

// ClusterResponse represents the response from the centralized API for listing clusters.
type ClusterResponse struct {
	Data cloudcluster.Cluster `json:"data"`
}

// NewClient creates a new centralized API client using functional options.
func NewClient(options ...Option) (*Client, error) {
	c := &Client{
		config: DefaultConfig(),
	}

	// Apply options and check for custom token source
	for _, option := range options {
		option.apply(c)
	}

	// If the client is disabled, return early with a nil client
	if !c.config.Enabled {
		return c, nil
	}

	// Validate configuration
	if err := c.config.Validate(); err != nil {
		return nil, errors.Wrapf(err, "invalid centralized API client configuration")
	}

	// If no custom IAP Token source is provided, then create one and use
	// its HTTP client.
	if c.httpClient == nil {
		iapTokenSource, err := roachprodutil.NewIAPTokenSource(roachprodutil.IAPTokenSourceOptions{
			OAuthClientID:       promhelperclient.OAuthClientID,
			ServiceAccountEmail: promhelperclient.ServiceAccountEmail,
		})
		if err != nil {
			return nil, err
		}
		c.httpClient = iapTokenSource.GetHTTPClient()
	}

	return c, nil
}

// NewClientWithConfig creates a new centralized API client using the legacy Config struct.
// Deprecated: Use NewClient with functional options instead.
func NewClientWithConfig(config Config) (*Client, error) {
	return NewClient(WithConfig(config))
}

func (c *Client) IsEnabled() bool {
	return c.config.Enabled
}

func (c *Client) RegisterCluster(
	ctx context.Context, l *logger.Logger, cluster *cloudcluster.Cluster,
) error {
	// If the centralized API is disabled, return an error.
	if !c.config.Enabled {
		return ErrDisabled
	}

	url := fmt.Sprintf("%s%s/register", c.config.BaseURL, clustertypes.ControllerPath)
	payload := clustertypes.InputRegisterClusterDTO{Cluster: *cluster}

	return c.makeRequest(ctx, l, "POST", url, payload, nil, "register cluster creation")
}

func (c *Client) RegisterClusterUpdate(
	ctx context.Context, l *logger.Logger, cluster *cloudcluster.Cluster,
) error {
	// If the centralized API is disabled, return an error.
	if !c.config.Enabled {
		return ErrDisabled
	}

	url := fmt.Sprintf("%s%s/register/%s", c.config.BaseURL, clustertypes.ControllerPath, cluster.Name)
	payload := clustertypes.InputRegisterClusterUpdateDTO{Cluster: *cluster}

	return c.makeRequest(ctx, l, "PUT", url, payload, nil, "register cluster update")
}

// DeleteCluster notifies the centralized API about a cluster deletion.
func (c *Client) RegisterClusterDelete(
	ctx context.Context, l *logger.Logger, clusterName string,
) error {

	// If the centralized API is disabled, return an error.
	if !c.config.Enabled {
		return ErrDisabled
	}

	url := fmt.Sprintf("%s%s/register/%s", c.config.BaseURL, clustertypes.ControllerPath, clusterName)

	return c.makeRequest(ctx, l, "DELETE", url, nil, nil, "register cluster deletion")
}

type ListClustersDTO struct {
	Clusters     cloudcluster.Clusters
	BadInstances vm.List
}

// ListClusters retrieves all clusters from the centralized API.
func (c *Client) ListClusters(
	ctx context.Context, l *logger.Logger, options ListClustersOptions,
) (*ListClustersDTO, error) {

	// If the centralized API is disabled, return an error.
	if !c.config.Enabled {
		return nil, ErrDisabled
	}

	apiUrl := fmt.Sprintf("%s%s?", c.config.BaseURL, clustertypes.ControllerPath)

	if options.Name != "" {
		apiUrl += url.QueryEscape(fmt.Sprintf("name[like]=%s-%%", options.Name))
	}

	var response ClustersResponse
	err := c.makeRequest(ctx, l, "GET", apiUrl, nil, &response, "list clusters")
	if err != nil {
		return nil, err
	}

	//TODO (golgeek): Implement bad instances handling in API
	cloudResult := &ListClustersDTO{
		Clusters:     response.Data,
		BadInstances: vm.List{},
	}

	return cloudResult, nil
}

func (c *Client) SyncDNS(ctx context.Context, l *logger.Logger) (*tasks.TaskResult, error) {

	if !c.config.Enabled {
		return nil, ErrDisabled
	}

	url := fmt.Sprintf("%s%s/sync", c.config.BaseURL, publicdns.ControllerPath)

	var response *tasks.TaskResult
	err := c.makeRequest(ctx, l, "POST", url, nil, response, "sync DNS")
	if err != nil {
		return nil, err
	}
	return response, nil
}

// makeRequest makes an HTTP request with retries and proper error handling.
func (c *Client) makeRequest(
	ctx context.Context,
	l *logger.Logger,
	method, url string,
	payload interface{},
	result interface{},
	operation string,
) error {

	var lastErr error

	attempt := 1
	for {
		err := c.doRequest(ctx, l, method, url, payload, result)
		if err == nil {
			return nil
		}

		lastErr = err

		// Don't retry on client errors (4xx), only server errors and network issues
		if isClientError(err) {
			break
		}

		l.Printf("DEBUG: centralized API %s failed (attempt %d/%d): %v", operation, attempt, c.config.RetryAttempts, err)

		if attempt >= c.config.RetryAttempts {
			break
		}

		attempt++

		// Wait before retry
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.config.RetryDelay):
		}
	}

	// Log the final error based on configuration
	l.Printf("ERROR: centralized API %s failed after %d attempts: %v", operation, attempt, lastErr)

	return lastErr
}

// doRequest performs a single HTTP request with OAuth2 authentication.
func (c *Client) doRequest(
	ctx context.Context,
	l *logger.Logger,
	method, url string,
	payload interface{},
	result interface{},
) error {
	var body io.Reader

	if payload != nil {
		jsonData, err := json.Marshal(payload)
		if err != nil {
			return errors.Wrap(err, "failed to marshal request payload")
		}
		body = bytes.NewReader(jsonData)
	}

	// Create request with context and timeout
	reqCtx, reqCancel := context.WithTimeout(ctx, c.config.Timeout)
	defer reqCancel()

	req, err := http.NewRequestWithContext(reqCtx, method, url, body)
	if err != nil {
		return errors.Wrap(err, "failed to create HTTP request")
	}

	// Set headers
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("User-Agent", "roachprod-client/1.0")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "HTTP request failed")
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return &HTTPError{
			StatusCode: resp.StatusCode,
			Body:       string(bodyBytes),
		}
	}

	// Parse response if result is provided
	if result != nil {

		bodyStr, _ := io.ReadAll(resp.Body)
		if err := json.Unmarshal(bodyStr, &result); err != nil {
			return errors.Wrapf(err, "failed to parse response: %s", string(bodyStr))
		}
	}

	return nil
}

// HTTPError represents an HTTP error response.
type HTTPError struct {
	StatusCode int
	Body       string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.StatusCode, e.Body)
}

// isClientError checks if the error is a 4xx client error.
func isClientError(err error) bool {
	httpErr := (*HTTPError)(nil)
	if errors.As(err, &httpErr) {
		return httpErr.StatusCode >= 400 && httpErr.StatusCode < 500
	}
	return false
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
