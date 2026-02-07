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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/client/auth"
	clustercontroller "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/clusters/types"
	publicdns "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/public-dns/types"
	tasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/tasks/types"
	clusterstypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters/types"
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
	DefaultBaseURL = "https://prod-iap.roachprod.testeng.crdb.io/api"
	// DefaultServiceAccountEmail is the default service account email used for IAP authentication.
	DefaultServiceAccountEmail = "prod-iap-rp-roachprod-sa@cockroach-testeng-infra.iam.gserviceaccount.com"

	CLIENT_VERSION   = "1.0"
	CLIENT_USERAGENT = "roachprod-client/" + CLIENT_VERSION
)

var (
	// ErrDisabled is returned when an operation is attempted on a disabled client.
	ErrDisabled = errors.New("centralized API client is disabled")
	// ErrClusterNotFound is propagated from the cluster service so callers only import the client package.
	ErrClusterNotFound = clusterstypes.ErrClusterNotFound

	errHTTPClientNotConfigured = errors.New("centralized API client HTTP transport is not configured")
)

// Client provides methods to interact with the roachprod-centralized API.
type Client struct {
	config     Config
	httpClient *http.Client
}

// requestOption is a functional option for configuring individual requests.
type requestOption func(*requestOptions)

// requestOptions contains options for customizing request behavior.
type requestOptions struct {
	suppressExpectedErrors bool  // Don't log errors that are expected by caller
	expectedStatusCodes    []int // Status codes that are "normal" for this operation
}

// withSuppressExpectedErrors returns an option to suppress logging of expected errors.
func withSuppressExpectedErrors() requestOption {
	return func(opts *requestOptions) {
		opts.suppressExpectedErrors = true
	}
}

// withExpectedStatusCodes returns an option to specify which status codes are expected.
func withExpectedStatusCodes(codes ...int) requestOption {
	return func(opts *requestOptions) {
		opts.expectedStatusCodes = codes
	}
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

	// Configure HTTP client based on auth mode (if not already set via options)
	if c.httpClient == nil {
		if err := c.configureHTTPClient(); err != nil {
			return nil, err
		}
	}

	return c, nil
}

// configureHTTPClient sets up the HTTP client based on the configured auth mode.
func (c *Client) configureHTTPClient() error {
	switch c.config.AuthMode {
	case AuthModeBearer:
		// Use bearer token from env var or keyring
		tokenSource, err := auth.NewBearerTokenSource()
		if err != nil {
			return errors.Wrap(err, "failed to create bearer token source")
		}
		httpClient, err := tokenSource.GetHTTPClient()
		if err != nil {
			return errors.Wrap(err, "failed to get HTTP client with bearer auth")
		}
		c.httpClient = httpClient

	case AuthModeIAP:
		// Use IAP authentication (legacy mode)
		iapTokenSource, err := roachprodutil.NewIAPTokenSource(roachprodutil.IAPTokenSourceOptions{
			OAuthClientID:       promhelperclient.OAuthClientID,
			ServiceAccountEmail: promhelperclient.ServiceAccountEmail,
		})
		if err != nil {
			return errors.Wrap(err, "failed to create IAP token source")
		}
		c.httpClient = iapTokenSource.GetHTTPClient()

	case AuthModeDisabled:
		// No authentication
		c.httpClient = &http.Client{}

	default:
		return errors.Newf("unknown auth mode: %s", c.config.AuthMode)
	}

	return nil
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
	if !c.IsEnabled() {
		return ErrDisabled
	}

	endpoint := fmt.Sprintf(
		"%s%s/register",
		c.config.BaseURL,
		clustercontroller.ControllerPath,
	)

	return c.makeRequest(
		ctx, l,
		"POST", endpoint,
		clustercontroller.InputRegisterClusterDTO{Cluster: *cluster},
		nil,
		"register cluster creation",
	)
}

func (c *Client) RegisterClusterUpdate(
	ctx context.Context, l *logger.Logger, cluster *cloudcluster.Cluster,
) error {
	if !c.IsEnabled() {
		return ErrDisabled
	}

	endpoint := fmt.Sprintf(
		"%s%s/register/%s",
		c.config.BaseURL,
		clustercontroller.ControllerPath,
		url.PathEscape(cluster.Name),
	)

	return c.makeRequest(
		ctx, l,
		"PUT", endpoint,
		clustercontroller.InputRegisterClusterUpdateDTO{Cluster: *cluster},
		nil,
		"register cluster update",
	)
}

func (c *Client) RegisterClusterUpsert(
	ctx context.Context, l *logger.Logger, cluster *cloudcluster.Cluster,
) error {
	if !c.IsEnabled() {
		return ErrDisabled
	}

	_, err := c.getCluster(
		ctx, l, cluster.Name,
		withSuppressExpectedErrors(),
		withExpectedStatusCodes(http.StatusNotFound),
	)
	if err == nil {
		return c.RegisterClusterUpdate(ctx, l, cluster)
	}

	if errors.Is(err, ErrClusterNotFound) {
		return c.RegisterCluster(ctx, l, cluster)
	}

	return err
}

// DeleteCluster notifies the centralized API about a cluster deletion.
func (c *Client) RegisterClusterDelete(
	ctx context.Context, l *logger.Logger, clusterName string,
) error {
	if !c.IsEnabled() {
		return ErrDisabled
	}

	endpoint := fmt.Sprintf(
		"%s%s/register/%s",
		c.config.BaseURL,
		clustercontroller.ControllerPath,
		url.PathEscape(clusterName),
	)

	return c.makeRequest(
		ctx, l,
		"DELETE", endpoint,
		nil,
		nil,
		"register cluster deletion",
	)
}

// getCluster fetches a single cluster by name from the centralized API.
// Internal method that accepts request options.
func (c *Client) getCluster(
	ctx context.Context, l *logger.Logger, clusterName string, opts ...requestOption,
) (*cloudcluster.Cluster, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}

	endpoint := fmt.Sprintf(
		"%s%s/%s",
		c.config.BaseURL,
		clustercontroller.ControllerPath,
		url.PathEscape(clusterName),
	)

	var response clustercontroller.ClusterResult
	if err := c.makeRequest(ctx, l, "GET", endpoint, nil, &response, "get cluster", opts...); err != nil {
		var httpErr *HTTPError
		if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusNotFound {
			var apiErr struct {
				Error string `json:"error"`
			}
			if json.Unmarshal([]byte(httpErr.Body), &apiErr) == nil && apiErr.Error == ErrClusterNotFound.Error() {
				return nil, ErrClusterNotFound
			}
			if strings.Contains(httpErr.Body, ErrClusterNotFound.Error()) {
				return nil, ErrClusterNotFound
			}
		}
		return nil, err
	}

	return response.Data, nil
}

// GetCluster fetches a single cluster by name from the centralized API.
func (c *Client) GetCluster(
	ctx context.Context, l *logger.Logger, clusterName string,
) (*cloudcluster.Cluster, error) {
	return c.getCluster(ctx, l, clusterName)
}

// ListClustersOptions represents the options for listing clusters from the API.
type ListClustersOptions struct {
	// Name filters clusters by name (optional)
	Name string
}

// ListClustersDTO is the result of ListClusters.
type ListClustersDTO struct {
	Clusters     cloudcluster.Clusters
	BadInstances vm.List
}

// ListClusters retrieves all clusters from the centralized API.
func (c *Client) ListClusters(
	ctx context.Context, l *logger.Logger, options ListClustersOptions,
) (*ListClustersDTO, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}

	endpoint := fmt.Sprintf(
		"%s%s?",
		c.config.BaseURL,
		clustercontroller.ControllerPath,
	)

	if options.Name != "" {
		params := url.Values{}
		params.Set("name[like]", fmt.Sprintf("%s-%%", options.Name))
		endpoint += params.Encode()
	}

	var response clustercontroller.ClustersResult
	err := c.makeRequest(
		ctx, l,
		"GET", endpoint,
		nil,
		&response,
		"list clusters",
	)
	if err != nil {
		return nil, err
	}

	//TODO(golgeek): Implement bad instances in order to migrate GC to
	// roachprod-centralized.
	cloudResult := &ListClustersDTO{
		Clusters:     *response.Data,
		BadInstances: vm.List{},
	}

	return cloudResult, nil
}

// SyncDNS triggers a synchronization of DNS records for all clusters with public DNS.
func (c *Client) SyncDNS(ctx context.Context, l *logger.Logger) (*tasks.TaskResult, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}

	endpoint := fmt.Sprintf(
		"%s%s/sync",
		c.config.BaseURL,
		publicdns.ControllerPath,
	)

	var response tasks.TaskResult
	if err := c.makeRequest(
		ctx, l,
		"POST", endpoint,
		nil,
		&response,
		"sync DNS",
	); err != nil {
		return nil, err
	}
	return &response, nil
}

// WhoAmI returns information about the currently authenticated principal.
func (c *Client) WhoAmI(ctx context.Context, l *logger.Logger) (*auth.WhoAmIResponse, error) {
	if !c.IsEnabled() {
		return nil, ErrDisabled
	}

	endpoint := c.config.BaseURL + "/v1/auth/whoami"
	var response auth.APIResponse[auth.WhoAmIResponse]
	if err := c.makeRequest(ctx, l, "GET", endpoint, nil, &response, "whoami"); err != nil {
		return nil, err
	}

	return &response.Data, nil
}

// RevokeToken revokes a specific token by ID.
func (c *Client) RevokeToken(ctx context.Context, l *logger.Logger, tokenID string) error {
	if !c.IsEnabled() {
		return ErrDisabled
	}

	endpoint := fmt.Sprintf("%s/v1/auth/tokens/%s", c.config.BaseURL, tokenID)
	return c.makeRequest(ctx, l, "DELETE", endpoint, nil, nil, "revoke token")
}

// makeRequest makes an HTTP request with retries and proper error handling.
func (c *Client) makeRequest(
	ctx context.Context,
	l *logger.Logger,
	method, endpoint string,
	payload any,
	result any,
	operation string,
	opts ...requestOption,
) error {
	// Parse options
	options := &requestOptions{}
	for _, opt := range opts {
		opt(options)
	}

	attempts := c.config.RetryAttempts
	if attempts <= 0 {
		attempts = 1
	}

	// Helper to check if an error is expected
	isExpectedError := func(err error) bool {
		if !options.suppressExpectedErrors || len(options.expectedStatusCodes) == 0 {
			return false
		}
		var httpErr *HTTPError
		if !errors.As(err, &httpErr) {
			return false
		}
		for _, code := range options.expectedStatusCodes {
			if httpErr.StatusCode == code {
				return true
			}
		}
		return false
	}

	var (
		lastErr        error
		actualAttempts int
	)

	for attempt := 1; attempt <= attempts; attempt++ {
		actualAttempts = attempt
		err := c.doRequest(ctx, method, endpoint, payload, result)
		if err == nil {
			return nil
		}

		lastErr = err
		if !shouldRetry(err) || attempt >= attempts {
			break
		}

		if !isExpectedError(err) && !c.config.SilentFailures {
			l.Printf(
				"DEBUG: centralized API %s failed (attempt %d/%d): %v",
				operation,
				attempt,
				attempts,
				err,
			)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.config.RetryDelay):
		}
	}

	if lastErr != nil && !isExpectedError(lastErr) && !c.config.SilentFailures {
		l.Printf(
			"ERROR: centralized API %s failed after %d attempt(s): %v",
			operation,
			actualAttempts,
			lastErr,
		)
	}

	return lastErr
}

// doRequest performs a single HTTP request with OAuth2 authentication.
func (c *Client) doRequest(
	ctx context.Context, method, endpoint string, payload any, result any,
) error {
	if c.httpClient == nil {
		return errHTTPClientNotConfigured
	}

	var body io.Reader
	if payload != nil {
		jsonData, err := json.Marshal(payload)
		if err != nil {
			return errors.Wrap(err, "failed to marshal request payload")
		}
		body = bytes.NewReader(jsonData)
	}

	reqCtx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, method, endpoint, body)
	if err != nil {
		return errors.Wrap(err, "failed to create HTTP request")
	}
	req.Header.Set("User-Agent", CLIENT_USERAGENT)

	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "HTTP request failed")
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return &HTTPError{StatusCode: resp.StatusCode, Body: string(bodyBytes)}
	}

	if result == nil {
		return nil
	}

	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(result); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return errors.Wrap(err, "failed to decode response body")
	}

	return nil
}

func shouldRetry(err error) bool {
	if err == nil {
		return false
	}

	// Don't retry configuration or context errors
	if errors.Is(err, errHTTPClientNotConfigured) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		// Network/transport errors are retriable
		return true
	}

	// Special cases: 408 (Request Timeout) and 429 (Too Many Requests) are retriable
	if httpErr.StatusCode == http.StatusRequestTimeout ||
		httpErr.StatusCode == http.StatusTooManyRequests {
		return true
	}

	// Client errors (400-499) are NOT retriable
	if httpErr.StatusCode >= 400 && httpErr.StatusCode < 500 {
		return false
	}

	// Server errors (500+) are retriable
	return httpErr.StatusCode >= 500
}

// HTTPError represents an HTTP error response.
type HTTPError struct {
	StatusCode int
	Body       string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.StatusCode, e.Body)
}
