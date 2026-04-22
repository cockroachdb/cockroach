// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package google implements the embedding.Embedder interface using
// Google's Vertex AI embedding API.
package google

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

const (
	defaultTimeout = 30 * time.Second
	// maxResponseBytes caps the response body to prevent unbounded reads.
	maxResponseBytes = 64 << 20 // 64 MB
)

// retryOpts defines the retry policy for transient HTTP errors.
var retryOpts = retry.Options{
	InitialBackoff: 500 * time.Millisecond,
	MaxBackoff:     10 * time.Second,
	Multiplier:     2,
	MaxRetries:     3,
}

// Client implements embedding.Embedder by calling the Google Vertex
// AI embeddings API. It is safe for concurrent use after construction.
//
// Supports two auth modes: a static access token (for testing, expires
// in 1 hour) or a service account key (auto-refreshes tokens).
type Client struct {
	accessToken string       // static token, used if tokenSrc is nil
	tokenSrc    *tokenSource // auto-refreshing, used if non-nil
	model       string
	dims        int
	endpoint    string
	httpClient  *httputil.Client
}

// NewClient creates a Client using a static access token.
func NewClient(accessToken, project, region, model string, dims int) *Client {
	return &Client{
		accessToken: accessToken,
		model:       model,
		dims:        dims,
		endpoint:    predictEndpoint(region, project, model),
		httpClient: httputil.NewClient(
			httputil.WithClientTimeout(defaultTimeout),
		),
	}
}

// NewClientWithServiceAccount creates a Client that auto-refreshes
// access tokens using a service account key.
func NewClientWithServiceAccount(
	saKey ServiceAccountKey, project, region, model string, dims int,
) *Client {
	httpClient := httputil.NewClient(
		httputil.WithClientTimeout(defaultTimeout),
	)
	return &Client{
		tokenSrc:   newTokenSource(saKey, httpClient),
		model:      model,
		dims:       dims,
		endpoint:   predictEndpoint(region, project, model),
		httpClient: httpClient,
	}
}

func predictEndpoint(region, project, model string) string {
	return fmt.Sprintf(
		"https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/google/models/%s:predict",
		region, project, region, model,
	)
}

// Dims returns the embedding dimension of the model.
func (c *Client) Dims() int {
	return c.dims
}

// Embed produces a normalized embedding vector for a single text
// input.
func (c *Client) Embed(ctx context.Context, text string) ([]float32, error) {
	vecs, err := c.EmbedBatch(ctx, []string{text})
	if err != nil {
		return nil, err
	}
	return vecs[0], nil
}

// EmbedBatch produces normalized embedding vectors for multiple texts
// by calling the Vertex AI predict endpoint.
func (c *Client) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}

	instances := make([]instance, len(texts))
	for i, t := range texts {
		instances[i] = instance{Content: t}
	}
	reqBody := predictRequest{Instances: instances}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, errors.Wrap(err, "marshaling predict request")
	}

	var resp predictResponse
	retries := retry.StartWithCtx(ctx, retryOpts)
	for retries.Next() {
		resp, err = c.doRequest(ctx, bodyBytes)
		if err == nil {
			break
		}
		if !isRetryable(err) {
			return nil, err
		}
	}
	if err != nil {
		return nil, err
	}

	if len(resp.Predictions) != len(texts) {
		return nil, errors.Newf(
			"vertex: expected %d predictions, got %d",
			len(texts), len(resp.Predictions),
		)
	}
	result := make([][]float32, len(texts))
	for i, p := range resp.Predictions {
		result[i] = p.Embeddings.Values
	}
	return result, nil
}

// doRequest sends a single POST to the Vertex AI predict endpoint
// and returns the parsed response.
func (c *Client) doRequest(ctx context.Context, body []byte) (predictResponse, error) {
	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, c.endpoint, bytes.NewReader(body),
	)
	if err != nil {
		return predictResponse{}, errors.Wrap(err, "creating request")
	}
	req.Header.Set("Content-Type", "application/json")
	token := c.accessToken
	if c.tokenSrc != nil {
		token, err = c.tokenSrc.token(ctx)
		if err != nil {
			return predictResponse{}, errors.Wrap(
				err, "vertex: obtaining access token",
			)
		}
	}
	req.Header.Set("Authorization", "Bearer "+token)

	httpResp, err := c.httpClient.Do(req)
	if err != nil {
		return predictResponse{}, pgerror.Wrap(
			err, pgcode.ConnectionFailure,
			"vertex: request failed",
		)
	}
	defer httpResp.Body.Close()

	respBody, err := io.ReadAll(
		io.LimitReader(httpResp.Body, maxResponseBytes),
	)
	if err != nil {
		return predictResponse{}, errors.Wrap(
			err, "vertex: reading response",
		)
	}

	if httpResp.StatusCode != http.StatusOK {
		return predictResponse{}, classifyHTTPError(
			httpResp.StatusCode, respBody,
		)
	}

	var resp predictResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return predictResponse{}, errors.Wrap(
			err, "vertex: decoding response",
		)
	}
	return resp, nil
}

// classifyHTTPError maps an HTTP status code to an appropriate
// pgerror. Access tokens are never included in error messages.
func classifyHTTPError(statusCode int, body []byte) error {
	var apiErr errorResponse
	msg := fmt.Sprintf("HTTP %d", statusCode)
	if json.Unmarshal(body, &apiErr) == nil && apiErr.Error.Message != "" {
		msg = apiErr.Error.Message
	}

	switch statusCode {
	case http.StatusUnauthorized:
		return pgerror.Newf(
			pgcode.InvalidAuthorizationSpecification,
			"vertex: authentication failed: %s", msg,
		)
	case http.StatusForbidden:
		return pgerror.Newf(
			pgcode.InsufficientPrivilege,
			"vertex: permission denied: %s", msg,
		)
	case http.StatusTooManyRequests:
		return pgerror.Newf(
			pgcode.InsufficientResources,
			"vertex: rate limited (retries exhausted): %s", msg,
		)
	case http.StatusBadRequest:
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			"vertex: bad request: %s", msg,
		)
	default:
		return pgerror.Newf(
			pgcode.ConnectionFailure,
			"vertex: server error: %s", msg,
		)
	}
}

// isRetryable returns true for transient errors that should be
// retried (HTTP 429 and 5xx).
func isRetryable(err error) bool {
	return pgerror.GetPGCode(err) == pgcode.InsufficientResources ||
		pgerror.GetPGCode(err) == pgcode.ConnectionFailure
}

// RegionFromHost extracts the region from a Vertex AI hostname. For
// example, "us-east1-aiplatform.googleapis.com" returns "us-east1".
// Returns empty string if the format is unrecognized.
func RegionFromHost(host string) string {
	const suffix = "-aiplatform.googleapis.com"
	if strings.HasSuffix(host, suffix) {
		return strings.TrimSuffix(host, suffix)
	}
	return ""
}

// Request and response types matching the Vertex AI predict API.

type predictRequest struct {
	Instances []instance `json:"instances"`
}

type instance struct {
	Content string `json:"content"`
}

type predictResponse struct {
	Predictions []prediction `json:"predictions"`
}

type prediction struct {
	Embeddings embeddingResult `json:"embeddings"`
}

type embeddingResult struct {
	Values []float32 `json:"values"`
}

type errorResponse struct {
	Error struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Status  string `json:"status"`
	} `json:"error"`
}
