// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package openai implements the embedding.Embedder interface using
// OpenAI's embedding API.
package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

const (
	defaultBaseURL = "https://api.openai.com/v1/embeddings"
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

// Client implements embedding.Embedder by calling the OpenAI
// embeddings API. It is safe for concurrent use after construction.
type Client struct {
	apiKey     string
	model      string
	dims       int
	endpoint   string
	httpClient *httputil.Client
}

// NewClient creates a Client for the given model and API key.
func NewClient(apiKey, model string, dims int) *Client {
	return &Client{
		apiKey:   apiKey,
		model:    model,
		dims:     dims,
		endpoint: defaultBaseURL,
		httpClient: httputil.NewClient(
			httputil.WithClientTimeout(defaultTimeout),
		),
	}
}

// Dims returns the embedding dimension of the model.
func (c *Client) Dims() int {
	return c.dims
}

// Embed produces a normalized embedding vector for a single text input.
func (c *Client) Embed(ctx context.Context, text string) ([]float32, error) {
	vecs, err := c.EmbedBatch(ctx, []string{text})
	if err != nil {
		return nil, err
	}
	return vecs[0], nil
}

// EmbedBatch produces normalized embedding vectors for multiple texts
// by calling the OpenAI embeddings API.
func (c *Client) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}

	reqBody := embeddingRequest{
		Input: texts,
		Model: c.model,
	}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, errors.Wrap(err, "marshaling embedding request")
	}

	var resp embeddingResponse
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

	// OpenAI may return embeddings out of order; sort by index.
	result := make([][]float32, len(texts))
	for _, d := range resp.Data {
		if d.Index < 0 || d.Index >= len(texts) {
			return nil, errors.Newf(
				"openai: response index %d out of range [0, %d)", d.Index, len(texts),
			)
		}
		result[d.Index] = d.Embedding
	}
	for i, v := range result {
		if v == nil {
			return nil, errors.Newf(
				"openai: missing embedding for input %d", i,
			)
		}
	}
	return result, nil
}

// doRequest sends a single POST to the OpenAI embeddings endpoint and
// returns the parsed response. Errors are classified with pgcodes.
func (c *Client) doRequest(ctx context.Context, body []byte) (embeddingResponse, error) {
	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, c.endpoint, bytes.NewReader(body),
	)
	if err != nil {
		return embeddingResponse{}, errors.Wrap(err, "creating request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	httpResp, err := c.httpClient.Do(req)
	if err != nil {
		return embeddingResponse{}, pgerror.Wrap(
			err, pgcode.ConnectionFailure,
			"openai: request failed",
		)
	}
	defer httpResp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(httpResp.Body, maxResponseBytes))
	if err != nil {
		return embeddingResponse{}, errors.Wrap(err, "openai: reading response")
	}

	if httpResp.StatusCode != http.StatusOK {
		return embeddingResponse{}, classifyHTTPError(httpResp.StatusCode, respBody)
	}

	var resp embeddingResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return embeddingResponse{}, errors.Wrap(err, "openai: decoding response")
	}
	return resp, nil
}

// classifyHTTPError maps an HTTP status code to an appropriate
// pgerror. The API key is never included in error messages.
func classifyHTTPError(statusCode int, body []byte) error {
	// Try to extract the error message from the response body.
	var apiErr apiErrorResponse
	msg := fmt.Sprintf("HTTP %d", statusCode)
	if json.Unmarshal(body, &apiErr) == nil && apiErr.Error.Message != "" {
		msg = apiErr.Error.Message
	}

	switch statusCode {
	case http.StatusUnauthorized:
		return pgerror.Newf(pgcode.InvalidAuthorizationSpecification,
			"openai: authentication failed: %s", msg)
	case http.StatusForbidden:
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"openai: permission denied: %s", msg)
	case http.StatusTooManyRequests:
		return pgerror.Newf(pgcode.InsufficientResources,
			"openai: rate limited (retries exhausted): %s", msg)
	case http.StatusBadRequest:
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"openai: bad request: %s", msg)
	default:
		return pgerror.Newf(pgcode.ConnectionFailure,
			"openai: server error: %s", msg)
	}
}

// isRetryable returns true for transient errors that should be retried
// (HTTP 429 and 5xx).
func isRetryable(err error) bool {
	return pgerror.GetPGCode(err) == pgcode.InsufficientResources ||
		pgerror.GetPGCode(err) == pgcode.ConnectionFailure
}

// Request and response types matching the OpenAI embeddings API.

type embeddingRequest struct {
	Input []string `json:"input"`
	Model string   `json:"model"`
}

type embeddingResponse struct {
	Data  []embeddingData `json:"data"`
	Usage embeddingUsage  `json:"usage"`
}

type embeddingData struct {
	Embedding []float32 `json:"embedding"`
	Index     int       `json:"index"`
}

type embeddingUsage struct {
	PromptTokens int `json:"prompt_tokens"`
	TotalTokens  int `json:"total_tokens"`
}

type apiErrorResponse struct {
	Error struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error"`
}
