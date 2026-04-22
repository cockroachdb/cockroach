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
	"encoding/base64"
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
// AI embeddings API. For multimodal models (multimodalembedding@001),
// it also implements embedding.ImageEmbedder, allowing both text and
// images to be embedded into the same vector space.
//
// Supports two auth modes: a static access token (for testing, expires
// in 1 hour) or a service account key (auto-refreshes tokens).
type Client struct {
	accessToken string       // static token, used if tokenSrc is nil
	tokenSrc    *tokenSource // auto-refreshing, used if non-nil
	model       string
	dims        int
	multimodal  bool // true for models that support image input
	endpoint    string
	httpClient  *httputil.Client
}

// NewClient creates a Client using a static access token.
func NewClient(accessToken, project, region, model string, dims int, multimodal bool) *Client {
	return &Client{
		accessToken: accessToken,
		model:       model,
		dims:        dims,
		multimodal:  multimodal,
		endpoint:    predictEndpoint(region, project, model),
		httpClient: httputil.NewClient(
			httputil.WithClientTimeout(defaultTimeout),
		),
	}
}

// NewClientWithServiceAccount creates a Client that auto-refreshes
// access tokens using a service account key.
func NewClientWithServiceAccount(
	saKey ServiceAccountKey, project, region, model string, dims int, multimodal bool,
) *Client {
	httpClient := httputil.NewClient(
		httputil.WithClientTimeout(defaultTimeout),
	)
	return &Client{
		tokenSrc:   newTokenSource(saKey, httpClient),
		model:      model,
		dims:       dims,
		multimodal: multimodal,
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

	var bodyBytes []byte
	var err error
	if c.multimodal {
		// Multimodal models use {"text": "..."} instead of
		// {"content": "..."}.
		instances := make([]mmInstance, len(texts))
		for i, t := range texts {
			instances[i] = mmInstance{Text: t}
		}
		bodyBytes, err = json.Marshal(
			mmPredictRequest{Instances: instances},
		)
	} else {
		instances := make([]instance, len(texts))
		for i, t := range texts {
			instances[i] = instance{Content: t}
		}
		bodyBytes, err = json.Marshal(
			predictRequest{Instances: instances},
		)
	}
	if err != nil {
		return nil, errors.Wrap(err, "marshaling predict request")
	}

	return c.doPredictBatch(ctx, bodyBytes, len(texts))
}

// EmbedImage produces a normalized embedding vector for a single
// image by calling the Vertex AI multimodal embedding endpoint.
func (c *Client) EmbedImage(ctx context.Context, image []byte) ([]float32, error) {
	vecs, err := c.EmbedImageBatch(ctx, [][]byte{image})
	if err != nil {
		return nil, err
	}
	return vecs[0], nil
}

// EmbedImageBatch produces normalized embedding vectors for multiple
// images by calling the Vertex AI multimodal embedding endpoint. Each
// image is base64-encoded and sent as a separate instance.
func (c *Client) EmbedImageBatch(ctx context.Context, images [][]byte) ([][]float32, error) {
	if len(images) == 0 {
		return nil, nil
	}

	instances := make([]mmInstance, len(images))
	for i, img := range images {
		instances[i] = mmInstance{
			Image: &mmImage{
				BytesBase64Encoded: base64.StdEncoding.EncodeToString(img),
			},
		}
	}
	bodyBytes, err := json.Marshal(
		mmPredictRequest{Instances: instances},
	)
	if err != nil {
		return nil, errors.Wrap(err, "marshaling predict request")
	}

	return c.doPredictBatch(ctx, bodyBytes, len(images))
}

// doPredictBatch sends a predict request and extracts embedding
// vectors from the response. It handles retry and response
// validation. Works for both text and image predictions — multimodal
// responses use textEmbedding for text and imageEmbedding for images.
func (c *Client) doPredictBatch(ctx context.Context, bodyBytes []byte, n int) ([][]float32, error) {
	var resp predictResponse
	var err error
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

	if len(resp.Predictions) != n {
		return nil, errors.Newf(
			"vertex: expected %d predictions, got %d",
			n, len(resp.Predictions),
		)
	}
	result := make([][]float32, n)
	for i, p := range resp.Predictions {
		// Multimodal models return separate textEmbedding and
		// imageEmbedding fields. Text-only models return
		// embeddings.values. Pick whichever is populated.
		switch {
		case len(p.ImageEmbedding) > 0:
			result[i] = p.ImageEmbedding
		case len(p.TextEmbedding) > 0:
			result[i] = p.TextEmbedding
		case len(p.Embeddings.Values) > 0:
			result[i] = p.Embeddings.Values
		default:
			return nil, errors.Newf(
				"vertex: empty embedding in prediction %d", i,
			)
		}
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

// Text-only embedding models (text-embedding-004, etc.)

type predictRequest struct {
	Instances []instance `json:"instances"`
}

type instance struct {
	Content string `json:"content"`
}

// Multimodal embedding models (multimodalembedding@001)

type mmPredictRequest struct {
	Instances []mmInstance `json:"instances"`
}

type mmInstance struct {
	Text  string   `json:"text,omitempty"`
	Image *mmImage `json:"image,omitempty"`
}

type mmImage struct {
	BytesBase64Encoded string `json:"bytesBase64Encoded"`
}

// Shared response types — the prediction struct carries fields for
// both text-only and multimodal response formats.

type predictResponse struct {
	Predictions []prediction `json:"predictions"`
}

type prediction struct {
	// Text-only models return embeddings.values.
	Embeddings embeddingResult `json:"embeddings"`
	// Multimodal models return separate fields.
	TextEmbedding  []float32 `json:"textEmbedding"`
	ImageEmbedding []float32 `json:"imageEmbedding"`
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
