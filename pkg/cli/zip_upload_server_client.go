// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/errors"
	"golang.org/x/oauth2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// uploadServerClientConfig holds configuration for creating a new
// uploadServerClient.
type uploadServerClientConfig struct {
	ServerURL   string
	APIKey      string
	CustomCAPEM string
	Timeout     time.Duration
}

// uploadTokenResponse is the JSON response from POST .../upload-token.
type uploadTokenResponse struct {
	AccessToken string `json:"access_token"`
	Bucket      string `json:"bucket"`
	Prefix      string `json:"prefix"`
	ExpiresAt   string `json:"expires_at"`
}

// uploadServerClient speaks the upload server REST protocol. It
// manages sessions and uploads artifacts via the GCS client library.
type uploadServerClient struct {
	httpClient  *httputil.Client
	serverURL   string
	apiKey      string
	sessionID   string // set after CreateSession
	uploadToken string // set after CreateSession

	// gcsClient is set by InitGCSClient and used for all uploads.
	// Uploads use the GCS client library (chunked resumable uploads
	// with automatic retry).
	gcsClient *storage.Client
	gcsBucket string
	gcsPrefix string
}

// createSessionRequest is the JSON body for POST /api/v1/sessions.
type createSessionRequest struct {
	ClusterID   string            `json:"cluster_id"`
	ClusterName string            `json:"cluster_name"`
	NodeCount   int               `json:"node_count"`
	CRDBVersion string            `json:"crdb_version"`
	Redacted    bool              `json:"redacted"`
	Labels      map[string]string `json:"labels,omitempty"`
}

// createSessionResponse is the JSON response from POST /api/v1/sessions.
type createSessionResponse struct {
	SessionID   string `json:"session_id"`
	UploadToken string `json:"upload_token"`
	ExpiresAt   string `json:"expires_at"`
}

// completeSessionRequest is the JSON body for POST .../complete.
type completeSessionRequest struct {
	Status            string  `json:"status"`
	ArtifactsUploaded int     `json:"artifacts_uploaded"`
	NodesCompleted    []int32 `json:"nodes_completed"`
}

// completeSessionResponse is the JSON response from POST .../complete.
type completeSessionResponse struct {
	CloudPath           string `json:"cloud_path"`
	ParquetFilesCreated int    `json:"parquet_files_created"`
}

// sessionStatusResponse is the JSON response from GET .../status.
type sessionStatusResponse struct {
	State             string         `json:"state"`
	ArtifactsReceived int            `json:"artifacts_received"`
	ArtifactsByNode   map[string]int `json:"artifacts_by_node"`
}

// newUploadServerClient creates a client configured to talk to the
// upload server.
func newUploadServerClient(cfg uploadServerClientConfig) *uploadServerClient {
	var opts []httputil.ClientOption
	if cfg.Timeout > 0 {
		opts = append(opts, httputil.WithClientTimeout(cfg.Timeout))
	}
	if cfg.CustomCAPEM != "" {
		opts = append(opts, httputil.WithCustomCAPEM(cfg.CustomCAPEM))
	}
	return &uploadServerClient{
		httpClient: httputil.NewClient(opts...),
		serverURL:  cfg.ServerURL,
		apiKey:     cfg.APIKey,
	}
}

// newUploadServerClientWithToken creates a client pre-authenticated
// with an upload token (used by per-node handlers that don't have the
// API key).
func newUploadServerClientWithToken(
	cfg uploadServerClientConfig, sessionID, uploadToken string,
) *uploadServerClient {
	c := newUploadServerClient(cfg)
	c.sessionID = sessionID
	c.uploadToken = uploadToken
	return c
}

// CreateSession creates a new upload session on the server. On
// success, it stores the session_id and upload_token for subsequent
// calls.
func (c *uploadServerClient) CreateSession(
	ctx context.Context,
	clusterID string,
	clusterName string,
	nodeCount int,
	crdbVersion string,
	redacted bool,
	labels map[string]string,
) error {
	reqBody := createSessionRequest{
		ClusterID:   clusterID,
		ClusterName: clusterName,
		NodeCount:   nodeCount,
		CRDBVersion: crdbVersion,
		Redacted:    redacted,
		Labels:      labels,
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return errors.Wrap(err, "marshaling session request")
	}

	url := fmt.Sprintf("%s/api/v1/sessions", c.serverURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return errors.Wrap(err, "creating session request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "sending session request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return readHTTPError(resp)
	}

	var result createSessionResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return errors.Wrap(err, "decoding session response")
	}
	c.sessionID = result.SessionID
	c.uploadToken = result.UploadToken
	return nil
}

// UploadArtifactStreaming uploads an artifact to GCS using the GCS
// client library, which provides chunked resumable uploads with
// automatic retry.
//
// The newBody callback is called once to obtain an io.ReadCloser.
// The GCS client handles retries internally at the chunk level.
func (c *uploadServerClient) UploadArtifactStreaming(
	ctx context.Context,
	artifactPath string,
	contentType string,
	newBody func() (io.ReadCloser, error),
) error {
	if c.gcsClient == nil {
		return errors.AssertionFailedf("GCS client not initialized; call InitGCSClient first")
	}
	body, err := newBody()
	if err != nil {
		return errors.Wrap(err, "creating body reader")
	}
	defer body.Close()
	return c.UploadArtifactGCS(ctx, artifactPath, contentType, body)
}

// UploadArtifact uploads a single artifact to GCS.
func (c *uploadServerClient) UploadArtifact(
	ctx context.Context, artifactPath string, contentType string, data []byte,
) error {
	return c.UploadArtifactStreaming(
		ctx, artifactPath, contentType,
		func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(data)), nil
		},
	)
}

// GetUploadToken requests a GCS upload token from the upload server.
// The token can be used with the GCS client library for chunked
// resumable uploads.
func (c *uploadServerClient) GetUploadToken(ctx context.Context) (*uploadTokenResponse, error) {
	url := fmt.Sprintf(
		"%s/api/v1/sessions/%s/upload-token",
		c.serverURL, c.sessionID,
	)
	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating upload token request")
	}
	req.Header.Set("Authorization", "Bearer "+c.uploadToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "requesting upload token")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, readHTTPError(resp)
	}

	var result uploadTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, errors.Wrap(err, "decoding upload token response")
	}
	return &result, nil
}

// InitGCSClient requests an upload token and creates a GCS client.
// Call this once per session. All errors are propagated to the caller.
func (c *uploadServerClient) InitGCSClient(ctx context.Context) error {
	tokenResp, err := c.GetUploadToken(ctx)
	if err != nil {
		return errors.Wrap(err, "getting upload token")
	}
	token := &oauth2.Token{AccessToken: tokenResp.AccessToken}
	client, err := storage.NewClient(ctx,
		option.WithTokenSource(oauth2.StaticTokenSource(token)),
	)
	if err != nil {
		return errors.Wrap(err, "creating GCS client")
	}
	c.gcsClient = client
	c.gcsBucket = tokenResp.Bucket
	prefix := tokenResp.Prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	c.gcsPrefix = prefix
	return nil
}

// UploadArtifactGCS uploads an artifact using the GCS client library
// with a bearer token. This provides chunked resumable uploads with
// automatic retry.
func (c *uploadServerClient) UploadArtifactGCS(
	ctx context.Context, artifactPath string, contentType string, body io.Reader,
) error {
	objectPath := c.gcsPrefix + artifactPath
	obj := c.gcsClient.Bucket(c.gcsBucket).Object(objectPath)
	w := obj.Retryer(
		storage.WithPolicy(storage.RetryAlways),
		storage.WithErrorFunc(shouldRetryGCS),
	).NewWriter(ctx)
	w.ContentType = contentType
	w.ChunkSize = 8 << 20 // 8MB chunks
	w.ChunkRetryDeadline = 60 * time.Second

	if _, err := io.Copy(w, body); err != nil {
		_ = w.Close()
		return errors.Wrapf(err, "writing artifact %s", artifactPath)
	}
	if err := w.Close(); err != nil {
		return errors.Wrapf(err, "finalizing artifact %s", artifactPath)
	}
	return nil
}

// Close releases resources held by the client. It closes the GCS
// client if one was initialized.
func (c *uploadServerClient) Close() error {
	if c.gcsClient != nil {
		return c.gcsClient.Close()
	}
	return nil
}

// shouldRetryGCS determines whether a GCS client error should be
// retried. This handles transient errors like network timeouts,
// HTTP 408/429/5xx, and connection resets.
func shouldRetryGCS(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	if opErr := (*net.OpError)(nil); errors.As(err, &opErr) {
		if strings.Contains(opErr.Error(), "use of closed network connection") {
			return true
		}
	}
	if apiErr := (*googleapi.Error)(nil); errors.As(err, &apiErr) {
		// Retry on 408, 429, and 5xx per GCS documentation.
		return apiErr.Code == 408 || apiErr.Code == 429 || (apiErr.Code >= 500 && apiErr.Code < 600)
	}
	if urlErr := (*url.Error)(nil); errors.As(err, &urlErr) {
		retriable := []string{"connection refused", "connection reset"}
		for _, s := range retriable {
			if strings.Contains(urlErr.Error(), s) {
				return true
			}
		}
	}
	if wrapped := (errors.Wrapper)(nil); errors.As(err, &wrapped) {
		return shouldRetryGCS(wrapped.Unwrap())
	}
	return false
}

// ReuploadSession reopens a completed or failed session on the upload
// server, obtaining a fresh upload token. This uses the cluster API
// key (not the upload token) for authentication.
func (c *uploadServerClient) ReuploadSession(
	ctx context.Context, sessionID string, reason string, nodeIDs []int32,
) error {
	data := map[string]interface{}{}
	if reason != "" {
		data["reason"] = reason
	}
	if len(nodeIDs) > 0 {
		data["node_ids"] = nodeIDs
	}
	var body []byte
	if len(data) > 0 {
		var err error
		body, err = json.Marshal(data)
		if err != nil {
			return errors.Wrap(err, "marshaling reupload request")
		}
	}

	url := fmt.Sprintf("%s/api/v1/sessions/%s/reupload", c.serverURL, sessionID)
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", url, bodyReader)
	if err != nil {
		return errors.Wrap(err, "creating reupload request")
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "sending reupload request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return readHTTPError(resp)
	}

	var result createSessionResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return errors.Wrap(err, "decoding reupload response")
	}
	c.sessionID = result.SessionID
	c.uploadToken = result.UploadToken
	return nil
}

// CompleteSession signals the server that the upload session is done.
func (c *uploadServerClient) CompleteSession(
	ctx context.Context, artifactsUploaded int, nodesCompleted []int32,
) error {
	reqBody := completeSessionRequest{
		Status:            "success",
		ArtifactsUploaded: artifactsUploaded,
		NodesCompleted:    nodesCompleted,
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return errors.Wrap(err, "marshaling complete request")
	}

	url := fmt.Sprintf("%s/api/v1/sessions/%s/complete", c.serverURL, c.sessionID)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return errors.Wrap(err, "creating complete request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.uploadToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "completing session")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return readHTTPError(resp)
	}
	return nil
}

// GetSessionStatus retrieves the current status of the upload session.
func (c *uploadServerClient) GetSessionStatus(ctx context.Context) (*sessionStatusResponse, error) {
	url := fmt.Sprintf("%s/api/v1/sessions/%s/status", c.serverURL, c.sessionID)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating status request")
	}
	req.Header.Set("Authorization", "Bearer "+c.uploadToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "fetching session status")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, readHTTPError(resp)
	}

	var result sessionStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, errors.Wrap(err, "decoding status response")
	}
	return &result, nil
}

// readHTTPError reads an error response body and returns a descriptive error.
func readHTTPError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return errors.Newf(
		"upload server returned HTTP %d: %s", resp.StatusCode, string(body),
	)
}
