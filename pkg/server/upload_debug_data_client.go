// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

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
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/errors"
	"golang.org/x/oauth2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// rpcUploadTokenResponse is the JSON response from POST .../upload-token.
type rpcUploadTokenResponse struct {
	AccessToken string `json:"access_token"`
	Bucket      string `json:"bucket"`
	Prefix      string `json:"prefix"`
	ExpiresAt   string `json:"expires_at"`
}

// uploadServerRPCClient is a minimal HTTP client for the upload
// server protocol, used by the RPC handlers in the server package.
// This is separate from the CLI's uploadServerClient because the
// server package cannot import pkg/cli.
type uploadServerRPCClient struct {
	httpClient  *httputil.Client
	serverURL   string
	apiKey      string
	sessionID   string
	uploadToken string

	// gcsClient is set by initGCSClient and used for all uploads.
	// Uploads use the GCS client library (chunked resumable uploads
	// with automatic retry).
	gcsClient      *storage.Client
	gcsBucket      string
	gcsPrefix      string
	gcsAccessToken string
}

// newUploadServerClientForRPC creates a client for the coordinator
// (has api_key, creates sessions).
func newUploadServerClientForRPC(
	serverURL, apiKey string, timeout time.Duration,
) *uploadServerRPCClient {
	return &uploadServerRPCClient{
		httpClient: httputil.NewClient(httputil.WithClientTimeout(timeout)),
		serverURL:  serverURL,
		apiKey:     apiKey,
	}
}

// newUploadServerClientForNodeRPC creates a client for per-node
// upload (pre-authenticated with upload token).
func newUploadServerClientForNodeRPC(
	serverURL, sessionID, uploadToken string, timeout time.Duration,
) *uploadServerRPCClient {
	return &uploadServerRPCClient{
		httpClient:  httputil.NewClient(httputil.WithClientTimeout(timeout)),
		serverURL:   serverURL,
		sessionID:   sessionID,
		uploadToken: uploadToken,
	}
}

// createSession creates a new upload session.
func (c *uploadServerRPCClient) createSession(
	ctx context.Context, clusterID string, nodeCount int, redacted bool, labels map[string]string,
) error {
	body := map[string]interface{}{
		"cluster_id": clusterID,
		"node_count": nodeCount,
		"redacted":   redacted,
		"labels":     labels,
	}
	data, err := json.Marshal(body)
	if err != nil {
		return errors.Wrap(err, "marshaling session request")
	}

	url := fmt.Sprintf("%s/api/v1/sessions", c.serverURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(data))
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
		return readUploadHTTPError(resp)
	}

	var result struct {
		SessionID   string `json:"session_id"`
		UploadToken string `json:"upload_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return errors.Wrap(err, "decoding session response")
	}
	c.sessionID = result.SessionID
	c.uploadToken = result.UploadToken
	return nil
}

// uploadArtifactBytes uploads raw bytes as an artifact via GCS.
func (c *uploadServerRPCClient) uploadArtifactBytes(
	ctx context.Context, artifactPath string, data []byte,
) error {
	return c.uploadArtifact(ctx, artifactPath, "application/octet-stream", data)
}

// uploadArtifactJSON marshals a value to JSON and uploads it via GCS.
func (c *uploadServerRPCClient) uploadArtifactJSON(
	ctx context.Context, artifactPath string, v interface{},
) error {
	data, err := json.Marshal(v)
	if err != nil {
		return errors.Wrap(err, "marshaling artifact JSON")
	}
	return c.uploadArtifact(ctx, artifactPath, "application/json", data)
}

// uploadArtifactStreaming uploads an artifact to GCS using the GCS
// client library, which provides chunked resumable uploads with
// automatic retry.
//
// The newBody callback is called once to obtain an io.ReadCloser.
// The GCS client handles retries internally at the chunk level.
func (c *uploadServerRPCClient) uploadArtifactStreaming(
	ctx context.Context,
	artifactPath string,
	contentType string,
	newBody func() (io.ReadCloser, error),
) error {
	if c.gcsClient == nil {
		return errors.AssertionFailedf("GCS client not initialized; call initGCSClient first")
	}
	body, err := newBody()
	if err != nil {
		return errors.Wrap(err, "creating body reader")
	}
	defer body.Close()
	return c.uploadArtifactGCS(ctx, artifactPath, contentType, body)
}

// uploadArtifact uploads an artifact to GCS.
func (c *uploadServerRPCClient) uploadArtifact(
	ctx context.Context, artifactPath string, contentType string, data []byte,
) error {
	return c.uploadArtifactStreaming(
		ctx, artifactPath, contentType,
		func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(data)), nil
		},
	)
}

// getUploadToken requests a GCS upload token from the upload server.
func (c *uploadServerRPCClient) getUploadToken(
	ctx context.Context,
) (*rpcUploadTokenResponse, error) {
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
		return nil, readUploadHTTPError(resp)
	}

	var result rpcUploadTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, errors.Wrap(err, "decoding upload token response")
	}
	return &result, nil
}

// initGCSClient requests an upload token and creates a GCS client.
// Call this once per session. All errors are propagated to the caller.
func (c *uploadServerRPCClient) initGCSClient(ctx context.Context) error {
	tokenResp, err := c.getUploadToken(ctx)
	if err != nil {
		return errors.Wrap(err, "getting upload token")
	}
	return c.initGCSClientWithCredentials(ctx, tokenResp.AccessToken, tokenResp.Bucket, tokenResp.Prefix)
}

// initGCSClientWithCredentials creates a GCS client from provided
// credentials. This allows nodes to reuse the coordinator's GCS
// prefix so all uploads land in a single session folder.
func (c *uploadServerRPCClient) initGCSClientWithCredentials(
	ctx context.Context, accessToken, bucket, prefix string,
) error {
	token := &oauth2.Token{AccessToken: accessToken}
	client, err := storage.NewClient(ctx,
		option.WithTokenSource(oauth2.StaticTokenSource(token)),
	)
	if err != nil {
		return errors.Wrap(err, "creating GCS client")
	}
	c.gcsClient = client
	c.gcsBucket = bucket
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	c.gcsPrefix = prefix
	c.gcsAccessToken = accessToken
	return nil
}

// uploadArtifactGCS uploads an artifact using the GCS client library.
// This provides chunked resumable uploads with automatic retry.
func (c *uploadServerRPCClient) uploadArtifactGCS(
	ctx context.Context, artifactPath string, contentType string, body io.Reader,
) error {
	objectPath := c.gcsPrefix + artifactPath
	obj := c.gcsClient.Bucket(c.gcsBucket).Object(objectPath)
	w := obj.Retryer(
		storage.WithPolicy(storage.RetryAlways),
		storage.WithErrorFunc(rpcShouldRetryGCS),
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

// closeGCS releases resources held by the GCS client if one was
// initialized.
func (c *uploadServerRPCClient) closeGCS() error {
	if c.gcsClient != nil {
		return c.gcsClient.Close()
	}
	return nil
}

// rpcShouldRetryGCS determines whether a GCS client error should be
// retried. This handles transient errors like network timeouts,
// HTTP 408/429/5xx, and connection resets.
func rpcShouldRetryGCS(err error) bool {
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
		return rpcShouldRetryGCS(wrapped.Unwrap())
	}
	return false
}

// reuploadSession reopens a completed or failed session on the upload
// server, obtaining a fresh upload token. This uses the cluster API
// key (not the upload token) for authentication. nodeIDs is passed
// to the upload server for tracking which nodes are being retried.
func (c *uploadServerRPCClient) reuploadSession(
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
		return readUploadHTTPError(resp)
	}

	var result struct {
		SessionID   string `json:"session_id"`
		UploadToken string `json:"upload_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return errors.Wrap(err, "decoding reupload response")
	}
	c.sessionID = result.SessionID
	c.uploadToken = result.UploadToken
	return nil
}

// completeSession signals the server that the upload is done.
func (c *uploadServerRPCClient) completeSession(
	ctx context.Context, artifactsUploaded int, nodesCompleted []int32,
) error {
	body := map[string]interface{}{
		"status":             "success",
		"artifacts_uploaded": artifactsUploaded,
		"nodes_completed":    nodesCompleted,
	}
	data, err := json.Marshal(body)
	if err != nil {
		return errors.Wrap(err, "marshaling complete request")
	}

	url := fmt.Sprintf("%s/api/v1/sessions/%s/complete", c.serverURL, c.sessionID)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(data))
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
		return readUploadHTTPError(resp)
	}
	return nil
}

// readUploadHTTPError reads an error response and returns a descriptive error.
func readUploadHTTPError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return errors.Newf("upload server returned HTTP %d: %s", resp.StatusCode, string(body))
}

// formatLSMStatsForUpload formats engine stats for upload.
func formatLSMStatsForUpload(resp *serverpb.EngineStatsResponse) string {
	return debug.FormatLSMStats(resp.StatsByStoreId)
}
