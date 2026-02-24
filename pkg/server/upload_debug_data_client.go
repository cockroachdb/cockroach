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
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

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
	ctx context.Context,
	clusterID string,
	nodeCount int,
	redacted bool,
	labels map[string]string,
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

// uploadArtifactBytes uploads raw bytes as an artifact.
func (c *uploadServerRPCClient) uploadArtifactBytes(
	ctx context.Context, artifactPath string, nodeID int32, artType string, data []byte,
) error {
	return c.uploadArtifact(ctx, artifactPath, nodeID, artType, "application/octet-stream", bytes.NewReader(data))
}

// uploadArtifactJSON marshals a value to JSON and uploads it.
func (c *uploadServerRPCClient) uploadArtifactJSON(
	ctx context.Context, artifactPath string, nodeID int32, artType string, v interface{},
) error {
	data, err := json.Marshal(v)
	if err != nil {
		return errors.Wrap(err, "marshaling artifact JSON")
	}
	return c.uploadArtifact(ctx, artifactPath, nodeID, artType, "application/json", bytes.NewReader(data))
}

// uploadArtifact is the common upload path.
func (c *uploadServerRPCClient) uploadArtifact(
	ctx context.Context,
	artifactPath string,
	nodeID int32,
	artType string,
	contentType string,
	body io.Reader,
) error {
	url := fmt.Sprintf(
		"%s/api/v1/sessions/%s/artifacts/%s",
		c.serverURL, c.sessionID, artifactPath,
	)
	req, err := http.NewRequestWithContext(ctx, "PUT", url, body)
	if err != nil {
		return errors.Wrap(err, "creating artifact request")
	}
	req.Header.Set("Authorization", "Bearer "+c.uploadToken)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-Node-ID", fmt.Sprintf("%d", nodeID))
	req.Header.Set("X-Artifact-Type", artType)
	req.Header.Set("X-Idempotency-Key", uuid.MakeV4().String())

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "uploading artifact")
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusCreated, http.StatusConflict:
		return nil
	default:
		return readUploadHTTPError(resp)
	}
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
