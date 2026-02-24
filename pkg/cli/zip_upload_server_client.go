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
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/errors"
)

// artifactType identifies the kind of artifact being uploaded to the
// upload server. The server uses this to decide how to process the
// artifact (e.g. transform to Parquet vs store as-is).
type artifactType string

const (
	artifactTypeProfile     artifactType = "profile"
	artifactTypeLog         artifactType = "log"
	artifactTypeTable       artifactType = "table"
	artifactTypeMetadata    artifactType = "metadata"
	artifactTypeStack       artifactType = "stack"
	artifactTypeTrace       artifactType = "trace"
	artifactTypeEngineStats artifactType = "engine-stats"
)

// uploadServerClientConfig holds configuration for creating a new
// uploadServerClient.
type uploadServerClientConfig struct {
	ServerURL   string
	APIKey      string
	CustomCAPEM string
	Timeout     time.Duration
}

// uploadServerClient speaks the upload server REST protocol. It
// manages sessions and uploads artifacts.
type uploadServerClient struct {
	httpClient  *httputil.Client
	serverURL   string
	apiKey      string
	sessionID   string // set after CreateSession
	uploadToken string // set after CreateSession
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

// uploadArtifactResponse is the JSON response from PUT artifacts.
type uploadArtifactResponse struct {
	ArtifactID    string `json:"artifact_id"`
	BytesReceived int64  `json:"bytes_received"`
	Status        string `json:"status,omitempty"`
}

// completeSessionRequest is the JSON body for POST .../complete.
type completeSessionRequest struct {
	Status            string  `json:"status"`
	ArtifactsUploaded int     `json:"artifacts_uploaded"`
	NodesCompleted    []int32 `json:"nodes_completed"`
}

// completeSessionResponse is the JSON response from POST .../complete.
type completeSessionResponse struct {
	CloudPath          string `json:"cloud_path"`
	ParquetFilesCreated int   `json:"parquet_files_created"`
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

// UploadArtifact uploads a single artifact to the server.
func (c *uploadServerClient) UploadArtifact(
	ctx context.Context,
	artifactPath string,
	nodeID int32,
	artType artifactType,
	contentType string,
	idempotencyKey string,
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
	req.Header.Set("X-Artifact-Type", string(artType))
	if idempotencyKey != "" {
		req.Header.Set("X-Idempotency-Key", idempotencyKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "uploading artifact")
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusCreated:
		return nil
	case http.StatusConflict:
		// Already uploaded (idempotent retry).
		return nil
	default:
		return readHTTPError(resp)
	}
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
func (c *uploadServerClient) GetSessionStatus(
	ctx context.Context,
) (*sessionStatusResponse, error) {
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
