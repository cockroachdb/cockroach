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
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/errors"
	"golang.org/x/oauth2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// uploadServerHTTPTimeout is the timeout used for control-plane HTTP calls
// to the upload server (session create / upload token / complete / reupload).
// Artifact data never flows through this client; it streams directly to GCS.
const uploadServerHTTPTimeout = 30 * time.Second

// gcsChunkSize is the chunk size used by the GCS resumable upload writer.
// Each chunk is uploaded and independently retried on transient failures.
const gcsChunkSize = 8 << 20 // 8 MiB

// gcsChunkRetryDeadline bounds how long the GCS client will retry a single
// chunk before giving up.
const gcsChunkRetryDeadline = 60 * time.Second

// uploadDebugDataClient speaks the REST protocol of the CockroachDB upload
// server and owns the short-lived GCS client used for artifact streaming.
// It is used both by the coordinator (which creates / reopens sessions) and
// by the per-node workers that only need GCS credentials.
//
// Lifecycle:
//  1. The coordinator constructs the client via newCoordinatorUploadClient,
//     then calls createSession (or reuploadSession for retry). That step
//     returns a session ID and an upload token.
//  2. The coordinator calls initGCSClient to mint GCS credentials from the
//     upload server and open a GCS client.
//  3. The GCS credentials are propagated over UploadNodeDebugData RPCs to
//     every worker node, which constructs a client via
//     newWorkerUploadClient and opens the GCS client via
//     initGCSClientWithCredentials.
//  4. Artifacts are streamed by every participant through uploadArtifact.
//  5. The coordinator calls completeSession once all workers are done.
//
// The client is not safe for concurrent use from multiple goroutines sharing
// a single instance during session-lifecycle calls; however, the GCS client
// library used for artifact streaming is safe for concurrent use.
type uploadDebugDataClient struct {
	httpClient *httputil.Client
	serverURL  string

	// Control-plane authentication. Exactly one of apiKey and uploadToken is
	// typically set depending on the caller: the coordinator holds apiKey
	// (authorizes session create/reopen), workers hold uploadToken
	// (authorizes token refresh and session complete calls).
	apiKey      string
	uploadToken string

	// sessionID is the upload-server session id in effect for this client.
	// It is populated by createSession / reuploadSession on the coordinator,
	// and plumbed in via the RPC on workers.
	sessionID string

	// GCS state, lazily populated by initGCSClient or
	// initGCSClientWithCredentials.
	gcsClient      *storage.Client
	gcsBucket      string
	gcsPrefix      string
	gcsAccessToken string
}

// newCoordinatorUploadClient returns a client authorized to create and
// reopen sessions on the upload server using the cluster API key.
func newCoordinatorUploadClient(serverURL, apiKey string) *uploadDebugDataClient {
	return &uploadDebugDataClient{
		httpClient: httputil.NewClient(httputil.WithClientTimeout(uploadServerHTTPTimeout)),
		serverURL:  serverURL,
		apiKey:     apiKey,
	}
}

// newWorkerUploadClient returns a client pre-authenticated for a single
// session. Workers never need the cluster API key: once the coordinator has
// created the session, the upload token is sufficient to complete any
// additional control-plane interactions required by workers.
func newWorkerUploadClient(serverURL, sessionID, uploadToken string) *uploadDebugDataClient {
	return &uploadDebugDataClient{
		httpClient:  httputil.NewClient(httputil.WithClientTimeout(uploadServerHTTPTimeout)),
		serverURL:   serverURL,
		sessionID:   sessionID,
		uploadToken: uploadToken,
	}
}

// SessionID returns the upload-server session id currently associated with
// this client.
func (c *uploadDebugDataClient) SessionID() string { return c.sessionID }

// UploadToken returns the short-lived session token returned by the upload
// server. Empty for a coordinator client until createSession (or
// reuploadSession) has been called successfully.
func (c *uploadDebugDataClient) UploadToken() string { return c.uploadToken }

// GCSAccessToken returns the OAuth2 access token minted by the upload
// server for the current session, or empty if the GCS client has not yet
// been initialized.
func (c *uploadDebugDataClient) GCSAccessToken() string { return c.gcsAccessToken }

// GCSBucket returns the GCS bucket that artifacts for the current session
// are written into.
func (c *uploadDebugDataClient) GCSBucket() string { return c.gcsBucket }

// GCSPrefix returns the (always trailing-slash terminated) object prefix
// that artifacts for the current session are written under.
func (c *uploadDebugDataClient) GCSPrefix() string { return c.gcsPrefix }

// sessionRequest is the JSON body of POST /api/v1/sessions.
type sessionRequest struct {
	ClusterID string            `json:"cluster_id"`
	NodeCount int               `json:"node_count"`
	Redacted  bool              `json:"redacted"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// sessionResponse is returned by both POST /api/v1/sessions and POST
// /api/v1/sessions/:id/reupload.
type sessionResponse struct {
	SessionID   string `json:"session_id"`
	UploadToken string `json:"upload_token"`
}

// createSession opens a new upload-server session and stashes the returned
// session id and upload token on the client. Requires a coordinator client
// (i.e. one constructed with newCoordinatorUploadClient).
func (c *uploadDebugDataClient) createSession(
	ctx context.Context, clusterID string, nodeCount int, redacted bool, labels map[string]string,
) error {
	body, err := json.Marshal(sessionRequest{
		ClusterID: clusterID,
		NodeCount: nodeCount,
		Redacted:  redacted,
		Labels:    labels,
	})
	if err != nil {
		return errors.Wrap(err, "marshaling session request")
	}
	resp, err := c.doJSON(ctx, http.MethodPost,
		fmt.Sprintf("%s/api/v1/sessions", c.serverURL),
		"Bearer "+c.apiKey, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return readUploadHTTPError(resp)
	}
	var out sessionResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return errors.Wrap(err, "decoding session response")
	}
	c.sessionID = out.SessionID
	c.uploadToken = out.UploadToken
	return nil
}

// reuploadSessionRequest is the JSON body of POST
// /api/v1/sessions/:id/reupload. NodeIDs is advisory metadata; the upload
// server records it so operators can tell apart a full retry from a
// targeted "retry failed nodes" retry.
type reuploadSessionRequest struct {
	Reason  string  `json:"reason,omitempty"`
	NodeIDs []int32 `json:"node_ids,omitempty"`
}

// reuploadSession reopens an existing session instead of creating a new
// one. The existing session must be in a completed or failed state on the
// upload server side. On success the client's session id and upload token
// are refreshed.
func (c *uploadDebugDataClient) reuploadSession(
	ctx context.Context, sessionID string, reason string, nodeIDs []int32,
) error {
	var body []byte
	if reason != "" || len(nodeIDs) > 0 {
		var err error
		body, err = json.Marshal(reuploadSessionRequest{Reason: reason, NodeIDs: nodeIDs})
		if err != nil {
			return errors.Wrap(err, "marshaling reupload request")
		}
	}
	resp, err := c.doJSON(ctx, http.MethodPost,
		fmt.Sprintf("%s/api/v1/sessions/%s/reupload", c.serverURL, sessionID),
		"Bearer "+c.apiKey, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return readUploadHTTPError(resp)
	}
	var out sessionResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return errors.Wrap(err, "decoding reupload response")
	}
	c.sessionID = out.SessionID
	c.uploadToken = out.UploadToken
	return nil
}

// uploadTokenResponse is the JSON response from POST
// /api/v1/sessions/:id/upload-token.
type uploadTokenResponse struct {
	AccessToken string `json:"access_token"`
	Bucket      string `json:"bucket"`
	Prefix      string `json:"prefix"`
	ExpiresAt   string `json:"expires_at"`
}

// getUploadToken requests a fresh GCS access token from the upload server
// for the current session.
func (c *uploadDebugDataClient) getUploadToken(ctx context.Context) (*uploadTokenResponse, error) {
	resp, err := c.doJSON(ctx, http.MethodPost,
		fmt.Sprintf("%s/api/v1/sessions/%s/upload-token", c.serverURL, c.sessionID),
		"Bearer "+c.uploadToken, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, readUploadHTTPError(resp)
	}
	var out uploadTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, errors.Wrap(err, "decoding upload token response")
	}
	return &out, nil
}

// initGCSClient requests a GCS upload token for the current session and
// opens a GCS storage.Client. Callers must invoke closeGCS when done.
func (c *uploadDebugDataClient) initGCSClient(ctx context.Context) error {
	tok, err := c.getUploadToken(ctx)
	if err != nil {
		return errors.Wrap(err, "minting GCS upload token")
	}
	return c.initGCSClientWithCredentials(ctx, tok.AccessToken, tok.Bucket, tok.Prefix)
}

// initGCSClientWithCredentials opens a GCS storage.Client using credentials
// that were minted elsewhere (typically by the coordinator and propagated
// to workers over the UploadNodeDebugData RPC). Using the same access token
// on every node ensures all artifacts for a session land under the same
// GCS prefix.
func (c *uploadDebugDataClient) initGCSClientWithCredentials(
	ctx context.Context, accessToken, bucket, prefix string,
) error {
	if accessToken == "" || bucket == "" {
		return errors.AssertionFailedf("GCS credentials missing (access_token or bucket)")
	}
	client, err := storage.NewClient(ctx,
		option.WithTokenSource(oauth2.StaticTokenSource(&oauth2.Token{AccessToken: accessToken})),
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

// closeGCS releases the GCS client, if any. Safe to call multiple times.
func (c *uploadDebugDataClient) closeGCS() error {
	if c.gcsClient == nil {
		return nil
	}
	err := c.gcsClient.Close()
	c.gcsClient = nil
	return err
}

// uploadArtifact streams a single artifact to GCS under the session's
// prefix. newBody is invoked exactly once; the returned reader is closed
// by uploadArtifact before returning. The GCS client library handles
// chunking and transient-error retries under the hood.
//
// initGCSClient (or initGCSClientWithCredentials) must have been called
// first.
func (c *uploadDebugDataClient) uploadArtifact(
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
		return errors.Wrapf(err, "opening artifact %q", artifactPath)
	}
	defer func() { _ = body.Close() }()

	obj := c.gcsClient.Bucket(c.gcsBucket).Object(c.gcsPrefix + artifactPath)
	w := obj.Retryer(
		storage.WithPolicy(storage.RetryAlways),
		storage.WithErrorFunc(shouldRetryGCSError),
	).NewWriter(ctx)
	w.ContentType = contentType
	w.ChunkSize = gcsChunkSize
	w.ChunkRetryDeadline = gcsChunkRetryDeadline

	if _, err := io.Copy(w, body); err != nil {
		_ = w.Close()
		return errors.Wrapf(err, "writing artifact %q", artifactPath)
	}
	if err := w.Close(); err != nil {
		return errors.Wrapf(err, "finalizing artifact %q", artifactPath)
	}
	return nil
}

// uploadArtifactBytes is a convenience wrapper around uploadArtifact for
// callers that already have the artifact fully buffered.
func (c *uploadDebugDataClient) uploadArtifactBytes(
	ctx context.Context, artifactPath, contentType string, data []byte,
) error {
	return c.uploadArtifact(ctx, artifactPath, contentType,
		func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(data)), nil
		})
}

// uploadArtifactJSON marshals v as JSON and uploads it.
func (c *uploadDebugDataClient) uploadArtifactJSON(
	ctx context.Context, artifactPath string, v interface{},
) error {
	data, err := json.Marshal(v)
	if err != nil {
		return errors.Wrap(err, "marshaling artifact JSON")
	}
	return c.uploadArtifactBytes(ctx, artifactPath, "application/json", data)
}

// completeSessionRequest is the JSON body of POST
// /api/v1/sessions/:id/complete.
type completeSessionRequest struct {
	Status            string  `json:"status"`
	ArtifactsUploaded int     `json:"artifacts_uploaded"`
	NodesCompleted    []int32 `json:"nodes_completed"`
}

// completeSession signals the upload server that this session is done. It
// should be called exactly once per session (either at the end of a
// successful run, or after a retry has finished); subsequent calls return
// an error.
func (c *uploadDebugDataClient) completeSession(
	ctx context.Context, artifactsUploaded int, nodesCompleted []int32,
) error {
	body, err := json.Marshal(completeSessionRequest{
		Status:            "success",
		ArtifactsUploaded: artifactsUploaded,
		NodesCompleted:    nodesCompleted,
	})
	if err != nil {
		return errors.Wrap(err, "marshaling complete request")
	}
	resp, err := c.doJSON(ctx, http.MethodPost,
		fmt.Sprintf("%s/api/v1/sessions/%s/complete", c.serverURL, c.sessionID),
		"Bearer "+c.uploadToken, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return readUploadHTTPError(resp)
	}
	return nil
}

// doJSON issues a Content-Type=application/json request against the upload
// server with the supplied Authorization header value. If body is nil, the
// request is sent without a body (and without a Content-Type header).
func (c *uploadDebugDataClient) doJSON(
	ctx context.Context, method, fullURL, authorization string, body []byte,
) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, fullURL, reader)
	if err != nil {
		return nil, errors.Wrapf(err, "building %s %s", method, fullURL)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "%s %s", method, fullURL)
	}
	return resp, nil
}

// readUploadHTTPError converts a non-2xx HTTP response from the upload
// server into a descriptive Go error. At most 4 KiB of the response body
// is read to avoid unbounded memory use on hostile / misbehaving servers.
func readUploadHTTPError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return errors.Newf("upload server returned HTTP %d: %s", resp.StatusCode, string(body))
}

// shouldRetryGCSError returns true for transient GCS errors that are worth
// retrying at the chunk level. It follows the retry guidance published by
// Google: https://cloud.google.com/storage/docs/retry-strategy.
func shouldRetryGCSError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) &&
		strings.Contains(opErr.Error(), "use of closed network connection") {
		return true
	}
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		return apiErr.Code == http.StatusRequestTimeout ||
			apiErr.Code == http.StatusTooManyRequests ||
			(apiErr.Code >= 500 && apiErr.Code < 600)
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		msg := urlErr.Error()
		if strings.Contains(msg, "connection refused") ||
			strings.Contains(msg, "connection reset") {
			return true
		}
	}
	if w, ok := err.(interface{ Unwrap() error }); ok {
		return shouldRetryGCSError(w.Unwrap())
	}
	return false
}
