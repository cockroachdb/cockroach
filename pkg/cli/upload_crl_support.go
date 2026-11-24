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
	"net/url"
	"os"
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	crlSupportAPIKeyEnvVar = "COCKROACH_CRL_SUPPORT_API_KEY"
	httpsProxyEnvVar       = "HTTPS_PROXY"

	uploadMaxRetries = 5
	// crlSupportTimeout caps each crl-support call (create session,
	// get URI, complete). Blob PUTs use a separate client with no
	// timeout because large debug zips can take a while.
	crlSupportTimeout = 30 * time.Second
	// maxControlPlaneBody bounds how much of a JSON response we read
	// from crl-support. Real responses are a few hundred bytes; this
	// guards against a misbehaving proxy or hostile server.
	maxControlPlaneBody = 1 << 20 // 1 MiB

	defaultContentType = "application/zip"
)

type uploadSessionInfo struct {
	SessionID   string
	UploadToken string
	SessionURI  string
	Bucket      string
	ObjectPath  string
	ContentType string
}

// doCRLHTTPRequest is a var so tests can swap in a dispatcher that
// returns canned responses keyed by URL path.
var doCRLHTTPRequest = func(client *http.Client, req *http.Request) (*http.Response, error) {
	return client.Do(req)
}

// crlSupportClient handles the JSON control-plane handshake against
// the CRL support upload server. It owns serverURL + httpClient so
// per-endpoint methods only deal with the path, auth token, payload,
// and response shape.
type crlSupportClient struct {
	serverURL  string
	httpClient *http.Client
}

func newCRLSupportClient(serverURL string, httpClient *http.Client) *crlSupportClient {
	return &crlSupportClient{serverURL: serverURL, httpClient: httpClient}
}

// postJSON marshals payload, POSTs it to path with Bearer authToken,
// and validates the response status against okStatuses. If dest is
// non-nil, the response body is unmarshaled into it. opName is woven
// into error wrapping so callers don't need to add their own context.
func (c *crlSupportClient) postJSON(
	ctx context.Context, opName, path, authToken string, payload, dest any, okStatuses ...int,
) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return errors.Wrapf(err, "marshaling %s request", opName)
	}
	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, c.serverURL+path, bytes.NewReader(body),
	)
	if err != nil {
		return errors.Wrapf(err, "building %s request", opName)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+authToken)

	resp, err := doCRLHTTPRequest(c.httpClient, req)
	if err != nil {
		return errors.Wrapf(err, "%s request", opName)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxControlPlaneBody))
	if err != nil {
		return errors.Wrapf(err, "reading %s response", opName)
	}
	if !slices.Contains(okStatuses, resp.StatusCode) {
		return errors.Newf("%s failed (%d): %s", opName, resp.StatusCode, respBody)
	}
	if dest == nil {
		return nil
	}
	if err := json.Unmarshal(respBody, dest); err != nil {
		return errors.Wrapf(err, "parsing %s response", opName)
	}
	return nil
}

// createUploadSession is a var so tests can stub the whole handshake.
// ticketID is sent on fresh sessions only; for resume the existing
// session already carries its ticket mapping.
var createUploadSession = func(
	ctx context.Context, c *crlSupportClient, apiKey, ticketID, resumeSessionID string,
) (uploadSessionInfo, error) {
	var (
		sessionID, uploadToken string
		err                    error
	)
	if resumeSessionID != "" {
		sessionID, uploadToken, err = c.reopenSession(ctx, resumeSessionID, apiKey)
		if err != nil {
			return uploadSessionInfo{}, errors.Wrap(err, "reopening session")
		}
	} else {
		sessionID, uploadToken, err = c.createSession(ctx, apiKey, ticketID)
		if err != nil {
			return uploadSessionInfo{}, errors.Wrap(err, "creating session")
		}
	}

	uri, bucket, objectPath, contentType, err := c.createResumableSession(
		ctx, sessionID, uploadToken, defaultContentType,
	)
	if err != nil {
		return uploadSessionInfo{}, errors.Wrap(err, "fetching resumable session URI")
	}

	return uploadSessionInfo{
		SessionID:   sessionID,
		UploadToken: uploadToken,
		SessionURI:  uri,
		Bucket:      bucket,
		ObjectPath:  objectPath,
		ContentType: contentType,
	}, nil
}

func (c *crlSupportClient) createSession(
	ctx context.Context, apiKey, ticketID string,
) (sessionID, uploadToken string, err error) {
	payload := struct {
		Mode   string            `json:"mode"`
		Labels map[string]string `json:"labels,omitempty"`
	}{Mode: "direct"}
	if ticketID != "" {
		payload.Labels = map[string]string{"ticket_id": ticketID}
	}
	var result struct {
		SessionID   string `json:"session_id"`
		UploadToken string `json:"upload_token"`
	}
	if err := c.postJSON(
		ctx, "session creation", "/api/v1/sessions", apiKey,
		payload, &result, http.StatusCreated,
	); err != nil {
		return "", "", err
	}
	return result.SessionID, result.UploadToken, nil
}

// reopenSession authenticates with the cluster API key (not the old
// upload token, which the dead client may have lost) and returns a
// fresh upload token.
func (c *crlSupportClient) reopenSession(
	ctx context.Context, sessionID, apiKey string,
) (string, string, error) {
	payload := struct {
		Reason string `json:"reason"`
	}{Reason: "client resume after interruption"}
	var result struct {
		SessionID   string `json:"session_id"`
		UploadToken string `json:"upload_token"`
	}
	if err := c.postJSON(
		ctx, "reopening session",
		fmt.Sprintf("/api/v1/sessions/%s/reupload", sessionID),
		apiKey, payload, &result, http.StatusOK,
	); err != nil {
		return "", "", err
	}
	return result.SessionID, result.UploadToken, nil
}

// createResumableSession is idempotent on (session_id, object_name): a
// retry after a partial upload returns the same URI so PUT progress is
// preserved. The server controls the object name; we send only the
// content type. The server returns 201 for a freshly minted URI and
// 200 for a cached one — both are success.
func (c *crlSupportClient) createResumableSession(
	ctx context.Context, sessionID, uploadToken, contentType string,
) (sessionURI, bucket, objectPath, respContentType string, err error) {
	payload := struct {
		ContentType string `json:"content_type"`
	}{ContentType: contentType}
	var result struct {
		SessionURI  string `json:"gcs_session_uri"`
		Bucket      string `json:"bucket"`
		ObjectPath  string `json:"object_path"`
		ContentType string `json:"content_type"`
		Reused      bool   `json:"reused"`
	}
	if err := c.postJSON(
		ctx, "resumable-session request",
		fmt.Sprintf("/api/v1/sessions/%s/resumable-session", sessionID),
		uploadToken, payload, &result,
		http.StatusOK, http.StatusCreated,
	); err != nil {
		return "", "", "", "", err
	}
	return result.SessionURI, result.Bucket, result.ObjectPath, result.ContentType, nil
}

// completeUploadSession is a var for test stubbing. The server
// requires a status field on /complete; we always pass "success" here
// because the orchestrator only calls this after the data PUT
// succeeded. artifacts_uploaded is fixed at 1 (one debug.zip per
// session).
var completeUploadSession = func(
	ctx context.Context, c *crlSupportClient, sessionID, uploadToken string,
) error {
	payload := struct {
		Status            string `json:"status"`
		ArtifactsUploaded int    `json:"artifacts_uploaded"`
	}{Status: "success", ArtifactsUploaded: 1}
	return c.postJSON(
		ctx, "session completion",
		fmt.Sprintf("/api/v1/sessions/%s/complete", sessionID),
		uploadToken, payload, nil, http.StatusOK,
	)
}

func uploadToCRLSupport(ctx context.Context, zipFilePath string) error {
	crlHTTPClient, err := newUploadHTTPClient(crlSupportTimeout)
	if err != nil {
		return err
	}
	// No timeout: blob PUT duration scales with file size
	// a fixed timeout would kill slow uploads
	// cancellation goes through ctx.
	blobClient, err := newUploadHTTPClient(0)
	if err != nil {
		return err
	}

	crlClient := newCRLSupportClient(debugUploadOpts.crlSupportURL, crlHTTPClient)

	sess, err := createUploadSession(
		ctx,
		crlClient,
		debugUploadOpts.crlSupportAPIKey,
		debugUploadOpts.crlSupportTicketID,
		debugUploadOpts.resumeSession,
	)
	if err != nil {
		return err
	}

	resuming := debugUploadOpts.resumeSession != ""
	if resuming {
		fmt.Fprintf(os.Stderr, "Resumed upload session: %s\n", sess.SessionID)
	} else {
		fmt.Fprintf(os.Stderr, "Upload session: %s\n", sess.SessionID)
	}

	uploader, err := newBlobUploader(
		ctx, sess.SessionURI, sess.Bucket, sess.ObjectPath, sess.ContentType, blobClient,
		resuming, /* resuming */
	)
	if err != nil {
		return errors.Wrap(err, "creating blob uploader")
	}
	defer func() {
		if cErr := uploader.Close(); cErr != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to close uploader: %v\n", cErr)
		}
	}()

	uploadStart := timeutil.Now()

	retryOpts := base.DefaultRetryOptions()
	retryOpts.MaxRetries = uploadMaxRetries

	// Seed err with ctx.Err() so a never-entered loop (ctx already
	// cancelled) surfaces the real cause instead of a misleading
	// "upload not attempted" sentinel. nil if ctx is healthy; the
	// loop's first attempt will overwrite it.
	err = ctx.Err()
	var bytesTransferred int64
	// Total = 1 initial attempt + uploadMaxRetries retries. Used to
	// drive the "attempt N/M" progress line and decide whether
	// "Retrying..." should be appended.
	totalAttempts := uploadMaxRetries + 1
	attempt := 0
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		attempt++
		_, bytesTransferred, err = uploader.Upload(ctx, zipFilePath)
		if err == nil {
			break
		}
		// Errors that cannot be revived by retrying: a dead session URI,
		// or caller cancellation / deadline. Break without printing
		// "Retrying..."
		if errors.Is(err, errSessionURIDead) ||
			errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {
			break
		}
		// Newline first to flush any dangling progress \r line so the
		// retry warning prints on its own line. Only say "Retrying..."
		// when there is actually another attempt left.
		fmt.Fprintln(os.Stderr)
		if attempt < totalAttempts {
			fmt.Fprintf(os.Stderr,
				"upload attempt failed (%d/%d): %v. Retrying...\n",
				attempt, totalAttempts, err,
			)
		} else {
			fmt.Fprintf(os.Stderr,
				"upload attempt failed (%d/%d): %v\n",
				attempt, totalAttempts, err,
			)
		}
	}
	if err != nil {
		if errors.Is(err, errSessionURIDead) ||
			errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return errors.Wrapf(err,
			"upload failed after %d attempts; resume with --resume-session=%s",
			totalAttempts, sess.SessionID,
		)
	}
	elapsed := timeutil.Since(uploadStart)

	if err := completeUploadSession(
		ctx,
		crlClient,
		sess.SessionID,
		sess.UploadToken,
	); err != nil {
		// The bytes are in storage but downstream processing won't
		// run — surface it as a non-zero exit. The session is
		// idempotent server-side: re-running with --resume-session
		// is safe.
		return errors.Wrapf(err,
			"upload completed but session finalisation failed (re-run with --resume-session=%s)",
			sess.SessionID,
		)
	}
	fmt.Fprintf(os.Stderr, "Session %s completed.\n", sess.SessionID)

	// Skip the throughput line when nothing actually moved (resume found
	// the object already complete on the server) — the elapsed time only
	// reflects the probe round-trip and would produce a misleading rate.
	if bytesTransferred > 0 && elapsed.Seconds() > 0 {
		throughput := float64(bytesTransferred) / elapsed.Seconds() / (1024 * 1024)
		fmt.Fprintf(os.Stderr,
			"Uploaded %s in %s (%.1f MB/s).\n",
			humanReadableSize(int(bytesTransferred)),
			elapsed.Round(time.Second), throughput,
		)
	}
	return nil
}

// validateUploadConfig confirms the destination-config flags are
// set. Independent of the file being uploaded.
func validateUploadConfig() error {
	if debugUploadOpts.crlSupportAPIKey == "" {
		return errors.New(
			"--crl-support-api-key is required (or set " + crlSupportAPIKeyEnvVar + ")",
		)
	}
	if debugUploadOpts.crlSupportURL == "" {
		return errors.New("--crl-support-url is required")
	}
	return nil
}

// newUploadHTTPClient honours --proxy / HTTPS_PROXY. timeout==0
// disables the per-request timeout.
func newUploadHTTPClient(timeout time.Duration) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()

	proxy := debugUploadOpts.proxy
	if proxy == "" {
		proxy = getEnvOrDefault(httpsProxyEnvVar, "")
	}
	if proxy != "" {
		proxyURL, err := url.Parse(proxy)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid proxy URL %q", proxy)
		}
		transport.Proxy = http.ProxyURL(proxyURL)
	}
	// Otherwise transport.Proxy is already http.ProxyFromEnvironment
	// (the http.DefaultTransport default), which still picks up
	// HTTP_PROXY / NO_PROXY.

	return &http.Client{Transport: transport, Timeout: timeout}, nil
}
