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
	"strings"
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

// createUploadSession is a var so tests can stub the whole handshake.
// ticketID is sent on fresh sessions only; for resume the existing
// session already carries its ticket mapping.
var createUploadSession = func(
	ctx context.Context, serverURL, apiKey, ticketID, resumeSessionID string, httpClient *http.Client,
) (uploadSessionInfo, error) {
	var (
		sessionID, uploadToken string
		err                    error
	)
	if resumeSessionID != "" {
		sessionID, uploadToken, err = reopenSession(ctx, serverURL, resumeSessionID, apiKey, httpClient)
		if err != nil {
			return uploadSessionInfo{}, errors.Wrap(err, "reopening session")
		}
	} else {
		sessionID, uploadToken, err = createSession(ctx, serverURL, apiKey, ticketID, httpClient)
		if err != nil {
			return uploadSessionInfo{}, errors.Wrap(err, "creating session")
		}
	}

	uri, bucket, objectPath, contentType, err := createResumableSession(
		ctx, serverURL, sessionID, uploadToken, defaultContentType, httpClient,
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

func createSession(
	ctx context.Context, serverURL, apiKey, ticketID string, httpClient *http.Client,
) (sessionID, uploadToken string, err error) {
	payload := struct {
		Mode   string            `json:"mode"`
		Labels map[string]string `json:"labels,omitempty"`
	}{Mode: "direct"}
	if ticketID != "" {
		payload.Labels = map[string]string{"ticket_id": ticketID}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", "", errors.Wrap(err, "marshaling session request")
	}

	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost,
		serverURL+"/api/v1/sessions",
		bytes.NewReader(body),
	)
	if err != nil {
		return "", "", errors.Wrap(err, "building session request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", "", errors.Wrap(err, "POST /sessions")
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxControlPlaneBody))
	if err != nil {
		return "", "", errors.Wrap(err, "reading session response")
	}

	if resp.StatusCode != http.StatusCreated {
		return "", "", errors.Newf(
			"session creation failed (%d): %s", resp.StatusCode, respBody,
		)
	}

	var result struct {
		SessionID   string `json:"session_id"`
		UploadToken string `json:"upload_token"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return "", "", errors.Wrap(err, "parsing session response")
	}

	return result.SessionID, result.UploadToken, nil
}

// reopenSession authenticates with the cluster API key (not the old
// upload token, which the dead client may have lost) and returns a
// fresh upload token.
func reopenSession(
	ctx context.Context, serverURL, sessionID, apiKey string, httpClient *http.Client,
) (string, string, error) {
	body, err := json.Marshal(struct {
		Reason string `json:"reason"`
	}{Reason: "client resume after interruption"})
	if err != nil {
		return "", "", errors.Wrap(err, "marshaling reupload request")
	}

	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost,
		fmt.Sprintf("%s/api/v1/sessions/%s/reupload", serverURL, sessionID),
		bytes.NewReader(body),
	)
	if err != nil {
		return "", "", errors.Wrap(err, "building reupload request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", "", errors.Wrap(err, "POST /reupload")
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxControlPlaneBody))
	if err != nil {
		return "", "", errors.Wrap(err, "reading reupload response")
	}

	if resp.StatusCode != http.StatusOK {
		return "", "", errors.Newf(
			"reopening session failed (%d): %s", resp.StatusCode, respBody,
		)
	}

	var result struct {
		SessionID   string `json:"session_id"`
		UploadToken string `json:"upload_token"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return "", "", errors.Wrap(err, "parsing reupload response")
	}

	return result.SessionID, result.UploadToken, nil
}

// createResumableSession is idempotent on (session_id, object_name): a
// retry after a partial upload returns the same URI so PUT progress is
// preserved. The server controls the object name; we send only the
// content type.
func createResumableSession(
	ctx context.Context,
	serverURL, sessionID, uploadToken, contentType string,
	httpClient *http.Client,
) (sessionURI, bucket, objectPath, respContentType string, err error) {
	body, err := json.Marshal(struct {
		ContentType string `json:"content_type"`
	}{ContentType: contentType})
	if err != nil {
		return "", "", "", "", errors.Wrap(err, "marshaling resumable-session request")
	}

	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost,
		fmt.Sprintf("%s/api/v1/sessions/%s/resumable-session", serverURL, sessionID),
		bytes.NewReader(body),
	)
	if err != nil {
		return "", "", "", "", errors.Wrap(err, "building resumable-session request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+uploadToken)

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", "", "", "", errors.Wrap(err, "POST /resumable-session")
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxControlPlaneBody))
	if err != nil {
		return "", "", "", "", errors.Wrap(err, "reading resumable-session response")
	}

	// 200 = cached URI reused; 201 = freshly minted.
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", "", "", "", errors.Newf(
			"resumable-session request failed (%d): %s", resp.StatusCode, respBody,
		)
	}

	var result struct {
		SessionURI  string `json:"gcs_session_uri"`
		Bucket      string `json:"bucket"`
		ObjectPath  string `json:"object_path"`
		ContentType string `json:"content_type"`
		Reused      bool   `json:"reused"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return "", "", "", "", errors.Wrap(err, "parsing resumable-session response")
	}

	return result.SessionURI, result.Bucket, result.ObjectPath, result.ContentType, nil
}

// completeUploadSession is a var for test stubbing. The server
// requires a status field on /complete; we always pass "success" here
// because the orchestrator only calls this after the data PUT
// succeeded. artifacts_uploaded is fixed at 1 (one debug.zip per
// session).
var completeUploadSession = func(
	ctx context.Context, serverURL, sessionID, uploadToken string, httpClient *http.Client,
) error {
	body, err := json.Marshal(struct {
		Status            string `json:"status"`
		ArtifactsUploaded int    `json:"artifacts_uploaded"`
	}{Status: "success", ArtifactsUploaded: 1})
	if err != nil {
		return errors.Wrap(err, "marshaling complete request")
	}
	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost,
		fmt.Sprintf("%s/api/v1/sessions/%s/complete", serverURL, sessionID),
		bytes.NewReader(body),
	)
	if err != nil {
		return errors.Wrap(err, "building complete request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+uploadToken)

	resp, err := httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "POST /complete")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.Newf(
			"session completion failed (%d): %s", resp.StatusCode, readBoundedBody(resp.Body),
		)
	}
	return nil
}

func uploadDebugZipToCRLSupport(ctx context.Context, zipFilePath string) error {
	crlSupportClient, err := newUploadHTTPClient(crlSupportTimeout)
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

	sess, err := createUploadSession(
		ctx,
		debugZipUploadOpts.crlSupportURL,
		debugZipUploadOpts.crlSupportAPIKey,
		debugZipUploadOpts.crlSupportTicketID,
		debugZipUploadOpts.resumeSession,
		crlSupportClient,
	)
	if err != nil {
		return err
	}

	resuming := debugZipUploadOpts.resumeSession != ""
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
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
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
		// retry warning prints on its own line.
		fmt.Fprintln(os.Stderr)
		fmt.Fprintf(os.Stderr,
			"Upload attempt failed: %v. Retrying...\n"+
				"  (resume manually with --resume-session=%s)\n",
			err, sess.SessionID,
		)
	}
	if err != nil {
		if errors.Is(err, errSessionURIDead) ||
			errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return errors.Wrapf(err,
			"upload failed after retries (resume with --resume-session=%s)",
			sess.SessionID,
		)
	}
	elapsed := timeutil.Since(uploadStart)

	if err := completeUploadSession(
		ctx,
		debugZipUploadOpts.crlSupportURL,
		sess.SessionID,
		sess.UploadToken,
		crlSupportClient,
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

func validateCRLSupportReadiness(inputPath string) error {
	if debugZipUploadOpts.crlSupportAPIKey == "" {
		return errors.New(
			"--crl-support-api-key is required for --destination=crl-support " +
				"(or set " + crlSupportAPIKeyEnvVar + ")",
		)
	}
	if debugZipUploadOpts.crlSupportURL == "" {
		return errors.New("--crl-support-url is required for --destination=crl-support")
	}

	if !strings.HasSuffix(strings.ToLower(inputPath), ".zip") {
		return errors.Newf("crl-support requires a .zip file, got %q", inputPath)
	}

	f, err := os.Open(inputPath)
	if err != nil {
		return errors.Wrapf(err, "cannot access %q", inputPath)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return errors.Wrapf(err, "cannot stat %q", inputPath)
	}
	if fi.Size() == 0 {
		return errors.Newf("zip file %q is empty", inputPath)
	}

	// Verify the ZIP magic number (PK\x03\x04).
	var magic [4]byte
	if _, err := io.ReadFull(f, magic[:]); err != nil {
		return errors.Wrapf(err, "reading %q", inputPath)
	}
	if magic != [4]byte{0x50, 0x4b, 0x03, 0x04} {
		return errors.Newf("%q does not appear to be a valid zip file", inputPath)
	}
	return nil
}

// newUploadHTTPClient honours --proxy / HTTPS_PROXY. timeout==0
// disables the per-request timeout.
func newUploadHTTPClient(timeout time.Duration) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()

	proxy := debugZipUploadOpts.proxy
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
