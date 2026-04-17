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
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	// uploadServerAPIKeyEnvVar is the environment variable for the upload
	// server API key.
	uploadServerAPIKeyEnvVar = "COCKROACH_UPLOAD_SERVER_API_KEY"

	// uploadMaxRetries is the maximum number of retry attempts for
	// uploading a debug zip to blob storage.0x
	uploadMaxRetries = 5
)

// uploadServerHTTPClient is used for control-plane calls to the upload
// server (session create, upload-token, complete). It has a 30-second
// timeout to avoid blocking indefinitely on a hung server.
var uploadServerHTTPClient = &http.Client{
	Timeout: 30 * time.Second,
}

// uploadSessionInfo holds the result of creating an upload session.
type uploadSessionInfo struct {
	SessionID   string
	UploadToken string
	Bucket      string
	Prefix      string
	GCSToken    string
}

// createUploadSession exchanges the API key for a session with the
// upload server, then fetches short-lived GCS credentials for direct
// upload. The flow is:
//  1. POST /api/v1/sessions with mode=direct → {session_id, upload_token}
//  2. POST /api/v1/sessions/{id}/upload-token → {access_token, bucket, prefix}
var createUploadSession = func(
	ctx context.Context, serverURL, apiKey string,
) (uploadSessionInfo, error) {

	sessionID, uploadToken, err := createSession(ctx, serverURL, apiKey)
	if err != nil {
		return uploadSessionInfo{}, err
	}

	bucket, prefix, gcsToken, err := getUploadToken(
		ctx, serverURL, sessionID, uploadToken,
	)
	if err != nil {
		return uploadSessionInfo{}, err
	}

	return uploadSessionInfo{
		SessionID:   sessionID,
		UploadToken: uploadToken,
		Bucket:      bucket,
		Prefix:      prefix,
		GCSToken:    gcsToken,
	}, nil
}

// createSession calls POST /api/v1/sessions with mode=direct.
func createSession(
	ctx context.Context, serverURL, apiKey string,
) (sessionID, uploadToken string, err error) {
	body, err := json.Marshal(struct {
		Mode string `json:"mode"`
	}{Mode: "direct"})
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

	resp, err := uploadServerHTTPClient.Do(req)
	if err != nil {
		return "", "", errors.Wrap(err, "creating session")
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
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

// getUploadToken calls POST /api/v1/sessions/{id}/upload-token to
// get short-lived GCS credentials for direct upload.
func getUploadToken(
	ctx context.Context, serverURL, sessionID, uploadToken string,
) (bucket, prefix, accessToken string, err error) {
	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost,
		fmt.Sprintf("%s/api/v1/sessions/%s/upload-token", serverURL, sessionID),
		nil,
	)
	if err != nil {
		return "", "", "", errors.Wrap(err, "building upload-token request")
	}
	req.Header.Set("Authorization", "Bearer "+uploadToken)

	resp, err := uploadServerHTTPClient.Do(req)
	if err != nil {
		return "", "", "", errors.Wrap(err, "fetching upload token")
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", "", errors.Wrap(err, "reading upload-token response")
	}

	if resp.StatusCode != http.StatusOK {
		return "", "", "", errors.Newf(
			"upload token request failed (%d): %s", resp.StatusCode, respBody,
		)
	}

	var result struct {
		AccessToken string `json:"access_token"`
		Bucket      string `json:"bucket"`
		Prefix      string `json:"prefix"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return "", "", "", errors.Wrap(err, "parsing upload-token response")
	}

	return result.Bucket, result.Prefix, result.AccessToken, nil
}

// completeUploadSession marks the session as complete on the upload
// server via POST /api/v1/sessions/{id}/complete.
var completeUploadSession = func(
	ctx context.Context, serverURL, sessionID, uploadToken string,
) error {
	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost,
		fmt.Sprintf("%s/api/v1/sessions/%s/complete", serverURL, sessionID),
		nil,
	)
	if err != nil {
		return errors.Wrap(err, "building complete request")
	}
	req.Header.Set("Authorization", "Bearer "+uploadToken)

	resp, err := uploadServerHTTPClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "completing session")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return errors.Newf(
			"session completion failed (%d): %s", resp.StatusCode, respBody,
		)
	}

	return nil
}

// uploadDebugZipViaServer uploads the raw debug.zip file via the upload
// server. The flow is:
//  1. Create a session with the upload server (gets GCS credentials)
//  2. Upload the zip directly to GCS using those credentials
//  3. Mark the session complete
func uploadDebugZipViaServer(ctx context.Context, zipFilePath string) error {

	sess, err := createUploadSession(
		ctx,
		debugZipUploadOpts.uploadServerURL,
		debugZipUploadOpts.uploadServerAPIKey,
	)
	if err != nil {
		return errors.Wrap(err, "creating upload session")
	}

	fmt.Fprintf(os.Stderr, "Upload session: %s\n", sess.SessionID)

	uploader, err := newBlobUploader(ctx, "gcs", sess.Bucket, sess.Prefix, sess.GCSToken)
	if err != nil {
		return err
	}
	defer func() {
		if cErr := uploader.Close(); cErr != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to close uploader: %v\n", cErr)
		}
	}()

	uploadStart := timeutil.Now()

	retryOpts := base.DefaultRetryOptions()
	retryOpts.MaxRetries = uploadMaxRetries

	for r := retry.Start(retryOpts); r.Next(); {
		_, err = uploader.Upload(ctx, zipFilePath)
		if err == nil {
			break
		}
		fmt.Fprintf(os.Stderr, "Upload attempt failed: %v. Retrying...\n", err)
	}
	if err != nil {
		return errors.Wrap(err, "upload failed after retries")
	}

	elapsed := timeutil.Since(uploadStart)

	if err := completeUploadSession(
		ctx,
		debugZipUploadOpts.uploadServerURL,
		sess.SessionID,
		sess.UploadToken,
	); err != nil {
		fmt.Fprintf(os.Stderr,
			"Warning: file was uploaded but session completion failed: %v\n", err,
		)
		fmt.Fprintf(os.Stderr,
			"The debug zip was uploaded but may not be processed automatically.\n",
		)
	} else {
		fmt.Fprintf(os.Stderr, "Session %s completed\n", sess.SessionID)
	}

	if fi, statErr := os.Stat(zipFilePath); statErr == nil && elapsed.Seconds() > 0 {
		throughput := float64(fi.Size()) / elapsed.Seconds() / (1024 * 1024)
		fmt.Fprintf(os.Stderr,
			"Uploaded debug zip in %s (%.1f MB/s).\n",
			elapsed.Round(time.Second), throughput,
		)
	}
	return nil
}

// validateUploadServerReadiness checks that the required flags are set
// for upload-server uploads and that the input file is valid.
func validateUploadServerReadiness(inputPath string) error {
	if debugZipUploadOpts.uploadServerAPIKey == "" {
		return errors.New(
			"--upload-server-api-key is required for upload-server destination",
		)
	}

	if debugZipUploadOpts.uploadServerURL == "" {
		return errors.New(
			"--upload-server-url is required for upload-server destination",
		)
	}

	if !strings.HasSuffix(strings.ToLower(inputPath), ".zip") {
		return errors.Newf(
			"upload-server requires a .zip file, got %q", inputPath,
		)
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

	// Verify the file starts with the ZIP magic number (PK\x03\x04).
	var magic [4]byte
	if _, err := io.ReadFull(f, magic[:]); err != nil {
		return errors.Wrapf(err, "reading %q", inputPath)
	}
	if magic != [4]byte{0x50, 0x4b, 0x03, 0x04} {
		return errors.Newf("%q does not appear to be a valid zip file", inputPath)
	}

	return nil
}
