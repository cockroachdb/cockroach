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
	"github.com/cockroachdb/errors"
)

const (
	// uploadServerAPIKeyEnvVar is the environment variable for the upload
	// server API key.
	uploadServerAPIKeyEnvVar = "COCKROACH_UPLOAD_SERVER_API_KEY"
)

// uploadServerHTTPClient is used for control-plane calls to the upload
// server (session create, upload-token, complete). It has a 30-second
// timeout to avoid blocking indefinitely on a hung server.
var uploadServerHTTPClient = &http.Client{
	Timeout: 30 * time.Second,
}

// createUploadSession exchanges the API key for a session with the
// upload server, then fetches short-lived GCS credentials for direct
// upload. The flow is:
//  1. POST /api/v1/sessions with mode=direct → {session_id, upload_token}
//  2. POST /api/v1/sessions/{id}/upload-token → {access_token, bucket, prefix}
var createUploadSession = func(
	ctx context.Context, serverURL, apiKey string,
) (sessionID, uploadToken, bucket, prefix, gcsToken string, err error) {

	sessionID, uploadToken, err = createSession(ctx, serverURL, apiKey)
	if err != nil {
		return "", "", "", "", "", err
	}

	bucket, prefix, gcsToken, err = getUploadToken(
		ctx, serverURL, sessionID, uploadToken,
	)
	if err != nil {
		return "", "", "", "", "", err
	}

	return sessionID, uploadToken, bucket, prefix, gcsToken, nil
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

	sessionID, uploadToken, bucket, prefix, gcsToken, err := createUploadSession(
		ctx,
		debugZipUploadOpts.uploadServerURL,
		debugZipUploadOpts.uploadServerAPIKey,
	)
	if err != nil {
		return errors.Wrap(err, "creating upload session")
	}

	fmt.Fprintf(os.Stderr, "Upload session: %s\n", sessionID)

	uploader, err := newBlobUploader(ctx, "gcs", bucket, prefix, gcsToken)
	if err != nil {
		return err
	}
	defer func() {
		if cErr := uploader.Close(); cErr != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to close uploader: %v\n", cErr)
		}
	}()

	retryOpts := base.DefaultRetryOptions()
	retryOpts.MaxRetries = 5

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

	if err := completeUploadSession(
		ctx,
		debugZipUploadOpts.uploadServerURL,
		sessionID,
		uploadToken,
	); err != nil {
		fmt.Fprintf(os.Stderr,
			"Warning: failed to complete upload session: %v\n", err,
		)
	} else {
		fmt.Fprintf(os.Stderr, "Session %s completed\n", sessionID)
	}

	fmt.Fprintf(os.Stderr, "Successfully uploaded debug zip.\n")
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

	fi, err := os.Stat(inputPath)
	if err != nil {
		return errors.Wrapf(err, "cannot access %q", inputPath)
	}
	if fi.Size() == 0 {
		return errors.Newf("zip file %q is empty", inputPath)
	}

	return nil
}
