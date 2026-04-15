// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestValidateUploadServerReadiness(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tmpDir := t.TempDir()
	zipPath := filepath.Join(tmpDir, "debug.zip")
	require.NoError(t, os.WriteFile(zipPath, []byte("fake-zip-content"), 0644))

	emptyZipPath := filepath.Join(tmpDir, "empty.zip")
	require.NoError(t, os.WriteFile(emptyZipPath, nil, 0644))

	dirPath := filepath.Join(tmpDir, "debug-dir")
	require.NoError(t, os.Mkdir(dirPath, 0755))

	origOpts := debugZipUploadOpts
	defer func() { debugZipUploadOpts = origOpts }()

	tests := []struct {
		name    string
		input   string
		setup   func()
		wantErr string
	}{
		{
			name:  "valid",
			input: zipPath,
			setup: func() {
				debugZipUploadOpts.uploadServerAPIKey = "test-key"
				debugZipUploadOpts.uploadServerURL = "https://example.com"
			},
		},
		{
			name:  "not a zip file",
			input: dirPath,
			setup: func() {
				debugZipUploadOpts.uploadServerAPIKey = "key"
				debugZipUploadOpts.uploadServerURL = "https://example.com"
			},
			wantErr: "upload-server requires a .zip file",
		},
		{
			name:  "empty zip file",
			input: emptyZipPath,
			setup: func() {
				debugZipUploadOpts.uploadServerAPIKey = "key"
				debugZipUploadOpts.uploadServerURL = "https://example.com"
			},
			wantErr: "is empty",
		},
		{
			name:  "missing api key",
			input: zipPath,
			setup: func() {
				debugZipUploadOpts.uploadServerAPIKey = ""
				debugZipUploadOpts.uploadServerURL = "https://example.com"
			},
			wantErr: "--upload-server-api-key is required",
		},
		{
			name:  "missing upload server url",
			input: zipPath,
			setup: func() {
				debugZipUploadOpts.uploadServerAPIKey = "key"
				debugZipUploadOpts.uploadServerURL = ""
			},
			wantErr: "--upload-server-url is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			debugZipUploadOpts = origOpts
			tt.setup()
			err := validateUploadServerReadiness(tt.input)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestUploadDebugZipViaServerSessionFailure verifies that an upload
// server session creation failure is propagated correctly.
func TestUploadDebugZipViaServerSessionFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tmpDir := t.TempDir()
	zipPath := filepath.Join(tmpDir, "debug.zip")
	require.NoError(t, os.WriteFile(zipPath, []byte("fake-zip"), 0644))

	origOpts := debugZipUploadOpts
	origCreate := createUploadSession
	defer func() {
		debugZipUploadOpts = origOpts
		createUploadSession = origCreate
	}()

	debugZipUploadOpts.uploadServerAPIKey = "bad-key"
	debugZipUploadOpts.uploadServerURL = "https://fake.example.com"

	createUploadSession = func(
		_ context.Context, _, _ string,
	) (string, string, string, string, string, error) {
		return "", "", "", "", "", errors.New("401 Unauthorized: invalid API key")
	}

	err := uploadDebugZipViaServer(context.Background(), zipPath)
	require.ErrorContains(t, err, "creating upload session")
	require.ErrorContains(t, err, "invalid API key")
}

// TestUploadDebugZipViaServerSuccess exercises the full upload-server
// flow end-to-end with all three var functions mocked. Verifies that
// session creation, upload, and completion are all called correctly.
func TestUploadDebugZipViaServerSuccess(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tmpDir := t.TempDir()
	zipPath := filepath.Join(tmpDir, "debug.zip")
	zipContent := []byte("PK-fake-zip-content")
	require.NoError(t, os.WriteFile(zipPath, zipContent, 0644))

	origOpts := debugZipUploadOpts
	origCreate := createUploadSession
	origComplete := completeUploadSession
	origNewUploader := newBlobUploader
	defer func() {
		debugZipUploadOpts = origOpts
		createUploadSession = origCreate
		completeUploadSession = origComplete
		newBlobUploader = origNewUploader
	}()

	debugZipUploadOpts.uploadServerAPIKey = "test-key"
	debugZipUploadOpts.uploadServerURL = "https://fake.example.com"

	mock := &mockBlobUploader{}
	var (
		completeCalled    bool
		completeSessionID string
		completeToken     string
	)

	createUploadSession = func(
		_ context.Context, serverURL, apiKey string,
	) (string, string, string, string, string, error) {
		require.Equal(t, "https://fake.example.com", serverURL)
		require.Equal(t, "test-key", apiKey)
		return "ses_test", "tok_test", "bucket", "prefix", "gcs-token", nil
	}

	newBlobUploader = func(
		_ context.Context, provider, bucket, prefix, token string,
	) (blobUploader, error) {
		require.Equal(t, "gcs", provider)
		require.Equal(t, "bucket", bucket)
		require.Equal(t, "prefix", prefix)
		require.Equal(t, "gcs-token", token)
		return mock, nil
	}

	completeUploadSession = func(
		_ context.Context, serverURL, sessionID, uploadToken string,
	) error {
		completeCalled = true
		completeSessionID = sessionID
		completeToken = uploadToken
		return nil
	}

	err := uploadDebugZipViaServer(context.Background(), zipPath)
	require.NoError(t, err)

	require.Equal(t, zipPath, mock.uploadedPath)
	require.Equal(t, zipContent, mock.content)

	require.True(t, completeCalled)
	require.Equal(t, "ses_test", completeSessionID)
	require.Equal(t, "tok_test", completeToken)
}

// TestUploadDebugZipViaServerRetryThenSuccess verifies that a
// transient upload failure is retried and succeeds on a subsequent
// attempt.
func TestUploadDebugZipViaServerRetryThenSuccess(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tmpDir := t.TempDir()
	zipPath := filepath.Join(tmpDir, "debug.zip")
	require.NoError(t, os.WriteFile(zipPath, []byte("PK-data"), 0644))

	origOpts := debugZipUploadOpts
	origCreate := createUploadSession
	origComplete := completeUploadSession
	origNewUploader := newBlobUploader
	defer func() {
		debugZipUploadOpts = origOpts
		createUploadSession = origCreate
		completeUploadSession = origComplete
		newBlobUploader = origNewUploader
	}()

	debugZipUploadOpts.uploadServerAPIKey = "key"
	debugZipUploadOpts.uploadServerURL = "https://fake.example.com"

	failsRemaining := 2

	createUploadSession = func(
		_ context.Context, _, _ string,
	) (string, string, string, string, string, error) {
		return "ses", "tok", "b", "p", "gcs-tok", nil
	}

	callCount := 0
	wrapperMock := &funcBlobUploader{
		uploadFn: func(ctx context.Context, srcPath string) (string, error) {
			callCount++
			if callCount <= failsRemaining {
				return "", errors.New("transient network error")
			}
			return "gs://b/p/" + filepath.Base(srcPath), nil
		},
		closeFn: func() error { return nil },
	}

	newBlobUploader = func(
		_ context.Context, _, _, _, _ string,
	) (blobUploader, error) {
		return wrapperMock, nil
	}

	completeUploadSession = func(
		_ context.Context, _, _, _ string,
	) error {
		return nil
	}

	err := uploadDebugZipViaServer(context.Background(), zipPath)
	require.NoError(t, err)
	require.Equal(t, 3, callCount)
}

// TestUploadDebugZipViaServerCompletionFailure verifies that a session
// completion failure does not cause the overall upload to fail.
func TestUploadDebugZipViaServerCompletionFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tmpDir := t.TempDir()
	zipPath := filepath.Join(tmpDir, "debug.zip")
	require.NoError(t, os.WriteFile(zipPath, []byte("PK-data"), 0644))

	origOpts := debugZipUploadOpts
	origCreate := createUploadSession
	origComplete := completeUploadSession
	origNewUploader := newBlobUploader
	defer func() {
		debugZipUploadOpts = origOpts
		createUploadSession = origCreate
		completeUploadSession = origComplete
		newBlobUploader = origNewUploader
	}()

	debugZipUploadOpts.uploadServerAPIKey = "key"
	debugZipUploadOpts.uploadServerURL = "https://fake.example.com"

	createUploadSession = func(
		_ context.Context, _, _ string,
	) (string, string, string, string, string, error) {
		return "ses", "tok", "b", "p", "gcs-tok", nil
	}

	newBlobUploader = func(
		_ context.Context, _, _, _, _ string,
	) (blobUploader, error) {
		return &mockBlobUploader{}, nil
	}

	completeUploadSession = func(
		_ context.Context, _, _, _ string,
	) error {
		return errors.New("server returned 500")
	}

	// Upload should succeed despite completion failure.
	err := uploadDebugZipViaServer(context.Background(), zipPath)
	require.NoError(t, err)
}

// TestRunDebugZipUploadRouting verifies that runDebugZipUpload routes
// to the correct handler based on the destination flag.
func TestRunDebugZipUploadRouting(t *testing.T) {
	defer leaktest.AfterTest(t)()

	origOpts := debugZipUploadOpts
	defer func() { debugZipUploadOpts = origOpts }()

	debugZipUploadOpts.destination = "invalid-dest"

	err := runDebugZipUpload(nil, []string{"/tmp/fake.zip"})
	require.ErrorContains(t, err, "unsupported destination")
	require.ErrorContains(t, err, "invalid-dest")
}

// funcBlobUploader is a blobUploader backed by function values, used
// for tests that need custom upload behavior per call.
type funcBlobUploader struct {
	uploadFn func(ctx context.Context, srcPath string) (string, error)
	closeFn  func() error
}

func (f *funcBlobUploader) Upload(ctx context.Context, srcPath string) (string, error) {
	return f.uploadFn(ctx, srcPath)
}

func (f *funcBlobUploader) Close() error {
	return f.closeFn()
}
