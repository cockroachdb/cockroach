// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestValidateCRLSupportReadiness(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tmpDir := t.TempDir()
	zipPath := filepath.Join(tmpDir, "debug.zip")
	require.NoError(t, os.WriteFile(zipPath, []byte("PK\x03\x04fake-zip-content"), 0644))

	notAZipPath := filepath.Join(tmpDir, "notazip.zip")
	require.NoError(t, os.WriteFile(notAZipPath, []byte("not-a-zip-file"), 0644))

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
				debugZipUploadOpts.crlSupportAPIKey = "test-key"
				debugZipUploadOpts.crlSupportURL = "https://example.com"
			},
		},
		{
			name:  "not a zip file",
			input: dirPath,
			setup: func() {
				debugZipUploadOpts.crlSupportAPIKey = "key"
				debugZipUploadOpts.crlSupportURL = "https://example.com"
			},
			wantErr: "crl-support requires a .zip file",
		},
		{
			name:  "invalid zip magic bytes",
			input: notAZipPath,
			setup: func() {
				debugZipUploadOpts.crlSupportAPIKey = "key"
				debugZipUploadOpts.crlSupportURL = "https://example.com"
			},
			wantErr: "does not appear to be a valid zip file",
		},
		{
			name:  "empty zip file",
			input: emptyZipPath,
			setup: func() {
				debugZipUploadOpts.crlSupportAPIKey = "key"
				debugZipUploadOpts.crlSupportURL = "https://example.com"
			},
			wantErr: "is empty",
		},
		{
			name:  "missing api key",
			input: zipPath,
			setup: func() {
				debugZipUploadOpts.crlSupportAPIKey = ""
				debugZipUploadOpts.crlSupportURL = "https://example.com"
			},
			wantErr: "--crl-support-api-key is required",
		},
		{
			name:  "missing crl support url",
			input: zipPath,
			setup: func() {
				debugZipUploadOpts.crlSupportAPIKey = "key"
				debugZipUploadOpts.crlSupportURL = ""
			},
			wantErr: "--crl-support-url is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			debugZipUploadOpts = origOpts
			tt.setup()
			err := validateCRLSupportReadiness(tt.input)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestUploadDebugZipToCRLSupportSessionFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	zipPath := setupUploadTest(t, []byte("fake-zip"), "bad-key", "https://fake.example.com")

	createUploadSession = func(
		_ context.Context, _, _, _, _ string, _ *http.Client,
	) (uploadSessionInfo, error) {
		return uploadSessionInfo{}, errors.New("401 Unauthorized: invalid API key")
	}

	err := uploadDebugZipToCRLSupport(context.Background(), zipPath)
	require.ErrorContains(t, err, "invalid API key")
}

func TestUploadDebugZipToCRLSupportSuccess(t *testing.T) {
	defer leaktest.AfterTest(t)()

	zipContent := []byte("PK-fake-zip-content")
	zipPath := setupUploadTest(t, zipContent, "test-key", "https://fake.example.com")

	mock := &mockBlobUploader{}
	var (
		completeCalled    bool
		completeSessionID string
		completeToken     string
		sawResume         string
	)

	createUploadSession = func(
		_ context.Context, serverURL, apiKey, ticketID, resumeSessionID string, _ *http.Client,
	) (uploadSessionInfo, error) {
		require.Equal(t, "https://fake.example.com", serverURL)
		require.Equal(t, "test-key", apiKey)
		_ = ticketID
		sawResume = resumeSessionID
		return uploadSessionInfo{
			SessionID:   "ses_test",
			UploadToken: "tok_test",
			SessionURI:  "https://storage.googleapis.test/upload?id=fake",
			Bucket:      "bucket",
			ObjectPath:  "ses_test/debug.zip",
			ContentType: "application/zip",
		}, nil
	}

	newBlobUploader = func(
		_ context.Context, sessionURI, bucket, objectPath, contentType string,
		_ *http.Client, resuming bool,
	) (blobUploader, error) {
		require.Equal(t, "https://storage.googleapis.test/upload?id=fake", sessionURI)
		require.Equal(t, "bucket", bucket)
		require.Equal(t, "ses_test/debug.zip", objectPath)
		require.Equal(t, "application/zip", contentType)
		require.False(t, resuming, "fresh upload should not be flagged as resuming")
		return mock, nil
	}

	completeUploadSession = func(
		_ context.Context, serverURL, sessionID, uploadToken string, _ *http.Client,
	) error {
		completeCalled = true
		completeSessionID = sessionID
		completeToken = uploadToken
		return nil
	}

	err := uploadDebugZipToCRLSupport(context.Background(), zipPath)
	require.NoError(t, err)

	require.Equal(t, "", sawResume, "fresh upload should not pass a resume session")
	require.Equal(t, zipPath, mock.uploadedPath)
	require.Equal(t, zipContent, mock.content)
	require.True(t, completeCalled)
	require.Equal(t, "ses_test", completeSessionID)
	require.Equal(t, "tok_test", completeToken)
}

func TestUploadDebugZipToCRLSupportResume(t *testing.T) {
	defer leaktest.AfterTest(t)()

	zipPath := setupUploadTest(t, []byte("PK-data"), "key", "https://fake.example.com")
	debugZipUploadOpts.resumeSession = "ses_existing"

	var sawResume string
	createUploadSession = func(
		_ context.Context, _, _, _, resumeSessionID string, _ *http.Client,
	) (uploadSessionInfo, error) {
		sawResume = resumeSessionID
		return uploadSessionInfo{
			SessionID:   "ses_existing",
			UploadToken: "tok_new",
			SessionURI:  "https://storage.googleapis.test/upload?id=cached",
			Bucket:      "b",
			ObjectPath:  "ses_existing/debug.zip",
			ContentType: "application/zip",
		}, nil
	}
	var sawResuming bool
	newBlobUploader = func(
		_ context.Context, _, _, _, _ string, _ *http.Client, resuming bool,
	) (blobUploader, error) {
		sawResuming = resuming
		return &mockBlobUploader{}, nil
	}
	completeUploadSession = func(_ context.Context, _, _, _ string, _ *http.Client) error {
		return nil
	}

	require.NoError(t, uploadDebugZipToCRLSupport(context.Background(), zipPath))
	require.Equal(t, "ses_existing", sawResume)
	require.True(t, sawResuming, "resume flow should propagate resuming=true to uploader")
}

func TestUploadDebugZipToCRLSupportRetryThenSuccess(t *testing.T) {
	defer leaktest.AfterTest(t)()

	zipPath := setupUploadTest(t, []byte("PK-data"), "key", "https://fake.example.com")

	createUploadSession = func(
		_ context.Context, _, _, _, _ string, _ *http.Client,
	) (uploadSessionInfo, error) {
		return uploadSessionInfo{
			SessionID: "ses", UploadToken: "tok",
			SessionURI: "https://storage.googleapis.test/upload?id=x",
			Bucket:     "b", ObjectPath: "ses/debug.zip",
			ContentType: "application/zip",
		}, nil
	}

	failsRemaining := 2
	callCount := 0
	wrapperMock := &funcBlobUploader{
		uploadFn: func(_ context.Context, srcPath string) (string, int64, error) {
			callCount++
			if callCount <= failsRemaining {
				return "", 0, errors.New("transient network error")
			}
			return "gs://b/ses/" + filepath.Base(srcPath), 7, nil
		},
		closeFn: func() error { return nil },
	}
	newBlobUploader = func(
		_ context.Context, _, _, _, _ string, _ *http.Client, _ bool,
	) (blobUploader, error) {
		return wrapperMock, nil
	}
	completeUploadSession = func(_ context.Context, _, _, _ string, _ *http.Client) error {
		return nil
	}

	require.NoError(t, uploadDebugZipToCRLSupport(context.Background(), zipPath))
	require.Equal(t, 3, callCount)
}

func TestUploadDebugZipToCRLSupportCompletionFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	zipPath := setupUploadTest(t, []byte("PK-data"), "key", "https://fake.example.com")

	createUploadSession = func(
		_ context.Context, _, _, _, _ string, _ *http.Client,
	) (uploadSessionInfo, error) {
		return uploadSessionInfo{
			SessionID: "ses", UploadToken: "tok",
			SessionURI: "https://storage.googleapis.test/upload?id=x",
			Bucket:     "b", ObjectPath: "ses/debug.zip",
			ContentType: "application/zip",
		}, nil
	}
	newBlobUploader = func(
		_ context.Context, _, _, _, _ string, _ *http.Client, _ bool,
	) (blobUploader, error) {
		return &mockBlobUploader{}, nil
	}
	completeUploadSession = func(_ context.Context, _, _, _ string, _ *http.Client) error {
		return errors.New("server returned 500")
	}

	err := uploadDebugZipToCRLSupport(context.Background(), zipPath)
	require.Error(t, err)
	require.ErrorContains(t, err, "session finalisation failed")
	require.ErrorContains(t, err, "--resume-session=ses")
	require.ErrorContains(t, err, "server returned 500")
}

// TestRunDebugZipUploadRouting verifies that runDebugZipUpload rejects
// an unknown destination value before doing any work.
func TestRunDebugZipUploadRouting(t *testing.T) {
	defer leaktest.AfterTest(t)()

	origOpts := debugZipUploadOpts
	defer func() { debugZipUploadOpts = origOpts }()

	debugZipUploadOpts.destination = "invalid-dest"

	// nil cmd is safe: the default branch returns before touching it.
	err := runDebugZipUpload(nil, []string{"/tmp/fake.zip"})
	require.ErrorContains(t, err, "unsupported destination")
	require.ErrorContains(t, err, "invalid-dest")
}

func TestNewUploadHTTPClientProxy(t *testing.T) {
	defer leaktest.AfterTest(t)()

	origOpts := debugZipUploadOpts
	defer func() { debugZipUploadOpts = origOpts }()

	t.Run("invalid proxy URL fails fast", func(t *testing.T) {
		debugZipUploadOpts.proxy = "://not-a-url"
		_, err := newUploadHTTPClient(crlSupportTimeout)
		require.Error(t, err)
	})

	t.Run("valid proxy URL is set on transport", func(t *testing.T) {
		debugZipUploadOpts.proxy = "http://proxy.example.com:3128"
		client, err := newUploadHTTPClient(crlSupportTimeout)
		require.NoError(t, err)

		transport, ok := client.Transport.(*http.Transport)
		require.True(t, ok)
		require.NotNil(t, transport.Proxy)

		req, err := http.NewRequest(http.MethodGet, "https://upload.example/", nil)
		require.NoError(t, err)
		proxyURL, err := transport.Proxy(req)
		require.NoError(t, err)
		require.NotNil(t, proxyURL)
		require.Equal(t, "proxy.example.com:3128", proxyURL.Host)
	})
}

// TestUploadDebugZipToCRLSupportRetryExhausted covers the path where
// every retry fails: the final error must wrap the last attempt's error
// and surface the resume hint.
func TestUploadDebugZipToCRLSupportRetryExhausted(t *testing.T) {
	defer leaktest.AfterTest(t)()

	zipPath := setupUploadTest(t, []byte("PK-data"), "key", "https://fake.example.com")

	createUploadSession = func(
		_ context.Context, _, _, _, _ string, _ *http.Client,
	) (uploadSessionInfo, error) {
		return uploadSessionInfo{
			SessionID: "ses_xyz", UploadToken: "tok",
			SessionURI: "https://x", Bucket: "b", ObjectPath: "o",
			ContentType: "application/zip",
		}, nil
	}
	attempts := 0
	newBlobUploader = func(
		_ context.Context, _, _, _, _ string, _ *http.Client, _ bool,
	) (blobUploader, error) {
		return &funcBlobUploader{
			uploadFn: func(_ context.Context, _ string) (string, int64, error) {
				attempts++
				return "", 0, errors.New("transient flake")
			},
			closeFn: func() error { return nil },
		}, nil
	}
	completeUploadSession = func(_ context.Context, _, _, _ string, _ *http.Client) error {
		return nil
	}

	err := uploadDebugZipToCRLSupport(context.Background(), zipPath)
	require.Error(t, err)
	require.ErrorContains(t, err, "upload failed after retries")
	require.ErrorContains(t, err, "--resume-session=ses_xyz")
	require.ErrorContains(t, err, "transient flake")
	require.Equal(t, uploadMaxRetries+1, attempts, "expected MaxRetries+1 attempts before giving up")
}

// TestUploadDebugZipToCRLSupportDeadSessionDoesNotRetry verifies that
// errSessionURIDead short-circuits the retry loop instead of burning
// attempts on a URI that can't recover.
func TestUploadDebugZipToCRLSupportDeadSessionDoesNotRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()

	zipPath := setupUploadTest(t, []byte("PK-data"), "key", "https://fake.example.com")

	createUploadSession = func(
		_ context.Context, _, _, _, _ string, _ *http.Client,
	) (uploadSessionInfo, error) {
		return uploadSessionInfo{
			SessionID: "ses", UploadToken: "tok",
			SessionURI: "https://x", Bucket: "b", ObjectPath: "o",
			ContentType: "application/zip",
		}, nil
	}
	attempts := 0
	newBlobUploader = func(
		_ context.Context, _, _, _, _ string, _ *http.Client, _ bool,
	) (blobUploader, error) {
		return &funcBlobUploader{
			uploadFn: func(_ context.Context, _ string) (string, int64, error) {
				attempts++
				return "", 0, errSessionURIDead
			},
			closeFn: func() error { return nil },
		}, nil
	}
	completeUploadSession = func(_ context.Context, _, _, _ string, _ *http.Client) error {
		return nil
	}

	err := uploadDebugZipToCRLSupport(context.Background(), zipPath)
	require.ErrorIs(t, err, errSessionURIDead)
	require.Equal(t, 1, attempts, "dead session URI should not be retried")
}

// TestNewUploadHTTPClientHTTPSProxyEnvVar verifies the env-var fallback
// and the precedence rule (--proxy wins over $HTTPS_PROXY).
func TestNewUploadHTTPClientHTTPSProxyEnvVar(t *testing.T) {
	defer leaktest.AfterTest(t)()

	origOpts := debugZipUploadOpts
	defer func() { debugZipUploadOpts = origOpts }()

	proxyForReq := func(c *http.Client) string {
		t.Helper()
		transport, ok := c.Transport.(*http.Transport)
		require.True(t, ok)
		require.NotNil(t, transport.Proxy)
		req, err := http.NewRequest(http.MethodGet, "https://upload.example/", nil)
		require.NoError(t, err)
		u, err := transport.Proxy(req)
		require.NoError(t, err)
		if u == nil {
			return ""
		}
		return u.Host
	}

	t.Run("env var honoured when --proxy unset", func(t *testing.T) {
		debugZipUploadOpts.proxy = ""
		t.Setenv("HTTPS_PROXY", "http://env-proxy.example:8080")
		c, err := newUploadHTTPClient(crlSupportTimeout)
		require.NoError(t, err)
		require.Equal(t, "env-proxy.example:8080", proxyForReq(c))
	})

	t.Run("--proxy wins over env var", func(t *testing.T) {
		debugZipUploadOpts.proxy = "http://flag-proxy.example:1234"
		t.Setenv("HTTPS_PROXY", "http://env-proxy.example:5678")
		c, err := newUploadHTTPClient(crlSupportTimeout)
		require.NoError(t, err)
		require.Equal(t, "flag-proxy.example:1234", proxyForReq(c))
	})
}

// TestCreateSessionWireFormat exercises createSession against a
// real HTTP server, asserting URL, method, headers, request body
// (mode + labels.ticket_id), and JSON unmarshaling of the response.
func TestCreateSessionWireFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var (
		gotMethod, gotPath, gotAuth, gotCT string
		gotBody                            map[string]any
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotCT = r.Header.Get("Content-Type")
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"session_id":"ses_x","upload_token":"tok_y"}`))
	}))
	defer srv.Close()

	id, tok, err := createSession(
		context.Background(), srv.URL, "the-api-key", "TICKET-123", srv.Client(),
	)
	require.NoError(t, err)
	require.Equal(t, "ses_x", id)
	require.Equal(t, "tok_y", tok)
	require.Equal(t, http.MethodPost, gotMethod)
	require.Equal(t, "/api/v1/sessions", gotPath)
	require.Equal(t, "Bearer the-api-key", gotAuth)
	require.Equal(t, "application/json", gotCT)
	require.Equal(t, "direct", gotBody["mode"])
	labels, ok := gotBody["labels"].(map[string]any)
	require.True(t, ok, "labels should be a JSON object, got %T", gotBody["labels"])
	require.Equal(t, "TICKET-123", labels["ticket_id"])
}

// TestCreateSessionOmitsLabelsWhenNoTicketID verifies that the labels
// field is left out of the request body entirely when no ticket-id is
// supplied.
func TestCreateSessionOmitsLabelsWhenNoTicketID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"session_id":"s","upload_token":"t"}`))
	}))
	defer srv.Close()

	_, _, err := createSession(context.Background(), srv.URL, "key", "", srv.Client())
	require.NoError(t, err)
	require.Equal(t, "direct", gotBody["mode"])
	_, hasLabels := gotBody["labels"]
	require.False(t, hasLabels, "labels must be omitted when no ticket-id is set, got %v", gotBody["labels"])
}

// TestCreateSessionNonCreatedReturnsError verifies that any non-201
// response surfaces as a wrapped error containing the status and body.
func TestCreateSessionNonCreatedReturnsError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte("invalid api key"))
	}))
	defer srv.Close()

	_, _, err := createSession(context.Background(), srv.URL, "bad", "", srv.Client())
	require.ErrorContains(t, err, "session creation failed (401)")
	require.ErrorContains(t, err, "invalid api key")
}

// TestReopenSessionUsesAPIKeyAuth pins down the contract that
// /reupload authenticates with the cluster API key, not the upload
// token (the whole point of this endpoint).
func TestReopenSessionUsesAPIKeyAuth(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var (
		gotMethod, gotPath, gotAuth string
		gotBody                     map[string]string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"session_id":"ses_old","upload_token":"tok_new"}`))
	}))
	defer srv.Close()

	id, tok, err := reopenSession(context.Background(), srv.URL, "ses_old", "the-api-key", srv.Client())
	require.NoError(t, err)
	require.Equal(t, "ses_old", id)
	require.Equal(t, "tok_new", tok)
	require.Equal(t, http.MethodPost, gotMethod)
	require.Equal(t, "/api/v1/sessions/ses_old/reupload", gotPath)
	require.Equal(t, "Bearer the-api-key", gotAuth)
	require.NotEmpty(t, gotBody["reason"])
}

// TestCreateResumableSessionAcceptsBoth200And201 verifies that both
// "freshly minted" (201) and "cached reuse" (200) are treated as
// success. Also pins down that the request body carries only
// content_type — object_name is not sent because the server picks it.
func TestCreateResumableSessionAcceptsBoth200And201(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		name   string
		status int
		reused bool
	}{
		{name: "freshly minted", status: http.StatusCreated, reused: false},
		{name: "cached reuse", status: http.StatusOK, reused: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var (
				gotAuth string
				gotBody map[string]any
			)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotAuth = r.Header.Get("Authorization")
				require.Equal(t, "/api/v1/sessions/ses_z/resumable-session", r.URL.Path)
				require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
				w.WriteHeader(tc.status)
				body := map[string]any{
					"gcs_session_uri": "https://gcs.example/upload?id=fake",
					"bucket":          "b",
					"object_path":     "ses_z/debug.zip",
					"content_type":    "application/zip",
					"reused":          tc.reused,
				}
				require.NoError(t, json.NewEncoder(w).Encode(body))
			}))
			defer srv.Close()

			uri, bucket, op, ct, err := createResumableSession(
				context.Background(), srv.URL, "ses_z", "tok",
				"application/zip", srv.Client(),
			)
			require.NoError(t, err)
			require.Equal(t, "https://gcs.example/upload?id=fake", uri)
			require.Equal(t, "b", bucket)
			require.Equal(t, "ses_z/debug.zip", op)
			require.Equal(t, "application/zip", ct)
			require.Equal(t, "Bearer tok", gotAuth, "must use upload token, not API key")
			require.Equal(t, "application/zip", gotBody["content_type"])
			_, hasObjectName := gotBody["object_name"]
			require.False(t, hasObjectName, "client must not send object_name; server controls naming")
		})
	}
}

// TestCompleteUploadSessionWireFormat pins down the /complete
// contract: POST with status="success" and artifacts_uploaded=1.
func TestCompleteUploadSessionWireFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var (
		gotMethod, gotPath, gotAuth, gotCT string
		gotBody                            map[string]any
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotCT = r.Header.Get("Content-Type")
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	require.NoError(t, completeUploadSession(
		context.Background(), srv.URL, "ses_a", "tok_b", srv.Client(),
	))
	require.Equal(t, http.MethodPost, gotMethod)
	require.Equal(t, "/api/v1/sessions/ses_a/complete", gotPath)
	require.Equal(t, "Bearer tok_b", gotAuth)
	require.Equal(t, "application/json", gotCT)
	require.Equal(t, "success", gotBody["status"])
	require.EqualValues(t, 1, gotBody["artifacts_uploaded"])
}

// setupUploadTest writes a temp zip file and restores all upload
// globals after the test so cases can mutate them in isolation.
func setupUploadTest(t *testing.T, zipContent []byte, apiKey, serverURL string) string {
	t.Helper()
	tmpDir := t.TempDir()
	zipPath := filepath.Join(tmpDir, "debug.zip")
	require.NoError(t, os.WriteFile(zipPath, zipContent, 0644))

	origOpts := debugZipUploadOpts
	origCreate := createUploadSession
	origComplete := completeUploadSession
	origNewUploader := newBlobUploader
	t.Cleanup(func() {
		debugZipUploadOpts = origOpts
		createUploadSession = origCreate
		completeUploadSession = origComplete
		newBlobUploader = origNewUploader
	})

	debugZipUploadOpts.crlSupportAPIKey = apiKey
	debugZipUploadOpts.crlSupportURL = serverURL
	debugZipUploadOpts.resumeSession = ""
	return zipPath
}
