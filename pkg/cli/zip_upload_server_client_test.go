// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

func TestUploadServerClientCreateSession(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "POST", r.Method)
		require.Equal(t, "/api/v1/sessions", r.URL.Path)
		require.Equal(t, "Bearer test-api-key", r.Header.Get("Authorization"))
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))

		var req createSessionRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		require.Equal(t, "cluster-123", req.ClusterID)
		require.Equal(t, "prod-east", req.ClusterName)
		require.Equal(t, 3, req.NodeCount)
		require.True(t, req.Redacted)

		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(createSessionResponse{
			SessionID:   "ses_abc",
			UploadToken: "tok_xyz",
			ExpiresAt:   "2026-02-21T12:00:00Z",
		}))
	}))
	defer srv.Close()

	client := newUploadServerClient(uploadServerClientConfig{
		ServerURL: srv.URL,
		APIKey:    "test-api-key",
	})

	ctx := context.Background()
	err := client.CreateSession(ctx, "cluster-123", "prod-east", 3, "v24.2.1", true, nil)
	require.NoError(t, err)
	require.Equal(t, "ses_abc", client.sessionID)
	require.Equal(t, "tok_xyz", client.uploadToken)
}

func TestGetSignedURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "POST", r.Method)
		require.Equal(t, "/api/v1/sessions/ses_abc/signed-url", r.URL.Path)
		require.Equal(t, "Bearer tok_xyz", r.Header.Get("Authorization"))
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))

		var req signedURLRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		require.Equal(t, "nodes/1/cpu.pprof", req.ArtifactPath)
		require.Equal(t, int32(1), req.NodeID)
		require.Equal(t, "profile", req.ArtifactType)
		require.Equal(t, "application/octet-stream", req.ContentType)
		require.Equal(t, "idem-123", req.IdempotencyKey)

		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(signedURLResponse{
			SignedURL: "https://storage.googleapis.com/bucket/obj?X-Goog-Signature=abc",
			ExpiresAt: "2026-02-21T12:15:00Z",
		}))
	}))
	defer srv.Close()

	client := newUploadServerClientWithToken(
		uploadServerClientConfig{ServerURL: srv.URL},
		"ses_abc", "tok_xyz",
	)

	ctx := context.Background()
	resp, err := client.GetSignedURL(
		ctx, "nodes/1/cpu.pprof", 1, artifactTypeProfile,
		"application/octet-stream", "idem-123",
	)
	require.NoError(t, err)
	require.Equal(t, "https://storage.googleapis.com/bucket/obj?X-Goog-Signature=abc", resp.SignedURL)
	require.Equal(t, "2026-02-21T12:15:00Z", resp.ExpiresAt)
}

func TestUploadToSignedURL(t *testing.T) {
	var receivedBody []byte
	gcsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "PUT", r.Method)
		require.Equal(t, "application/octet-stream", r.Header.Get("Content-Type"))
		// No Authorization header on GCS uploads.
		require.Empty(t, r.Header.Get("Authorization"))

		var err error
		receivedBody, err = io.ReadAll(r.Body)
		require.NoError(t, err)

		w.WriteHeader(http.StatusOK)
	}))
	defer gcsSrv.Close()

	client := newUploadServerClientWithToken(
		uploadServerClientConfig{ServerURL: "http://unused"},
		"ses_abc", "tok_xyz",
	)

	ctx := context.Background()
	data := []byte("fake-pprof-data")
	err := client.UploadToSignedURL(
		ctx, gcsSrv.URL+"/bucket/obj?sig=abc",
		"application/octet-stream",
		bytes.NewReader(data),
	)
	require.NoError(t, err)
	require.Equal(t, data, receivedBody)
}

func TestUploadServerClientUploadArtifact(t *testing.T) {
	// Mock GCS server that accepts the PUT.
	var receivedBody []byte
	gcsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "PUT", r.Method)
		require.Equal(t, "application/octet-stream", r.Header.Get("Content-Type"))
		require.Empty(t, r.Header.Get("Authorization"))

		var err error
		receivedBody, err = io.ReadAll(r.Body)
		require.NoError(t, err)
		w.WriteHeader(http.StatusOK)
	}))
	defer gcsSrv.Close()

	// Mock upload server that returns a signed URL pointing to gcsSrv.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "POST", r.Method)
		require.Equal(t, "/api/v1/sessions/ses_abc/signed-url", r.URL.Path)
		require.Equal(t, "Bearer tok_xyz", r.Header.Get("Authorization"))

		var req signedURLRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		require.Equal(t, "nodes/1/cpu.pprof", req.ArtifactPath)
		require.Equal(t, "idem-123", req.IdempotencyKey)

		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(signedURLResponse{
			SignedURL: gcsSrv.URL + "/bucket/nodes/1/cpu.pprof?sig=abc",
			ExpiresAt: "2026-02-21T12:15:00Z",
		}))
	}))
	defer srv.Close()

	client := newUploadServerClientWithToken(
		uploadServerClientConfig{ServerURL: srv.URL},
		"ses_abc", "tok_xyz",
	)

	ctx := context.Background()
	data := []byte("fake-pprof-data")
	err := client.UploadArtifact(
		ctx, "nodes/1/cpu.pprof", 1, artifactTypeProfile,
		"application/octet-stream", "idem-123", data,
	)
	require.NoError(t, err)
	require.Equal(t, data, receivedBody)
}

func TestUploadArtifactRetryOnExpiredURL(t *testing.T) {
	// Track GCS call count. First call returns 403, second succeeds.
	var gcsCallCount atomic.Int32
	gcsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := gcsCallCount.Add(1)
		if n == 1 {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte("AccessDenied: Request has expired"))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer gcsSrv.Close()

	// Track signed URL request count.
	var signedURLCallCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		signedURLCallCount.Add(1)
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(signedURLResponse{
			SignedURL: gcsSrv.URL + "/bucket/obj?sig=fresh",
			ExpiresAt: "2026-02-21T12:30:00Z",
		}))
	}))
	defer srv.Close()

	client := newUploadServerClientWithToken(
		uploadServerClientConfig{ServerURL: srv.URL},
		"ses_abc", "tok_xyz",
	)

	ctx := context.Background()
	err := client.UploadArtifact(
		ctx, "nodes/1/cpu.pprof", 1, artifactTypeProfile,
		"application/octet-stream", "", []byte("data"),
	)
	require.NoError(t, err)
	// Should have requested two signed URLs (original + retry).
	require.Equal(t, int32(2), signedURLCallCount.Load())
	// GCS should have been called twice.
	require.Equal(t, int32(2), gcsCallCount.Load())
}

func TestUploadArtifactPermanentFailure(t *testing.T) {
	// GCS always returns 500.
	gcsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	}))
	defer gcsSrv.Close()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(signedURLResponse{
			SignedURL: gcsSrv.URL + "/bucket/obj?sig=abc",
			ExpiresAt: "2026-02-21T12:15:00Z",
		}))
	}))
	defer srv.Close()

	client := newUploadServerClientWithToken(
		uploadServerClientConfig{ServerURL: srv.URL},
		"ses_abc", "tok_xyz",
	)

	ctx := context.Background()
	err := client.UploadArtifact(
		ctx, "nodes/1/cpu.pprof", 1, artifactTypeProfile,
		"application/octet-stream", "", []byte("data"),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "HTTP 500")
}

func TestUploadServerClientCompleteSession(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "POST", r.Method)
		require.Equal(t, "/api/v1/sessions/ses_abc/complete", r.URL.Path)
		require.Equal(t, "Bearer tok_xyz", r.Header.Get("Authorization"))

		var req completeSessionRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		require.Equal(t, "success", req.Status)
		require.Equal(t, 10, req.ArtifactsUploaded)
		require.Equal(t, []int32{1, 2, 3}, req.NodesCompleted)

		w.WriteHeader(http.StatusOK)
		require.NoError(t, json.NewEncoder(w).Encode(completeSessionResponse{
			CloudPath:           "gs://debug-uploads/ses_abc",
			ParquetFilesCreated: 5,
		}))
	}))
	defer srv.Close()

	client := newUploadServerClientWithToken(
		uploadServerClientConfig{ServerURL: srv.URL},
		"ses_abc", "tok_xyz",
	)

	ctx := context.Background()
	err := client.CompleteSession(ctx, 10, []int32{1, 2, 3})
	require.NoError(t, err)
}

func TestUploadServerClientGetSessionStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "GET", r.Method)
		require.Equal(t, "/api/v1/sessions/ses_abc/status", r.URL.Path)

		w.WriteHeader(http.StatusOK)
		require.NoError(t, json.NewEncoder(w).Encode(sessionStatusResponse{
			State:             "in_progress",
			ArtifactsReceived: 15,
			ArtifactsByNode:   map[string]int{"1": 5, "2": 5, "3": 5},
		}))
	}))
	defer srv.Close()

	client := newUploadServerClientWithToken(
		uploadServerClientConfig{ServerURL: srv.URL},
		"ses_abc", "tok_xyz",
	)

	ctx := context.Background()
	status, err := client.GetSessionStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, "in_progress", status.State)
	require.Equal(t, 15, status.ArtifactsReceived)
	require.Len(t, status.ArtifactsByNode, 3)
}

func TestUploadArtifactStreaming(t *testing.T) {
	// Verify that the factory is called once on success and the body
	// is streamed correctly to the GCS mock.
	var receivedBody []byte
	gcsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "PUT", r.Method)
		var err error
		receivedBody, err = io.ReadAll(r.Body)
		require.NoError(t, err)
		w.WriteHeader(http.StatusOK)
	}))
	defer gcsSrv.Close()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(signedURLResponse{
			SignedURL: gcsSrv.URL + "/bucket/obj?sig=abc",
			ExpiresAt: "2026-02-21T12:15:00Z",
		}))
	}))
	defer srv.Close()

	client := newUploadServerClientWithToken(
		uploadServerClientConfig{ServerURL: srv.URL},
		"ses_abc", "tok_xyz",
	)

	var factoryCalls atomic.Int32
	ctx := context.Background()
	err := client.UploadArtifactStreaming(
		ctx, "nodes/1/log.txt", 1, artifactTypeLog,
		"text/plain", "",
		func() (io.ReadCloser, error) {
			factoryCalls.Add(1)
			return io.NopCloser(bytes.NewReader([]byte("streamed-data"))), nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, int32(1), factoryCalls.Load())
	require.Equal(t, []byte("streamed-data"), receivedBody)
}

func TestUploadArtifactStreamingRetry(t *testing.T) {
	// First GCS call returns 403 (expired URL), second succeeds.
	// The factory should be called twice.
	var gcsCallCount atomic.Int32
	gcsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := gcsCallCount.Add(1)
		if n == 1 {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte("AccessDenied: Request has expired"))
			return
		}
		body, _ := io.ReadAll(r.Body)
		require.Equal(t, []byte("retry-data"), body)
		w.WriteHeader(http.StatusOK)
	}))
	defer gcsSrv.Close()

	var signedURLCallCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		signedURLCallCount.Add(1)
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(signedURLResponse{
			SignedURL: gcsSrv.URL + "/bucket/obj?sig=fresh",
			ExpiresAt: "2026-02-21T12:30:00Z",
		}))
	}))
	defer srv.Close()

	client := newUploadServerClientWithToken(
		uploadServerClientConfig{ServerURL: srv.URL},
		"ses_abc", "tok_xyz",
	)

	var factoryCalls atomic.Int32
	ctx := context.Background()
	err := client.UploadArtifactStreaming(
		ctx, "nodes/1/log.txt", 1, artifactTypeLog,
		"text/plain", "",
		func() (io.ReadCloser, error) {
			factoryCalls.Add(1)
			return io.NopCloser(bytes.NewReader([]byte("retry-data"))), nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, int32(2), factoryCalls.Load())
	require.Equal(t, int32(2), signedURLCallCount.Load())
	require.Equal(t, int32(2), gcsCallCount.Load())
}

func TestUploadArtifactStreamingFactoryError(t *testing.T) {
	// Factory returns an error — upload should fail with a wrapped error.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(signedURLResponse{
			SignedURL: "https://storage.googleapis.com/bucket/obj?sig=abc",
			ExpiresAt: "2026-02-21T12:15:00Z",
		}))
	}))
	defer srv.Close()

	client := newUploadServerClientWithToken(
		uploadServerClientConfig{ServerURL: srv.URL},
		"ses_abc", "tok_xyz",
	)

	ctx := context.Background()
	testErr := errors.New("cannot open source")
	err := client.UploadArtifactStreaming(
		ctx, "nodes/1/log.txt", 1, artifactTypeLog,
		"text/plain", "",
		func() (io.ReadCloser, error) {
			return nil, testErr
		},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "creating body reader")
	require.True(t, errors.Is(err, testErr))
}

func TestUploadServerClientHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal server error"))
	}))
	defer srv.Close()

	client := newUploadServerClient(uploadServerClientConfig{
		ServerURL: srv.URL,
		APIKey:    "test-key",
	})

	ctx := context.Background()
	err := client.CreateSession(ctx, "c1", "test", 1, "v1", false, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "HTTP 500")
}

func TestGetUploadToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "POST", r.Method)
		require.Equal(t, "/api/v1/sessions/ses_abc/upload-token", r.URL.Path)
		require.Equal(t, "Bearer tok_xyz", r.Header.Get("Authorization"))

		w.WriteHeader(http.StatusOK)
		require.NoError(t, json.NewEncoder(w).Encode(uploadTokenResponse{
			AccessToken: "gcs-access-token-123",
			Bucket:      "debug-uploads",
			Prefix:      "sessions/ses_abc/",
			ExpiresAt:   "2026-02-21T23:59:59Z",
		}))
	}))
	defer srv.Close()

	client := newUploadServerClientWithToken(
		uploadServerClientConfig{ServerURL: srv.URL},
		"ses_abc", "tok_xyz",
	)

	ctx := context.Background()
	resp, err := client.GetUploadToken(ctx)
	require.NoError(t, err)
	require.Equal(t, "gcs-access-token-123", resp.AccessToken)
	require.Equal(t, "debug-uploads", resp.Bucket)
	require.Equal(t, "sessions/ses_abc/", resp.Prefix)
	require.Equal(t, "2026-02-21T23:59:59Z", resp.ExpiresAt)
}

func TestGetUploadTokenNotFound(t *testing.T) {
	// Upload server returns 404 — doesn't support the token endpoint.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("not found"))
	}))
	defer srv.Close()

	client := newUploadServerClientWithToken(
		uploadServerClientConfig{ServerURL: srv.URL},
		"ses_abc", "tok_xyz",
	)

	ctx := context.Background()
	_, err := client.GetUploadToken(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "HTTP 404")
}

func TestInitGCSClientFallback(t *testing.T) {
	// Upload server returns 404 for upload-token — InitGCSClient
	// should succeed (no error) but leave gcsClient nil.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/sessions/ses_abc/upload-token" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("not found"))
			return
		}
		// Signed URL endpoint (should still work).
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(signedURLResponse{
			SignedURL: "https://storage.googleapis.com/bucket/obj?sig=abc",
			ExpiresAt: "2026-02-21T12:15:00Z",
		}))
	}))
	defer srv.Close()

	client := newUploadServerClientWithToken(
		uploadServerClientConfig{ServerURL: srv.URL},
		"ses_abc", "tok_xyz",
	)
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	err := client.InitGCSClient(ctx)
	require.NoError(t, err)
	// gcsClient should remain nil — signed URL path will be used.
	require.Nil(t, client.gcsClient)
}

func TestUploadArtifactStreamingGCSRouting(t *testing.T) {
	// When gcsClient is non-nil, UploadArtifactStreaming should NOT
	// call the signed URL endpoint. We verify this by checking that
	// the upload server never receives a signed-url request.
	var signedURLCalled atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "signed-url") {
			signedURLCalled.Add(1)
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	client := newUploadServerClientWithToken(
		uploadServerClientConfig{ServerURL: srv.URL},
		"ses_abc", "tok_xyz",
	)
	// Set a non-nil gcsClient to trigger the GCS path. We use a real
	// storage.Client that points to a non-existent endpoint — the
	// upload will fail, but we're testing the routing, not the GCS
	// upload itself.
	ctx := context.Background()
	gcsClient, err := storage.NewClient(ctx, option.WithoutAuthentication())
	require.NoError(t, err)
	defer func() { _ = gcsClient.Close() }()

	client.gcsClient = gcsClient
	client.gcsBucket = "test-bucket"
	client.gcsPrefix = "sessions/ses_abc/"

	// The upload will fail (no real GCS server) but should NOT
	// have tried the signed URL path.
	_ = client.UploadArtifactStreaming(
		ctx, "nodes/1/cpu.pprof", 1, artifactTypeProfile,
		"application/octet-stream", "",
		func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader([]byte("test-data"))), nil
		},
	)
	require.Equal(t, int32(0), signedURLCalled.Load(),
		"signed URL endpoint should not be called when GCS client is available")
}

func TestUploadArtifactStreamingSignedURLFallback(t *testing.T) {
	// When gcsClient is nil, UploadArtifactStreaming should use
	// the signed URL path.
	var receivedBody []byte
	gcsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "PUT", r.Method)
		var err error
		receivedBody, err = io.ReadAll(r.Body)
		require.NoError(t, err)
		w.WriteHeader(http.StatusOK)
	}))
	defer gcsSrv.Close()

	var signedURLCalled atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		signedURLCalled.Add(1)
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(signedURLResponse{
			SignedURL: gcsSrv.URL + "/bucket/obj?sig=abc",
			ExpiresAt: "2026-02-21T12:15:00Z",
		}))
	}))
	defer srv.Close()

	client := newUploadServerClientWithToken(
		uploadServerClientConfig{ServerURL: srv.URL},
		"ses_abc", "tok_xyz",
	)
	// gcsClient is nil — should use signed URL path.

	ctx := context.Background()
	err := client.UploadArtifactStreaming(
		ctx, "nodes/1/cpu.pprof", 1, artifactTypeProfile,
		"application/octet-stream", "",
		func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader([]byte("fallback-data"))), nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, int32(1), signedURLCalled.Load())
	require.Equal(t, []byte("fallback-data"), receivedBody)
}

func TestCloseNilGCSClient(t *testing.T) {
	client := newUploadServerClientWithToken(
		uploadServerClientConfig{ServerURL: "http://unused"},
		"ses_abc", "tok_xyz",
	)
	// Close on a client with no GCS client should be a no-op.
	err := client.Close()
	require.NoError(t, err)
}
