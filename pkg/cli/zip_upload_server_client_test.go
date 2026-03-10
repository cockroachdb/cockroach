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
	"testing"

	"github.com/stretchr/testify/require"
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

func TestUploadServerClientUploadArtifact(t *testing.T) {
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "PUT", r.Method)
		require.Equal(t, "/api/v1/sessions/ses_abc/artifacts/nodes/1/cpu.pprof", r.URL.Path)
		require.Equal(t, "Bearer tok_xyz", r.Header.Get("Authorization"))
		require.Equal(t, "application/octet-stream", r.Header.Get("Content-Type"))
		require.Equal(t, "1", r.Header.Get("X-Node-ID"))
		require.Equal(t, "profile", r.Header.Get("X-Artifact-Type"))
		require.Equal(t, "idem-123", r.Header.Get("X-Idempotency-Key"))

		var err error
		receivedBody, err = io.ReadAll(r.Body)
		require.NoError(t, err)

		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(uploadArtifactResponse{
			ArtifactID:    "art_001",
			BytesReceived: int64(len(receivedBody)),
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
		"application/octet-stream", "idem-123", bytes.NewReader(data),
	)
	require.NoError(t, err)
	require.Equal(t, data, receivedBody)
}

func TestUploadServerClientUploadArtifactConflict(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]string{
			"artifact_id": "art_001",
			"status":      "already_uploaded",
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
		"application/octet-stream", "", bytes.NewReader([]byte("data")),
	)
	require.NoError(t, err) // 409 is not an error for idempotent uploads
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
