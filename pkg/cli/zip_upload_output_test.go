// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUploadZipOutputCreateRaw(t *testing.T) {
	var mu sync.Mutex
	uploads := map[string][]byte{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		mu.Lock()
		uploads[r.URL.Path] = body
		mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(uploadArtifactResponse{
			ArtifactID:    "art_1",
			BytesReceived: int64(len(body)),
		}))
	}))
	defer srv.Close()

	client := newUploadServerClientWithToken(
		uploadServerClientConfig{ServerURL: srv.URL},
		"ses_test", "tok_test",
	)

	ctx := context.Background()
	output := newUploadZipOutput(ctx, client, 1)

	zr := &zipReporter{flowing: false, newline: true, inItem: true}
	err := output.createRaw(zr, "nodes/1/cpu.pprof", []byte("profile-data"))
	require.NoError(t, err)
	require.Equal(t, 1, output.artifactsUploaded())

	mu.Lock()
	require.Equal(t, []byte("profile-data"), uploads["/api/v1/sessions/ses_test/artifacts/nodes/1/cpu.pprof"])
	mu.Unlock()
}

func TestUploadZipOutputCreateJSON(t *testing.T) {
	var mu sync.Mutex
	uploads := map[string][]byte{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		mu.Lock()
		uploads[r.URL.Path] = body
		mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(uploadArtifactResponse{
			ArtifactID: "art_2",
		}))
	}))
	defer srv.Close()

	client := newUploadServerClientWithToken(
		uploadServerClientConfig{ServerURL: srv.URL},
		"ses_test", "tok_test",
	)

	ctx := context.Background()
	output := newUploadZipOutput(ctx, client, 0)

	zr := &zipReporter{flowing: false, newline: true, inItem: true}
	data := map[string]string{"key": "value"}
	err := output.createJSON(zr, "cluster/settings.json", data)
	require.NoError(t, err)
	require.Equal(t, 1, output.artifactsUploaded())
}

func TestUploadZipOutputCreateLocked(t *testing.T) {
	var mu sync.Mutex
	uploads := map[string][]byte{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		mu.Lock()
		uploads[r.URL.Path] = body
		mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(uploadArtifactResponse{
			ArtifactID: "art_3",
		}))
	}))
	defer srv.Close()

	client := newUploadServerClientWithToken(
		uploadServerClientConfig{ServerURL: srv.URL},
		"ses_test", "tok_test",
	)

	ctx := context.Background()
	output := newUploadZipOutput(ctx, client, 1)

	// Simulate the createLocked/Unlock pattern used by dumpTableDataForZip.
	output.Lock()
	w, err := output.createLocked("nodes/1/data.txt", time.Time{})
	require.NoError(t, err)
	_, err = w.Write([]byte("table-data-row-1\n"))
	require.NoError(t, err)
	_, err = w.Write([]byte("table-data-row-2\n"))
	require.NoError(t, err)
	output.Unlock()

	require.Equal(t, 1, output.artifactsUploaded())

	mu.Lock()
	require.Equal(t,
		"table-data-row-1\ntable-data-row-2\n",
		string(uploads["/api/v1/sessions/ses_test/artifacts/nodes/1/data.txt"]),
	)
	mu.Unlock()
}

func TestInferArtifactType(t *testing.T) {
	tests := []struct {
		name     string
		expected artifactType
	}{
		{"nodes/1/cpu.pprof", artifactTypeProfile},
		{"nodes/1/heap.pprof", artifactTypeProfile},
		{"nodes/1/logs/cockroach.log", artifactTypeLog},
		{"nodes/1/stacks.txt", artifactTypeStack},
		{"nodes/1/stacks_with_labels.txt", artifactTypeStack},
		{"nodes/1/lsm.txt", artifactTypeEngineStats},
		{"cluster/settings.json", artifactTypeMetadata},
		{"nodes/1/details.json", artifactTypeMetadata},
		{"debug/crdb_internal.node_runtime_info.txt", artifactTypeTable},
		{"jobs/123/456/trace.zip", artifactTypeTrace},
		{"debug/pprof-summary.sh", artifactTypeMetadata},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := inferArtifactType(tt.name)
			require.Equal(t, tt.expected, got)
		})
	}
}
