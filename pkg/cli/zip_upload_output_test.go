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

// newTestSignedURLServers creates a mock upload server (for signed URL
// requests) and a mock GCS server (for actual data PUTs). The upload
// server returns signed URLs pointing to the GCS server. The GCS
// server records all received bodies keyed by URL path.
func newTestSignedURLServers(
	t *testing.T,
) (uploadSrv *httptest.Server, gcsSrv *httptest.Server, gcsUploads func() map[string][]byte) {
	t.Helper()

	var mu sync.Mutex
	uploads := map[string][]byte{}

	gcsSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "PUT", r.Method)
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		mu.Lock()
		uploads[r.URL.Path] = body
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))

	uploadSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "POST", r.Method)
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))

		var req signedURLRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)

		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(signedURLResponse{
			SignedURL: gcsSrv.URL + "/" + req.ArtifactPath + "?sig=test",
			ExpiresAt: "2026-02-21T12:15:00Z",
		}))
	}))

	return uploadSrv, gcsSrv, func() map[string][]byte {
		mu.Lock()
		defer mu.Unlock()
		cp := make(map[string][]byte, len(uploads))
		for k, v := range uploads {
			cp[k] = v
		}
		return cp
	}
}

func TestUploadZipOutputCreateRaw(t *testing.T) {
	uploadSrv, gcsSrv, gcsUploads := newTestSignedURLServers(t)
	defer uploadSrv.Close()
	defer gcsSrv.Close()

	client := newUploadServerClientWithToken(
		uploadServerClientConfig{ServerURL: uploadSrv.URL},
		"ses_test", "tok_test",
	)

	ctx := context.Background()
	output := newUploadZipOutput(ctx, client, 1)

	zr := &zipReporter{flowing: false, newline: true, inItem: true}
	err := output.createRaw(zr, "nodes/1/cpu.pprof", []byte("profile-data"))
	require.NoError(t, err)
	require.Equal(t, 1, output.artifactsUploaded())

	uploads := gcsUploads()
	require.Equal(t, []byte("profile-data"), uploads["/nodes/1/cpu.pprof"])
}

func TestUploadZipOutputCreateJSON(t *testing.T) {
	uploadSrv, gcsSrv, gcsUploads := newTestSignedURLServers(t)
	defer uploadSrv.Close()
	defer gcsSrv.Close()

	client := newUploadServerClientWithToken(
		uploadServerClientConfig{ServerURL: uploadSrv.URL},
		"ses_test", "tok_test",
	)

	ctx := context.Background()
	output := newUploadZipOutput(ctx, client, 0)

	zr := &zipReporter{flowing: false, newline: true, inItem: true}
	data := map[string]string{"key": "value"}
	err := output.createJSON(zr, "cluster/settings.json", data)
	require.NoError(t, err)
	require.Equal(t, 1, output.artifactsUploaded())

	uploads := gcsUploads()
	require.Contains(t, string(uploads["/cluster/settings.json"]), `"key"`)
}

func TestUploadZipOutputCreateLocked(t *testing.T) {
	uploadSrv, gcsSrv, gcsUploads := newTestSignedURLServers(t)
	defer uploadSrv.Close()
	defer gcsSrv.Close()

	client := newUploadServerClientWithToken(
		uploadServerClientConfig{ServerURL: uploadSrv.URL},
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

	uploads := gcsUploads()
	require.Equal(t,
		"table-data-row-1\ntable-data-row-2\n",
		string(uploads["/nodes/1/data.txt"]),
	)
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
