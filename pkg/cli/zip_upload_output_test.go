// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

// newTestGCSClient creates a test uploadServerClient with a GCS client
// that uses the provided storage emulator host. The client is configured
// with a fake bucket and prefix, and a non-nil gcsClient so that
// upload methods use the GCS path.
func newTestGCSClient(t *testing.T, ctx context.Context, emulatorHost string) *uploadServerClient {
	t.Helper()

	client := newUploadServerClientWithToken(
		uploadServerClientConfig{ServerURL: "http://unused"},
		"ses_test", "tok_test",
	)

	gcsClient, err := storage.NewClient(ctx,
		option.WithEndpoint("http://"+emulatorHost+"/storage/v1/"),
		option.WithoutAuthentication(),
	)
	require.NoError(t, err)
	client.gcsClient = gcsClient
	client.gcsBucket = "test-bucket"
	client.gcsPrefix = "sessions/ses_test/"
	return client
}

func TestUploadZipOutputCreateRaw(t *testing.T) {
	t.Skip("requires GCS emulator")

	ctx := context.Background()
	client := newTestGCSClient(t, ctx, "localhost:4443")
	defer func() { _ = client.Close() }()

	output := newUploadZipOutput(ctx, client, 1)

	zr := &zipReporter{flowing: false, newline: true, inItem: true}
	err := output.createRaw(zr, "nodes/1/cpu.pprof", []byte("profile-data"))
	require.NoError(t, err)
	require.Equal(t, 1, output.artifactsUploaded())
}

func TestUploadZipOutputCreateJSON(t *testing.T) {
	t.Skip("requires GCS emulator")

	ctx := context.Background()
	client := newTestGCSClient(t, ctx, "localhost:4443")
	defer func() { _ = client.Close() }()

	output := newUploadZipOutput(ctx, client, 0)

	zr := &zipReporter{flowing: false, newline: true, inItem: true}
	data := map[string]string{"key": "value"}
	err := output.createJSON(zr, "cluster/settings.json", data)
	require.NoError(t, err)
	require.Equal(t, 1, output.artifactsUploaded())
}

func TestUploadZipOutputCreateLocked(t *testing.T) {
	t.Skip("requires GCS emulator")

	ctx := context.Background()
	client := newTestGCSClient(t, ctx, "localhost:4443")
	defer func() { _ = client.Close() }()

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
}
