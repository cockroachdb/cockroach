// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestNewBlobUploaderRejectsEmptyURI(t *testing.T) {
	defer leaktest.AfterTest(t)()

	_, err := newBlobUploader(
		context.Background(), "", "bucket", "obj", "application/zip",
		http.DefaultClient, false, /* resuming */
	)
	require.ErrorContains(t, err, "session URI is empty")
}

func TestGCSSessionURIUploaderPUTs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tmpDir := t.TempDir()
	zipPath := filepath.Join(tmpDir, "debug.zip")
	payload := []byte("PK\x03\x04fake-zip-payload")
	require.NoError(t, os.WriteFile(zipPath, payload, 0644))

	var (
		sawMethod        string
		sawContentType   string
		sawContentLength int64
		sawBody          []byte
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawMethod = r.Method
		sawContentType = r.Header.Get("Content-Type")
		sawContentLength = r.ContentLength
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		sawBody = body
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	uploader, err := newBlobUploader(
		context.Background(), srv.URL+"/session?upload_id=fake",
		"bucket-a", "ses_test/debug.zip", "application/zip",
		srv.Client(), false, /* resuming */
	)
	require.NoError(t, err)
	defer func() { _ = uploader.Close() }()

	dest, _, err := uploader.Upload(context.Background(), zipPath)
	require.NoError(t, err)

	require.Equal(t, http.MethodPut, sawMethod)
	require.Equal(t, "application/zip", sawContentType)
	require.Equal(t, int64(len(payload)), sawContentLength)
	require.Equal(t, payload, sawBody)
	require.Equal(t, "gs://bucket-a/ses_test/debug.zip", dest)
}

func TestGCSSessionURIUploaderNon2xx(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tmpDir := t.TempDir()
	zipPath := filepath.Join(tmpDir, "debug.zip")
	require.NoError(t, os.WriteFile(zipPath, []byte("PK\x03\x04"), 0644))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("session URI expired"))
	}))
	defer srv.Close()

	uploader, err := newBlobUploader(
		context.Background(), srv.URL, "b", "o", "application/zip", srv.Client(),
		false, /* resuming */
	)
	require.NoError(t, err)

	_, _, err = uploader.Upload(context.Background(), zipPath)
	require.ErrorContains(t, err, "blob upload failed (403)")
	require.ErrorContains(t, err, "session URI expired")
}

func TestGCSSessionURIUploaderResumeAtOffset(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tmpDir := t.TempDir()
	zipPath := filepath.Join(tmpDir, "debug.zip")
	payload := []byte("PK\x03\x04abcdefghijklmnopqrstuvwxyz") // 30 bytes
	require.NoError(t, os.WriteFile(zipPath, payload, 0644))
	const alreadyOnGCS = 10 // GCS holds bytes [0, 10), resume at byte 10

	var (
		probeContentRange string
		putContentRange   string
		putBody           []byte
		putContentLength  int64
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPut, r.Method)
		switch {
		case r.ContentLength == 0:
			// Probe: respond as if GCS has bytes [0, alreadyOnGCS).
			probeContentRange = r.Header.Get("Content-Range")
			w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", alreadyOnGCS-1))
			w.WriteHeader(http.StatusPermanentRedirect)
		default:
			putContentRange = r.Header.Get("Content-Range")
			putContentLength = r.ContentLength
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			putBody = body
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	uploader, err := newBlobUploader(
		context.Background(), srv.URL, "b", "ses/debug.zip", "application/zip",
		srv.Client(), true, /* resuming */
	)
	require.NoError(t, err)

	dest, _, err := uploader.Upload(context.Background(), zipPath)
	require.NoError(t, err)

	require.Equal(t, fmt.Sprintf("bytes */%d", len(payload)), probeContentRange)
	require.Equal(t,
		fmt.Sprintf("bytes %d-%d/%d", alreadyOnGCS, len(payload)-1, len(payload)),
		putContentRange,
	)
	require.Equal(t, int64(len(payload)-alreadyOnGCS), putContentLength)
	require.Equal(t, payload[alreadyOnGCS:], putBody)
	require.Equal(t, "gs://b/ses/debug.zip", dest)
}

func TestGCSSessionURIUploaderResumeAlreadyComplete(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tmpDir := t.TempDir()
	zipPath := filepath.Join(tmpDir, "debug.zip")
	require.NoError(t, os.WriteFile(zipPath, []byte("PK\x03\x04done"), 0644))

	var requests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		require.Equal(t, int64(0), r.ContentLength,
			"second request would mean we tried to PUT after probe said complete")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	uploader, err := newBlobUploader(
		context.Background(), srv.URL, "b", "ses/debug.zip", "application/zip",
		srv.Client(), true, /* resuming */
	)
	require.NoError(t, err)

	dest, _, err := uploader.Upload(context.Background(), zipPath)
	require.NoError(t, err)
	require.Equal(t, 1, requests, "should only probe; should not re-PUT")
	require.Equal(t, "gs://b/ses/debug.zip", dest)
}

func TestGCSSessionURIUploaderResumeURIExpired(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tmpDir := t.TempDir()
	zipPath := filepath.Join(tmpDir, "debug.zip")
	require.NoError(t, os.WriteFile(zipPath, []byte("PK\x03\x04gone"), 0644))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusGone)
	}))
	defer srv.Close()

	uploader, err := newBlobUploader(
		context.Background(), srv.URL, "b", "ses/debug.zip", "application/zip",
		srv.Client(), true, /* resuming */
	)
	require.NoError(t, err)

	_, _, err = uploader.Upload(context.Background(), zipPath)
	require.ErrorIs(t, err, errSessionURIDead)
}

// TestGCSSessionURIUploaderResumeNothingOnServer covers 308-with-no-
// Range-header: GCS has accepted no bytes yet, so the client PUTs the
// whole file with no Content-Range.
func TestGCSSessionURIUploaderResumeNothingOnServer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tmpDir := t.TempDir()
	zipPath := filepath.Join(tmpDir, "debug.zip")
	payload := []byte("PK\x03\x04hello")
	require.NoError(t, os.WriteFile(zipPath, payload, 0644))

	var (
		putContentRange string
		putContentLen   int64
		putBody         []byte
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength == 0 {
			// Probe response: 308 with no Range header.
			w.WriteHeader(http.StatusPermanentRedirect)
			return
		}
		putContentRange = r.Header.Get("Content-Range")
		putContentLen = r.ContentLength
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		putBody = body
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	uploader, err := newBlobUploader(
		context.Background(), srv.URL, "b", "ses/debug.zip", "application/zip",
		srv.Client(), true, /* resuming */
	)
	require.NoError(t, err)

	_, _, err = uploader.Upload(context.Background(), zipPath)
	require.NoError(t, err)
	require.Empty(t, putContentRange,
		"offset 0 means a normal PUT; Content-Range would lie about partial bytes")
	require.Equal(t, int64(len(payload)), putContentLen)
	require.Equal(t, payload, putBody)
}

func TestParseResumeRangeHeader(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name           string
		input          string
		expectedOffset int64
		expectedErr    string
	}{
		{name: "single byte received", input: "bytes=0-0", expectedOffset: 1},
		{name: "many bytes received", input: "bytes=0-1023", expectedOffset: 1024},
		{name: "no bytes prefix", input: "0-1023", expectedOffset: 1024},
		{name: "missing dash", input: "bytes=1023", expectedErr: "missing '-'"},
		{name: "empty input", input: "", expectedErr: "missing '-'"},
		{name: "non-zero start rejected", input: "bytes=10-100", expectedErr: "unexpected range start"},
		{name: "non-numeric end", input: "bytes=0-abc", expectedErr: "parsing range end"},
		{name: "negative end rejected", input: "bytes=0--1", expectedErr: "negative range end"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseResumeRangeHeader(tc.input)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedOffset, got)
		})
	}
}

// TestGCSSessionURIUploaderResumeProbeUnexpectedStatus covers the
// default branch of queryUploadOffset (anything outside the documented
// 200/201/308/404/410 set surfaces a "probe failed" error with the
// status code and body).
func TestGCSSessionURIUploaderResumeProbeUnexpectedStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tmpDir := t.TempDir()
	zipPath := filepath.Join(tmpDir, "debug.zip")
	require.NoError(t, os.WriteFile(zipPath, []byte("PK\x03\x04abc"), 0644))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("backend down"))
	}))
	defer srv.Close()

	uploader, err := newBlobUploader(
		context.Background(), srv.URL, "b", "ses/debug.zip", "application/zip",
		srv.Client(), true, /* resuming */
	)
	require.NoError(t, err)

	_, _, err = uploader.Upload(context.Background(), zipPath)
	require.ErrorContains(t, err, "probe failed (503)")
	require.ErrorContains(t, err, "backend down")
}

// TestGCSSessionURIUploaderResumeMalformedRangeHeader verifies that a
// 308 with a Range header parseResumeRangeHeader rejects surfaces with
// the GCS-Range context wrap.
func TestGCSSessionURIUploaderResumeMalformedRangeHeader(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tmpDir := t.TempDir()
	zipPath := filepath.Join(tmpDir, "debug.zip")
	require.NoError(t, os.WriteFile(zipPath, []byte("PK\x03\x04abc"), 0644))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Range", "bytes=10-100") // non-zero start, rejected
		w.WriteHeader(http.StatusPermanentRedirect)
	}))
	defer srv.Close()

	uploader, err := newBlobUploader(
		context.Background(), srv.URL, "b", "ses/debug.zip", "application/zip",
		srv.Client(), true, /* resuming */
	)
	require.NoError(t, err)

	_, _, err = uploader.Upload(context.Background(), zipPath)
	require.ErrorContains(t, err, "parsing GCS Range header")
	require.ErrorContains(t, err, "bytes=10-100")
}
