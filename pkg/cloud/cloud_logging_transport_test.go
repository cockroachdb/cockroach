// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloud

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestMaybeAddLogging(t *testing.T) {
	// Test that logging is only added when log.V(1) is true
	originalVModule := log.GetVModule()
	defer func() { _ = log.SetVModule(originalVModule) }()

	t.Run("logging-disabled", func(t *testing.T) {
		_ = log.SetVModule("")
		inner := &http.Transport{}
		result := maybeAddLogging(inner)
		require.Equal(t, inner, result, "should return inner transport when logging is disabled")
	})

	t.Run("logging-enabled", func(t *testing.T) {
		_ = log.SetVModule("*=1")
		inner := &http.Transport{}
		result := maybeAddLogging(inner)
		require.NotEqual(t, inner, result, "should return wrapped transport when logging is enabled")

		loggingTransport, ok := result.(*loggingTransport)
		require.True(t, ok, "should return loggingTransport type")
		require.Equal(t, inner, loggingTransport.inner)
	})
}

func TestCloudLoggingTransport(t *testing.T) {
	// Set up logging scope for testing
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	ctx := context.Background()

	// Create a test server that echoes back the request body
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Read the request body to test request body tracking
		reqBody, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("failed to read request body: %v", err)
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		r.Body.Close()

		// Echo back the request body
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, err = w.Write(reqBody)
		require.NoError(t, err)
	}))
	defer server.Close()

	// Create a client with the logging transport
	client := &http.Client{
		Transport: &loggingTransport{inner: http.DefaultTransport},
	}

	// Test POST request with body
	reqBody := "test request body content"
	req, err := http.NewRequestWithContext(ctx, "POST", server.URL+"/echo", strings.NewReader(reqBody))
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Read the response body to trigger logging
	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, reqBody, string(respBody))
	resp.Body.Close()

	closer := resp.Body.(*responseBodyTracker)

	require.NotZero(t, closer.requestLatency)
	require.Equal(t, closer.status, redact.SafeString("200 OK"))
	require.Equal(t, closer.method, redact.SafeString("POST"))
	require.Equal(t, closer.url, server.URL+"/echo")
	require.Equal(t, closer.requestBytes, int64(len(reqBody)))
	require.Equal(t, closer.responseBytes, int64(len(respBody)))
	require.Equal(t, closer.readBytes, int64(len(respBody)))
	require.ErrorIs(t, closer.readErr, io.EOF)
	require.NotZero(t, closer.readTime)
}
