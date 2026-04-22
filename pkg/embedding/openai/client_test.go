// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package openai

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/stretchr/testify/require"
)

// newTestClient creates a Client pointed at the given test server.
func newTestClient(t *testing.T, server *httptest.Server, dims int) *Client {
	t.Helper()
	c := NewClient("test-key", "text-embedding-3-small", dims)
	c.endpoint = server.URL
	return c
}

func TestEmbedSingle(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "Bearer test-key", r.Header.Get("Authorization"))
			require.Equal(t, "application/json", r.Header.Get("Content-Type"))

			var req embeddingRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			require.Len(t, req.Input, 1)
			require.Equal(t, "hello world", req.Input[0])

			resp := embeddingResponse{
				Data: []embeddingData{
					{Embedding: []float32{0.1, 0.2, 0.3}, Index: 0},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			require.NoError(t, json.NewEncoder(w).Encode(resp))
		},
	))
	defer server.Close()

	c := newTestClient(t, server, 3)
	vec, err := c.Embed(context.Background(), "hello world")
	require.NoError(t, err)
	require.Equal(t, []float32{0.1, 0.2, 0.3}, vec)
}

func TestEmbedBatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			var req embeddingRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			require.Len(t, req.Input, 2)

			// Return out of order to verify index handling.
			resp := embeddingResponse{
				Data: []embeddingData{
					{Embedding: []float32{0.4, 0.5, 0.6}, Index: 1},
					{Embedding: []float32{0.1, 0.2, 0.3}, Index: 0},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			require.NoError(t, json.NewEncoder(w).Encode(resp))
		},
	))
	defer server.Close()

	c := newTestClient(t, server, 3)
	vecs, err := c.EmbedBatch(context.Background(), []string{"hello", "world"})
	require.NoError(t, err)
	require.Len(t, vecs, 2)
	require.Equal(t, []float32{0.1, 0.2, 0.3}, vecs[0])
	require.Equal(t, []float32{0.4, 0.5, 0.6}, vecs[1])
}

func TestEmbedBatchEmpty(t *testing.T) {
	c := NewClient("test-key", "model", 3)
	vecs, err := c.EmbedBatch(context.Background(), nil)
	require.NoError(t, err)
	require.Nil(t, vecs)
}

func TestHTTPErrorClassification(t *testing.T) {
	tests := []struct {
		name         string
		statusCode   int
		responseBody string
		expectedCode pgcode.Code
		expectedMsg  string
	}{
		{
			name:         "unauthorized",
			statusCode:   http.StatusUnauthorized,
			responseBody: `{"error":{"message":"invalid api key"}}`,
			expectedCode: pgcode.InvalidAuthorizationSpecification,
			expectedMsg:  "authentication failed",
		},
		{
			name:         "forbidden",
			statusCode:   http.StatusForbidden,
			responseBody: `{"error":{"message":"access denied"}}`,
			expectedCode: pgcode.InsufficientPrivilege,
			expectedMsg:  "permission denied",
		},
		{
			name:         "rate limited",
			statusCode:   http.StatusTooManyRequests,
			responseBody: `{"error":{"message":"rate limit exceeded"}}`,
			expectedCode: pgcode.InsufficientResources,
			expectedMsg:  "rate limited",
		},
		{
			name:         "bad request",
			statusCode:   http.StatusBadRequest,
			responseBody: `{"error":{"message":"invalid model"}}`,
			expectedCode: pgcode.InvalidParameterValue,
			expectedMsg:  "bad request",
		},
		{
			name:         "server error",
			statusCode:   http.StatusInternalServerError,
			responseBody: `{"error":{"message":"internal error"}}`,
			expectedCode: pgcode.ConnectionFailure,
			expectedMsg:  "server error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(
				func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(tc.statusCode)
					_, err := w.Write([]byte(tc.responseBody))
					require.NoError(t, err)
				},
			))
			defer server.Close()

			c := newTestClient(t, server, 3)
			_, err := c.Embed(context.Background(), "test")
			require.Error(t, err)
			require.Equal(t, tc.expectedCode, pgerror.GetPGCode(err))
			require.ErrorContains(t, err, tc.expectedMsg)
		})
	}
}

func TestContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			// Block forever; the context should cancel.
			select {}
		},
	))
	defer server.Close()

	c := newTestClient(t, server, 3)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.Embed(ctx, "test")
	require.Error(t, err)
}

func TestMalformedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, err := w.Write([]byte("not json"))
			require.NoError(t, err)
		},
	))
	defer server.Close()

	c := newTestClient(t, server, 3)
	_, err := c.Embed(context.Background(), "test")
	require.Error(t, err)
	require.ErrorContains(t, err, "decoding response")
}
