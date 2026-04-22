// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package google

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

func TestEmbed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, http.MethodPost, r.Method)
			require.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))

			var req predictRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			require.Len(t, req.Instances, 1)
			require.Equal(t, "hello world", req.Instances[0].Content)

			resp := predictResponse{
				Predictions: []prediction{
					{Embeddings: embeddingResult{
						Values: make([]float32, 768),
					}},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			require.NoError(t, json.NewEncoder(w).Encode(resp))
		},
	))
	defer server.Close()

	c := NewClient("test-token", "test-project", "us-east1", "text-embedding-004", 768)
	c.endpoint = server.URL

	vec, err := c.Embed(context.Background(), "hello world")
	require.NoError(t, err)
	require.Len(t, vec, 768)
}

func TestEmbedBatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			var req predictRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))

			resp := predictResponse{
				Predictions: make([]prediction, len(req.Instances)),
			}
			for i := range resp.Predictions {
				resp.Predictions[i] = prediction{
					Embeddings: embeddingResult{
						Values: make([]float32, 768),
					},
				}
			}
			w.Header().Set("Content-Type", "application/json")
			require.NoError(t, json.NewEncoder(w).Encode(resp))
		},
	))
	defer server.Close()

	c := NewClient("test-token", "test-project", "us-east1", "text-embedding-004", 768)
	c.endpoint = server.URL

	vecs, err := c.EmbedBatch(context.Background(), []string{"a", "b", "c"})
	require.NoError(t, err)
	require.Len(t, vecs, 3)
	for _, v := range vecs {
		require.Len(t, v, 768)
	}
}

func TestEmbedBatchEmpty(t *testing.T) {
	c := NewClient("test-token", "test-project", "us-east1", "text-embedding-004", 768)
	vecs, err := c.EmbedBatch(context.Background(), nil)
	require.NoError(t, err)
	require.Nil(t, vecs)
}

func TestHTTPErrorClassification(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		pgCode     pgcode.Code
	}{
		{"unauthorized", http.StatusUnauthorized, pgcode.InvalidAuthorizationSpecification},
		{"forbidden", http.StatusForbidden, pgcode.InsufficientPrivilege},
		{"rate_limited", http.StatusTooManyRequests, pgcode.InsufficientResources},
		{"bad_request", http.StatusBadRequest, pgcode.InvalidParameterValue},
		{"server_error", http.StatusInternalServerError, pgcode.ConnectionFailure},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(
				func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(tt.statusCode)
					resp := errorResponse{}
					resp.Error.Message = "test error"
					require.NoError(t, json.NewEncoder(w).Encode(resp))
				},
			))
			defer server.Close()

			c := NewClient("test-token", "test-project", "us-east1", "text-embedding-004", 768)
			c.endpoint = server.URL

			_, err := c.Embed(context.Background(), "hello")
			require.Error(t, err)
			require.Equal(t, tt.pgCode, pgerror.GetPGCode(err))
		})
	}
}

func TestRegionFromHost(t *testing.T) {
	tests := []struct {
		host   string
		region string
	}{
		{"us-east1-aiplatform.googleapis.com", "us-east1"},
		{"europe-west4-aiplatform.googleapis.com", "europe-west4"},
		{"aiplatform.googleapis.com", ""},
		{"example.com", ""},
	}
	for _, tt := range tests {
		t.Run(tt.host, func(t *testing.T) {
			require.Equal(t, tt.region, RegionFromHost(tt.host))
		})
	}
}

func TestContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			// Block forever — context cancellation should interrupt.
			select {}
		},
	))
	defer server.Close()

	c := NewClient("test-token", "test-project", "us-east1", "text-embedding-004", 768)
	c.endpoint = server.URL

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.Embed(ctx, "hello")
	require.Error(t, err)
}
