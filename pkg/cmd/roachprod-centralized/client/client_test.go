// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/client/auth"
	cloudcluster "github.com/cockroachdb/cockroach/pkg/roachprod/cloud/types"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/stretchr/testify/assert"
	"golang.org/x/oauth2"
)

func createTestLogger(t *testing.T) *logger.Logger {
	logConf := logger.Config{}
	l, err := logConf.NewLogger("")
	assert.NoError(t, err)
	return l
}

func TestClient_CreateOrUpdateCluster_Disabled(t *testing.T) {
	client, err := NewClient(WithEnabled(false))

	// Should create a no-op client when disabled
	assert.NoError(t, err)

	err = client.RegisterCluster(
		context.Background(),
		createTestLogger(t),
		&cloudcluster.Cluster{
			Name: "test-cluster",
		},
	)

	// Should be no-op when disabled
	assert.ErrorIs(t, err, ErrDisabled)
}

func TestClient_CreateOrUpdateCluster_Success(t *testing.T) {
	// Mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			// Cluster exists, return it
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"name": "test-cluster",
				},
			})
		case "PUT":
			// Update succeeds
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"name": "test-cluster",
				},
			})
		case "POST":
			// Create succeeds
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"name": "test-cluster",
				},
			})
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	mockSource := &mockIAPTokenSource{
		token: &oauth2.Token{
			AccessToken: "test-token",
			TokenType:   "Bearer",
		},
		httpClient: &http.Client{},
	}

	client, err := NewClient(
		WithEnabled(true),
		WithBaseURL(server.URL),
		WithTimeout(5*time.Second),
		WithRetryAttempts(1),
		WithIAPTokenSource(mockSource),
	)
	assert.NoError(t, err)

	err = client.RegisterClusterUpdate(
		context.Background(),
		createTestLogger(t),
		&cloudcluster.Cluster{
			Name: "test-cluster",
		},
	)

	assert.NoError(t, err)
}

func TestClient_CreateOrUpdateCluster_UpdateNotFoundThenCreate(t *testing.T) {
	// Mock server that succeeds on POST (create)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			// Create succeeds
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"name": "test-cluster",
				},
			})
		} else {
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	mockSource := &mockIAPTokenSource{
		token: &oauth2.Token{
			AccessToken: "test-token",
			TokenType:   "Bearer",
		},
		httpClient: &http.Client{},
	}

	client, err := NewClient(
		WithEnabled(true),
		WithBaseURL(server.URL),
		WithTimeout(5*time.Second),
		WithRetryAttempts(1),
		WithIAPTokenSource(mockSource),
	)
	assert.NoError(t, err)

	err = client.RegisterCluster(
		context.Background(),
		createTestLogger(t),
		&cloudcluster.Cluster{
			Name: "test-cluster",
		},
	)

	assert.NoError(t, err)
}

func TestClient_DeleteCluster_Success(t *testing.T) {
	// Mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "DELETE" && r.URL.Path == "/v1/clusters/register/test-cluster" {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	mockSource := &mockIAPTokenSource{
		token: &oauth2.Token{
			AccessToken: "test-token",
			TokenType:   "Bearer",
		},
		httpClient: &http.Client{},
	}

	client, err := NewClient(
		WithEnabled(true),
		WithBaseURL(server.URL),
		WithTimeout(5*time.Second),
		WithRetryAttempts(1),
		WithIAPTokenSource(mockSource),
	)
	assert.NoError(t, err)

	err = client.RegisterClusterDelete(context.Background(), createTestLogger(t), "test-cluster")

	assert.NoError(t, err)
}

func TestClient_RetryOnServerError(t *testing.T) {
	callCount := 0

	// Mock server that fails first time, succeeds second time
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++

		if callCount == 1 {
			// First PUT call fails with server error
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("server error"))
		} else if callCount == 2 {
			// Second PUT call succeeds - update cluster
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"name": "test-cluster",
				},
			})
		}
	}))
	defer server.Close()

	mockSource := &mockIAPTokenSource{
		token: &oauth2.Token{
			AccessToken: "test-token",
			TokenType:   "Bearer",
		},
		httpClient: &http.Client{},
	}

	client, err := NewClient(
		WithEnabled(true),
		WithBaseURL(server.URL),
		WithTimeout(5*time.Second),
		WithRetryAttempts(2),
		WithRetryDelay(10*time.Millisecond), // Fast retry for test
		WithIAPTokenSource(mockSource),
	)
	assert.NoError(t, err)

	err = client.RegisterClusterUpdate(
		context.Background(),
		createTestLogger(t),
		&cloudcluster.Cluster{
			Name: "test-cluster",
		},
	)

	assert.NoError(t, err)
	assert.Equal(t, 2, callCount) // First attempt fails, second succeeds
}

func TestClient_NoRetryOnClientError(t *testing.T) {
	callCount := 0

	// Mock server that always returns client error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("bad request"))
	}))
	defer server.Close()

	mockSource := &mockIAPTokenSource{
		token: &oauth2.Token{
			AccessToken: "test-token",
			TokenType:   "Bearer",
		},
		httpClient: &http.Client{},
	}

	client, err := NewClient(
		WithEnabled(true),
		WithBaseURL(server.URL),
		WithTimeout(5*time.Second),
		WithRetryAttempts(2),
		WithRetryDelay(10*time.Millisecond),
		WithIAPTokenSource(mockSource),
	)
	assert.NoError(t, err)

	err = client.RegisterCluster(
		context.Background(),
		createTestLogger(t),
		&cloudcluster.Cluster{
			Name: "test-cluster",
		},
	)

	assert.Error(t, err)
	assert.Equal(t, 1, callCount) // Should not retry on client error
}

func TestLoadConfigFromEnv(t *testing.T) {
	// Set environment variables
	t.Setenv(EnvEnabled, "true")
	t.Setenv(EnvBaseURL, "https://api.example.com")
	t.Setenv(EnvForceFetchCreds, "true")
	t.Setenv(EnvTimeout, "30s")
	t.Setenv(EnvRetryAttempts, "3")

	config := LoadConfigFromEnv()

	assert.True(t, config.Enabled)
	assert.Equal(t, "https://api.example.com", config.BaseURL)
	assert.True(t, config.ForceFetchCreds)
	assert.Equal(t, 30*time.Second, config.Timeout)
	assert.Equal(t, 3, config.RetryAttempts)
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid disabled config",
			config: Config{
				Enabled: false,
				Timeout: 10 * time.Second,
			},
			wantErr: false,
		},
		{
			name: "valid enabled config",
			config: Config{
				Enabled: true,
				BaseURL: "https://api.example.com",
				Timeout: 10 * time.Second,
			},
			wantErr: false,
		},
		{
			name: "missing base URL when enabled",
			config: Config{
				Enabled: true,
				Timeout: 10 * time.Second,
			},
			wantErr: true,
		},
		{
			name: "invalid timeout",
			config: Config{
				Enabled: true,
				BaseURL: "https://api.example.com",
				Timeout: -1,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestNewClient_WithOptions(t *testing.T) {

	mockSource := &mockIAPTokenSource{
		token: &oauth2.Token{
			AccessToken: "test-token",
			TokenType:   "Bearer",
		},
		httpClient: &http.Client{},
	}

	// Test individual options
	client, err := NewClient(
		WithEnabled(true),
		WithBaseURL("https://custom.api.com"),
		WithTimeout(15*time.Second),
		WithRetryAttempts(5),
		WithRetryDelay(2*time.Second),
		WithForceFetchCreds(true),
		WithSilentFailures(false),
		WithIAPTokenSource(mockSource),
	)
	assert.NoError(t, err)
	assert.True(t, client.config.Enabled)
	assert.Equal(t, "https://custom.api.com", client.config.BaseURL)
	assert.Equal(t, 15*time.Second, client.config.Timeout)
	assert.Equal(t, 5, client.config.RetryAttempts)
	assert.Equal(t, 2*time.Second, client.config.RetryDelay)
	assert.True(t, client.config.ForceFetchCreds)
	assert.False(t, client.config.SilentFailures)
}

func TestNewClient_WithConfig(t *testing.T) {
	// Test legacy config option
	config := Config{
		Enabled:       true,
		BaseURL:       "https://legacy.api.com",
		Timeout:       20 * time.Second,
		RetryAttempts: 3,
	}

	mockSource := &mockIAPTokenSource{
		token: &oauth2.Token{
			AccessToken: "test-token",
			TokenType:   "Bearer",
		},
		httpClient: &http.Client{},
	}

	client, err := NewClient(WithConfig(config), WithIAPTokenSource(mockSource))
	assert.NoError(t, err)
	assert.True(t, client.config.Enabled)
	assert.Equal(t, "https://legacy.api.com", client.config.BaseURL)
	assert.Equal(t, 20*time.Second, client.config.Timeout)
	assert.Equal(t, 3, client.config.RetryAttempts)
}

// TestEnvPrefixSync verifies that the auth subpackage env vars use the same
// prefix as the client package. This guards against accidental divergence
// since the auth package defines its own unexported copy of the prefix.
func TestEnvPrefixSync(t *testing.T) {
	assert.True(t, strings.HasPrefix(auth.EnvAPIToken, EnvPrefix))
	assert.True(t, strings.HasPrefix(auth.EnvOktaIssuer, EnvPrefix))
	assert.True(t, strings.HasPrefix(auth.EnvOktaClientID, EnvPrefix))
}

type mockIAPTokenSource struct {
	token      *oauth2.Token
	httpClient *http.Client
}

func (m *mockIAPTokenSource) Token() (*oauth2.Token, error) {
	return m.token, nil
}

func (m *mockIAPTokenSource) GetHTTPClient() *http.Client {
	return m.httpClient
}
