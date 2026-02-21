// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOktaDeviceFlow_StartDeviceAuthorization(t *testing.T) {
	skip.UnderStress(t)
	t.Run("missing client ID", func(t *testing.T) {
		flow := NewOktaDeviceFlow("https://example.okta.com", "")

		_, err := flow.StartDeviceAuthorization(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "client ID is required")
	})

	t.Run("successful device authorization", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "POST", r.Method)
			assert.Equal(t, "/oauth2/v1/device/authorize", r.URL.Path)
			assert.Equal(t, "application/x-www-form-urlencoded", r.Header.Get("Content-Type"))

			err := r.ParseForm()
			require.NoError(t, err)
			assert.Equal(t, "test-client-id", r.Form.Get("client_id"))
			assert.Contains(t, r.Form.Get("scope"), "openid")

			resp := DeviceCodeResponse{
				DeviceCode:              "device-code-123",
				UserCode:                "USER-CODE",
				VerificationURI:         "https://example.okta.com/activate",
				VerificationURIComplete: "https://example.okta.com/activate?user_code=USER-CODE",
				ExpiresIn:               600,
				Interval:                5,
			}
			w.Header().Set("Content-Type", "application/json")
			require.NoError(t, json.NewEncoder(w).Encode(resp))
		}))
		defer server.Close()

		flow := NewOktaDeviceFlow(server.URL, "test-client-id")

		resp, err := flow.StartDeviceAuthorization(context.Background())
		require.NoError(t, err)
		assert.Equal(t, "device-code-123", resp.DeviceCode)
		assert.Equal(t, "USER-CODE", resp.UserCode)
		assert.Equal(t, 5, resp.Interval)
	})

	t.Run("server error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, err := w.Write([]byte("internal error"))
			require.NoError(t, err)
		}))
		defer server.Close()

		flow := NewOktaDeviceFlow(server.URL, "test-client-id")

		_, err := flow.StartDeviceAuthorization(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "500")
	})
}

func TestOktaDeviceFlow_pollOnce(t *testing.T) {
	skip.UnderStress(t)
	t.Run("authorization pending", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			require.NoError(t, json.NewEncoder(w).Encode(map[string]string{
				"error":             "authorization_pending",
				"error_description": "The user has not yet completed authorization",
			}))
		}))
		defer server.Close()

		flow := NewOktaDeviceFlow(server.URL, "test-client-id")

		_, err := flow.pollOnce(context.Background(), server.URL+"/oauth2/v1/token", nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrAuthorizationPending)
	})

	t.Run("slow down", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			require.NoError(t, json.NewEncoder(w).Encode(map[string]string{
				"error":             "slow_down",
				"error_description": "Slow down polling",
			}))
		}))
		defer server.Close()

		flow := NewOktaDeviceFlow(server.URL, "test-client-id")

		_, err := flow.pollOnce(context.Background(), server.URL+"/oauth2/v1/token", nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrSlowDown)
	})

	t.Run("access denied", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			require.NoError(t, json.NewEncoder(w).Encode(map[string]string{
				"error":             "access_denied",
				"error_description": "User denied access",
			}))
		}))
		defer server.Close()

		flow := NewOktaDeviceFlow(server.URL, "test-client-id")

		_, err := flow.pollOnce(context.Background(), server.URL+"/oauth2/v1/token", nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrAccessDenied)
	})

	t.Run("expired token", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			require.NoError(t, json.NewEncoder(w).Encode(map[string]string{
				"error":             "expired_token",
				"error_description": "Device code has expired",
			}))
		}))
		defer server.Close()

		flow := NewOktaDeviceFlow(server.URL, "test-client-id")

		_, err := flow.pollOnce(context.Background(), server.URL+"/oauth2/v1/token", nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDeviceCodeExpired)
	})

	t.Run("successful token response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			resp := TokenResponse{
				AccessToken: "access-token-123",
				IDToken:     "id-token-456",
				TokenType:   "Bearer",
				ExpiresIn:   3600,
				Scope:       "openid email profile",
			}
			require.NoError(t, json.NewEncoder(w).Encode(resp))
		}))
		defer server.Close()

		flow := NewOktaDeviceFlow(server.URL, "test-client-id")

		resp, err := flow.pollOnce(context.Background(), server.URL+"/oauth2/v1/token", nil)
		require.NoError(t, err)
		assert.Equal(t, "access-token-123", resp.AccessToken)
		assert.Equal(t, "id-token-456", resp.IDToken)
		assert.Equal(t, "Bearer", resp.TokenType)
	})
}
