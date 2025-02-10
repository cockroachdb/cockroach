// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package promhelperclient

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"gopkg.in/yaml.v2"
)

func TestNewPromClient(t *testing.T) {
	mockToken := &oauth2.Token{
		AccessToken: "test-token",
		TokenType:   "Bearer",
	}
	mockSource := &mockIAPTokenSource{
		token:      mockToken,
		httpClient: &http.Client{},
	}

	t.Run("with custom URL and IAP token source", func(t *testing.T) {
		promURL := "http://test.com"
		client, err := NewPromClient(
			WithCustomURL(promURL),
			WithIAPTokenSource(mockSource),
		)
		require.NoError(t, err)
		require.Equal(t, promURL, client.promUrl)
		require.False(t, client.disabled)
		require.Equal(t, mockSource.GetHTTPClient(), client.httpClient)
	})

	t.Run("with default URL", func(t *testing.T) {
		client, err := NewPromClient(
			WithIAPTokenSource(mockSource),
		)
		require.NoError(t, err)
		require.Equal(t, promRegistrationUrl, client.promUrl)
		require.NotNil(t, client.httpClient)
	})

	t.Run("verify supported providers", func(t *testing.T) {
		tests := []struct {
			provider    string
			environment CloudEnvironment
			expected    Reachability
		}{
			{"gce", "cockroach-ephemeral", Private},
			{"gce", Default, Public},
			{"aws", Default, Public},
			{"azure", Default, Public},
			{"unknown", Default, None},
		}

		for _, tc := range tests {
			reachability := ProviderReachability(tc.provider, tc.environment)
			require.Equal(t, tc.expected, reachability)
		}
	})
}

func TestPromClientDisabled(t *testing.T) {
	l := func() *logger.Logger {
		l, err := logger.RootLogger("", logger.TeeToStdout)
		require.NoError(t, err)
		return l
	}()

	c := &PromClient{
		disabled: true,
	}

	ctx := context.Background()
	err := c.UpdatePrometheusTargets(ctx, "test-cluster", NodeTargets{}, l)
	require.NoError(t, err)

	err = c.DeleteClusterConfig(ctx, "test-cluster", l)
	require.NoError(t, err)
}

func TestUpdatePrometheusTargets(t *testing.T) {

	mockToken := &oauth2.Token{
		AccessToken: "test-token",
		TokenType:   "Bearer",
	}
	mockSource := &mockIAPTokenSource{
		token:      mockToken,
		httpClient: &http.Client{},
	}

	ctx := context.Background()
	promURL := "http://test.com"

	t.Run("buildCreateRequestBody", func(t *testing.T) {
		nodes := NodeTargets{
			1: {
				{
					Target: "127.0.0.1:8080",
					CustomLabels: map[string]string{
						"label1": "value1",
					},
				},
			},
			2: {
				{
					Target: "127.0.0.1:8081",
				},
			},
		}

		client, err := NewPromClient(WithCustomURL(promURL), WithIAPTokenSource(mockSource))
		require.NoError(t, err)
		body, err := client.buildCreateRequestBody(nodes)
		require.NoError(t, err)

		buf := new([]byte)
		_, err = body.Read(*buf)
		require.NoError(t, err)

		// Unmarshal the json
		var instanceConfig instanceConfigRequest
		err = json.NewDecoder(body).Decode(&instanceConfig)
		require.NoError(t, err)

		// Unmarshal the yaml
		var ccParams []*CCParams
		err = yaml.UnmarshalStrict([]byte(instanceConfig.Config), &ccParams)
		require.NoError(t, err)

		require.Len(t, ccParams, 2)

		for _, params := range ccParams {
			if params.Targets[0] == "127.0.0.1:8080" {
				require.Equal(t, map[string]string{"label1": "value1"}, params.Labels)
			} else if params.Targets[0] == "127.0.0.1:8081" {
				require.Empty(t, params.Labels)
			} else {
				t.Fatalf("unexpected target: %s", params.Targets[0])
			}
		}
	})

	t.Run("buildCreateRequest", func(t *testing.T) {
		nodes := NodeTargets{
			1: {
				{
					Target: "127.0.0.1:8080",
					CustomLabels: map[string]string{
						"label1": "value1",
					},
				},
			},
		}

		client, err := NewPromClient(WithCustomURL(promURL), WithIAPTokenSource(mockSource))
		require.NoError(t, err)
		req, err := client.buildCreateRequest(ctx, "test-cluster", nodes)
		require.NoError(t, err)

		require.Equal(t, promURL+"/v1/instance-configs/test-cluster", req.URL.String())
		require.Equal(t, "PUT", req.Method)
		require.Equal(t, "application/json", req.Header.Get("Content-Type"))
	})
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
