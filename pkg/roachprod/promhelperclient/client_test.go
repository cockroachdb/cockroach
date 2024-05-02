// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package promhelperclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"google.golang.org/api/idtoken"
)

func TestUpdatePrometheusTargets(t *testing.T) {
	l := func() *logger.Logger {
		l, err := logger.RootLogger("", logger.TeeToStdout)
		if err != nil {
			panic(err)
		}
		return l
	}()
	ctx := context.Background()
	promUrl := "http://prom_url.com"
	t.Run("UpdatePrometheusTargets fails with 400", func(t *testing.T) {
		httpPut = func(ctx context.Context, reqUrl string, h *httputil.RequestHeaders, body io.Reader) (
			resp *http.Response, err error) {
			require.Equal(t, getUrl(promUrl, "c1"), reqUrl)
			return &http.Response{
				StatusCode: 400,
				Body:       io.NopCloser(strings.NewReader("failed")),
			}, nil
		}
		err := UpdatePrometheusTargets(ctx, promUrl, "c1", false, []string{"n1"}, l)
		require.NotNil(t, err)
		require.Equal(t, "request failed with status 400 and error failed", err.Error())
	})
	t.Run("UpdatePrometheusTargets succeeds", func(t *testing.T) {
		httpPut = func(ctx context.Context, url string, h *httputil.RequestHeaders, body io.Reader) (
			resp *http.Response, err error) {
			require.Equal(t, getUrl(promUrl, "c1"), url)
			ir, err := getInstanceConfigRequest(io.NopCloser(body))
			require.Nil(t, err)
			require.Equal(t, `---
- targets:
  - n1
  labels:
    node: "1"
    tenant: system

- targets:
  - n2
  labels:
    node: "2"
    tenant: system

`, ir.Config)
			return &http.Response{
				StatusCode: 200,
			}, nil
		}
		err := UpdatePrometheusTargets(ctx, promUrl, "c1", false, []string{"n1", "n2"}, l)
		require.Nil(t, err)
	})
}

func TestDeleteClusterConfig(t *testing.T) {
	l := func() *logger.Logger {
		l, err := logger.RootLogger(filepath.Join(t.TempDir(), "test.log"), logger.NoTee)
		if err != nil {
			panic(err)
		}
		return l
	}()
	ctx := context.Background()
	promUrl := "http://prom_url.com"
	t.Run("DeleteClusterConfig fails with 400", func(t *testing.T) {
		httpDelete = func(ctx context.Context, url string, h *httputil.RequestHeaders) (
			resp *http.Response, err error) {
			require.Equal(t, getUrl(promUrl, "c1"), url)
			return &http.Response{
				StatusCode: 400,
				Body:       io.NopCloser(strings.NewReader("failed")),
			}, nil
		}
		err := DeleteClusterConfig(ctx, promUrl, "c1", false, l)
		require.NotNil(t, err)
		require.Equal(t, "request failed with status 400 and error failed", err.Error())
	})
	t.Run("DeleteClusterConfig succeeds", func(t *testing.T) {
		httpDelete = func(ctx context.Context, url string, h *httputil.RequestHeaders) (
			resp *http.Response, err error) {
			require.Equal(t, getUrl(promUrl, "c1"), url)
			return &http.Response{
				StatusCode: 204,
			}, nil
		}
		err := DeleteClusterConfig(ctx, promUrl, "c1", false, l)
		require.Nil(t, err)
	})
}

// getInstanceConfigRequest returns the instanceConfigRequest after parsing the request json
func getInstanceConfigRequest(body io.ReadCloser) (*instanceConfigRequest, error) {
	var insConfigReq instanceConfigRequest
	if err := json.NewDecoder(body).Decode(&insConfigReq); err != nil {
		return nil, err
	}
	return &insConfigReq, nil
}

func Test_getToken(t *testing.T) {
	ctx := context.Background()
	l := func() *logger.Logger {
		l, err := logger.RootLogger("", logger.TeeToStdout)
		if err != nil {
			panic(err)
		}
		return l
	}()
	t.Run("insecure url", func(t *testing.T) {
		token, err := getToken(ctx, "http://test.com", false, l)
		require.Nil(t, err)
		require.Empty(t, token)
	})
	t.Run("invalid credentials", func(t *testing.T) {
		os.Setenv(ServiceAccountJson, "{}")
		os.Setenv(ServiceAccountAudience, "dummy_audience")
		newTokenSource = func(ctx context.Context, audience string, opts ...idtoken.ClientOption) (oauth2.TokenSource, error) {
			return nil, fmt.Errorf("invalid")
		}
		token, err := getToken(ctx, "https://test.com", false, l)
		require.NotNil(t, err)
		require.Empty(t, token)
		require.Equal(t, "error creating GCS oauth token source from specified credential: invalid", err.Error())
	})
	t.Run("invalid token", func(t *testing.T) {
		os.Setenv(ServiceAccountJson, "{}")
		os.Setenv(ServiceAccountAudience, "dummy_audience")
		newTokenSource = func(ctx context.Context, audience string, opts ...idtoken.ClientOption) (oauth2.TokenSource, error) {
			return &mockToken{token: "", err: fmt.Errorf("failed")}, nil
		}
		token, err := getToken(ctx, "https://test.com", false, l)
		require.NotNil(t, err)
		require.Empty(t, token)
		require.Equal(t, "error getting identity token: failed", err.Error())
	})
	t.Run("success", func(t *testing.T) {
		os.Setenv(ServiceAccountJson, "{}")
		os.Setenv(ServiceAccountAudience, "dummy_audience")
		newTokenSource = func(ctx context.Context, audience string, opts ...idtoken.ClientOption) (oauth2.TokenSource, error) {
			return &mockToken{token: "token"}, nil
		}
		token, err := getToken(ctx, "https://test.com", false, l)
		require.Nil(t, err)
		require.Equal(t, "Bearer token", token)
	})
}

type mockToken struct {
	token string
	err   error
}

func (tk *mockToken) Token() (*oauth2.Token, error) {
	if tk.err != nil {
		return nil, tk.err
	}
	return &oauth2.Token{AccessToken: tk.token}, nil
}
