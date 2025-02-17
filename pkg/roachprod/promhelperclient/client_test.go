// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/roachprod/roachprodutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"google.golang.org/api/idtoken"
	"gopkg.in/yaml.v2"
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
	c := NewPromClient()
	c.setUrl(promUrl)
	t.Run("UpdatePrometheusTargets fails with 400", func(t *testing.T) {
		c.httpPut = func(ctx context.Context, reqUrl string, h *http.Header, body io.Reader) (
			resp *http.Response, err error) {
			require.Equal(t, getUrl(promUrl, "c1"), reqUrl)
			return &http.Response{
				StatusCode: 400,
				Body:       io.NopCloser(strings.NewReader("failed")),
			}, nil
		}
		err := c.UpdatePrometheusTargets(ctx, "c1", false,
			map[int][]*NodeInfo{1: {{Target: "n1"}}}, true, l)
		require.NotNil(t, err)
		require.Equal(t, fmt.Sprintf(ErrorMessage, 400, getUrl(promUrl, "c1"), "failed"), err.Error())
	})
	t.Run("UpdatePrometheusTargets succeeds", func(t *testing.T) {
		nodeInfos := map[int][]*NodeInfo{
			1: {{
				Target: "n1",
			}},
			3: {{
				Target:       "n3",
				CustomLabels: map[string]string{"custom": "label"},
			}},
		}
		c.httpPut = func(ctx context.Context, url string, h *http.Header, body io.Reader) (
			resp *http.Response, err error) {
			require.Equal(t, getUrl(promUrl, "c1"), url)
			ir, err := getInstanceConfigRequest(io.NopCloser(body))
			require.Nil(t, err)
			require.NotNil(t, ir.Config)
			configs := make([]*CCParams, 0)
			require.Nil(t, yaml.UnmarshalStrict([]byte(ir.Config), &configs))
			require.Len(t, configs, 2)
			for _, c := range configs {
				if c.Targets[0] == "n1" {
					require.Empty(t, nodeInfos[1][0].CustomLabels)
				} else {
					require.Equal(t, "n3", c.Targets[0])
					for k, v := range nodeInfos[3][0].CustomLabels {
						require.Equal(t, v, c.Labels[k])
					}
				}
			}
			return &http.Response{
				StatusCode: 200,
			}, nil
		}
		err := c.UpdatePrometheusTargets(ctx, "c1", false, nodeInfos, true, l)
		require.Nil(t, err)
	})
}

func TestDeleteClusterConfig(t *testing.T) {
	l := func() *logger.Logger {
		l, err := logger.RootLogger(filepath.Join(t.TempDir(), "test.log"), logger.TeeToStdout)
		if err != nil {
			panic(err)
		}
		return l
	}()
	ctx := context.Background()
	promUrl := "http://prom_url.com"
	c := NewPromClient()
	c.setUrl(promUrl)
	t.Run("DeleteClusterConfig fails with 400", func(t *testing.T) {
		c.httpDelete = func(ctx context.Context, url string, h *http.Header) (
			resp *http.Response, err error) {
			require.Equal(t, getUrl(promUrl, "c1"), url)
			return &http.Response{
				StatusCode: 400,
				Body:       io.NopCloser(strings.NewReader("failed")),
			}, nil
		}
		err := c.DeleteClusterConfig(ctx, "c1", false, false, l)
		require.NotNil(t, err)
		require.Equal(
			t,
			fmt.Sprintf(ErrorMessage, 400, "http://prom_url.com/v1/instance-configs/c1", "failed"),
			err.Error(),
		)
	})
	t.Run("DeleteClusterConfig succeeds", func(t *testing.T) {
		c.httpDelete = func(ctx context.Context, url string, h *http.Header) (
			resp *http.Response, err error) {
			require.Equal(t, getUrl(promUrl, "c1"), url)
			return &http.Response{
				StatusCode: 204,
			}, nil
		}
		err := c.DeleteClusterConfig(ctx, "c1", false, false /* insecure */, l)
		require.Nil(t, err)
	})
	t.Run("DeleteClusterConfig insecure succeeds", func(t *testing.T) {
		c.httpDelete = func(ctx context.Context, url string, h *http.Header) (
			resp *http.Response, err error) {
			require.Equal(t, fmt.Sprintf("%s?insecure=true", getUrl(promUrl, "c1")), url)
			return &http.Response{
				StatusCode: 204,
			}, nil
		}
		err := c.DeleteClusterConfig(ctx, "c1", false, true /* insecure */, l)
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
	c := NewPromClient()
	t.Run("insecure url", func(t *testing.T) {
		c.setUrl("http://test.com")
		token, err := c.getToken(ctx, false, l)
		require.Nil(t, err)
		require.Empty(t, token)
	})
	t.Run("invalid credentials", func(t *testing.T) {
		err := os.Setenv(roachprodutil.ServiceAccountJson, "{}")
		require.Nil(t, err)
		err = os.Setenv(roachprodutil.ServiceAccountAudience, "dummy_audience")
		require.Nil(t, err)
		c.newTokenSource = func(ctx context.Context, audience string, opts ...idtoken.ClientOption) (oauth2.TokenSource, error) {
			return nil, fmt.Errorf("invalid")
		}
		c.setUrl("https://test.com")
		token, err := c.getToken(ctx, false, l)
		require.NotNil(t, err)
		require.Empty(t, token)
		require.Equal(t, "error creating GCS oauth token source from specified credential: invalid", err.Error())
	})
	t.Run("invalid token", func(t *testing.T) {
		err := os.Setenv(roachprodutil.ServiceAccountJson, "{}")
		require.Nil(t, err)
		err = os.Setenv(roachprodutil.ServiceAccountAudience, "dummy_audience")
		require.Nil(t, err)
		c.newTokenSource = func(ctx context.Context, audience string, opts ...idtoken.ClientOption) (oauth2.TokenSource, error) {
			return &mockToken{token: "", err: fmt.Errorf("failed")}, nil
		}
		c.setUrl("https://test.com")
		token, err := c.getToken(ctx, false, l)
		require.NotNil(t, err)
		require.Empty(t, token)
		require.Equal(t, "error getting identity token: failed", err.Error())
	})
	t.Run("success", func(t *testing.T) {
		err := os.Setenv(roachprodutil.ServiceAccountJson, "{}")
		require.Nil(t, err)
		err = os.Setenv(roachprodutil.ServiceAccountAudience, "dummy_audience")
		require.Nil(t, err)
		c.newTokenSource = func(ctx context.Context, audience string, opts ...idtoken.ClientOption) (oauth2.TokenSource, error) {
			return &mockToken{token: "token"}, nil
		}
		c.setUrl("https://test.com")
		token, err := c.getToken(ctx, false, l)
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

func TestIsNotFoundError(t *testing.T) {
	t.Run("IsNotFoundError true", func(t *testing.T) {
		err := fmt.Errorf(ErrorMessage, 404, "http://prom_url.com/v1/instance-configs/c1", "failed")
		require.True(t, IsNotFoundError(err))
	})
	t.Run("IsNotFoundError false", func(t *testing.T) {
		err := fmt.Errorf(ErrorMessage, 500, "http://prom_url.com/v1/instance-configs/c1", "failed")
		require.False(t, IsNotFoundError(err))
	})
}
