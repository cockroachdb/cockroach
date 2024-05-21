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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
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
	t.Run("UpdatePrometheusTargets fails with 400", func(t *testing.T) {
		c.httpPut = func(ctx context.Context, reqUrl string, h *http.Header, body io.Reader) (
			resp *http.Response, err error) {
			require.Equal(t, getUrl(promUrl, "c1"), reqUrl)
			return &http.Response{
				StatusCode: 400,
				Body:       io.NopCloser(strings.NewReader("failed")),
			}, nil
		}
		err := c.UpdatePrometheusTargets(ctx, promUrl, "c1", false,
			map[int]*NodeInfo{1: {Target: "n1"}}, true, l)
		require.NotNil(t, err)
		require.Equal(t, "request failed with status 400 and error failed", err.Error())
	})
	t.Run("UpdatePrometheusTargets succeeds", func(t *testing.T) {
		nodeInfos := map[int]*NodeInfo{1: {Target: "n1"}, 3: {
			Target:       "n3",
			CustomLabels: map[string]string{"custom": "label"},
		}}
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
				nodeID, err := strconv.Atoi(c.Labels["node"])
				require.NoError(t, err)
				require.Equal(t, nodeInfos[nodeID].Target, c.Targets[0])
				require.Equal(t, "system", c.Labels["tenant"])
				require.Equal(t, "cockroachdb", c.Labels["job"])
				for k, v := range nodeInfos[nodeID].CustomLabels {
					require.Equal(t, v, c.Labels[k])
				}
			}
			return &http.Response{
				StatusCode: 200,
			}, nil
		}
		err := c.UpdatePrometheusTargets(ctx, promUrl, "c1", false, nodeInfos, true, l)
		require.Nil(t, err)
	})
}

func Test_getPrometheusTargets(t *testing.T) {
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
	t.Run("getClusterConfig fails with 400", func(t *testing.T) {
		c.httpGet = func(ctx context.Context, reqUrl string, h *http.Header) (
			resp *http.Response, err error) {
			require.Equal(t, getUrl(promUrl, "c1"), reqUrl)
			return &http.Response{
				StatusCode: 400,
				Body:       io.NopCloser(strings.NewReader("failed")),
			}, nil
		}
		configs, err := c.getClusterConfig(ctx, promUrl, "c1", false, false, l)
		require.NotNil(t, err)
		require.Equal(t, "request failed with status 400 and error failed", err.Error())
		require.Nil(t, configs)
	})
	t.Run("getClusterConfig succeeds", func(t *testing.T) {
		c.httpGet = func(ctx context.Context, url string, h *http.Header) (
			resp *http.Response, err error) {
			require.Equal(t, getUrl(promUrl, "c1?insecure=true"), url)

			config := &instanceConfig{
				Config:   validConfig,
				Insecure: true,
			}
			b, err := json.Marshal(config)
			require.Nil(t, err)
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewReader(b)),
			}, nil
		}
		configs, err := c.getClusterConfig(ctx, promUrl, "c1", false, true, l)
		require.Equal(t, 2, len(configs))
		require.Equal(t, "10.142.0.133:29001", configs[0].Targets[0])
		require.Equal(t, "10.142.1.10:29001", configs[1].Targets[0])
		require.Equal(t, "10.142.0.133", configs[0].Labels["host_ip"])
		require.Equal(t, "1", configs[1].Labels["node"])
		require.Nil(t, err)
	})
	t.Run("getClusterConfig fails in yaml unmarshalling", func(t *testing.T) {
		c.httpGet = func(ctx context.Context, url string, h *http.Header) (
			resp *http.Response, err error) {
			require.Equal(t, getUrl(promUrl, "c1?insecure=true"), url)
			config := &instanceConfig{
				Config:   `bad`,
				Insecure: true,
			}
			b, err := json.Marshal(config)
			require.Nil(t, err)
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewReader(b)),
			}, nil
		}
		configs, err := c.getClusterConfig(ctx, promUrl, "c1", false, true, l)
		require.Zero(t, len(configs))
		require.Equal(t, "yaml: unmarshal errors:\n  line 1: cannot unmarshal !!str `bad` into []*promhelperclient.CCParams", err.Error())
	})
	t.Run("getClusterConfig fails in json unmarshalling", func(t *testing.T) {
		c.httpGet = func(ctx context.Context, url string, h *http.Header) (
			resp *http.Response, err error) {
			require.Equal(t, getUrl(promUrl, "c1?insecure=true"), url)
			require.Nil(t, err)
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewBufferString("bad")),
			}, nil
		}
		configs, err := c.getClusterConfig(ctx, promUrl, "c1", false, true, l)
		require.Zero(t, len(configs))
		require.Equal(t, "invalid character 'b' looking for beginning of value", err.Error())
	})
}

func TestAppendOrUpdateLabels(t *testing.T) {
	l := func() *logger.Logger {
		l, err := logger.RootLogger("", logger.TeeToStdout)
		if err != nil {
			panic(err)
		}
		return l
	}()
	ctx := context.Background()
	promUrl := "http://prom_url.com"
	t.Run("get cluster config fails", func(t *testing.T) {
		c := NewPromClient()
		c.httpGet = func(ctx context.Context, url string, h *http.Header) (
			resp *http.Response, err error) {
			require.Equal(t, getUrl(promUrl, "c1?insecure=true"), url)
			require.Nil(t, err)
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewBufferString("bad")),
			}, nil
		}
		err := c.AppendOrUpdateLabels(ctx, promUrl, "c1", false, true, nil, l)
		require.Equal(t, "invalid character 'b' looking for beginning of value", err.Error())
	})
	t.Run("invalid node ID in get cluster config", func(t *testing.T) {
		c := NewPromClient()
		c.httpGet = func(ctx context.Context, url string, h *http.Header) (
			resp *http.Response, err error) {
			require.Equal(t, getUrl(promUrl, "c1?insecure=true"), url)
			config := &instanceConfig{
				Config: `- targets:
  - 10.142.0.133:29001
  labels:
    job: cockroachdb
    node: "invalid"`,
				Insecure: true,
			}
			b, err := json.Marshal(config)
			require.Nil(t, err)
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewReader(b)),
			}, nil
		}
		err := c.AppendOrUpdateLabels(ctx, promUrl, "c1", false, true, nil, l)
		require.Equal(t, "strconv.Atoi: parsing \"invalid\": invalid syntax", err.Error())
	})
	t.Run("successful update", func(t *testing.T) {
		c := NewPromClient()
		c.httpGet = func(ctx context.Context, url string, h *http.Header) (
			resp *http.Response, err error) {
			require.Equal(t, getUrl(promUrl, "c1?insecure=true"), url)
			config := &instanceConfig{
				Config:   validConfig,
				Insecure: true,
			}
			b, err := json.Marshal(config)
			require.Nil(t, err)
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewReader(b)),
			}, nil
		}
		newLabels := map[int]map[string]string{
			2: {"newLabel": "withValue"},
			1: {"anotherLabel": "anotherValue", "host_ip": "new_ip"},
		}
		c.httpPut = func(ctx context.Context, url string, h *http.Header, body io.Reader) (resp *http.Response, err error) {
			require.Equal(t, getUrl(promUrl, "c1"), url)
			ir, err := getInstanceConfigRequest(io.NopCloser(body))
			require.Nil(t, err)
			require.NotNil(t, ir.Config)
			configs := make([]*CCParams, 0)
			require.Nil(t, yaml.UnmarshalStrict([]byte(ir.Config), &configs))
			require.Equal(t, 2, len(configs))
			for _, c := range configs {
				nodeID, err := strconv.Atoi(c.Labels["node"])
				require.NoError(t, err)
				for k, v := range newLabels[nodeID] {
					require.Equal(t, v, c.Labels[k])
				}
			}
			return &http.Response{
				StatusCode: 200,
			}, nil
		}
		err := c.AppendOrUpdateLabels(ctx, promUrl, "c1", false, true,
			newLabels, l)
		require.Nil(t, err)
	})
	t.Run("no value passed for update", func(t *testing.T) {
		c := NewPromClient()
		c.httpGet = func(ctx context.Context, url string, h *http.Header) (
			resp *http.Response, err error) {
			require.Equal(t, getUrl(promUrl, "c1?insecure=true"), url)
			config := &instanceConfig{
				Config:   validConfig,
				Insecure: true,
			}
			b, err := json.Marshal(config)
			require.Nil(t, err)
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewReader(b)),
			}, nil
		}
		c.httpPut = func(ctx context.Context, url string, h *http.Header, body io.Reader) (resp *http.Response, err error) {
			return nil, fmt.Errorf("invalid invocation")
		}
		err := c.AppendOrUpdateLabels(ctx, promUrl, "c1", false, true,
			make(map[int]map[string]string), l)
		require.Nil(t, err)
	})
}

func TestRemoveLabels(t *testing.T) {
	l := func() *logger.Logger {
		l, err := logger.RootLogger("", logger.TeeToStdout)
		if err != nil {
			panic(err)
		}
		return l
	}()
	ctx := context.Background()
	promUrl := "http://prom_url.com"
	t.Run("get cluster config fails", func(t *testing.T) {
		c := NewPromClient()
		c.httpGet = func(ctx context.Context, url string, h *http.Header) (
			resp *http.Response, err error) {
			require.Equal(t, getUrl(promUrl, "c1?insecure=true"), url)
			require.Nil(t, err)
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewBufferString("bad")),
			}, nil
		}
		err := c.RemoveLabels(ctx, promUrl, "c1", false, true, nil, l)
		require.Equal(t, "invalid character 'b' looking for beginning of value", err.Error())
	})
	t.Run("invalid node ID in get cluster config", func(t *testing.T) {
		c := NewPromClient()
		c.httpGet = func(ctx context.Context, url string, h *http.Header) (
			resp *http.Response, err error) {
			require.Equal(t, getUrl(promUrl, "c1?insecure=true"), url)
			config := &instanceConfig{
				Config: `- targets:
  - 10.142.0.133:29001
  labels:
    job: cockroachdb
    node: "invalid"`,
				Insecure: true,
			}
			b, err := json.Marshal(config)
			require.Nil(t, err)
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewReader(b)),
			}, nil
		}
		err := c.RemoveLabels(ctx, promUrl, "c1", false, true, nil, l)
		require.Equal(t, "strconv.Atoi: parsing \"invalid\": invalid syntax", err.Error())
	})
	t.Run("successful delete", func(t *testing.T) {
		c := NewPromClient()
		c.httpGet = func(ctx context.Context, url string, h *http.Header) (
			resp *http.Response, err error) {
			require.Equal(t, getUrl(promUrl, "c1?insecure=true"), url)
			config := &instanceConfig{
				Config:   validConfig,
				Insecure: true,
			}
			b, err := json.Marshal(config)
			require.Nil(t, err)
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewReader(b)),
			}, nil
		}
		newLabels := map[int][]string{
			2: {"cluster", "project"},
			1: {"instance"},
		}
		c.httpPut = func(ctx context.Context, url string, h *http.Header, body io.Reader) (resp *http.Response, err error) {
			require.Equal(t, getUrl(promUrl, "c1"), url)
			ir, err := getInstanceConfigRequest(io.NopCloser(body))
			require.Nil(t, err)
			require.NotNil(t, ir.Config)
			configs := make([]*CCParams, 0)
			require.Nil(t, yaml.UnmarshalStrict([]byte(ir.Config), &configs))
			require.Equal(t, 2, len(configs))
			for _, c := range configs {
				nodeID, err := strconv.Atoi(c.Labels["node"])
				require.NoError(t, err)
				for _, k := range newLabels[nodeID] {
					require.Equal(t, "", c.Labels[k])
				}
			}
			return &http.Response{
				StatusCode: 200,
			}, nil
		}
		err := c.RemoveLabels(ctx, promUrl, "c1", false, true,
			newLabels, l)
		require.Nil(t, err)
	})
	t.Run("no value passed for update", func(t *testing.T) {
		c := NewPromClient()
		c.httpGet = func(ctx context.Context, url string, h *http.Header) (
			resp *http.Response, err error) {
			require.Equal(t, getUrl(promUrl, "c1?insecure=true"), url)
			config := &instanceConfig{
				Config:   validConfig,
				Insecure: true,
			}
			b, err := json.Marshal(config)
			require.Nil(t, err)
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewReader(b)),
			}, nil
		}
		c.httpPut = func(ctx context.Context, url string, h *http.Header, body io.Reader) (resp *http.Response, err error) {
			return nil, fmt.Errorf("invalid invocation")
		}
		err := c.RemoveLabels(ctx, promUrl, "c1", false, true,
			make(map[int][]string), l)
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
	t.Run("DeleteClusterConfig fails with 400", func(t *testing.T) {
		c.httpDelete = func(ctx context.Context, url string, h *http.Header) (
			resp *http.Response, err error) {
			require.Equal(t, getUrl(promUrl, "c1"), url)
			return &http.Response{
				StatusCode: 400,
				Body:       io.NopCloser(strings.NewReader("failed")),
			}, nil
		}
		err := c.DeleteClusterConfig(ctx, promUrl, "c1", false, l)
		require.NotNil(t, err)
		require.Equal(t, "request failed with status 400 and error failed", err.Error())
	})
	t.Run("DeleteClusterConfig succeeds", func(t *testing.T) {
		c.httpDelete = func(ctx context.Context, url string, h *http.Header) (
			resp *http.Response, err error) {
			require.Equal(t, getUrl(promUrl, "c1"), url)
			return &http.Response{
				StatusCode: 204,
			}, nil
		}
		err := c.DeleteClusterConfig(ctx, promUrl, "c1", false, l)
		require.Nil(t, err)
	})
}

// getInstanceConfigRequest returns the instanceConfigRequest after parsing the request json
func getInstanceConfigRequest(body io.ReadCloser) (*instanceConfig, error) {
	var insConfigReq instanceConfig
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
		token, err := c.getToken(ctx, "http://test.com", false, l)
		require.Nil(t, err)
		require.Empty(t, token)
	})
	t.Run("invalid credentials", func(t *testing.T) {
		err := os.Setenv(ServiceAccountJson, "{}")
		require.Nil(t, err)
		err = os.Setenv(ServiceAccountAudience, "dummy_audience")
		require.Nil(t, err)
		c.newTokenSource = func(ctx context.Context, audience string, opts ...idtoken.ClientOption) (oauth2.TokenSource, error) {
			return nil, fmt.Errorf("invalid")
		}
		token, err := c.getToken(ctx, "https://test.com", false, l)
		require.NotNil(t, err)
		require.Empty(t, token)
		require.Equal(t, "error creating GCS oauth token source from specified credential: invalid", err.Error())
	})
	t.Run("invalid token", func(t *testing.T) {
		err := os.Setenv(ServiceAccountJson, "{}")
		require.Nil(t, err)
		err = os.Setenv(ServiceAccountAudience, "dummy_audience")
		require.Nil(t, err)
		c.newTokenSource = func(ctx context.Context, audience string, opts ...idtoken.ClientOption) (oauth2.TokenSource, error) {
			return &mockToken{token: "", err: fmt.Errorf("failed")}, nil
		}
		token, err := c.getToken(ctx, "https://test.com", false, l)
		require.NotNil(t, err)
		require.Empty(t, token)
		require.Equal(t, "error getting identity token: failed", err.Error())
	})
	t.Run("success", func(t *testing.T) {
		err := os.Setenv(ServiceAccountJson, "{}")
		require.Nil(t, err)
		err = os.Setenv(ServiceAccountAudience, "dummy_audience")
		require.Nil(t, err)
		c.newTokenSource = func(ctx context.Context, audience string, opts ...idtoken.ClientOption) (oauth2.TokenSource, error) {
			return &mockToken{token: "token"}, nil
		}
		token, err := c.getToken(ctx, "https://test.com", false, l)
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

const validConfig = `- targets:
  - 10.142.0.133:29001
  labels:
    cluster: bhaskarbora-test
    host_ip: 10.142.0.133
    instance: bhaskarbora-test-0002
    job: cockroachdb
    node: "2"
    project: cockroach-ephemeral
    region: us-east1
    tenant: system
    zone: us-east1-b
- targets:
  - 10.142.1.10:29001
  labels:
    cluster: bhaskarbora-test
    host_ip: 10.142.1.10
    instance: bhaskarbora-test-0001
    job: cockroachdb
    node: "1"
    project: cockroach-ephemeral
    region: us-east1
    tenant: system
    zone: us-east1-b`
