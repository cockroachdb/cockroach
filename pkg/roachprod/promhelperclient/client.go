// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Utility to connect and invoke APIs of the promhelperservice.
// Doc reference - https://cockroachlabs.atlassian.net/wiki/x/MAAlzg

package promhelperclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/errors"
	"golang.org/x/oauth2"
	"google.golang.org/api/idtoken"
	"gopkg.in/yaml.v2"
)

const (
	resourceName           = "instance-configs"
	resourceVersion        = "v1"
	serviceAccountJson     = "PROM_HELPER_SERVICE_ACCOUNT_JSON"
	serviceAccountAudience = "PROM_HELPER_SERVICE_ACCOUNT_AUDIENCE"
)

// SupportedPromProjects are the projects supported for prometheus target
var SupportedPromProjects = map[string]struct{}{gce.DefaultProject(): {}}

// The URL for the Prometheus registration service. An empty string means that the
// Prometheus integration is disabled. Should be accessed through
// getPrometheusRegistrationUrl().
var promRegistrationUrl = config.EnvOrDefaultString("ROACHPROD_PROM_HOST_URL",
	"https://grafana.testeng.crdb.io/promhelpers")

// PromClient is used to communicate with the prometheus helper service
// keeping the functions as a variable enables us to override the value for unit testing
type PromClient struct {
	promUrl  string
	disabled bool

	// httpPut is used for http PUT operation.
	httpPut func(
		ctx context.Context, url string, h *http.Header, body io.Reader,
	) (resp *http.Response, err error)
	// httpDelete is used for http DELETE operation.
	httpDelete func(ctx context.Context, url string, h *http.Header) (
		resp *http.Response, err error)
	// newTokenSource is the token generator source.
	newTokenSource func(ctx context.Context, audience string, opts ...idtoken.ClientOption) (
		oauth2.TokenSource, error)
}

// DefaultPromClient is the default instance of PromClient. This instance should
// be used unless custom configuration is needed.
var DefaultPromClient = NewPromClient()

// NewPromClient returns a new instance of PromClient
func NewPromClient() *PromClient {
	return &PromClient{
		promUrl:        promRegistrationUrl,
		disabled:       promRegistrationUrl == "",
		httpPut:        httputil.Put,
		httpDelete:     httputil.Delete,
		newTokenSource: idtoken.NewTokenSource,
	}
}

func (c *PromClient) setUrl(url string) {
	c.promUrl = url
	c.disabled = false
}

// instanceConfigRequest is the HTTP request received for generating instance config
type instanceConfigRequest struct {
	//Config is the content of the yaml file
	Config   string `json:"config"`
	Insecure bool   `json:"insecure"`
}

// UpdatePrometheusTargets updates the cluster config in the promUrl
func (c *PromClient) UpdatePrometheusTargets(
	ctx context.Context,
	clusterName string,
	forceFetchCreds bool,
	nodes map[int]*NodeInfo,
	insecure bool,
	l *logger.Logger,
) error {
	if c.disabled {
		l.Printf("Prometheus registration is disabled")
		return nil
	}
	req, err := buildCreateRequest(nodes, insecure)
	if err != nil {
		return err
	}
	token, err := c.getToken(ctx, forceFetchCreds, l)
	if err != nil {
		return err
	}
	url := getUrl(c.promUrl, clusterName)
	l.Printf("invoking PUT for URL: %s", url)
	h := &http.Header{}
	h.Set("ContentType", "application/json")
	if token != "" {
		h.Set("Authorization", token)
	}
	response, err := c.httpPut(ctx, url, h, req)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		defer func() { _ = response.Body.Close() }()
		if response.StatusCode == http.StatusUnauthorized && !forceFetchCreds {
			l.Printf("request failed - this may be due to a stale token. retrying with forceFetchCreds true ...")
			return c.UpdatePrometheusTargets(ctx, clusterName, true, nodes, insecure, l)
		}
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return err
		}
		return errors.Newf("request failed with status %d and error %s", response.StatusCode,
			string(body))
	}
	return nil
}

// DeleteClusterConfig deletes the cluster config in the promUrl
func (c *PromClient) DeleteClusterConfig(
	ctx context.Context, clusterName string, forceFetchCreds, insecure bool, l *logger.Logger,
) error {

	if c.disabled {
		return nil
	}
	token, err := c.getToken(ctx, forceFetchCreds, l)
	if err != nil {
		return err
	}
	url := getUrl(c.promUrl, clusterName)
	if insecure {
		url = fmt.Sprintf("%s?insecure=true", url)
	}
	l.Printf("invoking DELETE for URL: %s", url)
	h := &http.Header{}
	h.Set("Authorization", token)
	response, err := c.httpDelete(ctx, url, h)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusNoContent {
		defer func() { _ = response.Body.Close() }()
		if response.StatusCode == http.StatusUnauthorized && !forceFetchCreds {
			return c.DeleteClusterConfig(ctx, clusterName, true, insecure, l)
		}
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return err
		}
		return errors.Newf("request failed with status %d and error %s", response.StatusCode,
			string(body))
	}
	return nil
}

func getUrl(promUrl, clusterName string) string {
	return fmt.Sprintf("%s/%s/%s/%s", promUrl, resourceVersion, resourceName, clusterName)
}

// CCParams are the params for the cluster configs
type CCParams struct {
	Targets []string          `yaml:"targets"`
	Labels  map[string]string `yaml:"labels"`
}

// NodeInfo contains the target and labels for the node
type NodeInfo struct {
	Target       string            // Name of the node
	CustomLabels map[string]string // Custom labels to be added to the cluster config
}

// createClusterConfigFile creates the cluster config file per node
func buildCreateRequest(nodes map[int]*NodeInfo, insecure bool) (io.Reader, error) {
	configs := make([]*CCParams, 0)
	for i, n := range nodes {
		params := &CCParams{
			Targets: []string{n.Target},
			Labels:  map[string]string{"node": strconv.Itoa(i)},
		}
		// custom labels - this can override the default labels if needed
		for n, v := range n.CustomLabels {
			params.Labels[n] = v
		}
		configs = append(configs, params)
	}
	cb, err := yaml.Marshal(&configs)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(&instanceConfigRequest{Config: string(cb), Insecure: insecure})
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(b), nil
}

// getToken gets the Authorization token for grafana
func (c *PromClient) getToken(
	ctx context.Context, forceFetchCreds bool, l *logger.Logger,
) (string, error) {
	if strings.HasPrefix(c.promUrl, "http:/") {
		// no token needed for insecure URL
		return "", nil
	}
	// Read in the service account key and audience, so we can retrieve the identity token.
	if _, err := setPromHelperCredsEnv(ctx, forceFetchCreds, l); err != nil {
		return "", err
	}
	grafanaKey := os.Getenv(serviceAccountJson)
	grafanaAudience := os.Getenv(serviceAccountAudience)
	ts, err := c.newTokenSource(ctx, grafanaAudience, idtoken.WithCredentialsJSON([]byte(grafanaKey)))
	if err != nil {
		return "", errors.Wrap(err, "error creating GCS oauth token source from specified credential")
	}
	token, err := ts.Token()
	if err != nil {
		return "", errors.Wrap(err, "error getting identity token")
	}
	return fmt.Sprintf("Bearer %s", token.AccessToken), nil
}
