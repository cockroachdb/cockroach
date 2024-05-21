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

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/errors"
	"golang.org/x/oauth2"
	"google.golang.org/api/idtoken"
	"gopkg.in/yaml.v2"
)

const (
	resourceName    = "instance-configs"
	resourceVersion = "v1"

	ServiceAccountJson     = "PROM_HELPER_SERVICE_ACCOUNT_JSON"
	ServiceAccountAudience = "PROM_HELPER_SERVICE_ACCOUNT_AUDIENCE"
)

// PromClient is used to communicate with the prometheus helper service
// keeping the functions as a variable enables us to override the value for unit testing
type PromClient struct {
	// httpPut is used for http PUT operation.
	httpPut func(
		ctx context.Context, url string, h *http.Header, body io.Reader,
	) (resp *http.Response, err error)
	// httpDelete is used for http DELETE operation.
	httpDelete func(ctx context.Context, url string, h *http.Header) (
		resp *http.Response, err error)
	// httpGet is used for http GET operation.
	httpGet func(ctx context.Context, url string, h *http.Header) (resp *http.Response, err error)
	// newTokenSource is the token generator source.
	newTokenSource func(ctx context.Context, audience string, opts ...idtoken.ClientOption) (
		oauth2.TokenSource, error)
}

// NewPromClient returns a new instance of PromClient
func NewPromClient() *PromClient {
	return &PromClient{
		httpPut:        httputil.Put,
		httpDelete:     httputil.Delete,
		httpGet:        httputil.GetWithHeaders,
		newTokenSource: idtoken.NewTokenSource,
	}
}

// instanceConfig is the HTTP request received for generating instance config
type instanceConfig struct {
	//Config is the content of the yaml file
	Config   string `json:"config"`
	Insecure bool   `json:"insecure"`
}

// UpdatePrometheusTargets updates the cluster config in the promUrl
func (c *PromClient) UpdatePrometheusTargets(
	ctx context.Context,
	promUrl, clusterName string,
	forceFetchCreds bool,
	nodes map[int]*NodeInfo,
	insecure bool,
	l *logger.Logger,
) error {
	req, err := buildCreateRequest(nodes, insecure)
	if err != nil {
		return err
	}
	return c.updatePromTargets(ctx, promUrl, clusterName, forceFetchCreds, insecure, req, l)
}

// updatePromTargets is an internal function to update the prometheus config
// This takes the req as io.Reader.
func (c *PromClient) updatePromTargets(
	ctx context.Context,
	promUrl string,
	clusterName string,
	forceFetchCreds bool,
	insecure bool,
	req io.Reader,
	l *logger.Logger,
) error {
	token, err := c.getToken(ctx, promUrl, forceFetchCreds, l)
	if err != nil {
		return err
	}
	url := getUrl(promUrl, clusterName)
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
			return c.updatePromTargets(ctx, promUrl, clusterName, true, insecure, req, l)
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
	ctx context.Context, promUrl, clusterName string, forceFetchCreds bool, l *logger.Logger,
) error {
	token, err := c.getToken(ctx, promUrl, forceFetchCreds, l)
	if err != nil {
		return err
	}
	url := getUrl(promUrl, clusterName)
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
			return c.DeleteClusterConfig(ctx, promUrl, clusterName, true, l)
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

// AppendOrUpdateLabels appends or updates the labels in the cluster config. This reads the current
// cluster config and modifies the labels and then, updates the config
func (c *PromClient) AppendOrUpdateLabels(
	ctx context.Context,
	promUrl, clusterName string,
	forceFetchCreds, insecure bool,
	nodeLabels map[int]map[string]string,
	l *logger.Logger,
) error {
	ccParams, err := c.getClusterConfig(ctx, promUrl, clusterName, forceFetchCreds, insecure, l)
	if err != nil {
		return err
	}
	updateNeeded := false
	for i := range ccParams {
		// extract the node ID
		nodeID, err := strconv.Atoi(ccParams[i].Labels["node"])
		if err != nil {
			return err
		}
		// check if the node ID has and entry for add/update labels
		if labels, ok := nodeLabels[nodeID]; ok {
			for k, v := range labels {
				// add/overwrite the label
				ccParams[i].Labels[k] = v
				updateNeeded = true
			}
		}
	}
	if !updateNeeded {
		l.Printf("No update needed for cluster config")
		return nil
	}
	req, err := marshalCreateRequest(ccParams, insecure)
	if err != nil {
		return err
	}
	return c.updatePromTargets(ctx, promUrl, clusterName, forceFetchCreds, insecure, req, l)
}

// RemoveLabels removes the labels in the cluster config. This reads the current
// cluster config and delete the labels and then, updates the config
func (c *PromClient) RemoveLabels(
	ctx context.Context,
	promUrl, clusterName string,
	forceFetchCreds, insecure bool,
	nodeLabelKeys map[int][]string,
	l *logger.Logger,
) error {
	ccParams, err := c.getClusterConfig(ctx, promUrl, clusterName, forceFetchCreds, insecure, l)
	if err != nil {
		return err
	}
	updateNeeded := false
	for i := range ccParams {
		// extract the node ID
		nodeID, err := strconv.Atoi(ccParams[i].Labels["node"])
		if err != nil {
			return err
		}
		// check if the node ID has and entry for add/update labels
		if labels, ok := nodeLabelKeys[nodeID]; ok {
			for _, k := range labels {
				// add/overwrite the label
				delete(ccParams[i].Labels, k)
				updateNeeded = true
			}
		}
	}
	if !updateNeeded {
		l.Printf("No delete needed for cluster config")
		return nil
	}
	req, err := marshalCreateRequest(ccParams, insecure)
	if err != nil {
		return err
	}
	return c.updatePromTargets(ctx, promUrl, clusterName, forceFetchCreds, insecure, req, l)
}

// getClusterConfig gets the cluster config in the promUrl
func (c *PromClient) getClusterConfig(
	ctx context.Context,
	promUrl, clusterName string,
	forceFetchCreds, insecure bool,
	l *logger.Logger,
) ([]*CCParams, error) {
	token, err := c.getToken(ctx, promUrl, forceFetchCreds, l)
	if err != nil {
		return nil, err
	}
	url := getUrl(promUrl, clusterName)
	if insecure {
		// add the query parameter as insecure
		url = fmt.Sprintf("%s?insecure=true", url)
	}
	l.Printf("invoking GET for URL: %s", url)
	h := &http.Header{}
	h.Set("Authorization", token)
	response, err := c.httpGet(ctx, url, h)
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		defer func() { _ = response.Body.Close() }()
		if response.StatusCode == http.StatusUnauthorized && !forceFetchCreds {
			return c.getClusterConfig(ctx, promUrl, clusterName, true, insecure, l)
		}
		return nil, errors.Newf("request failed with status %d and error %s", response.StatusCode,
			string(body))
	}
	res := &instanceConfig{}
	if err = json.Unmarshal(body, res); err != nil {
		return nil, err
	}
	configs := make([]*CCParams, 0)
	if err = yaml.UnmarshalStrict([]byte(res.Config), &configs); err != nil {
		return nil, err
	}
	return configs, nil
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
			Labels: map[string]string{
				// default labels
				"node":   strconv.Itoa(i),
				"tenant": install.SystemInterfaceName,
				"job":    "cockroachdb",
			},
		}
		// custom labels - this can override the default labels if needed
		for n, v := range n.CustomLabels {
			params.Labels[n] = v
		}
		configs = append(configs, params)
	}
	return marshalCreateRequest(configs, insecure)
}

// marshalCreateRequest creates the request from list of CCParams
func marshalCreateRequest(configs []*CCParams, insecure bool) (io.Reader, error) {
	cb, err := yaml.Marshal(&configs)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(&instanceConfig{Config: string(cb), Insecure: insecure})
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(b), nil
}

// getToken gets the Authorization token for grafana
func (c *PromClient) getToken(
	ctx context.Context, promUrl string, forceFetchCreds bool, l *logger.Logger,
) (string, error) {
	if strings.HasPrefix(promUrl, "http:/") {
		// no token needed for insecure URL
		return "", nil
	}
	// Read in the service account key and audience, so we can retrieve the identity token.
	if _, err := SetPromHelperCredsEnv(ctx, forceFetchCreds, l); err != nil {
		return "", err
	}
	grafanaKey := os.Getenv(ServiceAccountJson)
	grafanaAudience := os.Getenv(ServiceAccountAudience)
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
