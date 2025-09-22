// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/roachprodutil"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/aws"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/azure"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v2"
)

const (
	resourceName    = "instance-configs"
	resourceVersion = "v1"

	// ErrorMessage is the generic error message used to return an error
	// when a requests to the prometheus helper service yields a non 200 status.
	ErrorMessage = `request failed with status %d on url %s and error %s`

	// OauthClientID is the client ID for the OAuth client.
	OAuthClientID = "1063333028845-p47csl1ukrgnpnnjc7lrtrto6uqs9t37.apps.googleusercontent.com"
	// ServiceAccountEmail is the service account email to impersonate to access
	// the Identity-Aware Proxy protected backend.
	ServiceAccountEmail = "prom-helper-service@cockroach-testeng-infra.iam.gserviceaccount.com"
)

// CloudEnvironment is the environment of Cloud provider.
// In GCE, this would be the project, in AWS, this would be the account ID,
// and in Azure, this would be the subscription ID.
type CloudEnvironment string

const (
	Default CloudEnvironment = "default"
)

// Reachability is the reachability of the node provider.
type Reachability int

const (
	// None indicates that the node is unreachable with this provider.
	None Reachability = iota
	// Private indicates that the node is reachable via private network.
	Private
	// Public indicates that the node is only reachable via public network.
	Public
)

var (
	// The URL for the Prometheus registration service. An empty string means
	// that the Prometheus integration is disabled. Should be accessed through
	// getPrometheusRegistrationUrl().
	promRegistrationUrl = config.EnvOrDefaultString(
		"ROACHPROD_PROM_HOST_URL",
		"https://grafana.testeng.crdb.io/promhelpers",
	)
	// supportedPromProviders are the providers supported for prometheus target
	// and their reachability.
	supportedPromProviders = map[string]map[CloudEnvironment]Reachability{
		gce.ProviderName: {
			Default:               Public,
			"cockroach-ephemeral": Private,
		},
		aws.ProviderName: {
			Default: Public,
		},
		azure.ProviderName: {
			Default: Public,
		},
	}
)

// PromClient is used to communicate with the prometheus helper service
// keeping the functions as a variable enables us to override the value for unit testing
type PromClient struct {
	promUrl  string
	disabled bool

	// client is the http client
	httpClient *http.Client
}

// NewPromClient returns a new instance of PromClient
func NewPromClient(options ...Option) (*PromClient, error) {
	c := &PromClient{
		promUrl:  promRegistrationUrl,
		disabled: promRegistrationUrl == "",
	}

	for _, option := range options {
		option.apply(c)
	}

	// If the client is not set, create a new client
	if c.httpClient == nil {
		iapTokenSource, err := roachprodutil.NewIAPTokenSource(roachprodutil.IAPTokenSourceOptions{
			OAuthClientID:       OAuthClientID,
			ServiceAccountEmail: ServiceAccountEmail,
		})
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create IAP token source")
		}
		c.httpClient = iapTokenSource.GetHTTPClient()
	}

	return c, nil
}

// UpdatePrometheusTargets updates the cluster config in the promUrl
func (c *PromClient) UpdatePrometheusTargets(
	ctx context.Context, clusterName string, nodeTargets NodeTargets, l *logger.Logger,
) error {
	if c.disabled {
		l.Printf("Prometheus registration is disabled")
		return nil
	}

	request, err := c.buildCreateRequest(ctx, clusterName, nodeTargets)
	if err != nil {
		return err
	}

	l.Printf("invoking PUT for URL: %s", request.URL)

	err = c.makeRequest(request)
	if err != nil {
		return errors.Wrapf(err, "UpdatePrometheusTargets: failed on url: %s", request.URL)
	}
	return nil
}

// DeleteClusterConfig deletes the cluster config in the promUrl
func (c *PromClient) DeleteClusterConfig(
	ctx context.Context, clusterName string, l *logger.Logger,
) error {

	if c.disabled {
		return nil
	}

	request, err := c.buildDeleteRequest(ctx, clusterName)
	if err != nil {
		return err
	}

	err = c.makeRequest(request)
	if err != nil {
		return errors.Wrapf(err, "DeleteClusterConfig: failed on url: %s", request.URL)
	}
	return nil
}

// ProviderReachability returns the reachability of the provider
func ProviderReachability(provider string, cloudEnvironment CloudEnvironment) Reachability {

	// If the provider is not supported, return None.
	providerReachability, ok := supportedPromProviders[provider]
	if !ok {
		return None
	}

	// If the cloudEnvironment is supported and has a specific reachability
	// defined for the specified CloudEnvironment, return this reachability.
	if reachability, ok := providerReachability[cloudEnvironment]; ok {
		return reachability
	}

	// Return the default reachability for the provider.
	return providerReachability[Default]
}

// getUrl returns the URL for the prometheus helper service for a given cluster
func (c *PromClient) getUrl(clusterName string) string {
	return fmt.Sprintf("%s/%s/%s/%s", c.promUrl, resourceVersion, resourceName, clusterName)
}

// makeRequest makes the http request and returns an error with the body if an error occurs
func (c *PromClient) makeRequest(request *http.Request) error {
	response, err := c.httpClient.Do(request)
	if err != nil {
		return err
	}

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return err
		}
		return errors.Newf(ErrorMessage, response.StatusCode, request.URL, string(body))
	}
	return nil
}

// buildDeleteRequest creates the http.Request to delete the cluster config
func (c *PromClient) buildDeleteRequest(
	ctx context.Context, clusterName string,
) (*http.Request, error) {
	return http.NewRequestWithContext(ctx, http.MethodDelete, c.getUrl(clusterName), nil)
}

// NodeTargets contains prometheus scrape targets for each node.
type NodeTargets map[int][]*NodeInfo

func (nt NodeTargets) String() string {
	var parts []string
	for port, infos := range nt {
		var targets []string
		for _, info := range infos {
			targets = append(targets, info.Target)
		}
		parts = append(parts, fmt.Sprintf("%d:[%s]", port, strings.Join(targets, ",")))
	}
	return strings.Join(parts, " ")
}

// buildCreateRequest builds the http.Request to create the cluster config
func (c *PromClient) buildCreateRequest(
	ctx context.Context, clusterName string, nodes NodeTargets,
) (*http.Request, error) {
	body, err := c.buildCreateRequestBody(nodes)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.getUrl(clusterName), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", httputil.JSONContentType)
	return req, nil
}

// createBuildRequestBody creates the cluster config file per node
func (c *PromClient) buildCreateRequestBody(nodes NodeTargets) (io.Reader, error) {
	configs := make([]*CCParams, 0)
	for _, n := range nodes {
		for _, node := range n {
			params := &CCParams{
				Targets: []string{node.Target},
				Labels:  map[string]string{},
			}
			// custom labels - this can override the default labels if needed
			for n, v := range node.CustomLabels {
				params.Labels[n] = v
			}
			configs = append(configs, params)
		}
	}
	cb, err := yaml.Marshal(&configs)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(&instanceConfigRequest{
		Config: string(cb),
	})
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(b), nil
}

// instanceConfigRequest is the HTTP request received for generating instance config
type instanceConfigRequest struct {
	//Config is the content of the yaml file
	Config   string `json:"config"`
	Insecure bool   `json:"insecure"`
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

type Option interface {
	apply(*PromClient)
}

type OptionFunc func(*PromClient)

func (o OptionFunc) apply(c *PromClient) { o(c) }

func WithIAPTokenSource(tokenSource roachprodutil.IAPTokenSource) OptionFunc {
	return func(c *PromClient) {
		c.httpClient = tokenSource.GetHTTPClient()
	}
}

func WithCustomURL(url string) OptionFunc {
	return func(c *PromClient) {
		c.promUrl = url
		c.disabled = false
	}
}
