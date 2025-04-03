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
	"golang.org/x/oauth2"
	"google.golang.org/api/idtoken"
	"gopkg.in/yaml.v2"
)

const (
	resourceName    = "instance-configs"
	resourceVersion = "v1"

	// ErrorMessage is the generic error message used to return an error
	// when a requests to the prometheus helper service yields a non 200 status.
	ErrorMessage       = ErrorMessagePrefix + ` on url %s and error %s`
	ErrorMessagePrefix = "request failed with status %d"
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

// IsNotFoundError returns true if the error is a 404 error.
func IsNotFoundError(err error) bool {
	return strings.Contains(err.Error(), fmt.Sprintf(ErrorMessagePrefix, http.StatusNotFound))
}

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
	nodes map[int][]*NodeInfo,
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
		return errors.Newf(ErrorMessage, response.StatusCode, url, string(body))
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
	h := &http.Header{}
	h.Set("Authorization", token)
	response, err := c.httpDelete(ctx, url, h)
	if err != nil {
		return errors.Wrapf(err, "DeleteClusterConfig: failed on url: %s", url)
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
		return errors.Newf(ErrorMessage, response.StatusCode, url, string(body))
	}
	return nil
}

func getUrl(promUrl, clusterName string) string {
	return fmt.Sprintf("%s/%s/%s/%s", promUrl, resourceVersion, resourceName, clusterName)
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
func buildCreateRequest(nodes map[int][]*NodeInfo, insecure bool) (io.Reader, error) {
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
	if credSrc, err := roachprodutil.SetServiceAccountCredsEnv(ctx, forceFetchCreds); err != nil {
		return "", err
	} else {
		l.Printf("Prometheus credentials obtained from %s", credSrc)
	}
	token, err := roachprodutil.GetServiceAccountToken(ctx, c.newTokenSource)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Bearer %s", token), nil
}
