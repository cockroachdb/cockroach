// Copyright 2024 The Cockroach Authors.

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
	"text/template"

	"github.com/cockroachdb/errors"
	"google.golang.org/api/idtoken"

	"github.com/cockroachdb/cockroach/pkg/util/httputil"
)

const (
	resourceName    = "instance-configs"
	resourceVersion = "v1"

	ServiceAccountJson     = "GRAFANA_SERVICE_ACCOUNT_JSON"
	ServiceAccountAudience = "GRAFANA_SERVICE_ACCOUNT_AUDIENCE"
)

// Node has the configuration of a specific node of a cluster
type Node struct {
	NodeID string `json:"node_id"`
	// Targets are the list of targets in the host:port format
	Targets []string `json:"targets"`
	// Labels are the labels to be added for the node. Note that the "node" label is automatically added from NodeID
	Labels map[string]string `json:"labels"`
}

// instanceConfigRequest is the HTTP request received for generating instance config
type instanceConfigRequest struct {
	//Config is the content of the yaml file
	Config string `json:"config"`
}

// UpdatePrometheusTargets updates the cluster config in the promUrl
func UpdatePrometheusTargets(ctx context.Context, promUrl, clusterName string, nodes []string) error {
	req, err := buildCreateRequest(nodes)
	if err != nil {
		return err
	}
	token, err := getToken(ctx, promUrl)
	if err != nil {
		return err
	}
	response, err := httputil.Put(ctx, getUrl(promUrl, clusterName), &httputil.RequestHeaders{
		ContentType:   "application/json",
		Authorization: token,
	}, req)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		defer response.Body.Close()
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
func DeleteClusterConfig(ctx context.Context, promUrl, clusterName string) error {
	token, err := getToken(ctx, promUrl)
	if err != nil {
		return err
	}
	response, err := httputil.Delete(ctx, getUrl(promUrl, clusterName), &httputil.RequestHeaders{
		Authorization: token,
	})
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusNoContent {
		defer response.Body.Close()
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

// ccParams are the params for the clusterConfFileTemplate
type ccParams struct {
	Targets []string
	Labels  []string
}

const clusterConfFileTemplate = `- targets:
{{range $val := .Targets}}  - {{$val}}
{{end}}  labels:
{{range $val := .Labels}}    {{$val}}
{{end}}
`

// createClusterConfigFile creates the cluster config file per node
func buildCreateRequest(nodes []string) (io.Reader, error) {
	buffer := bytes.NewBufferString("---\n")
	for i, n := range nodes {
		labels := make(map[string]string)
		// automatically add the node based on the nodeID
		labels["node"] = fmt.Sprintf("\"%s\"", strconv.Itoa(i+1))
		labels["tenant"] = "system"
		params := &ccParams{
			Targets: []string{n},
			Labels:  make([]string, 0),
		}
		for key, value := range labels {
			params.Labels = append(params.Labels, fmt.Sprintf("%s: %s", key, value))
		}
		t := template.Must(template.New("start").Parse(clusterConfFileTemplate))
		if err := t.Execute(buffer, params); err != nil {
			return nil, err
		}
	}

	b, err := json.Marshal(&instanceConfigRequest{Config: buffer.String()})
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(b), nil
}

// getToken gets the Authorization token for grafana
func getToken(ctx context.Context, promUrl string) (string, error) {
	if strings.HasPrefix(promUrl, "http:/") {
		// no token needed for insecure URL
		return "", nil
	}
	// Read in the service account key and audience, so we can retrieve the identity token.
	grafanaKey := os.Getenv(ServiceAccountJson)
	if grafanaKey == "" {
		return "", errors.Newf("%s env variable was not found", ServiceAccountJson)
	}
	grafanaAudience := os.Getenv(ServiceAccountAudience)
	if grafanaAudience == "" {
		return "", errors.Newf("%s env variable was not found", ServiceAccountAudience)
	}
	ts, err := idtoken.NewTokenSource(ctx, grafanaAudience, idtoken.WithCredentialsJSON([]byte(grafanaKey)))
	if err != nil {
		return "", errors.Wrap(err, "error creating GCS oauth token source from specified credential")
	}
	token, err := ts.Token()
	if err != nil {
		return "", errors.Wrap(err, "error getting identity token")
	}
	fmt.Println(token.AccessToken)
	return fmt.Sprintf("Bearer %s", token.AccessToken), nil
}
