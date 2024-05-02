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
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/errors"
	"google.golang.org/api/idtoken"
)

const (
	resourceName    = "instance-configs"
	resourceVersion = "v1"

	ServiceAccountJson     = "PROM_HELPER_SERVICE_ACCOUNT_JSON"
	ServiceAccountAudience = "PROM_HELPER_SERVICE_ACCOUNT_AUDIENCE"
)

// keeping the following as a variable enables us to override the value for unit testing
var (
	// httpPut is used for http PUT operation.
	httpPut = httputil.Put
	// httpDelete is used for http DELETE operation.
	httpDelete = httputil.Delete
	// newTokenSource is the token generator source.
	newTokenSource = idtoken.NewTokenSource
)

// instanceConfigRequest is the HTTP request received for generating instance config
type instanceConfigRequest struct {
	//Config is the content of the yaml file
	Config string `json:"config"`
}

// UpdatePrometheusTargets updates the cluster config in the promUrl
func UpdatePrometheusTargets(
	ctx context.Context,
	promUrl, clusterName string,
	forceFetchCreds bool,
	nodes []string,
	l *logger.Logger,
) error {
	req, err := buildCreateRequest(nodes)
	if err != nil {
		return err
	}
	token, err := getToken(ctx, promUrl, forceFetchCreds, l)
	if err != nil {
		return err
	}
	url := getUrl(promUrl, clusterName)
	l.Printf("invoking PUT for URL: %s", url)
	response, err := httpPut(ctx, url, &httputil.RequestHeaders{
		ContentType:   "application/json",
		Authorization: token,
	}, req)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		defer response.Body.Close()
		if response.StatusCode == http.StatusUnauthorized && !forceFetchCreds {
			l.Printf("request failed - this may be due to a stale token. retrying with forceFetchCreds true ...")
			return UpdatePrometheusTargets(ctx, promUrl, clusterName, true, nodes, l)
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
func DeleteClusterConfig(
	ctx context.Context, promUrl, clusterName string, forceFetchCreds bool, l *logger.Logger,
) error {
	if _, err := SetPromHelperCredsEnv(ctx, false, l); err != nil {
		return err
	}
	token, err := getToken(ctx, promUrl, forceFetchCreds, l)
	if err != nil {
		return err
	}
	url := getUrl(promUrl, clusterName)
	l.Printf("invoking DELETE for URL: %s", url)
	response, err := httpDelete(ctx, url, &httputil.RequestHeaders{
		Authorization: token,
	})
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusNoContent {
		defer response.Body.Close()
		if response.StatusCode == http.StatusUnauthorized && !forceFetchCreds {
			return DeleteClusterConfig(ctx, promUrl, clusterName, true, l)
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
		params := &ccParams{
			Targets: []string{n},
			Labels:  make([]string, 0),
		}
		params.Labels = append(params.Labels,
			fmt.Sprintf("node: \"%s\"", strconv.Itoa(i+1)),
			"tenant: system",
		)
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
func getToken(
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
	ts, err := newTokenSource(ctx, grafanaAudience, idtoken.WithCredentialsJSON([]byte(grafanaKey)))
	if err != nil {
		return "", errors.Wrap(err, "error creating GCS oauth token source from specified credential")
	}
	token, err := ts.Token()
	if err != nil {
		return "", errors.Wrap(err, "error getting identity token")
	}
	return fmt.Sprintf("Bearer %s", token.AccessToken), nil
}
