// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package grafana

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/errors"
	"github.com/go-openapi/strfmt"
	grafclient "github.com/grafana/grafana-openapi-client-go/client"
	"github.com/grafana/grafana-openapi-client-go/models"
	"google.golang.org/api/idtoken"
)

const ServiceAccountJson = "GRAFANA_SERVICE_ACCOUNT_JSON"

// newGrafanaClient is a helper function that creates an HTTP client to
// create grafana api calls with.  If secure is true, it tries to get a
// GCS identity token by using the service account specified by the env
// variable ServiceAccountJson. This identity token is passed in
// every request to authenticate with grafana.
func newGrafanaClient(
	ctx context.Context, host string, secure bool,
) (*grafclient.GrafanaHTTPAPI, error) {
	const grafanaAudience = "1063333028845-p47csl1ukrgnpnnjc7lrtrto6uqs9t37.apps.googleusercontent.com"
	headers := map[string]string{}
	scheme := "http"

	if secure {
		scheme = "https"

		grafanaKey := os.Getenv(ServiceAccountJson)
		if grafanaKey == "" {
			return nil, errors.Newf("%s env variable was not found", ServiceAccountJson)
		}
		ts, err := idtoken.NewTokenSource(ctx, grafanaAudience, idtoken.WithCredentialsJSON([]byte(grafanaKey)))
		if err != nil {
			return nil, errors.Wrap(err, "Error creating GCS oauth token source from specified credential")
		}
		token, err := ts.Token()
		if err != nil {
			return nil, errors.Wrap(err, "Error getting token")
		}

		headers["Authorization"] = fmt.Sprintf("Bearer %s", token.AccessToken)
	}

	headers[httputil.ContentTypeHeader] = httputil.JSONContentType
	cfg := &grafclient.TransportConfig{
		Host:        host,
		BasePath:    "/api",
		Schemes:     []string{scheme},
		HTTPHeaders: headers,
	}

	return grafclient.NewHTTPClientWithConfig(strfmt.Default, cfg), nil
}

// AddGrafanaAnnotation creates an HTTP client and sends a POST request to the
// specified host name to create an annotation. If successful, it returns
// the result.
func AddGrafanaAnnotation(
	ctx context.Context, host string, secure bool, req AddAnnotationRequest,
) (string, error) {
	// Cleanup the host name. The Grafana OpenAPI already appends the scheme
	// and leading slash.
	host, _ = strings.CutPrefix(host, "https://")
	host, _ = strings.CutPrefix(host, "http://")
	host = strings.Trim(host, "/")

	httpClient, err := newGrafanaClient(ctx, host, secure)
	if err != nil {
		return "", err
	}

	body := &models.PostAnnotationsCmd{
		Text:         &req.Text,
		Tags:         req.Tags,
		DashboardUID: req.DashboardUID,
		Time:         req.StartTime,
		TimeEnd:      req.EndTime,
	}

	res, err := httpClient.Annotations.PostAnnotation(body)
	if err != nil {
		emptyResponseBodyErr := fmt.Errorf("&{ <nil> } (*models.ErrorResponseBody) is not supported by the TextConsumer, can be resolved by supporting TextUnmarshaler interface")
		// The Grafana OpenAPI does not correctly handle empty responses, i.e. renegotiating
		// the content-type dynamically if there is an error with no body. This leads to a
		// cryptic `(*models.ErrorResponseBody) is not supported by the TextConsumer` error,
		// but is most likely a 401 authentication issue so give the caller a hint.
		if errors.Is(err, emptyResponseBodyErr) {
			return "", errors.Wrap(err, "AddGrafanaAnnotation failed - HINT: if issue is not transient, check that you are authenticating correctly")
		}
		return "", err
	}
	if !res.IsSuccess() {
		return "", errors.Newf("AddGrafanaAnnotation failed with: %s", res)
	}

	return res.String(), nil
}

// AddAnnotationRequest are the fields used to create a Grafana annotation
// post request. Only Text is a required field.
type AddAnnotationRequest struct {
	Text         string
	DashboardUID string
	Tags         []string
	// The Grafana API expects the start and end time to be in epoch millisecond time.
	// If empty, creates an annotation at the current time. If both StartTime and
	// EndTime are specified, creates an annotation over a time range.
	StartTime int64
	EndTime   int64
}
