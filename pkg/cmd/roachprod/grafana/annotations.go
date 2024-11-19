// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package grafana

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod/roachprodutil"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/errors"
	"github.com/go-openapi/strfmt"
	grafana "github.com/grafana/grafana-openapi-client-go/client"
	"github.com/grafana/grafana-openapi-client-go/models"
	"google.golang.org/api/idtoken"
)

// newGrafanaClient is a helper function that creates an HTTP client to
// create grafana api calls with.  If secure is true, it tries to get a
// GCS identity token by using the service account helpers in `roachprodutil/identity.go`.
// This identity token is passed in every request to authenticate with grafana.
func newGrafanaClient(
	ctx context.Context, host string, secure bool,
) (*grafana.GrafanaHTTPAPI, error) {
	headers := map[string]string{}
	scheme := "http"

	if secure {
		scheme = "https"

		// Read in the service account key and audience, so we can retrieve the identity token.
		if _, err := roachprodutil.SetServiceAccountCredsEnv(ctx, false); err != nil {
			return nil, err
		}

		token, err := roachprodutil.GetServiceAccountToken(ctx, idtoken.NewTokenSource)
		if err != nil {
			return nil, err
		}
		headers["Authorization"] = fmt.Sprintf("Bearer %s", token)
	}

	headers[httputil.ContentTypeHeader] = httputil.JSONContentType
	cfg := &grafana.TransportConfig{
		Host:        host,
		BasePath:    "/api",
		Schemes:     []string{scheme},
		HTTPHeaders: headers,
	}

	return grafana.NewHTTPClientWithConfig(strfmt.Default, cfg), nil
}

// AddAnnotation creates an HTTP client and sends a POST request to the
// specified host name to create an annotation. If successful, it returns
// the result.
func AddAnnotation(ctx context.Context, host string, secure bool, req AddAnnotationRequest) error {
	// Cleanup the host name. The Grafana OpenAPI already appends the scheme
	// and leading slash.
	host = strings.Trim(host[strings.Index(host, ":")+1:], "/")

	httpClient, err := newGrafanaClient(ctx, host, secure)
	if err != nil {
		return err
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
		const emptyResponseBodyErr = "can be resolved by supporting TextUnmarshaler interface"
		// The Grafana OpenAPI does not correctly handle empty responses, i.e. renegotiating
		// the content-type dynamically if there is an error with no body. This leads to a
		// cryptic `(*models.ErrorResponseBody) is not supported by the TextConsumer` error,
		// but is most likely a 401 authentication issue so give the caller a hint.
		if strings.Contains(err.Error(), emptyResponseBodyErr) {
			return errors.WithHint(err, "If issue is not transient, check that you are authenticating correctly")
		}
		return err
	}
	if !res.IsSuccess() {
		return errors.Newf("AddAnnotation failed with: %s", res)
	}

	return nil
}

// AddAnnotationRequest are the fields used to create a Grafana annotation
// post request. Only Text is a required field.
type AddAnnotationRequest struct {
	Text         string
	DashboardUID string
	Tags         []string
	// The Grafana API expects the start and end time to be in epoch millisecond time.
	// If empty, creates an annotation at the current time. If both StartTime and
	// EndTime are specified, creates an annotation over a time range. The time range
	// is inclusive of EndTime: [StartTime, EndTime].
	StartTime int64
	EndTime   int64
}
