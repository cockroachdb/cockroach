// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package prometheus

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/roachprodutil"
	"github.com/cockroachdb/errors"
	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"google.golang.org/api/idtoken"
)

type (
	// Client is an interface allowing raw promql queries against Prometheus.
	Client interface {
		Query(ctx context.Context, query string, ts time.Time, opts ...promv1.Option) (model.Value, promv1.Warnings, error)
		QueryRange(ctx context.Context, query string, r promv1.Range, opts ...promv1.Option) (model.Value, promv1.Warnings, error)
	}
	// Custom HTTP transport is needed to pass (auth) headers to Prometheus.
	customTransport struct {
		Transport http.RoundTripper
		Header    map[string]string
	}

	// StatPoint represents a cluster metric value at a point in time.
	StatPoint struct {
		Time  int64
		Value float64
	}

	// StatSeries is a collection of StatPoints, sorted in ascending order by
	// StatPoint.Time.
	StatSeries []StatPoint
)

const (
	DefaultScrapeInterval = 30 * time.Second
)

var DefaultPromHostUrl = config.EnvOrDefaultString("ROACHPROD_PROM_HOST_URL",
	"https://grafana.testeng.crdb.io/prometheus/")

var ErrEmptyHost = errors.New("prometheus host url is empty")

func (c *customTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	for key, value := range c.Header {
		req.Header.Set(key, value)
	}
	return c.Transport.RoundTrip(req)
}

func NewPromClient(ctx context.Context, host string, secure bool) (Client, error) {
	if host == "" {
		return nil, ErrEmptyHost
	}
	// Check if the host encodes a URL scheme that's compatible with the secure param.
	if idx := strings.Index(host, "://"); idx != -1 {
		protocol := host[:idx]
		if (secure && protocol != "https") || (!secure && protocol != "http") {
			return nil, errors.Newf("host url=%s is incompatible with secure=%t", protocol, secure)
		}
		// Strip out the URL scheme since	we add it below.
		host = host[idx+3:]
	}
	headers := map[string]string{}
	scheme := "http://"

	if secure {
		scheme = "https://"

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
	// Create a new HTTP client with the custom transport
	httpClient := &http.Client{
		Transport: &customTransport{
			Transport: http.DefaultTransport,
			Header:    headers,
		},
	}

	promClient, err := promapi.NewClient(promapi.Config{
		Address: scheme + host,
		Client:  httpClient,
	})
	if err != nil {
		return nil, err
	}

	return promv1.NewAPI(promClient), nil
}

// CollectInterval collects a timeseries for the given query, that is contained
// within the interval provided. It will populate a map with values, for every
// label that is returned, with the tagged values. For example, if the query
// were rebalancing_queriespersecond in the interval [100,110] and there were
// two stores (1,2) with ip addresses 10.0.0.1 and 127.0.0.1, for store 2 and
// store 1 respectively.
//
//	{
//	   "store": {
//	       "1": {
//	           {Time: 100, Value: 777},
//	           {Time: 110, Value: 888}
//	       },
//	       "2": {
//	           {Time: 100, Value: 42},
//	           {Time: 110, Value 42},
//	       },
//	   },
//	   "instance": {
//	        "10.0.0.1":  {
//	           {Time: 100, Value: 42},
//	           {Time: 110, Value 42},
//	       },
//	        "127.0.0.1": {
//	           {Time: 100, Value: 777},
//	           {Time: 110, Value: 888}
//	       },
//	    },
//	}
func QueryRange(
	ctx context.Context, client Client, query string, from, to time.Time, step time.Duration,
) (map[string]map[string]StatSeries, error) {
	if client == nil {
		return nil, errors.AssertionFailedf("prometheus client is nil")
	}
	if to.Before(from) {
		return nil, errors.Newf("to %s < from %s", to.Format(time.RFC3339), from.Format(time.RFC3339))
	}
	r := promv1.Range{Start: from, End: to, Step: step}
	fromVal, warnings, err := client.QueryRange(ctx, query, r)
	if err != nil {
		return nil, err
	}
	if len(warnings) > 0 {
		return nil, errors.Newf("found warnings querying prometheus: %s", warnings)
	}

	fromMatrixTagged, ok := fromVal.(model.Matrix)
	if !ok {
		return nil, errors.Newf(
			"Unexpected result type %T for query %s @ [%s,%s] (%v)",
			fromVal,
			query,
			from.Format(time.RFC3339),
			to.Format(time.RFC3339),
			fromVal,
		)
	}
	if len(fromMatrixTagged) == 0 {
		return nil, errors.Newf(
			"Empty matrix result for query %s @ [%s,%s] (%v)",
			query,
			from.Format(time.RFC3339),
			to.Format(time.RFC3339),
			fromVal,
		)
	}

	result := make(map[string]map[string]StatSeries)
	for _, stream := range fromMatrixTagged {
		statSeries := make(StatSeries, len(stream.Values))
		for i := range stream.Values {
			statSeries[i] = StatPoint{
				Time:  stream.Values[i].Timestamp.Time().UnixNano(),
				Value: float64(stream.Values[i].Value),
			}
		}

		for labelName, labelValue := range stream.Metric {
			if _, ok := result[string(labelName)]; !ok {
				result[string(labelName)] = make(map[string]StatSeries)
			}
			result[string(labelName)][string(labelValue)] = statSeries
		}

		// When there is no tag associated with the result, put in a default
		// tag for the result.
		if len(stream.Metric) == 0 {
			result["default"] = map[string]StatSeries{"default": statSeries}
		}
	}

	return result, nil
}

func Query(
	ctx context.Context, client Client, query string, at time.Time,
) (map[string]map[string]StatPoint, error) {
	if client == nil {
		return nil, errors.AssertionFailedf("prometheus client is nil")
	}
	fromVal, warnings, err := client.Query(ctx, query, at)
	if err != nil {
		return nil, err
	}
	if len(warnings) > 0 {
		return nil, errors.Newf("found warnings querying prometheus: %s", warnings)
	}
	fromVec, ok := fromVal.(model.Vector)
	if !ok {
		return nil, errors.Newf("Unexpected result type %T for query %s @ %s (%v)", fromVal, query, at.Format(time.RFC3339), fromVal)
	}
	if len(fromVec) == 0 {
		return nil, errors.Newf("Empty vector result for query %s @ %s (%v)", query, at.Format(time.RFC3339), fromVal)
	}

	result := make(map[string]map[string]StatPoint)
	for _, sample := range fromVec {
		statPoint := StatPoint{
			Time:  sample.Timestamp.Time().UnixNano(),
			Value: float64(sample.Value),
		}
		for labelName, labelValue := range sample.Metric {
			if _, ok := result[string(labelName)]; !ok {
				result[string(labelName)] = make(map[string]StatPoint)
			}
			result[string(labelName)][string(labelValue)] = statPoint
		}
	}

	return result, nil
}

func Values(series StatSeries) []float64 {
	var values []float64
	for _, point := range series {
		values = append(values, point.Value)
	}
	return values
}
