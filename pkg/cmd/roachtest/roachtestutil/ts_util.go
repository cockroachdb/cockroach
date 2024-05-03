// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachtestutil

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
)

// TsQueryType represents the type of the time series query to retrieve. In
// most cases, tests are verifying either the "total" or "rate" metrics, so
// this enum type simplifies the API of tspb.Query.
type TsQueryType int

const (
	// Total indicates to query the total of the metric. Specifically,
	// downsampler will be average, aggregator will be sum, and derivative will
	// be none.
	Total TsQueryType = iota
	// Rate indicates to query the rate of change of the metric. Specifically,
	// downsampler will be average, aggregator will be sum, and derivative will
	// be non-negative derivative.
	Rate
)

// TsQuery represents a query against a timeseries database.
type TsQuery struct {
	// Name specifies the name of the metric being queried.
	Name string
	// QueryType specifies the type of the value being queried. See TsQueryType.
	QueryType TsQueryType
	// Sources is a list of nodes being queried. See tspb.Query.
	Sources []string
}

func GetMetricsWithSamplePeriod(
	ctx context.Context,
	c cluster.Cluster,
	t Fataler,
	adminURL string,
	start, end time.Time,
	samplePeriod time.Duration,
	tsQueries []TsQuery,
) (tspb.TimeSeriesQueryResponse, error) {
	url := "http://" + adminURL + "/ts/query"
	queries := make([]tspb.Query, len(tsQueries))
	for i := 0; i < len(tsQueries); i++ {
		switch tsQueries[i].QueryType {
		case Total:
			queries[i] = tspb.Query{
				Name:             tsQueries[i].Name,
				Downsampler:      tspb.TimeSeriesQueryAggregator_AVG.Enum(),
				SourceAggregator: tspb.TimeSeriesQueryAggregator_SUM.Enum(),
				Sources:          tsQueries[i].Sources,
			}
		case Rate:
			queries[i] = tspb.Query{
				Name:             tsQueries[i].Name,
				Downsampler:      tspb.TimeSeriesQueryAggregator_AVG.Enum(),
				SourceAggregator: tspb.TimeSeriesQueryAggregator_SUM.Enum(),
				Derivative:       tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE.Enum(),
				Sources:          tsQueries[i].Sources,
			}
		default:
			panic("unexpected")
		}
	}
	request := tspb.TimeSeriesQueryRequest{
		StartNanos: start.UnixNano(),
		EndNanos:   end.UnixNano(),
		// Ask for one minute intervals. We can't just ask for the whole hour
		// because the time series query system does not support downsampling
		// offsets.
		SampleNanos: samplePeriod.Nanoseconds(),
		Queries:     queries,
	}
	var response tspb.TimeSeriesQueryResponse
	client := DefaultHTTPClient(c, t.L(), HTTPTimeout(500*time.Millisecond))
	err := client.PostProtobuf(ctx, url, &request, &response)
	return response, err

}
