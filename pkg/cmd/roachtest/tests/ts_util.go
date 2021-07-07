// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
)

// tsQueryType represents the type of the time series query to retrieve. In
// most cases, tests are verifying either the "total" or "rate" metrics, so
// this enum type simplifies the API of tspb.Query.
type tsQueryType int

const (
	// total indicates to query the total of the metric. Specifically,
	// downsampler will be average, aggregator will be sum, and derivative will
	// be none.
	total tsQueryType = iota
	// rate indicates to query the rate of change of the metric. Specifically,
	// downsampler will be average, aggregator will be sum, and derivative will
	// be non-negative derivative.
	rate
)

type tsQuery struct {
	name      string
	queryType tsQueryType
}

func mustGetMetrics(
	t test.Test, adminURL string, start, end time.Time, tsQueries []tsQuery,
) tspb.TimeSeriesQueryResponse {
	response, err := getMetrics(adminURL, start, end, tsQueries)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func getMetrics(
	adminURL string, start, end time.Time, tsQueries []tsQuery,
) (tspb.TimeSeriesQueryResponse, error) {
	url := "http://" + adminURL + "/ts/query"
	queries := make([]tspb.Query, len(tsQueries))
	for i := 0; i < len(tsQueries); i++ {
		switch tsQueries[i].queryType {
		case total:
			queries[i] = tspb.Query{
				Name:             tsQueries[i].name,
				Downsampler:      tspb.TimeSeriesQueryAggregator_AVG.Enum(),
				SourceAggregator: tspb.TimeSeriesQueryAggregator_SUM.Enum(),
			}
		case rate:
			queries[i] = tspb.Query{
				Name:             tsQueries[i].name,
				Downsampler:      tspb.TimeSeriesQueryAggregator_AVG.Enum(),
				SourceAggregator: tspb.TimeSeriesQueryAggregator_SUM.Enum(),
				Derivative:       tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE.Enum(),
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
		SampleNanos: (1 * time.Minute).Nanoseconds(),
		Queries:     queries,
	}
	var response tspb.TimeSeriesQueryResponse
	err := httputil.PostJSON(http.Client{Timeout: 500 * time.Millisecond}, url, &request, &response)
	return response, err

}

func verifyTxnPerSecond(
	ctx context.Context,
	c cluster.Cluster,
	t test.Test,
	adminNode option.NodeListOption,
	start, end time.Time,
	txnTarget, maxPercentTimeUnderTarget float64,
) {
	// Query needed information over the timespan of the query.
	adminUIAddrs, err := c.ExternalAdminUIAddr(ctx, adminNode)
	if err != nil {
		t.Fatal(err)
	}
	adminURL := adminUIAddrs[0]
	response := mustGetMetrics(t, adminURL, start, end, []tsQuery{
		{name: "cr.node.txn.commits", queryType: rate},
		{name: "cr.node.txn.commits", queryType: total},
	})

	// Drop the first two minutes of datapoints as a "ramp-up" period.
	perMinute := response.Results[0].Datapoints[2:]
	cumulative := response.Results[1].Datapoints[2:]

	// Check average txns per second over the entire test was above the target.
	totalTxns := cumulative[len(cumulative)-1].Value - cumulative[0].Value
	avgTxnPerSec := totalTxns / float64(end.Sub(start)/time.Second)

	if avgTxnPerSec < txnTarget {
		t.Fatalf("average txns per second %f was under target %f", avgTxnPerSec, txnTarget)
	} else {
		t.L().Printf("average txns per second: %f", avgTxnPerSec)
	}

	// Verify that less than the specified limit of each individual one minute
	// period was underneath the target.
	minutesBelowTarget := 0.0
	for _, dp := range perMinute {
		if dp.Value < txnTarget {
			minutesBelowTarget++
		}
	}
	if perc := minutesBelowTarget / float64(len(perMinute)); perc > maxPercentTimeUnderTarget {
		t.Fatalf(
			"spent %f%% of time below target of %f txn/s, wanted no more than %f%%",
			perc*100, txnTarget, maxPercentTimeUnderTarget*100,
		)
	} else {
		t.L().Printf("spent %f%% of time below target of %f txn/s", perc*100, txnTarget)
	}
}

func verifyLookupsPerSec(
	ctx context.Context,
	c cluster.Cluster,
	t test.Test,
	adminNode option.NodeListOption,
	start, end time.Time,
	rangeLookupsTarget float64,
) {
	// Query needed information over the timespan of the query.
	adminUIAddrs, err := c.ExternalAdminUIAddr(ctx, adminNode)
	if err != nil {
		t.Fatal(err)
	}
	adminURL := adminUIAddrs[0]
	response := mustGetMetrics(t, adminURL, start, end, []tsQuery{
		{name: "cr.node.distsender.rangelookups", queryType: rate},
	})

	// Drop the first two minutes of datapoints as a "ramp-up" period.
	perMinute := response.Results[0].Datapoints[2:]

	// Verify that each individual one minute periods were below the target.
	for _, dp := range perMinute {
		if dp.Value > rangeLookupsTarget {
			t.Fatalf("Found minute interval with %f lookup/sec above target of %f lookup/sec\n", dp.Value, rangeLookupsTarget)
		} else {
			t.L().Printf("Found minute interval with %f lookup/sec\n", dp.Value)
		}
	}
}
