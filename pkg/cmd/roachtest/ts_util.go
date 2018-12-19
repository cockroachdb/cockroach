// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"context"
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
)

func verifyTxnPerSecond(
	ctx context.Context,
	c *cluster,
	t *test,
	adminNode nodeListOption,
	start, end time.Time,
	txnTarget, maxPercentTimeUnderTarget float64,
) {
	// Query needed information over the timespan of the query.
	adminURLs := c.ExternalAdminUIAddr(ctx, adminNode)
	url := "http://" + adminURLs[0] + "/ts/query"
	request := tspb.TimeSeriesQueryRequest{
		StartNanos: start.UnixNano(),
		EndNanos:   end.UnixNano(),
		// Ask for one minute intervals. We can't just ask for the whole hour
		// because the time series query system does not support downsampling
		// offsets.
		SampleNanos: (1 * time.Minute).Nanoseconds(),
		Queries: []tspb.Query{
			{
				Name:             "cr.node.txn.commits",
				Downsampler:      tspb.TimeSeriesQueryAggregator_AVG.Enum(),
				SourceAggregator: tspb.TimeSeriesQueryAggregator_SUM.Enum(),
				Derivative:       tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE.Enum(),
			},
			// Query *without* the derivative applied so we can get a total count of
			// txns over the time period.
			{
				Name:             "cr.node.txn.commits",
				Downsampler:      tspb.TimeSeriesQueryAggregator_AVG.Enum(),
				SourceAggregator: tspb.TimeSeriesQueryAggregator_SUM.Enum(),
			},
		},
	}
	var response tspb.TimeSeriesQueryResponse
	if err := httputil.PostJSON(http.Client{}, url, &request, &response); err != nil {
		t.Fatal(err)
	}

	// Drop the first two minutes of datapoints as a "ramp-up" period.
	perMinute := response.Results[0].Datapoints[2:]
	cumulative := response.Results[1].Datapoints[2:]

	// Check average txns per second over the entire test was above the target.
	totalTxns := cumulative[len(cumulative)-1].Value - cumulative[0].Value
	avgTxnPerSec := totalTxns / float64(end.Sub(start)/time.Second)

	if avgTxnPerSec < txnTarget {
		t.Fatalf("average txns per second %f was under target %f", avgTxnPerSec, txnTarget)
	} else {
		t.l.Printf("average txns per second: %f", avgTxnPerSec)
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
		t.l.Printf("spent %f%% of time below target of %f txn/s", perc*100, txnTarget)
	}
}
