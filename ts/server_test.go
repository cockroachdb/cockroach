// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License.
//
// Author: Matt Tracy (matt@cockroachlabs.com)

package ts_test

import (
	"sort"
	"testing"

	"github.com/gogo/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/ts"
	"github.com/cockroachdb/cockroach/ts/tspb"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()
	tsrv := s.(*server.TestServer)

	// Populate data directly.
	tsdb := tsrv.TsDB()
	if err := tsdb.StoreData(ts.Resolution10s, []tspb.TimeSeriesData{
		{
			Name:   "test.metric",
			Source: "source1",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{
					TimestampNanos: 400 * 1e9,
					Value:          100.0,
				},
				{
					TimestampNanos: 500 * 1e9,
					Value:          200.0,
				},
				{
					TimestampNanos: 520 * 1e9,
					Value:          300.0,
				},
			},
		},
		{
			Name:   "test.metric",
			Source: "source2",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{
					TimestampNanos: 400 * 1e9,
					Value:          100.0,
				},
				{
					TimestampNanos: 500 * 1e9,
					Value:          200.0,
				},
				{
					TimestampNanos: 510 * 1e9,
					Value:          250.0,
				},
				{
					TimestampNanos: 530 * 1e9,
					Value:          350.0,
				},
			},
		},
		{
			Name: "other.metric",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{
					TimestampNanos: 400 * 1e9,
					Value:          100.0,
				},
				{
					TimestampNanos: 500 * 1e9,
					Value:          200.0,
				},
				{
					TimestampNanos: 510 * 1e9,
					Value:          250.0,
				},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	expectedResult := &tspb.TimeSeriesQueryResponse{
		Results: []tspb.TimeSeriesQueryResponse_Result{
			{
				Query: tspb.Query{
					Name:    "test.metric",
					Sources: []string{"source1", "source2"},
				},
				Datapoints: []tspb.TimeSeriesDatapoint{
					{
						TimestampNanos: 505 * 1e9,
						Value:          400.0,
					},
					{
						TimestampNanos: 515 * 1e9,
						Value:          500.0,
					},
					{
						TimestampNanos: 525 * 1e9,
						Value:          600.0,
					},
				},
			},
			{
				Query: tspb.Query{
					Name:    "other.metric",
					Sources: []string{""},
				},
				Datapoints: []tspb.TimeSeriesDatapoint{
					{
						TimestampNanos: 505 * 1e9,
						Value:          200.0,
					},
					{
						TimestampNanos: 515 * 1e9,
						Value:          250.0,
					},
				},
			},
			{
				Query: tspb.Query{
					Name:             "test.metric",
					Sources:          []string{"source1", "source2"},
					Downsampler:      tspb.TimeSeriesQueryAggregator_MAX.Enum(),
					SourceAggregator: tspb.TimeSeriesQueryAggregator_MAX.Enum(),
					Derivative:       tspb.TimeSeriesQueryDerivative_DERIVATIVE.Enum(),
				},
				Datapoints: []tspb.TimeSeriesDatapoint{
					{
						TimestampNanos: 505 * 1e9,
						Value:          1.0,
					},
					{
						TimestampNanos: 515 * 1e9,
						Value:          5.0,
					},
					{
						TimestampNanos: 525 * 1e9,
						Value:          5.0,
					},
				},
			},
		},
	}

	conn, err := tsrv.RPCContext().GRPCDial(tsrv.Ctx.Addr)
	if err != nil {
		t.Fatal(err)
	}
	response, err := tspb.NewTimeSeriesClient(conn).Query(context.Background(), &tspb.TimeSeriesQueryRequest{
		StartNanos: 500 * 1e9,
		EndNanos:   526 * 1e9,
		Queries: []tspb.Query{
			{
				Name: "test.metric",
			},
			{
				Name: "other.metric",
			},
			{
				Name:             "test.metric",
				Downsampler:      tspb.TimeSeriesQueryAggregator_MAX.Enum(),
				SourceAggregator: tspb.TimeSeriesQueryAggregator_MAX.Enum(),
				Derivative:       tspb.TimeSeriesQueryDerivative_DERIVATIVE.Enum(),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range response.Results {
		sort.Strings(r.Sources)
	}
	if !proto.Equal(response, expectedResult) {
		t.Fatalf("actual response \n%v\n did not match expected response \n%v",
			response, expectedResult)
	}
}
