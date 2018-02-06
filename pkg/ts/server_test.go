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

package ts_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/roachpb"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestServerQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &storage.StoreTestingKnobs{
				DisableTimeSeriesMaintenanceQueue: true,
			},
		},
	})
	defer s.Stopper().Stop(context.TODO())
	tsrv := s.(*server.TestServer)

	// Populate data directly.
	tsdb := tsrv.TsDB()
	if err := tsdb.StoreData(context.TODO(), ts.Resolution10s, []tspb.TimeSeriesData{
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

	conn, err := tsrv.RPCContext().GRPCDial(tsrv.Cfg.Addr).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	client := tspb.NewTimeSeriesClient(conn)
	response, err := client.Query(context.Background(), &tspb.TimeSeriesQueryRequest{
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

	// Test a query with downsampling enabled. The query is a sum of maximums.
	expectedResult = &tspb.TimeSeriesQueryResponse{
		Results: []tspb.TimeSeriesQueryResponse_Result{
			{
				Query: tspb.Query{
					Name:        "test.metric",
					Sources:     []string{"source1", "source2"},
					Downsampler: tspb.TimeSeriesQueryAggregator_MAX.Enum(),
				},
				Datapoints: []tspb.TimeSeriesDatapoint{
					{
						TimestampNanos: 250 * 1e9,
						Value:          200.0,
					},
					{
						TimestampNanos: 750 * 1e9,
						Value:          650.0,
					},
				},
			},
		},
	}
	response, err = client.Query(context.Background(), &tspb.TimeSeriesQueryRequest{
		StartNanos:  0,
		EndNanos:    1000 * 1e9,
		SampleNanos: 500 * 1e9,
		Queries: []tspb.Query{
			{
				Name:        "test.metric",
				Downsampler: tspb.TimeSeriesQueryAggregator_MAX.Enum(),
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

// TestServerQueryStarvation tests a very specific scenario, wherein a single
// query request has more queries than the server's MaxWorkers count.
func TestServerQueryStarvation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	workerCount := 20
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		TimeSeriesQueryWorkerMax: workerCount,
	})
	defer s.Stopper().Stop(context.TODO())
	tsrv := s.(*server.TestServer)

	seriesCount := workerCount * 2
	if err := populateSeries(seriesCount, 10, 3, tsrv.TsDB()); err != nil {
		t.Fatal(err)
	}

	conn, err := tsrv.RPCContext().GRPCDial(tsrv.Cfg.Addr).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	client := tspb.NewTimeSeriesClient(conn)

	queries := make([]tspb.Query, 0, seriesCount)
	for i := 0; i < seriesCount; i++ {
		queries = append(queries, tspb.Query{
			Name: seriesName(i),
		})
	}

	if _, err := client.Query(context.Background(), &tspb.TimeSeriesQueryRequest{
		StartNanos: 0 * 1e9,
		EndNanos:   500 * 1e9,
		Queries:    queries,
	}); err != nil {
		t.Fatal(err)
	}
}

// TestServerQueryMemoryManagement verifies that queries succeed under
// constrained memory requirements.
func TestServerQueryMemoryManagement(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Number of workers that will be available to process data.
	workerCount := 20
	// Number of series that will be queried.
	seriesCount := workerCount * 2
	// Number of data sources that will be generated.
	sourceCount := 6
	// Number of slabs (hours) of data we want to generate
	slabCount := 5
	// Generated datapoints every 100 seconds, so compute how many we want to
	// generate data across the target number of hours.
	valueCount := int(ts.Resolution10s.SlabDuration()/(100*1e9)) * slabCount

	// MemoryBudget is a function of slab size and source count.
	samplesPerSlab := ts.Resolution10s.SlabDuration() / ts.Resolution10s.SampleDuration()
	sizeOfSlab := int64(unsafe.Sizeof(roachpb.InternalTimeSeriesData{})) + (int64(unsafe.Sizeof(roachpb.InternalTimeSeriesSample{})) * samplesPerSlab)
	budget := 3 * sizeOfSlab * int64(sourceCount) * int64(workerCount)

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		TimeSeriesQueryWorkerMax:    workerCount,
		TimeSeriesQueryMemoryBudget: budget,
	})
	defer s.Stopper().Stop(context.TODO())
	tsrv := s.(*server.TestServer)

	if err := populateSeries(seriesCount, sourceCount, valueCount, tsrv.TsDB()); err != nil {
		t.Fatal(err)
	}

	conn, err := tsrv.RPCContext().GRPCDial(tsrv.Cfg.Addr).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	client := tspb.NewTimeSeriesClient(conn)

	queries := make([]tspb.Query, 0, seriesCount)
	for i := 0; i < seriesCount; i++ {
		queries = append(queries, tspb.Query{
			Name: seriesName(i),
		})
	}

	if _, err := client.Query(context.Background(), &tspb.TimeSeriesQueryRequest{
		StartNanos: 0 * 1e9,
		EndNanos:   5 * 3600 * 1e9,
		Queries:    queries,
	}); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkServerQuery(b *testing.B) {
	s, _, _ := serverutils.StartServer(b, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	tsrv := s.(*server.TestServer)

	// Populate data for large number of time series.
	seriesCount := 50
	sourceCount := 10
	if err := populateSeries(seriesCount, sourceCount, 3, tsrv.TsDB()); err != nil {
		b.Fatal(err)
	}

	conn, err := tsrv.RPCContext().GRPCDial(tsrv.Cfg.Addr).Connect(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	client := tspb.NewTimeSeriesClient(conn)

	queries := make([]tspb.Query, 0, seriesCount)
	for i := 0; i < seriesCount; i++ {
		queries = append(queries, tspb.Query{
			Name: fmt.Sprintf("metric.%d", i),
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := client.Query(context.Background(), &tspb.TimeSeriesQueryRequest{
			StartNanos: 0 * 1e9,
			EndNanos:   500 * 1e9,
			Queries:    queries,
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func seriesName(seriesNum int) string {
	return fmt.Sprintf("metric.%d", seriesNum)
}

func sourceName(sourceNum int) string {
	return fmt.Sprintf("source.%d", sourceNum)
}

func generateTimeSeriesDatapoints(valueCount int) []tspb.TimeSeriesDatapoint {
	result := make([]tspb.TimeSeriesDatapoint, 0, valueCount)
	var i int64
	for i = 0; i < int64(valueCount); i++ {
		result = append(result, tspb.TimeSeriesDatapoint{
			TimestampNanos: i * 100 * 1e9,
			Value:          float64(i * 100),
		})
	}
	return result
}

func populateSeries(seriesCount, sourceCount, valueCount int, tsdb *ts.DB) error {
	for series := 0; series < seriesCount; series++ {
		for source := 0; source < sourceCount; source++ {
			if err := tsdb.StoreData(context.TODO(), ts.Resolution10s, []tspb.TimeSeriesData{
				{
					Name:       seriesName(series),
					Source:     sourceName(source),
					Datapoints: generateTimeSeriesDatapoints(valueCount),
				},
			}); err != nil {
				return errors.Errorf(
					"error storing data for series %d, source %d: %s", series, source, err,
				)
			}
		}
	}
	return nil
}
