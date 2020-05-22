// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ts_test

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"testing"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/kr/pretty"
)

func TestServerQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableTimeSeriesMaintenanceQueue: true,
			},
		},
	})
	defer s.Stopper().Stop(context.Background())
	tsrv := s.(*server.TestServer)

	// Populate data directly.
	tsdb := tsrv.TsDB()
	if err := tsdb.StoreData(context.Background(), ts.Resolution10s, []tspb.TimeSeriesData{
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
						TimestampNanos: 500 * 1e9,
						Value:          400.0,
					},
					{
						TimestampNanos: 510 * 1e9,
						Value:          500.0,
					},
					{
						TimestampNanos: 520 * 1e9,
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
						TimestampNanos: 500 * 1e9,
						Value:          200.0,
					},
					{
						TimestampNanos: 510 * 1e9,
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
						TimestampNanos: 500 * 1e9,
						Value:          1.0,
					},
					{
						TimestampNanos: 510 * 1e9,
						Value:          5.0,
					},
					{
						TimestampNanos: 520 * 1e9,
						Value:          5.0,
					},
				},
			},
		},
	}

	conn, err := tsrv.RPCContext().GRPCDialNode(tsrv.Cfg.Addr, tsrv.NodeID(),
		rpc.DefaultClass).Connect(context.Background())
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
						TimestampNanos: 0,
						Value:          200.0,
					},
					{
						TimestampNanos: 500 * 1e9,
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
	defer s.Stopper().Stop(context.Background())
	tsrv := s.(*server.TestServer)

	seriesCount := workerCount * 2
	if err := populateSeries(seriesCount, 10, 3, tsrv.TsDB()); err != nil {
		t.Fatal(err)
	}

	conn, err := tsrv.RPCContext().GRPCDialNode(tsrv.Cfg.Addr, tsrv.NodeID(),
		rpc.DefaultClass).Connect(context.Background())
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
	defer s.Stopper().Stop(context.Background())
	tsrv := s.(*server.TestServer)

	if err := populateSeries(seriesCount, sourceCount, valueCount, tsrv.TsDB()); err != nil {
		t.Fatal(err)
	}

	conn, err := tsrv.RPCContext().GRPCDialNode(tsrv.Cfg.Addr, tsrv.NodeID(),
		rpc.DefaultClass).Connect(context.Background())
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

func TestServerDump(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableTimeSeriesMaintenanceQueue: true,
			},
		},
	})
	defer s.Stopper().Stop(context.Background())
	tsrv := s.(*server.TestServer)

	seriesCount := 10
	sourceCount := 5
	// Number of slabs (hours) of data we want to generate
	slabCount := 5
	// Generated datapoints every 100 seconds, so compute how many we want to
	// generate data across the target number of hours.
	valueCount := int(ts.Resolution10s.SlabDuration()/(100*1e9)) * slabCount

	if err := populateSeries(seriesCount, sourceCount, valueCount, tsrv.TsDB()); err != nil {
		t.Fatal(err)
	}

	conn, err := tsrv.RPCContext().GRPCDialNode(tsrv.Cfg.Addr, tsrv.NodeID(),
		rpc.DefaultClass).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	client := tspb.NewTimeSeriesClient(conn)

	dumpClient, err := client.Dump(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	// Read data from dump command.
	resultMap := make(map[string]map[string]tspb.TimeSeriesData)
	totalMsgCount := 0
	for {
		msg, err := dumpClient.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		// The dump will include all of the real time series metrics recorded by the
		// server. Filter out everything other than the metrics we are creating
		// for this test.
		if !strings.HasPrefix(msg.Name, "metric.") {
			continue
		}
		sourceMap, ok := resultMap[msg.Name]
		if !ok {
			sourceMap = make(map[string]tspb.TimeSeriesData)
			resultMap[msg.Name] = sourceMap
		}
		if data, ok := sourceMap[msg.Source]; !ok {
			sourceMap[msg.Source] = *msg
		} else {
			data.Datapoints = append(data.Datapoints, msg.Datapoints...)
			sourceMap[msg.Source] = data
		}
		totalMsgCount++
	}

	// Generate expected data.
	expectedMap := make(map[string]map[string]tspb.TimeSeriesData)
	for series := 0; series < seriesCount; series++ {
		sourceMap := make(map[string]tspb.TimeSeriesData)
		expectedMap[seriesName(series)] = sourceMap
		for source := 0; source < sourceCount; source++ {
			sourceMap[sourceName(source)] = tspb.TimeSeriesData{
				Name:       seriesName(series),
				Source:     sourceName(source),
				Datapoints: generateTimeSeriesDatapoints(valueCount),
			}
		}
	}

	if a, e := totalMsgCount, seriesCount*sourceCount*slabCount; a != e {
		t.Fatalf("dump returned %d messages, expected %d", a, e)
	}
	if a, e := resultMap, expectedMap; !reflect.DeepEqual(a, e) {
		for _, diff := range pretty.Diff(a, e) {
			t.Error(diff)
		}
	}
}

func BenchmarkServerQuery(b *testing.B) {
	s, _, _ := serverutils.StartServer(b, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	tsrv := s.(*server.TestServer)

	// Populate data for large number of time series.
	seriesCount := 50
	sourceCount := 10
	if err := populateSeries(seriesCount, sourceCount, 3, tsrv.TsDB()); err != nil {
		b.Fatal(err)
	}

	conn, err := tsrv.RPCContext().GRPCDialNode(tsrv.Cfg.Addr, tsrv.NodeID(),
		rpc.DefaultClass).Connect(context.Background())
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
			if err := tsdb.StoreData(context.Background(), ts.Resolution10s, []tspb.TimeSeriesData{
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
