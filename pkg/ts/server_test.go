// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ts_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

func TestServerQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		// For now, direct access to the tsdb is reserved to the storage layer.
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,

		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableTimeSeriesMaintenanceQueue: true,
			},
		},
	})
	defer s.Stopper().Stop(context.Background())

	// Populate data directly.
	tsdb := s.TsDB().(*ts.DB)
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

	conn := s.RPCClientConn(t, username.RootUserName())
	client := conn.NewTimeSeriesClient()
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
	if !response.Equal(expectedResult) {
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
	if !response.Equal(expectedResult) {
		t.Fatalf("actual response \n%v\n did not match expected response \n%v",
			response, expectedResult)
	}
}

// TestServerQueryCombinedBatchReducesKVRPCs verifies that the combined-batch
// optimization actually reduces the number of KV BatchRequests compared to
// querying each metric individually. This is the core performance property of
// the optimization.
func TestServerQueryCombinedBatchReducesKVRPCs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var tsBatchCount atomic.Int64
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableTimeSeriesMaintenanceQueue: true,
				TestingRequestFilter: func(
					_ context.Context, req *kvpb.BatchRequest,
				) *kvpb.Error {
					for _, ru := range req.Requests {
						if bytes.HasPrefix(
							ru.GetInner().Header().Key,
							keys.TimeseriesPrefix,
						) {
							tsBatchCount.Add(1)
							break
						}
					}
					return nil
				},
			},
		},
	})
	defer s.Stopper().Stop(context.Background())

	tsdb := s.TsDB().(*ts.DB)

	// Store data for 5 metrics across 2 sources.
	metricCount := 5
	for i := 0; i < metricCount; i++ {
		require.NoError(t, tsdb.StoreData(
			context.Background(), ts.Resolution10s, []tspb.TimeSeriesData{
				{
					Name:   fmt.Sprintf("metric.%d", i),
					Source: "node1",
					Datapoints: []tspb.TimeSeriesDatapoint{
						{TimestampNanos: 400 * 1e9, Value: float64(i * 10)},
						{TimestampNanos: 500 * 1e9, Value: float64(i*10 + 1)},
					},
				},
				{
					Name:   fmt.Sprintf("metric.%d", i),
					Source: "node2",
					Datapoints: []tspb.TimeSeriesDatapoint{
						{TimestampNanos: 400 * 1e9, Value: float64(i * 20)},
						{TimestampNanos: 500 * 1e9, Value: float64(i*20 + 1)},
					},
				},
			},
		))
	}

	conn := s.RPCClientConn(t, username.RootUserName())
	client := conn.NewTimeSeriesClient()

	queries := make([]tspb.Query, metricCount)
	for i := 0; i < metricCount; i++ {
		queries[i] = tspb.Query{
			Name:    fmt.Sprintf("metric.%d", i),
			Sources: []string{"node1", "node2"},
		}
	}

	// Measure individual queries: send each metric as a separate request.
	tsBatchCount.Store(0)
	for _, q := range queries {
		_, err := client.Query(context.Background(), &tspb.TimeSeriesQueryRequest{
			StartNanos: 400 * 1e9,
			EndNanos:   500 * 1e9,
			Queries:    []tspb.Query{q},
		})
		require.NoError(t, err)
	}
	individualBatches := tsBatchCount.Load()

	// Measure combined query: send all metrics in a single request.
	tsBatchCount.Store(0)
	resp, err := client.Query(context.Background(), &tspb.TimeSeriesQueryRequest{
		StartNanos: 400 * 1e9,
		EndNanos:   500 * 1e9,
		Queries:    queries,
	})
	require.NoError(t, err)
	require.Len(t, resp.Results, metricCount)
	combinedBatches := tsBatchCount.Load()

	t.Logf("individual requests: %d KV batches, combined request: %d KV batches",
		individualBatches, combinedBatches)

	// On a single-node test server all TS data is on one range, so:
	// - Individual: each request produces its own BatchRequest = metricCount.
	// - Combined: all queries share a single BatchRequest = 1.
	require.Equal(t, int64(metricCount), individualBatches,
		"each individual query should produce one KV BatchRequest")
	require.Equal(t, int64(1), combinedBatches,
		"combined query should produce exactly one KV BatchRequest")

	// Disable the combined-batch optimization and verify that a multi-query
	// request falls back to per-query reads, producing the same number of
	// KV batches as individual requests.
	ts.CombinedBatchEnabled.Override(context.Background(), &s.ClusterSettings().SV, false)

	tsBatchCount.Store(0)
	resp, err = client.Query(context.Background(), &tspb.TimeSeriesQueryRequest{
		StartNanos: 400 * 1e9,
		EndNanos:   500 * 1e9,
		Queries:    queries,
	})
	require.NoError(t, err)
	require.Len(t, resp.Results, metricCount)
	disabledBatches := tsBatchCount.Load()

	t.Logf("combined request with setting disabled: %d KV batches", disabledBatches)
	require.Equal(t, int64(metricCount), disabledBatches,
		"with combined batch disabled, each query should produce its own KV BatchRequest")
}

// TestServerQueryCombinedBatch verifies that the combined-batch optimization
// in Server.Query works correctly: multiple queries in a single request share
// one KV batch, and the results are equivalent to querying each metric
// individually.
func TestServerQueryCombinedBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableTimeSeriesMaintenanceQueue: true,
			},
		},
	})
	defer s.Stopper().Stop(context.Background())

	tsdb := s.TsDB().(*ts.DB)

	// Store data for three different metrics across two sources.
	require.NoError(t, tsdb.StoreData(context.Background(), ts.Resolution10s, []tspb.TimeSeriesData{
		// metric.cpu: node1 and node2
		{
			Name:   "metric.cpu",
			Source: "node1",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{TimestampNanos: 400 * 1e9, Value: 10.0},
				{TimestampNanos: 500 * 1e9, Value: 20.0},
			},
		},
		{
			Name:   "metric.cpu",
			Source: "node2",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{TimestampNanos: 400 * 1e9, Value: 30.0},
				{TimestampNanos: 500 * 1e9, Value: 40.0},
			},
		},
		// metric.mem: node1 and node2
		{
			Name:   "metric.mem",
			Source: "node1",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{TimestampNanos: 400 * 1e9, Value: 100.0},
				{TimestampNanos: 500 * 1e9, Value: 200.0},
			},
		},
		{
			Name:   "metric.mem",
			Source: "node2",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{TimestampNanos: 400 * 1e9, Value: 300.0},
				{TimestampNanos: 500 * 1e9, Value: 400.0},
			},
		},
		// metric.disk: no explicit source stored, uses empty source
		// so it will be read via Scan when sources are not specified.
		{
			Name: "metric.disk",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{TimestampNanos: 400 * 1e9, Value: 1000.0},
				{TimestampNanos: 500 * 1e9, Value: 2000.0},
			},
		},
	}))

	conn := s.RPCClientConn(t, username.RootUserName())
	client := conn.NewTimeSeriesClient()

	// First, query each metric individually to establish expected results.
	individual := make([]*tspb.TimeSeriesQueryResponse, 3)
	for i, q := range []tspb.Query{
		{Name: "metric.cpu", Sources: []string{"node1", "node2"}},
		{Name: "metric.mem", Sources: []string{"node1", "node2"}},
		{Name: "metric.disk"}, // no sources -> Scan path
	} {
		resp, err := client.Query(context.Background(), &tspb.TimeSeriesQueryRequest{
			StartNanos: 400 * 1e9,
			EndNanos:   500 * 1e9,
			Queries:    []tspb.Query{q},
		})
		require.NoError(t, err)
		individual[i] = resp
	}

	// Now send all three queries in a single request. The combined batch
	// should produce identical results.
	combined, err := client.Query(context.Background(), &tspb.TimeSeriesQueryRequest{
		StartNanos: 400 * 1e9,
		EndNanos:   500 * 1e9,
		Queries: []tspb.Query{
			{Name: "metric.cpu", Sources: []string{"node1", "node2"}},
			{Name: "metric.mem", Sources: []string{"node1", "node2"}},
			{Name: "metric.disk"}, // no sources -> Scan path
		},
	})
	require.NoError(t, err)
	require.Len(t, combined.Results, 3)

	// Verify each combined result matches the corresponding individual result.
	for i := 0; i < 3; i++ {
		sort.Strings(individual[i].Results[0].Sources)
		sort.Strings(combined.Results[i].Sources)
		require.Equal(t, individual[i].Results[0].Datapoints, combined.Results[i].Datapoints,
			"metric %d datapoints mismatch", i)
		require.Equal(t, individual[i].Results[0].Sources, combined.Results[i].Sources,
			"metric %d sources mismatch", i)
	}

	// Verify actual values.
	// metric.cpu: SUM(node1, node2) at each timestamp.
	require.Len(t, combined.Results[0].Datapoints, 2)
	require.Equal(t, 40.0, combined.Results[0].Datapoints[0].Value) // 10+30
	require.Equal(t, 60.0, combined.Results[0].Datapoints[1].Value) // 20+40

	// metric.mem: SUM(node1, node2).
	require.Len(t, combined.Results[1].Datapoints, 2)
	require.Equal(t, 400.0, combined.Results[1].Datapoints[0].Value) // 100+300
	require.Equal(t, 600.0, combined.Results[1].Datapoints[1].Value) // 200+400

	// metric.disk: single source (Scan path).
	require.Len(t, combined.Results[2].Datapoints, 2)
	require.Equal(t, 1000.0, combined.Results[2].Datapoints[0].Value)
	require.Equal(t, 2000.0, combined.Results[2].Datapoints[1].Value)
}

// TestServerQueryMixedCombinedAndFallback verifies that when a single request
// contains queries that take the combined-batch path and queries that require
// the per-query chunked read fallback, both produce correct results.
func TestServerQueryMixedCombinedAndFallback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	workerCount := 8
	// Compute budget using columnar slab sizes (the default format).
	samplesPerSlab := ts.Resolution10s.SlabDuration() / ts.Resolution10s.SampleDuration()
	sizeOfTimeSeriesData := int64(unsafe.Sizeof(roachpb.InternalTimeSeriesData{}))
	// Columnar format: each sample is int32 (offset) + float64 (value).
	sizeOfSlab := sizeOfTimeSeriesData +
		(int64(unsafe.Sizeof(int32(0)))+int64(unsafe.Sizeof(float64(0))))*samplesPerSlab

	// Set budget so that queries with few sources are combinable but queries
	// with many sources exceed the per-worker budget for the query timespan.
	//
	// Per-worker budget = 300 * sizeOfSlab.
	// For 2 sources: maxWidth ≈ (150 - 2) hours >> 3 hours → combinable.
	// For 100 sources: maxWidth ≈ (3 - 2) hours = 1 hour < 3 hours → fallback.
	budget := 300 * sizeOfSlab * int64(workerCount)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant:           base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		TimeSeriesQueryWorkerMax:    workerCount,
		TimeSeriesQueryMemoryBudget: budget,
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableTimeSeriesMaintenanceQueue: true,
			},
		},
	})
	defer s.Stopper().Stop(context.Background())

	tsdb := s.TsDB().(*ts.DB)

	// Store 4 hours of data for two metrics. The query timespan will be 3 hours.
	var data []tspb.TimeSeriesData
	for hour := 0; hour < 4; hour++ {
		baseTs := int64(hour) * 3600 * 1e9
		for _, src := range []string{"node1", "node2"} {
			data = append(data,
				tspb.TimeSeriesData{
					Name:   "metric.small",
					Source: src,
					Datapoints: []tspb.TimeSeriesDatapoint{
						{TimestampNanos: baseTs + 100*1e9, Value: float64(hour*10 + 1)},
					},
				},
				tspb.TimeSeriesData{
					Name:   "metric.large",
					Source: src,
					Datapoints: []tspb.TimeSeriesDatapoint{
						{TimestampNanos: baseTs + 100*1e9, Value: float64(hour*100 + 1)},
					},
				},
			)
		}
	}
	require.NoError(t, tsdb.StoreData(context.Background(), ts.Resolution10s, data))

	conn := s.RPCClientConn(t, username.RootUserName())
	client := conn.NewTimeSeriesClient()

	// Build a list of 100 sources for the large query. Only node1 and node2
	// have data; the rest produce empty Gets but inflate EstimatedSources
	// enough to force the query into the chunked fallback path.
	largeSources := make([]string, 100)
	largeSources[0] = "node1"
	largeSources[1] = "node2"
	for i := 2; i < 100; i++ {
		largeSources[i] = fmt.Sprintf("node%d", i+1)
	}

	startNanos := int64(0)
	endNanos := int64(3 * 3600 * 1e9)

	// Query individually to establish expected results.
	smallResp, err := client.Query(context.Background(), &tspb.TimeSeriesQueryRequest{
		StartNanos: startNanos,
		EndNanos:   endNanos,
		Queries:    []tspb.Query{{Name: "metric.small", Sources: []string{"node1", "node2"}}},
	})
	require.NoError(t, err)

	largeResp, err := client.Query(context.Background(), &tspb.TimeSeriesQueryRequest{
		StartNanos: startNanos,
		EndNanos:   endNanos,
		Queries:    []tspb.Query{{Name: "metric.large", Sources: largeSources}},
	})
	require.NoError(t, err)

	// Combined request: small query (combinable) + large query (fallback).
	combined, err := client.Query(context.Background(), &tspb.TimeSeriesQueryRequest{
		StartNanos: startNanos,
		EndNanos:   endNanos,
		Queries: []tspb.Query{
			{Name: "metric.small", Sources: []string{"node1", "node2"}},
			{Name: "metric.large", Sources: largeSources},
		},
	})
	require.NoError(t, err)
	require.Len(t, combined.Results, 2)

	require.Equal(t, smallResp.Results[0].Datapoints, combined.Results[0].Datapoints,
		"small metric (combined path) datapoints mismatch")
	require.Equal(t, largeResp.Results[0].Datapoints, combined.Results[1].Datapoints,
		"large metric (fallback path) datapoints mismatch")
}

// TestServerQueryRollupFallback verifies that queries with a sample duration
// compatible with the rollup resolution (Resolution30m) correctly fall back to
// the per-query read path. The combined batch only reads Resolution10s and
// would miss rolled-up data at coarser resolutions.
func TestServerQueryRollupFallback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableTimeSeriesMaintenanceQueue: true,
			},
		},
	})
	defer s.Stopper().Stop(context.Background())

	tsdb := s.TsDB().(*ts.DB)

	// Store data at Resolution10s spanning multiple hours.
	var data []tspb.TimeSeriesData
	for hour := 0; hour < 3; hour++ {
		baseTs := int64(hour) * 3600 * 1e9
		data = append(data, tspb.TimeSeriesData{
			Name:   "metric.rollup",
			Source: "node1",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{TimestampNanos: baseTs + 100*1e9, Value: float64(hour*10 + 1)},
			},
		})
	}
	require.NoError(t, tsdb.StoreData(context.Background(), ts.Resolution10s, data))

	conn := s.RPCClientConn(t, username.RootUserName())
	client := conn.NewTimeSeriesClient()

	// Query with 30-minute sample duration, which triggers needsRollup=true
	// because SampleDurationNanos is compatible with Resolution30m. This forces
	// the query through the per-query fallback path (db.Query), which tries
	// Resolution30m first then falls back to Resolution10s data.
	resp, err := client.Query(context.Background(), &tspb.TimeSeriesQueryRequest{
		StartNanos:  0,
		EndNanos:    3 * 3600 * 1e9,
		SampleNanos: 30 * 60 * 1e9, // 30 minutes
		Queries: []tspb.Query{
			{Name: "metric.rollup", Sources: []string{"node1"}},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.Results[0].Datapoints,
		"rollup fallback should return data from Resolution10s")
}

// TestServerQueryCombinedBatchTenant verifies that tenant-scoped queries
// correctly filter data when going through the combined-batch path. Both the
// scan path (no explicit sources) and the get path (explicit sources) in
// extractReadResults must respect tenant boundaries.
func TestServerQueryCombinedBatchTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableTimeSeriesMaintenanceQueue: true,
			},
		},
	})
	defer s.Stopper().Stop(context.Background())

	tsdb := s.TsDB().(*ts.DB)

	// Store aggregate data (no tenant prefix) and per-tenant data (tenant 2).
	require.NoError(t, tsdb.StoreData(context.Background(), ts.Resolution10s, []tspb.TimeSeriesData{
		// Aggregate source "1" contains the sum across all tenants.
		{
			Name:   "sql.query.count",
			Source: "1",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{TimestampNanos: 400 * 1e9, Value: 100.0},
				{TimestampNanos: 500 * 1e9, Value: 200.0},
			},
		},
		// Tenant 2's individual contribution, stored with source "1-2".
		{
			Name:   "sql.query.count",
			Source: "1-2",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{TimestampNanos: 400 * 1e9, Value: 10.0},
				{TimestampNanos: 500 * 1e9, Value: 20.0},
			},
		},
	}))

	conn := s.RPCClientConn(t, username.RootUserName())
	client := conn.NewTimeSeriesClient()

	tenantID := roachpb.MustMakeTenantID(2)

	// Query with tenant ID set and no explicit sources (scan path through
	// extractReadResults). The scan returns all rows; extractReadResults
	// filters to only tenant 2's data.
	scanResp, err := client.Query(context.Background(), &tspb.TimeSeriesQueryRequest{
		StartNanos: 400 * 1e9,
		EndNanos:   500 * 1e9,
		Queries: []tspb.Query{
			{Name: "sql.query.count", TenantID: tenantID},
		},
	})
	require.NoError(t, err)
	require.Len(t, scanResp.Results[0].Datapoints, 2)
	require.Equal(t, 10.0, scanResp.Results[0].Datapoints[0].Value)
	require.Equal(t, 20.0, scanResp.Results[0].Datapoints[1].Value)

	// Query with tenant ID and explicit sources (get path through
	// addQueryReadOps, which formats keys with MakeTenantSource).
	getResp, err := client.Query(context.Background(), &tspb.TimeSeriesQueryRequest{
		StartNanos: 400 * 1e9,
		EndNanos:   500 * 1e9,
		Queries: []tspb.Query{
			{Name: "sql.query.count", TenantID: tenantID, Sources: []string{"1"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, getResp.Results[0].Datapoints, 2)
	require.Equal(t, 10.0, getResp.Results[0].Datapoints[0].Value)
	require.Equal(t, 20.0, getResp.Results[0].Datapoints[1].Value)

	// System tenant should see the aggregate data, not double-counted.
	systemID := roachpb.MustMakeTenantID(1)
	sysResp, err := client.Query(context.Background(), &tspb.TimeSeriesQueryRequest{
		StartNanos: 400 * 1e9,
		EndNanos:   500 * 1e9,
		Queries: []tspb.Query{
			{Name: "sql.query.count", TenantID: systemID},
		},
	})
	require.NoError(t, err)
	require.Len(t, sysResp.Results[0].Datapoints, 2)
	require.Equal(t, 100.0, sysResp.Results[0].Datapoints[0].Value)
	require.Equal(t, 200.0, sysResp.Results[0].Datapoints[1].Value)
}

// TestServerQueryStarvation tests a very specific scenario, wherein a single
// query request has more queries than the server's MaxWorkers count.
func TestServerQueryStarvation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	workerCount := 20
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		// For now, direct access to the tsdb is reserved to the storage layer.
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,

		TimeSeriesQueryWorkerMax: workerCount,
	})
	defer s.Stopper().Stop(context.Background())

	seriesCount := workerCount * 2
	if err := populateSeries(seriesCount, 10, 3, s.TsDB().(*ts.DB)); err != nil {
		t.Fatal(err)
	}

	conn := s.RPCClientConn(t, username.RootUserName())
	client := conn.NewTimeSeriesClient()

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

func TestServerQueryTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,

		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableTimeSeriesMaintenanceQueue: true,
			},
		},
	})
	defer s.Stopper().Stop(context.Background())

	// This metric exists in the tenant registry since it's SQL-specific.
	tenantMetricName := "sql.insert.count"
	// This metric exists only in the host/system registry since it's process-level.
	hostMetricName := "sys.rss"
	// This is a store-level metric identified by isStoreTenantMetric.
	storeMetricName := "cr.store.livebytes"

	// Populate data directly. Aggregate sources ("1", "10") contain the sum
	// of all tenants' data. Per-tenant sources ("1-2", "10-2") track tenant 2's
	// individual contribution. The aggregate values are set to be the obvious
	// sum so that the test clearly demonstrates no double-counting occurs:
	//   node 1 aggregate (101) = tenant 2 (1) + other tenants (100)
	//   node 10 aggregate (204) = tenant 2 (4) + other tenants (200)
	tsdb := s.TsDB().(*ts.DB)
	if err := tsdb.StoreData(context.Background(), ts.Resolution10s, []tspb.TimeSeriesData{
		{
			Name:   tenantMetricName,
			Source: "1",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{
					TimestampNanos: 400 * 1e9,
					Value:          101.0,
				},
				{
					TimestampNanos: 500 * 1e9,
					Value:          202.0,
				},
			},
		},
		{
			Name:   tenantMetricName,
			Source: "1-2",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{
					TimestampNanos: 400 * 1e9,
					Value:          1.0,
				},
				{
					TimestampNanos: 500 * 1e9,
					Value:          2.0,
				},
			},
		},
		{
			Name:   tenantMetricName,
			Source: "10",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{
					TimestampNanos: 400 * 1e9,
					Value:          204.0,
				},
				{
					TimestampNanos: 500 * 1e9,
					Value:          405.0,
				},
			},
		},
		{
			Name:   tenantMetricName,
			Source: "10-2",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{
					TimestampNanos: 400 * 1e9,
					Value:          4.0,
				},
				{
					TimestampNanos: 500 * 1e9,
					Value:          5.0,
				},
			},
		},
		{
			Name:   hostMetricName,
			Source: "1",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{
					TimestampNanos: 400 * 1e9,
					Value:          13.0,
				},
				{
					TimestampNanos: 500 * 1e9,
					Value:          27.0,
				},
			},
		},
		{
			Name:   hostMetricName,
			Source: "10",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{
					TimestampNanos: 400 * 1e9,
					Value:          31.0,
				},
				{
					TimestampNanos: 500 * 1e9,
					Value:          57.0,
				},
			},
		},
		// Store-level tenant metric data follows the same aggregate/per-tenant
		// pattern. Aggregate "1" = 50 (includes tenant 2's 10), "1-2" = 10.
		{
			Name:   storeMetricName,
			Source: "1",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{
					TimestampNanos: 400 * 1e9,
					Value:          50.0,
				},
				{
					TimestampNanos: 500 * 1e9,
					Value:          70.0,
				},
			},
		},
		{
			Name:   storeMetricName,
			Source: "1-2",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{
					TimestampNanos: 400 * 1e9,
					Value:          10.0,
				},
				{
					TimestampNanos: 500 * 1e9,
					Value:          20.0,
				},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	// Undefined tenant ID should return aggregate values only (no double-counting).
	// The aggregate source "1" has values 101.0 and 202.0, and source "10" has 204.0 and 405.0.
	// Note these are the aggregate values, NOT aggregate + per-tenant (which would be
	// 102.0 and 204.0 for source "1" if double-counting occurred).
	expectedAggregatedResult := &tspb.TimeSeriesQueryResponse{
		Results: []tspb.TimeSeriesQueryResponse_Result{
			{
				Query: tspb.Query{
					Name:    tenantMetricName,
					Sources: []string{"1"},
				},
				Datapoints: []tspb.TimeSeriesDatapoint{
					{
						TimestampNanos: 400 * 1e9,
						Value:          101.0,
					},
					{
						TimestampNanos: 500 * 1e9,
						Value:          202.0,
					},
				},
			},
			{
				Query: tspb.Query{
					Name:    tenantMetricName,
					Sources: []string{"1", "10"},
				},
				Datapoints: []tspb.TimeSeriesDatapoint{
					{
						TimestampNanos: 400 * 1e9,
						Value:          305.0,
					},
					{
						TimestampNanos: 500 * 1e9,
						Value:          607.0,
					},
				},
			},
		},
	}

	conn := s.RPCClientConn(t, username.RootUserName())
	client := conn.NewTimeSeriesClient()
	aggregatedResponse, err := client.Query(context.Background(), &tspb.TimeSeriesQueryRequest{
		StartNanos: 400 * 1e9,
		EndNanos:   500 * 1e9,
		Queries: []tspb.Query{
			{
				Name:    tenantMetricName,
				Sources: []string{"1"},
			},
			{
				// Not providing a source (nodeID or storeID) will aggregate across all sources.
				Name: tenantMetricName,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range aggregatedResponse.Results {
		sort.Strings(r.Sources)
	}
	require.Equal(t, expectedAggregatedResult, aggregatedResponse)

	// System tenant ID should provide system tenant ts data (same as aggregate).
	systemID := roachpb.MustMakeTenantID(1)
	expectedSystemResult := &tspb.TimeSeriesQueryResponse{
		Results: []tspb.TimeSeriesQueryResponse_Result{
			{
				Query: tspb.Query{
					Name:     tenantMetricName,
					Sources:  []string{"1"},
					TenantID: systemID,
				},
				Datapoints: []tspb.TimeSeriesDatapoint{
					{
						TimestampNanos: 400 * 1e9,
						Value:          101.0,
					},
					{
						TimestampNanos: 500 * 1e9,
						Value:          202.0,
					},
				},
			},
			{
				Query: tspb.Query{
					Name:     tenantMetricName,
					Sources:  []string{"1", "10"},
					TenantID: systemID,
				},
				Datapoints: []tspb.TimeSeriesDatapoint{
					{
						TimestampNanos: 400 * 1e9,
						Value:          305.0,
					},
					{
						TimestampNanos: 500 * 1e9,
						Value:          607.0,
					},
				},
			},
		},
	}

	systemResponse, err := client.Query(context.Background(), &tspb.TimeSeriesQueryRequest{
		StartNanos: 400 * 1e9,
		EndNanos:   500 * 1e9,
		Queries: []tspb.Query{
			{
				Name:     tenantMetricName,
				Sources:  []string{"1"},
				TenantID: systemID,
			},
			{
				Name:     tenantMetricName,
				TenantID: systemID,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range systemResponse.Results {
		sort.Strings(r.Sources)
	}
	require.Equal(t, expectedSystemResult, systemResponse)

	// App tenant should only report metrics with its tenant ID in the secondary source field
	tenantID := roachpb.MustMakeTenantID(2)
	expectedTenantResponse := &tspb.TimeSeriesQueryResponse{
		Results: []tspb.TimeSeriesQueryResponse_Result{
			{
				Query: tspb.Query{
					Name:     tenantMetricName,
					Sources:  []string{"1"},
					TenantID: tenantID,
				},
				Datapoints: []tspb.TimeSeriesDatapoint{
					{
						TimestampNanos: 400 * 1e9,
						Value:          1.0,
					},
					{
						TimestampNanos: 500 * 1e9,
						Value:          2.0,
					},
				},
			},
			{
				Query: tspb.Query{
					Name:     tenantMetricName,
					Sources:  []string{"1", "10"},
					TenantID: tenantID,
				},
				Datapoints: []tspb.TimeSeriesDatapoint{
					{
						TimestampNanos: 400 * 1e9,
						Value:          5.0,
					},
					{
						TimestampNanos: 500 * 1e9,
						Value:          7.0,
					},
				},
			},
		},
	}

	tenant, _ := serverutils.StartTenant(t, s, base.TestTenantArgs{TenantID: tenantID})
	require.NoError(t, s.GrantTenantCapabilities(
		context.Background(), tenantID,
		map[tenantcapabilitiespb.ID]string{tenantcapabilitiespb.CanViewTSDBMetrics: "true"}))
	tenantConn := tenant.RPCClientConn(t, username.RootUserName())
	tenantClient := tenantConn.NewTimeSeriesClient()

	tenantResponse, err := tenantClient.Query(context.Background(), &tspb.TimeSeriesQueryRequest{
		StartNanos: 400 * 1e9,
		EndNanos:   500 * 1e9,
		Queries: []tspb.Query{
			{
				Name:    tenantMetricName,
				Sources: []string{"1"},
			},
			{
				// Not providing a source (nodeID or storeID) will aggregate across all sources.
				Name: tenantMetricName,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range tenantResponse.Results {
		sort.Strings(r.Sources)
	}
	require.Equal(t, expectedTenantResponse, tenantResponse)

	// Store-level tenant metrics (e.g. cr.store.livebytes) should be scoped
	// to the tenant via isStoreTenantMetric, even though they're not in the
	// tenant registry. Tenant 2 should see only its per-tenant data.
	expectedTenantStoreResponse := &tspb.TimeSeriesQueryResponse{
		Results: []tspb.TimeSeriesQueryResponse_Result{
			{
				Query: tspb.Query{
					Name:     storeMetricName,
					Sources:  []string{"1"},
					TenantID: tenantID,
				},
				Datapoints: []tspb.TimeSeriesDatapoint{
					{
						TimestampNanos: 400 * 1e9,
						Value:          10.0,
					},
					{
						TimestampNanos: 500 * 1e9,
						Value:          20.0,
					},
				},
			},
		},
	}
	tenantStoreResponse, err := tenantClient.Query(context.Background(), &tspb.TimeSeriesQueryRequest{
		StartNanos: 400 * 1e9,
		EndNanos:   500 * 1e9,
		Queries: []tspb.Query{
			{
				Name:    storeMetricName,
				Sources: []string{"1"},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, expectedTenantStoreResponse, tenantStoreResponse)

	// The system tenant should see the aggregate store metric values.
	expectedSystemStoreResponse := &tspb.TimeSeriesQueryResponse{
		Results: []tspb.TimeSeriesQueryResponse_Result{
			{
				Query: tspb.Query{
					Name:    storeMetricName,
					Sources: []string{"1"},
				},
				Datapoints: []tspb.TimeSeriesDatapoint{
					{
						TimestampNanos: 400 * 1e9,
						Value:          50.0,
					},
					{
						TimestampNanos: 500 * 1e9,
						Value:          70.0,
					},
				},
			},
		},
	}
	systemStoreResponse, err := client.Query(context.Background(), &tspb.TimeSeriesQueryRequest{
		StartNanos: 400 * 1e9,
		EndNanos:   500 * 1e9,
		Queries: []tspb.Query{
			{
				Name:    storeMetricName,
				Sources: []string{"1"},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, expectedSystemStoreResponse, systemStoreResponse)

	// Test that host metrics are inaccessible to tenant without capability.
	hostMetricsRequest := &tspb.TimeSeriesQueryRequest{
		StartNanos: 400 * 1e9,
		EndNanos:   500 * 1e9,
		Queries: []tspb.Query{
			{
				Name:    hostMetricName,
				Sources: []string{"1"},
			},
		},
	}

	_, err = tenantClient.Query(context.Background(), hostMetricsRequest)
	require.Error(t, err)

	// Test that after enabling all metrics capability, host metrics are returned.
	expectedTenantHostMetricsResponse := &tspb.TimeSeriesQueryResponse{
		Results: []tspb.TimeSeriesQueryResponse_Result{
			{
				Query: tspb.Query{
					Name:    hostMetricName,
					Sources: []string{"1"},
				},
				Datapoints: []tspb.TimeSeriesDatapoint{
					{
						TimestampNanos: 400 * 1e9,
						Value:          13.0,
					},
					{
						TimestampNanos: 500 * 1e9,
						Value:          27.0,
					},
				},
			},
		},
	}
	require.NoError(t, s.GrantTenantCapabilities(
		context.Background(), tenantID,
		map[tenantcapabilitiespb.ID]string{tenantcapabilitiespb.CanViewAllMetrics: "true"}))

	tenantResponse, err = tenantClient.Query(context.Background(), &tspb.TimeSeriesQueryRequest{
		StartNanos: 400 * 1e9,
		EndNanos:   500 * 1e9,
		Queries: []tspb.Query{
			{
				Name:    hostMetricName,
				Sources: []string{"1"},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, expectedTenantHostMetricsResponse, tenantResponse)
}

// TestServerQueryMemoryManagement verifies that queries succeed under
// constrained memory requirements.
func TestServerQueryMemoryManagement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		// For now, direct access to the tsdb is reserved to the storage layer.
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,

		TimeSeriesQueryWorkerMax:    workerCount,
		TimeSeriesQueryMemoryBudget: budget,
	})
	defer s.Stopper().Stop(context.Background())

	if err := populateSeries(seriesCount, sourceCount, valueCount, s.TsDB().(*ts.DB)); err != nil {
		t.Fatal(err)
	}

	conn := s.RPCClientConn(t, username.RootUserName())
	client := conn.NewTimeSeriesClient()

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
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	seriesCount := 10
	sourceCount := 5
	// Number of slabs (hours) of data we want to generate
	slabCount := 5
	// Number of datapoints to generate every hour. Generated datapoints every
	// 100 seconds, so compute how many we want to generate data across one hour.
	numPointsEachHour := int(ts.Resolution10s.SlabDuration() / (100 * 1e9))
	// Number of total datapoints.
	valueCount := numPointsEachHour * slabCount
	// We'll dump [startVal, endVal) below. To simplify the test, pick them
	// according to a slab boundary.
	startSlab, endSlab := 2, 4
	startVal := numPointsEachHour * startSlab
	endVal := numPointsEachHour * endSlab

	// Generate expected data.
	expectedMap := make(map[string]map[string]tspb.TimeSeriesData)
	for series := 0; series < seriesCount; series++ {
		sourceMap := make(map[string]tspb.TimeSeriesData)
		expectedMap[seriesName(series)] = sourceMap
		for source := 0; source < sourceCount; source++ {
			sourceMap[sourceName(source)] = tspb.TimeSeriesData{
				Name:       seriesName(series),
				Source:     sourceName(source),
				Datapoints: generateTimeSeriesDatapoints(startVal, endVal),
			}
		}
	}

	expTotalMsgCount := seriesCount * sourceCount * (endSlab - startSlab)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		// For now, direct access to the tsdb is reserved to the storage layer.
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,

		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableTimeSeriesMaintenanceQueue: true,
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	if err := populateSeries(seriesCount, sourceCount, valueCount, s.TsDB().(*ts.DB)); err != nil {
		t.Fatal(err)
	}

	names := make([]string, 0, seriesCount)
	for series := 0; series < seriesCount; series++ {
		names = append(names, seriesName(series))
	}

	conn := s.RPCClientConn(t, username.RootUserName())
	client := conn.NewTimeSeriesClient()

	dumpClient, err := client.Dump(ctx, &tspb.DumpRequest{
		Names:      names,
		StartNanos: datapointTimestampNanos(startVal),
		EndNanos:   datapointTimestampNanos(endVal),
	})
	if err != nil {
		t.Fatal(err)
	}

	readDataFromDump := func(t *testing.T, dumpClient tspb.RPCTimeSeries_DumpClient) (totalMsgCount int, _ map[string]map[string]tspb.TimeSeriesData) {
		t.Helper()
		// Read data from dump command.
		resultMap := make(map[string]map[string]tspb.TimeSeriesData)
		for {
			msg, err := dumpClient.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
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
		return totalMsgCount, resultMap
	}

	totalMsgCount, resultMap := readDataFromDump(t, dumpClient)

	if a, e := totalMsgCount, expTotalMsgCount; a != e {
		t.Fatalf("dump returned %d messages, expected %d", a, e)
	}
	if a, e := resultMap, expectedMap; !reflect.DeepEqual(a, e) {
		for _, diff := range pretty.Diff(a, e) {
			t.Error(diff)
		}
	}

	// Verify that when we dump the raw KVs, we get the right number.
	// The next subtest verifies them in detail.
	dumpRawClient, err := client.DumpRaw(ctx, &tspb.DumpRequest{
		Names:      names,
		StartNanos: datapointTimestampNanos(startVal),
		EndNanos:   datapointTimestampNanos(endVal),
	})
	require.NoError(t, err)
	var kvs []*roachpb.KeyValue
	for {
		kv, err := dumpRawClient.Recv()
		if err == io.EOF {
			break
		}
		kvs = append(kvs, kv)
	}
	require.EqualValues(t, expTotalMsgCount, len(kvs))

	s.Stopper().Stop(ctx)

	// Start a new server, into which to write the raw dump.
	s = serverutils.StartServerOnly(t, base.TestServerArgs{
		// For now, direct access to the tsdb is reserved to the storage layer.
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,

		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableTimeSeriesMaintenanceQueue: true,
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	var b kv.Batch
	for _, kv := range kvs {
		p := kvpb.NewPut(kv.Key, kv.Value)
		p.(*kvpb.PutRequest).Inline = true
		b.AddRawRequest(p)
	}
	// Write and check multiple times, to make sure there aren't any issues
	// when overwriting existing timeseries kv pairs (which are usually written
	// via Merge commands).
	for i := 0; i < 3; i++ {
		require.NoError(t, s.DB().Run(ctx, &b))

		conn := s.RPCClientConn(t, username.RootUserName())
		client := conn.NewTimeSeriesClient()

		dumpClient, err := client.Dump(ctx, &tspb.DumpRequest{
			Names:      names,
			StartNanos: datapointTimestampNanos(startVal),
			EndNanos:   datapointTimestampNanos(endVal),
		})
		if err != nil {
			t.Fatal(err)
		}

		_, resultsMap := readDataFromDump(t, dumpClient)
		require.Equal(t, expectedMap, resultsMap)
	}
}

func TestServerDumpChildMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		// For now, direct access to the tsdb is reserved to the storage layer.
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,

		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableTimeSeriesMaintenanceQueue: true,
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	tsdb := s.TsDB().(*ts.DB)

	// Store parent metric (without labels)
	parentMetric := "cr.node.changefeed.emitted_messages"
	if err := tsdb.StoreData(ctx, ts.Resolution10s, []tspb.TimeSeriesData{
		{
			Name:   parentMetric,
			Source: "1",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{TimestampNanos: 100 * 1e9, Value: 1000.0},
				{TimestampNanos: 200 * 1e9, Value: 2000.0},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	// Store child metrics (with labels encoded in name)
	childMetric1 := fmt.Sprintf(`%s{feed_id="123",scope="default"}`, parentMetric)
	childMetric2 := fmt.Sprintf(`%s{feed_id="456",scope="system"}`, parentMetric)
	if err := tsdb.StoreData(ctx, ts.Resolution1m, []tspb.TimeSeriesData{
		{
			Name:   childMetric1,
			Source: "1",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{TimestampNanos: 100 * 1e9, Value: 500.0},
				{TimestampNanos: 200 * 1e9, Value: 1500.0},
			},
		},
		{
			Name:   childMetric2,
			Source: "1",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{TimestampNanos: 100 * 1e9, Value: 300.0},
				{TimestampNanos: 200 * 1e9, Value: 800.0},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	conn := s.RPCClientConn(t, username.RootUserName())
	client := conn.NewTimeSeriesClient()

	t.Run("includes child metrics", func(t *testing.T) {
		dumpClient, err := client.Dump(ctx, &tspb.DumpRequest{
			Names:      []string{parentMetric},
			StartNanos: 100 * 1e9,
			EndNanos:   300 * 1e9,
		})
		require.NoError(t, err)

		resultMap := make(map[string][]tspb.TimeSeriesDatapoint)
		for {
			msg, err := dumpClient.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			resultMap[msg.Name] = append(resultMap[msg.Name], msg.Datapoints...)
		}

		// Should have parent metric AND both child metrics
		require.Contains(t, resultMap, parentMetric, "parent metric should be included")
		require.Contains(t, resultMap, childMetric1, "child metric 1 should be included")
		require.Contains(t, resultMap, childMetric2, "child metric 2 should be included")
		require.Equal(t, 3, len(resultMap), "should have parent and both child metrics")

		// Verify data correctness for parent metric
		require.Len(t, resultMap[parentMetric], 2, "parent metric should have 2 datapoints")
		require.Equal(t, 1000.0, resultMap[parentMetric][0].Value)
		require.Equal(t, 2000.0, resultMap[parentMetric][1].Value)

		// Verify data correctness for child metrics
		require.Len(t, resultMap[childMetric1], 2, "child metric 1 should have 2 datapoints")
		require.Equal(t, 500.0, resultMap[childMetric1][0].Value)
		require.Equal(t, 1500.0, resultMap[childMetric1][1].Value)

		require.Len(t, resultMap[childMetric2], 2, "child metric 2 should have 2 datapoints")
		require.Equal(t, 300.0, resultMap[childMetric2][0].Value)
		require.Equal(t, 800.0, resultMap[childMetric2][1].Value)
	})

	t.Run("DumpRaw sees child metrics", func(t *testing.T) {
		dumpRawClient, err := client.DumpRaw(ctx, &tspb.DumpRequest{
			Names:      []string{parentMetric},
			StartNanos: 100 * 1e9,
			EndNanos:   300 * 1e9,
		})
		require.NoError(t, err)

		var kvCount int
		seenMetrics := make(map[string]bool)
		for {
			kv, err := dumpRawClient.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			kvCount++
			// Decode the key to verify it contains child metrics
			// The key contains the encoded metric name
			keyStr := string(kv.Key)
			if strings.Contains(keyStr, "{") {
				seenMetrics["child"] = true
			}
		}

		require.Greater(t, kvCount, 0, "should have raw KVs")
		require.True(t, seenMetrics["child"], "should have seen child metrics in raw dump")
	})
}

// TestServerDumpHistogramChildMetrics exercises the dump path for labeled
// histogram child metrics. The CLI expands histogram metric names with
// quantile/count/sum suffixes (e.g. "cr.node.foo-p99"), but labeled histogram
// children are stored as "cr.node.foo{labels}-anysuffix" -- labels are
// inserted before the suffix. The dump server must strip the histogram suffix
// when looking up the base in AllowedChildMetrics and use the base as the
// scan prefix; otherwise labeled histogram children are missed entirely
// because '{' (0x7B) > '-' (0x2D) puts them outside the [name-suffix,
// name-suffix|) span.
func TestServerDumpHistogramChildMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableTimeSeriesMaintenanceQueue: true,
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	tsdb := s.TsDB().(*ts.DB)

	// changefeed.sink_backpressure_nanos is a histogram in the
	// AllowedChildMetrics list.
	parentBase := "cr.node.changefeed.sink_backpressure_nanos"

	// Store the parent histogram series (one per quantile/computer suffix).
	var parentSeries []tspb.TimeSeriesData
	for _, c := range metric.HistogramMetricComputers {
		parentSeries = append(parentSeries, tspb.TimeSeriesData{
			Name:   parentBase + c.Suffix,
			Source: "1",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{TimestampNanos: 100 * 1e9, Value: 1.0},
			},
		})
	}
	require.NoError(t, tsdb.StoreData(ctx, ts.Resolution10s, parentSeries))

	// Store labeled histogram child series in production format:
	// "<basename>{labels}<suffix>" -- labels appear BEFORE the suffix.
	var childSeries []tspb.TimeSeriesData
	for _, c := range metric.HistogramMetricComputers {
		childSeries = append(childSeries, tspb.TimeSeriesData{
			Name:   fmt.Sprintf(`%s{scope="default"}%s`, parentBase, c.Suffix),
			Source: "1",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{TimestampNanos: 100 * 1e9, Value: 2.0},
			},
		})
	}
	require.NoError(t, tsdb.StoreData(ctx, ts.Resolution1m, childSeries))

	// The CLI passes histogram-expanded names ("<base>-<suffix>"), one per
	// quantile, mirroring GetInternalTimeseriesNamesFromServer.
	var requestNames []string
	for _, c := range metric.HistogramMetricComputers {
		requestNames = append(requestNames, parentBase+c.Suffix)
	}

	conn := s.RPCClientConn(t, username.RootUserName())
	client := conn.NewTimeSeriesClient()

	dumpClient, err := client.Dump(ctx, &tspb.DumpRequest{
		Names:      requestNames,
		StartNanos: 100 * 1e9,
		EndNanos:   300 * 1e9,
	})
	require.NoError(t, err)

	seen := make(map[string]struct{})
	for {
		msg, err := dumpClient.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		seen[msg.Name] = struct{}{}
	}

	// Each labeled histogram child must appear in the dump output.
	for _, c := range metric.HistogramMetricComputers {
		want := fmt.Sprintf(`%s{scope="default"}%s`, parentBase, c.Suffix)
		require.Contains(t, seen, want,
			"missing labeled histogram child %s; got %v", want, seen)
	}
}

func BenchmarkServerQuery(b *testing.B) {
	defer log.Scope(b).Close(b)

	s := serverutils.StartServerOnly(b, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	// Populate data for large number of time series.
	seriesCount := 50
	sourceCount := 10
	if err := populateSeries(seriesCount, sourceCount, 3, s.TsDB().(*ts.DB)); err != nil {
		b.Fatal(err)
	}

	conn := s.RPCClientConn(b, username.RootUserName())
	client := conn.NewTimeSeriesClient()

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

func datapointTimestampNanos(val int) int64 {
	return int64(val * 100 * 1e9)
}

func datapointValue(val int) float64 {
	return float64(val * 100)
}

func generateTimeSeriesDatapoints(startValue, endValue int) []tspb.TimeSeriesDatapoint {
	result := make([]tspb.TimeSeriesDatapoint, 0, endValue-startValue)
	for i := startValue; i < endValue; i++ {
		result = append(result, tspb.TimeSeriesDatapoint{
			TimestampNanos: datapointTimestampNanos(i),
			Value:          datapointValue(i),
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
					Datapoints: generateTimeSeriesDatapoints(0 /* startValue */, valueCount),
				},
			}); err != nil {
				return errors.Wrapf(
					err, "error storing data for series %d, source %d", series, source,
				)
			}
		}
	}
	return nil
}
