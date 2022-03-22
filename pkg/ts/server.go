// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ts

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/ts/catalog"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// URLPrefix is the prefix for all time series endpoints hosted by the
	// server.
	URLPrefix = "/ts/"
	// queryWorkerMax is the default maximum number of worker goroutines that
	// the time series server can use to service incoming queries.
	queryWorkerMax = 8
	// DefaultQueryMemoryMax is a soft limit for the amount of total
	// memory used by time series queries. This is not currently enforced,
	// but is used for monitoring purposes.
	DefaultQueryMemoryMax = int64(64 * 1024 * 1024) // 64MiB
	// dumpBatchSize is the number of keys processed in each batch by the dump
	// command.
	dumpBatchSize = 100
)

// ClusterNodeCountFn is a function that returns the number of nodes active on
// the cluster.
type ClusterNodeCountFn func() int64

// ServerConfig provides a means for tests to override settings in the time
// series server.
type ServerConfig struct {
	// The maximum number of query workers used by the server. If this
	// value is zero, a default non-zero value is used instead.
	QueryWorkerMax int
	// The maximum amount of memory that should be used for processing queries
	// across all workers. If this value is zero, a default non-zero value is
	// used instead.
	QueryMemoryMax int64
}

// Server handles incoming external requests related to time series data.
//
// The server attempts to constrain the total amount of memory it uses for
// processing incoming queries. This is accomplished with a multi-pronged
// strategy:
// + The server has a worker memory limit, which is a quota for the amount of
//   memory that can be used across all currently executing queries.
// + The server also has a pre-set limit on the number of parallel workers that
//   can be executing at one time. Each worker is given an even share of the
//   server's total memory limit, which it should not exceed.
// + Each worker breaks its task into chunks which it will process sequentially;
//   the size of each chunk is calculated to avoid exceeding the memory limit.
//
// In addition to this strategy, the server uses a memory monitor to track the
// amount of memory being used in reality by worker tasks. This is intended to
// verify the calculations of the individual workers are correct.
//
// A second memory monitor is used to track the space used by the results of
// query workers, which are longer lived; an incoming request may utilize
// several workers, but the results of each worker cannot be released until
// being returned to the requestor. Result memory is not currently limited,
// as in practical usage it is dwarfed by the memory needed by workers to
// generate the results.
type Server struct {
	log.AmbientContext
	db               *DB
	stopper          *stop.Stopper
	nodeCountFn      ClusterNodeCountFn
	queryMemoryMax   int64
	queryWorkerMax   int
	workerMemMonitor *mon.BytesMonitor
	resultMemMonitor *mon.BytesMonitor
	workerSem        *quotapool.IntPool
}

// MakeServer instantiates a new Server which services requests with data from
// the supplied DB.
func MakeServer(
	ambient log.AmbientContext,
	db *DB,
	nodeCountFn ClusterNodeCountFn,
	cfg ServerConfig,
	memoryMonitor *mon.BytesMonitor,
	stopper *stop.Stopper,
) Server {
	ambient.AddLogTag("ts-srv", nil)
	ctx := ambient.AnnotateCtx(context.Background())

	// Override default values from configuration.
	queryWorkerMax := queryWorkerMax
	if cfg.QueryWorkerMax != 0 {
		queryWorkerMax = cfg.QueryWorkerMax
	}
	queryMemoryMax := DefaultQueryMemoryMax
	if cfg.QueryMemoryMax > DefaultQueryMemoryMax {
		queryMemoryMax = cfg.QueryMemoryMax
	}
	workerSem := quotapool.NewIntPool("ts.Server worker", uint64(queryWorkerMax))
	stopper.AddCloser(workerSem.Closer("stopper"))
	s := Server{
		AmbientContext: ambient,
		db:             db,
		stopper:        stopper,
		nodeCountFn:    nodeCountFn,
		workerMemMonitor: mon.NewMonitorInheritWithLimit(
			"timeseries-workers",
			queryMemoryMax*2,
			memoryMonitor,
		),
		resultMemMonitor: mon.NewMonitorInheritWithLimit(
			"timeseries-results",
			math.MaxInt64,
			memoryMonitor,
		),
		queryMemoryMax: queryMemoryMax,
		queryWorkerMax: queryWorkerMax,
		workerSem:      workerSem,
	}

	s.workerMemMonitor.Start(ctx, memoryMonitor, mon.BoundAccount{})
	stopper.AddCloser(stop.CloserFn(func() {
		s.workerMemMonitor.Stop(ctx)
	}))

	s.resultMemMonitor.Start(ambient.AnnotateCtx(context.Background()), memoryMonitor, mon.BoundAccount{})
	stopper.AddCloser(stop.CloserFn(func() {
		s.resultMemMonitor.Stop(ctx)
	}))

	return s
}

// RegisterService registers the GRPC service.
func (s *Server) RegisterService(g *grpc.Server) {
	tspb.RegisterTimeSeriesServer(g, s)
}

// RegisterGateway starts the gateway (i.e. reverse proxy) that proxies HTTP requests
// to the appropriate gRPC endpoints.
func (s *Server) RegisterGateway(
	ctx context.Context, mux *gwruntime.ServeMux, conn *grpc.ClientConn,
) error {
	return tspb.RegisterTimeSeriesHandler(ctx, mux, conn)
}

// Query is an endpoint that returns data for one or more metrics over a
// specific time span.
func (s *Server) Query(
	ctx context.Context, request *tspb.TimeSeriesQueryRequest,
) (*tspb.TimeSeriesQueryResponse, error) {
	ctx = s.AnnotateCtx(ctx)
	if len(request.Queries) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Queries cannot be empty")
	}

	// If not set, sampleNanos should default to ten second resolution.
	sampleNanos := request.SampleNanos
	if sampleNanos == 0 {
		sampleNanos = Resolution10s.SampleDuration()
	}

	// For the interpolation limit, use the time limit until stores are considered
	// dead. This is a conservatively long span, but gives us a good indication of
	// when a gap likely indicates an outage (and thus missing values should not
	// be interpolated).
	interpolationLimit := kvserver.TimeUntilStoreDead.Get(&s.db.st.SV).Nanoseconds()

	// Get the estimated number of nodes on the cluster, used to compute more
	// accurate memory usage estimates. Set a minimum of 1 in order to avoid
	// divide-by-zero panics.
	estimatedClusterNodeCount := s.nodeCountFn()
	if estimatedClusterNodeCount == 0 {
		estimatedClusterNodeCount = 1
	}

	response := tspb.TimeSeriesQueryResponse{
		Results: make([]tspb.TimeSeriesQueryResponse_Result, len(request.Queries)),
	}

	// Defer cancellation of context passed to worker tasks; if main task
	// returns early, worker tasks should be torn down quickly.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Channel which workers use to report their result, which is either an
	// error or nil (when successful).
	workerOutput := make(chan error)

	// Create a separate memory management context for each query, allowing them
	// to be run in parallel.
	memContexts := make([]QueryMemoryContext, len(request.Queries))
	defer func() {
		for idx := range memContexts {
			memContexts[idx].Close(ctx)
		}
	}()

	timespan := QueryTimespan{
		StartNanos:          request.StartNanos,
		EndNanos:            request.EndNanos,
		SampleDurationNanos: sampleNanos,
		NowNanos:            timeutil.Now().UnixNano(),
	}

	// Start a task which is itself responsible for starting per-query worker
	// tasks. This is needed because RunAsyncTaskEx can block; in the
	// case where a single request has more queries than the semaphore limit,
	// a deadlock would occur because queries cannot complete until
	// they have written their result to the "output" channel, which is
	// processed later in the main function.
	if err := s.stopper.RunAsyncTask(ctx, "ts.Server: queries", func(ctx context.Context) {
		for queryIdx, query := range request.Queries {
			queryIdx := queryIdx
			query := query

			if err := s.stopper.RunAsyncTaskEx(
				ctx,
				stop.TaskOpts{
					TaskName:   "ts.Server: query",
					Sem:        s.workerSem,
					WaitForSem: true,
				},
				func(ctx context.Context) {
					// Estimated source count is either the count of requested sources
					// *or* the estimated cluster node count if no sources are specified.
					var estimatedSourceCount int64
					if len(query.Sources) > 0 {
						estimatedSourceCount = int64(len(query.Sources))
					} else {
						estimatedSourceCount = estimatedClusterNodeCount
					}

					// Create a memory account for the results of this query.
					memContexts[queryIdx] = MakeQueryMemoryContext(
						s.workerMemMonitor,
						s.resultMemMonitor,
						QueryMemoryOptions{
							BudgetBytes:             s.queryMemoryMax / int64(s.queryWorkerMax),
							EstimatedSources:        estimatedSourceCount,
							InterpolationLimitNanos: interpolationLimit,
						},
					)

					datapoints, sources, err := s.db.Query(
						ctx,
						query,
						Resolution10s,
						timespan,
						memContexts[queryIdx],
					)
					if err == nil {
						response.Results[queryIdx] = tspb.TimeSeriesQueryResponse_Result{
							Query:      query,
							Datapoints: datapoints,
						}
						response.Results[queryIdx].Sources = sources
					}
					select {
					case workerOutput <- err:
					case <-ctx.Done():
					}
				},
			); err != nil {
				// Stopper has been closed and is draining. Return an error and
				// exit the worker-spawning loop.
				select {
				case workerOutput <- err:
				case <-ctx.Done():
				}
				return
			}
		}
	}); err != nil {
		return nil, err
	}

	for range request.Queries {
		select {
		case err := <-workerOutput:
			if err != nil {
				// Return the first error encountered. This will cancel the
				// worker context and cause all other in-progress workers to
				// exit.
				return nil, err
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return &response, nil
}

// Dump returns a stream of raw timeseries data that has been stored on the
// server. Only data from the 10-second resolution is returned; rollup data is
// not currently returned. Data is returned in the order it is read from disk,
// and will thus not be totally organized by series.
//
// TODO(tbg): needs testing that restricting to individual timeseries works
// and that the date range restrictions are respected. Should be easy enough to
// set up a KV store and write some keys into it (`MakeDataKey`) to do so without
// setting up a `*Server`.
func (s *Server) Dump(req *tspb.DumpRequest, stream tspb.TimeSeries_DumpServer) error {
	d := defaultDumper{stream}.Dump
	return dumpImpl(stream.Context(), s.db.db, req, d)

}

// DumpRaw is like Dump, but it returns a stream of raw KV pairs.
func (s *Server) DumpRaw(req *tspb.DumpRequest, stream tspb.TimeSeries_DumpRawServer) error {
	d := rawDumper{stream}.Dump
	return dumpImpl(stream.Context(), s.db.db, req, d)
}

func dumpImpl(
	ctx context.Context, db *kv.DB, req *tspb.DumpRequest, d func(*roachpb.KeyValue) error,
) error {
	names := req.Names
	if len(names) == 0 {
		names = catalog.AllInternalTimeseriesMetricNames()
	}
	resolutions := req.Resolutions
	if len(resolutions) == 0 {
		resolutions = []tspb.TimeSeriesResolution{tspb.TimeSeriesResolution_RESOLUTION_10S}
	}
	for _, seriesName := range names {
		for _, res := range resolutions {
			if err := dumpTimeseriesAllSources(
				ctx,
				db,
				seriesName,
				ResolutionFromProto(res),
				req.StartNanos,
				req.EndNanos,
				d,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

type defaultDumper struct {
	stream tspb.TimeSeries_DumpServer
}

func (dd defaultDumper) Dump(kv *roachpb.KeyValue) error {
	name, source, _, _, err := DecodeDataKey(kv.Key)
	if err != nil {
		return err
	}
	var idata roachpb.InternalTimeSeriesData
	if err := kv.Value.GetProto(&idata); err != nil {
		return err
	}

	tsdata := &tspb.TimeSeriesData{
		Name:       name,
		Source:     source,
		Datapoints: make([]tspb.TimeSeriesDatapoint, idata.SampleCount()),
	}
	for i := 0; i < idata.SampleCount(); i++ {
		if idata.IsColumnar() {
			tsdata.Datapoints[i].TimestampNanos = idata.TimestampForOffset(idata.Offset[i])
			tsdata.Datapoints[i].Value = idata.Last[i]
		} else {
			tsdata.Datapoints[i].TimestampNanos = idata.TimestampForOffset(idata.Samples[i].Offset)
			tsdata.Datapoints[i].Value = idata.Samples[i].Sum
		}
	}
	return dd.stream.Send(tsdata)
}

type rawDumper struct {
	stream tspb.TimeSeries_DumpRawServer
}

func (rd rawDumper) Dump(kv *roachpb.KeyValue) error {
	return rd.stream.Send(kv)
}

func dumpTimeseriesAllSources(
	ctx context.Context,
	db *kv.DB,
	seriesName string,
	diskResolution Resolution,
	startNanos, endNanos int64,
	dump func(*roachpb.KeyValue) error,
) error {
	if endNanos == 0 {
		endNanos = math.MaxInt64
	}

	if delta := diskResolution.SlabDuration() - 1; endNanos > math.MaxInt64-delta {
		endNanos = math.MaxInt64
	} else {
		endNanos += delta
	}

	span := &roachpb.Span{
		Key: MakeDataKey(
			seriesName, "" /* source */, diskResolution, startNanos,
		),
		EndKey: MakeDataKey(
			seriesName, "" /* source */, diskResolution, endNanos,
		),
	}

	for span != nil {
		b := &kv.Batch{}
		scan := roachpb.NewScan(span.Key, span.EndKey, false /* forUpdate */)
		b.AddRawRequest(scan)
		b.Header.MaxSpanRequestKeys = dumpBatchSize
		err := db.Run(ctx, b)
		if err != nil {
			return err
		}
		resp := b.RawResponse().Responses[0].GetScan()
		span = resp.ResumeSpan
		for i := range resp.Rows {
			if err := dump(&resp.Rows[i]); err != nil {
				return err
			}
		}
	}
	return nil
}
