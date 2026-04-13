// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ts

import (
	"context"
	"math"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/ts/tsutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"storj.io/drpc"
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

var CombinedBatchEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"timeseries.query.combine_read_batches.enabled",
	"if true, multiple time series queries in a single request share a "+
		"combined KV batch to reduce RPC overhead",
	true,
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
//   - The server has a worker memory limit, which is a quota for the amount of
//     memory that can be used across all currently executing queries.
//   - The server also has a pre-set limit on the number of parallel workers that
//     can be executing at one time. Each worker is given an even share of the
//     server's total memory limit, which it should not exceed.
//   - Each worker breaks its task into chunks which it will process sequentially;
//     the size of each chunk is calculated to avoid exceeding the memory limit.
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

var _ tspb.TimeSeriesServer = &Server{}

type TenantServer struct {
	// NB: TenantServer only implements Query from tspb.TimeSeriesServer
	tspb.UnimplementedTimeSeriesServer

	log.AmbientContext
	tenantID       roachpb.TenantID
	tenantConnect  kvtenant.Connector
	tenantRegistry *metric.Registry
}

var _ tspb.TenantTimeSeriesServer = &TenantServer{}

// Query delegates to the tenant connector to query
// the tsdb on the system tenant. The only authorization
// necessary is the tenant capability check on the
// connector.
func (t *TenantServer) Query(
	ctx context.Context, req *tspb.TimeSeriesQueryRequest,
) (*tspb.TimeSeriesQueryResponse, error) {
	ctx = t.AnnotateCtx(ctx)
	// Currently, secondary tenants are only able to view their own metrics.
	for i, q := range req.Queries {
		// Tenant-scoped metrics get marked with the tenantID. This includes both
		// app-level metrics (in tenantRegistry) and store-level tenant metrics
		// (identified by isStoreTenantMetric).
		metricName := strings.TrimPrefix(q.Name, "cr.store.")
		if t.tenantRegistry.Contains(q.Name) || isStoreTenantMetric(metricName) {
			req.Queries[i].TenantID = t.tenantID
		}
	}
	return t.tenantConnect.Query(ctx, req)
}

// storeTenantMetrics mirrors kvbase.TenantsStorageMetricsSet. We maintain a
// hardcoded copy here to avoid import cycles with kvbase.
var storeTenantMetrics = map[string]struct{}{
	"livebytes": {}, "sysbytes": {}, "keybytes": {}, "valbytes": {},
	"rangekeybytes": {}, "rangevalbytes": {}, "totalbytes": {},
	"intentbytes": {}, "lockbytes": {}, "livecount": {}, "keycount": {},
	"valcount": {}, "rangekeycount": {}, "rangevalcount": {},
	"intentcount": {}, "lockcount": {}, "lockage": {}, "gcbytesage": {},
	"syscount": {}, "abortspanbytes": {},
}

// isStoreTenantMetric returns true if name is in storeTenantMetrics.
func isStoreTenantMetric(name string) bool {
	_, ok := storeTenantMetrics[name]
	return ok
}

// RegisterService registers the GRPC service.
func (s *TenantServer) RegisterService(g *grpc.Server) {
	tspb.RegisterTimeSeriesServer(g, s)
}

type drpcTenantServer struct {
	*TenantServer
}

// RegisterService registers the DRPC service.
func (s *TenantServer) RegisterDRPCService(d drpc.Mux) error {
	return tspb.DRPCRegisterTimeSeries(d, &drpcTenantServer{TenantServer: s})
}

// RegisterGateway starts the gateway (i.e. reverse proxy) that proxies HTTP requests
// to the appropriate gRPC endpoints.
func (s *TenantServer) RegisterGateway(
	ctx context.Context, mux *gwruntime.ServeMux, conn *grpc.ClientConn,
) error {
	return tspb.RegisterTimeSeriesHandler(ctx, mux, conn)
}

func MakeTenantServer(
	ambient log.AmbientContext,
	tenantConnect kvtenant.Connector,
	tenantID roachpb.TenantID,
	registry *metric.Registry,
) *TenantServer {
	return &TenantServer{
		AmbientContext: ambient,
		tenantConnect:  tenantConnect,
		tenantID:       tenantID,
		tenantRegistry: registry,
	}
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
			mon.MakeName("timeseries-workers"),
			queryMemoryMax*2,
			memoryMonitor,
			true, /* longLiving */
		),
		resultMemMonitor: mon.NewMonitorInheritWithLimit(
			mon.MakeName("timeseries-results"),
			math.MaxInt64,
			memoryMonitor,
			true, /* longLiving */
		),
		queryMemoryMax: queryMemoryMax,
		queryWorkerMax: queryWorkerMax,
		workerSem:      workerSem,
	}
	s.workerMemMonitor.StartNoReserved(ctx, memoryMonitor)
	s.resultMemMonitor.StartNoReserved(ambient.AnnotateCtx(context.Background()), memoryMonitor)
	return s
}

// RegisterService registers the GRPC service.
func (s *Server) RegisterService(g *grpc.Server) {
	tspb.RegisterTimeSeriesServer(g, s)
}

type drpcServer struct {
	*Server
}

// RegisterService registers the DRPC service.
func (s *Server) RegisterDRPCService(d drpc.Mux) error {
	return tspb.DRPCRegisterTimeSeries(d, &drpcServer{Server: s})
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
//
// The implementation uses a two-phase approach to reduce RPC overhead:
//
// Phase 1 (combined read): Eligible queries' KV read operations are collected
// into a single kv.Batch and executed with one db.Run() call. DistSender
// splits the batch into per-range RPCs, so queries targeting the same time
// series ranges share RPCs instead of each issuing independent ones.
//
// Phase 2 (parallel processing): The read results are distributed to per-query
// goroutines for post-processing (key-to-span conversion, downsampling,
// aggregation). This CPU-bound work benefits from parallelism.
//
// Queries that require memory-based chunking (timespan exceeds budget) fall
// back to the original per-query read path via db.Query().
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
	interpolationLimit := liveness.TimeUntilNodeDead.Get(&s.db.st.SV).Nanoseconds()

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
	timespan.normalize()

	if err := timespan.verifyBounds(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}
	if err := timespan.verifyDiskResolution(Resolution10s); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}
	if err := timespan.adjustForCurrentTime(Resolution10s); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// Validate all queries upfront.
	for _, query := range request.Queries {
		if err := verifySourceAggregator(query.GetSourceAggregator()); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "%s", err)
		}
		if err := verifyDownsampler(query.GetDownsampler()); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "%s", err)
		}
	}

	// Determine whether rollup data may exist for this timespan. The combined
	// batch only reads Resolution10s and would miss rolled-up data at coarser
	// resolutions, so queries eligible for rollup must fall back to DB.Query.
	rollupRes, hasRollup := Resolution10s.TargetRollupResolution()
	needsRollup := hasRollup && timespan.verifyDiskResolution(rollupRes) == nil

	// Check whether the combined-batch optimization is enabled.
	combineEnabled := CombinedBatchEnabled.Get(&s.db.st.SV)

	// Initialize memory contexts and determine which queries can participate
	// in the combined batch (single-chunk) vs. which need individual chunked
	// processing.
	canCombine := make([]bool, len(request.Queries))
	for i, query := range request.Queries {
		var estimatedSourceCount int64
		if len(query.Sources) > 0 {
			estimatedSourceCount = int64(len(query.Sources))
		} else {
			estimatedSourceCount = estimatedClusterNodeCount
		}
		memContexts[i] = MakeQueryMemoryContext(
			s.workerMemMonitor,
			s.resultMemMonitor,
			QueryMemoryOptions{
				BudgetBytes:             s.queryMemoryMax / int64(s.queryWorkerMax),
				EstimatedSources:        estimatedSourceCount,
				InterpolationLimitNanos: interpolationLimit,
				Columnar:                s.db.WriteColumnar(),
			},
		)
		if !combineEnabled || needsRollup {
			canCombine[i] = false
			continue
		}
		maxWidth, err := memContexts[i].GetMaxTimespan(Resolution10s)
		if err != nil {
			// Insufficient memory budget; will fall back to per-query path
			// which will return the same error.
			canCombine[i] = false
			continue
		}
		canCombine[i] = maxWidth > timespan.width()
	}

	// Phase 1: Build a combined KV batch for all combinable queries.
	batch := &kv.Batch{}
	plans := make([]queryReadPlan, len(request.Queries))
	for i, query := range request.Queries {
		if !canCombine[i] {
			continue
		}
		plans[i] = s.db.addQueryReadOps(
			batch, query, Resolution10s, timespan, interpolationLimit,
		)
	}

	// Execute the combined batch in a single db.Run() call. DistSender
	// splits this into per-range RPCs, so queries targeting overlapping
	// ranges share RPCs.
	if len(batch.Results) > 0 {
		if err := s.db.runBatch(ctx, batch); err != nil {
			return nil, err
		}
	}

	// Phase 2: Process results in parallel. Combined-batch queries extract
	// their data from the shared batch; fallback queries use the original
	// per-query read path.
	//
	// Concurrency safety: after runBatch returns, the shared batch is never
	// mutated. Each goroutine reads a disjoint range of batch.Results
	// (determined by its queryReadPlan) and writes to its own
	// response.Results[queryIdx], so no synchronization is needed.
	workerOutput := make(chan error)
	// Start a task which is itself responsible for starting per-query worker
	// tasks. This is needed because RunAsyncTaskEx can block; in the case
	// where a single request has more queries than the semaphore limit, a
	// deadlock would occur because queries cannot complete until they have
	// written their result to the "output" channel, which is processed
	// later in the main function.
	if err := s.stopper.RunAsyncTask(ctx, "ts.Server: queries", func(ctx context.Context) {
		for queryIdx, query := range request.Queries {
			if err := s.stopper.RunAsyncTaskEx(
				ctx,
				stop.TaskOpts{
					TaskName:   "ts.Server: query",
					Sem:        s.workerSem,
					WaitForSem: true,
				},
				func(ctx context.Context) {
					var err error
					var result tspb.TimeSeriesQueryResponse_Result
					if canCombine[queryIdx] {
						result, err = s.processFromBatch(
							ctx, batch, plans[queryIdx], query,
							Resolution10s, timespan, memContexts[queryIdx],
						)
					} else {
						// Fallback: per-query read when combined batching is
						// disabled or the query needs memory-based chunking.
						var datapoints []tspb.TimeSeriesDatapoint
						var sources []string
						datapoints, sources, err = s.db.Query(
							ctx, query, Resolution10s, timespan,
							memContexts[queryIdx],
						)
						if err == nil {
							result = tspb.TimeSeriesQueryResponse_Result{
								Query:      query,
								Datapoints: datapoints,
							}
							result.Sources = sources
						}
					}
					if err == nil {
						response.Results[queryIdx] = result
					}
					select {
					case workerOutput <- err:
					case <-ctx.Done():
					}
				},
			); err != nil {
				// Stopper has been closed and is draining. Return an error
				// and exit the worker-spawning loop.
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

// processFromBatch extracts a single query's KV data from a shared batch and
// runs the post-processing pipeline.
func (s *Server) processFromBatch(
	ctx context.Context,
	batch *kv.Batch,
	plan queryReadPlan,
	query tspb.Query,
	diskResolution Resolution,
	timespan QueryTimespan,
	mem QueryMemoryContext,
) (tspb.TimeSeriesQueryResponse_Result, error) {
	data, err := extractReadResults(batch, plan)
	if err != nil {
		return tspb.TimeSeriesQueryResponse_Result{}, err
	}

	datapoints, sourceSet, err := s.db.processQueryData(
		ctx, data, query, diskResolution, timespan, mem,
	)
	if err != nil {
		return tspb.TimeSeriesQueryResponse_Result{}, err
	}

	sources := make([]string, 0, len(sourceSet))
	for source := range sourceSet {
		sources = append(sources, source)
	}
	result := tspb.TimeSeriesQueryResponse_Result{
		Query:      query,
		Datapoints: datapoints,
	}
	result.Sources = sources
	return result, nil
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
	return s.dump(req, stream)
}

// Dump returns a stream of raw timeseries data that has been stored on the
// server.
func (s *drpcServer) Dump(req *tspb.DumpRequest, stream tspb.DRPCTimeSeries_DumpStream) error {
	return s.dump(req, stream)
}

func (s *Server) dump(req *tspb.DumpRequest, stream tspb.RPCTimeSeries_DumpStream) error {
	d := DefaultDumper{stream.Send}.Dump
	return dumpImpl(stream.Context(), s.db.db, req, d)

}

// DumpRaw is like Dump, but it returns a stream of raw KV pairs.
func (s *Server) DumpRaw(req *tspb.DumpRequest, stream tspb.TimeSeries_DumpRawServer) error {
	return s.dumpRaw(req, stream)
}

// DumpRaw is like Dump, but it returns a stream of raw KV pairs.
func (s *drpcServer) DumpRaw(
	req *tspb.DumpRequest, stream tspb.DRPCTimeSeries_DumpRawStream,
) error {
	return s.dumpRaw(req, stream)
}

func (s *Server) dumpRaw(req *tspb.DumpRequest, stream tspb.RPCTimeSeries_DumpRawStream) error {
	d := rawDumper{stream}.Dump
	return dumpImpl(stream.Context(), s.db.db, req, d)
}

// DumpRaw is like Dump, but it returns a stream of raw KV pairs.
func (s *drpcTenantServer) DumpRaw(_ *tspb.DumpRequest, _ tspb.DRPCTimeSeries_DumpRawStream) error {
	return s.dumpRaw()
}

func (t *TenantServer) DumpRaw(_ *tspb.DumpRequest, _ tspb.TimeSeries_DumpRawServer) error {
	return t.dumpRaw()
}

func (t *TenantServer) dumpRaw() error {
	return status.Errorf(codes.Unimplemented, "DumpRaw is not implemented for virtual clusters. "+
		"If you are attempting to take a tsdump, please connect to the system virtual cluster, "+
		"not an application virtual cluster. System virtual clusters will dump all persisted "+
		"metrics from all virtual clusters.")
}

func (s *drpcTenantServer) Dump(_ *tspb.DumpRequest, _ tspb.DRPCTimeSeries_DumpStream) error {
	return s.dump()
}

func (t *TenantServer) Dump(_ *tspb.DumpRequest, _ tspb.TimeSeries_DumpServer) error {
	return t.dump()
}

func (t *TenantServer) dump() error {
	return status.Errorf(codes.Unimplemented, "Dump is not implemented for virtual clusters. "+
		"If you are attempting to take a tsdump, please connect to the system virtual cluster, "+
		"not an application virtual cluster. System virtual clusters will dump all persisted "+
		"metrics from all virtual clusters.")
}

func dumpImpl(
	ctx context.Context, db *kv.DB, req *tspb.DumpRequest, d func(*roachpb.KeyValue) error,
) error {
	if len(req.Names) == 0 {
		// In 23.2 behavior changed. Prior to it, tsdump would send an empty slice and
		// we'd populate it here. Now the client is expected to fill in the slice itself.
		// Probably the client is running `./cockroach debug tsdump` on a <=23.1 binary.
		return errors.Errorf("no timeseries names provided, does your cli binary match server's?")
	}
	resolutions := req.Resolutions
	if len(resolutions) == 0 {
		resolutions = []tspb.TimeSeriesResolution{tspb.TimeSeriesResolution_RESOLUTION_10S}
	}
	for _, seriesName := range req.Names {
		for _, res := range resolutions {
			if err := dumpTimeseriesAllSources(
				ctx,
				db,
				seriesName,
				ResolutionFromProto(res),
				req.StartNanos,
				req.EndNanos,
				false,
				d,
			); err != nil {
				return err
			}
		}
	}

	// Dump child metrics only for allowed metrics at 1M resolution
	for _, seriesName := range req.Names {
		if !tsutil.IsAllowedChildMetric(seriesName) {
			continue
		}
		if err := dumpTimeseriesAllSources(
			ctx,
			db,
			seriesName,
			Resolution1m,
			req.StartNanos,
			req.EndNanos,
			true,
			d,
		); err != nil {
			return err
		}
	}
	return nil
}

// DefaultDumper translates *roachpb.KeyValue into TimeSeriesData.
type DefaultDumper struct {
	Send func(*tspb.TimeSeriesData) error
}

func (dd DefaultDumper) Dump(kv *roachpb.KeyValue) error {
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
	return dd.Send(tsdata)
}

type rawDumper struct {
	stream tspb.RPCTimeSeries_DumpRawStream
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
	includeChildMetrics bool,
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

	var endKeyName string
	if includeChildMetrics {
		// Create a span that covers the metric's children.
		endKeyName = seriesName + string(rune(0x7C)) // '|' is the next char after '{'
	} else {
		endKeyName = seriesName
	}

	span := &roachpb.Span{
		Key: MakeDataKey(
			seriesName, "" /* source */, diskResolution, startNanos,
		),
		EndKey: MakeDataKey(
			endKeyName, "" /* source */, diskResolution, endNanos,
		),
	}

	for span != nil {
		b := &kv.Batch{}
		scan := kvpb.NewScan(span.Key, span.EndKey)
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
