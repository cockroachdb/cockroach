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

package ts

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/util/mon"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
)

const (
	// URLPrefix is the prefix for all time series endpoints hosted by the
	// server.
	URLPrefix = "/ts/"
	// queryWorkerMax is the default maximum number of worker goroutines that
	// the time series server can use to service incoming queries.
	queryWorkerMax = 8
	// queryMemoryMax is a soft limit for the amount of total memory used by
	// time series queries. This is not currently enforced, but is used for
	// monitoring purposes.
	queryMemoryMax = int64(64 * 1024 * 1024) // 64MiB
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
	workerMemMonitor mon.BytesMonitor
	resultMemMonitor mon.BytesMonitor
	workerSem        chan struct{}
}

// MakeServer instantiates a new Server which services requests with data from
// the supplied DB.
func MakeServer(
	ambient log.AmbientContext,
	db *DB,
	nodeCountFn ClusterNodeCountFn,
	cfg ServerConfig,
	stopper *stop.Stopper,
) Server {
	ambient.AddLogTag("ts-srv", nil)

	// Override default values from configuration.
	queryWorkerMax := queryWorkerMax
	if cfg.QueryWorkerMax != 0 {
		queryWorkerMax = cfg.QueryWorkerMax
	}
	queryMemoryMax := queryMemoryMax
	if cfg.QueryMemoryMax != 0 {
		queryMemoryMax = cfg.QueryMemoryMax
	}

	return Server{
		AmbientContext: ambient,
		db:             db,
		stopper:        stopper,
		nodeCountFn:    nodeCountFn,
		workerMemMonitor: mon.MakeUnlimitedMonitor(
			context.Background(),
			"timeseries-workers",
			mon.MemoryResource,
			nil,
			nil,
			// Begin logging messages if we exceed our planned memory usage by
			// more than double.
			queryMemoryMax*2,
			db.st,
		),
		resultMemMonitor: mon.MakeUnlimitedMonitor(
			context.Background(),
			"timeseries-results",
			mon.MemoryResource,
			nil,
			nil,
			math.MaxInt64,
			db.st,
		),
		queryMemoryMax: queryMemoryMax,
		queryWorkerMax: queryWorkerMax,
		workerSem:      make(chan struct{}, queryWorkerMax),
	}
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
	interpolationLimit := storage.TimeUntilStoreDead.Get(&s.db.st.SV).Nanoseconds()

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

	// Create a separate account for each query, allowing them to be run in
	// parallel.
	resultAccounts := make([]mon.BoundAccount, len(request.Queries))
	defer func() {
		for idx := range resultAccounts {
			resultAccounts[idx].Close(ctx)
		}
	}()

	// Start a task which is itself responsible for starting per-query worker
	// tasks. This is needed because RunLimitedAsyncTask can block; in the
	// case where a single request has more queries than the semaphore limit,
	// a deadlock would occur because queries cannot complete until
	// they have written their result to the "output" channel, which is
	// processed later in the main function.
	if err := s.stopper.RunAsyncTask(ctx, "ts.Server: queries", func(ctx context.Context) {
		for queryIdx, query := range request.Queries {
			queryIdx := queryIdx
			query := query

			if err := s.stopper.RunLimitedAsyncTask(
				ctx,
				"ts.Server: query",
				s.workerSem,
				true, /* wait */
				func(ctx context.Context) {
					// Create a memory account for the results of this query.
					resultAccounts[queryIdx] = s.resultMemMonitor.MakeBoundAccount()

					// Estimated source count is either the count of requested sources
					// *or* the estimated cluster node count if no sources are specified.
					var estimatedSourceCount int64
					if len(query.Sources) > 0 {
						estimatedSourceCount = int64(len(query.Sources))
					} else {
						estimatedSourceCount = estimatedClusterNodeCount
					}

					datapoints, sources, err := s.db.QueryMemoryConstrained(
						ctx,
						query,
						Resolution10s,
						sampleNanos,
						request.StartNanos,
						request.EndNanos,
						interpolationLimit,
						&resultAccounts[queryIdx],
						&s.workerMemMonitor,
						// The worker is allotted an even share of the total worker memory
						// budget for the server.
						s.queryMemoryMax/int64(s.queryWorkerMax),
						estimatedSourceCount,
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
