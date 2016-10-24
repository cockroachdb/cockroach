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

package ts

import (
	"golang.org/x/net/context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
)

const (
	// URLPrefix is the prefix for all time series endpoints hosted by the
	// server.
	URLPrefix = "/ts/"
	// QueryWorkerMax determines the maximum number of worker goroutines that
	// the time series server can use to service incoming queries.
	QueryWorkerMax = 250
)

// TimeSeriesTestingKnobs provides a means for tests to override settings in the
// test server that are not normally overridden.
type TimeSeriesTestingKnobs struct {
	// Adjusts the maximum number of query workers used by the server.
	QueryWorkerMax *int
}

var _ base.ModuleTestingKnobs = &TimeSeriesTestingKnobs{}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*TimeSeriesTestingKnobs) ModuleTestingKnobs() {}

// Server handles incoming external requests related to time series data.
type Server struct {
	log.AmbientContext
	db        *DB
	stopper   *stop.Stopper
	workerSem chan struct{}
}

// MakeServer instantiates a new Server which services requests with data from
// the supplied DB.
func MakeServer(
	ambient log.AmbientContext, db *DB, testingKnobs TimeSeriesTestingKnobs, stopper *stop.Stopper,
) Server {
	ambient.AddLogTag("ts-srv", nil)
	queryWorkerMax := QueryWorkerMax
	if testingKnobs.QueryWorkerMax != nil {
		queryWorkerMax = *testingKnobs.QueryWorkerMax
	}
	return Server{
		AmbientContext: ambient,
		db:             db,
		stopper:        stopper,
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
		return nil, grpc.Errorf(codes.InvalidArgument, "Queries cannot be empty")
	}

	// If not set, sampleNanos should default to ten second resolution.
	sampleNanos := request.SampleNanos
	if sampleNanos == 0 {
		sampleNanos = Resolution10s.SampleDuration()
	}

	// Structure used by query workers to return their result to the main task.
	// "queryNum" is necessary because we guarantee that results are returned
	// in the same order as the queries in the request.
	type queryResultWithError struct {
		queryNum int
		result   tspb.TimeSeriesQueryResponse_Result
		err      error
	}
	workerOutput := make(chan queryResultWithError)

	// Defer cancellation of context passed to worker tasks; if main task
	// returns early, worker tasks should be torn down quickly.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start a task which is itself responsible for starting per-query worker
	// tasks. This is needed because RunLimitedAsyncTask can block; in the
	// case where a single request has more queries than the semaphore limit,
	// a deadlock would occur because queries cannot complete until
	// they have written their result to the "output" channel, which is
	// processed later in the main function.
	if err := s.stopper.RunAsyncTask(ctx, func(loopCtx context.Context) {
		for queryNum := 0; queryNum < len(request.Queries); queryNum++ {
			// If context has been canceled, there is no need to queue
			// additional workers.
			select {
			case <-loopCtx.Done():
				return
			default:
			}

			queryNum := queryNum
			if err := s.stopper.RunLimitedAsyncTask(
				loopCtx,
				s.workerSem,
				func(workerCtx context.Context) {
					datapoints, sources, err := s.db.Query(
						workerCtx,
						request.Queries[queryNum],
						Resolution10s,
						sampleNanos,
						request.StartNanos,
						request.EndNanos,
					)

					result := queryResultWithError{
						queryNum: queryNum,
					}
					if err != nil {
						result.err = grpc.Errorf(codes.Internal, err.Error())
					} else {
						result.result = tspb.TimeSeriesQueryResponse_Result{
							Query:      request.Queries[queryNum],
							Datapoints: datapoints,
						}
						result.result.Sources = sources
					}

					select {
					case workerOutput <- result:
					case <-workerCtx.Done():
					}
				},
			); err != nil {
				// Stopper has been closed and is draining. Return an error and
				// exit the worker-spawning loop.
				select {
				case workerOutput <- queryResultWithError{
					queryNum: queryNum,
					err:      err,
				}:
				case <-loopCtx.Done():
				}
				return
			}
		}
	}); err != nil {
		return nil, err
	}

	response := tspb.TimeSeriesQueryResponse{
		Results: make([]tspb.TimeSeriesQueryResponse_Result, len(request.Queries)),
	}

	completed := 0
	for completed < len(request.Queries) {
		select {
		case queryResult := <-workerOutput:
			if queryResult.err != nil {
				// Return the first error encountered. This will cancel the
				// worker context and cause all other in-progress workers to
				// exit.
				return nil, queryResult.err
			}
			response.Results[queryResult.queryNum] = queryResult.result
			completed++
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return &response, nil
}
