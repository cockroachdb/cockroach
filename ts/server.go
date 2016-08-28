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
	"github.com/cockroachdb/cockroach/ts/tspb"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

const (
	// URLPrefix is the prefix for all time series endpoints hosted by the
	// server.
	URLPrefix = "/ts/"
)

// Server handles incoming external requests related to time series data.
type Server struct {
	db *DB
}

// MakeServer instantiates a new Server which services requests with data from
// the supplied DB.
func MakeServer(db *DB) Server {
	return Server{
		db: db,
	}
}

// RegisterService registers the GRPC service.
func (s *Server) RegisterService(g *grpc.Server) {
	tspb.RegisterTimeSeriesServer(g, s)
}

// RegisterGateway starts the gateway (i.e. reverse proxy) that proxies HTTP requests
// to the appropriate gRPC endpoints.
func (s *Server) RegisterGateway(
	ctx context.Context,
	mux *gwruntime.ServeMux,
	conn *grpc.ClientConn,
) error {
	return tspb.RegisterTimeSeriesHandler(ctx, mux, conn)
}

// Query is an endpoint that returns data for one or more metrics over a
// specific time span.
func (s *Server) Query(ctx context.Context, request *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error) {
	if len(request.Queries) == 0 {
		return nil, grpc.Errorf(codes.InvalidArgument, "Queries cannot be empty")
	}

	response := tspb.TimeSeriesQueryResponse{
		Results: make([]tspb.TimeSeriesQueryResponse_Result, 0, len(request.Queries)),
	}
	for _, query := range request.Queries {
		datapoints, sources, err := s.db.Query(query, Resolution10s, request.StartNanos, request.EndNanos)
		if err != nil {
			return nil, grpc.Errorf(codes.Internal, err.Error())
		}
		result := tspb.TimeSeriesQueryResponse_Result{
			Query:      query,
			Datapoints: datapoints,
		}

		result.Sources = sources
		response.Results = append(response.Results, result)
	}

	return &response, nil
}
