// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
	"time"
)

var _ serverpb.SpanStatsServer = &spanStatsServer{}

type spanStatsServer struct {
	server *Server
}

func (s *spanStatsServer) SetSpanBoundaries(ctx context.Context, request *serverpb.SetSpanBoundariesRequest) (*serverpb.SetSpanBoundariesResponse, error) {
	s.server.node.stores.VisitStores(func(st *kvserver.Store) error {
		st.SetBucketBoundaries()
		return nil
	})
	return &serverpb.SetSpanBoundariesResponse{}, nil
}

func (s *spanStatsServer) RegisterService(g *grpc.Server) {
	serverpb.RegisterSpanStatsServer(g, s)
}

// RegisterGateway starts the gateway (i.e. reverse proxy) that proxies HTTP requests
// to the appropriate gRPC endpoints.
func (s *spanStatsServer) RegisterGateway(
	ctx context.Context, mux *gwruntime.ServeMux, conn *grpc.ClientConn,
) error {
	return serverpb.RegisterSpanStatsHandler(ctx, mux, conn)
}

//func (s *spanStatsServer) SetSpanBoundarie


// GetSpanStatistics implements the SpanStatsServer interface.
func (s *spanStatsServer) GetSpanStatistics(
	ctx context.Context, req *serverpb.GetSpanStatisticsRequest,
) (*serverpb.GetSpanStatisticsResponse, error) {

	res := serverpb.GetSpanStatisticsResponse{Samples: make([]*serverpb.Sample, 0)}

	if err := s.server.node.stores.VisitStores(func(st *kvserver.Store) error {
		// XXX: make sure this works for multiple stores.
		// TODO: combine samples across stores.

		sample, err := st.GetSpanStats(ctx, req.Start, req.End)
		if err != nil {
			return err
		}

		//c := hlc.NewClock(hlc.UnixNano,0)
		t := hlc.Timestamp{WallTime: time.Now().UnixNano()}
		res.Samples = append(res.Samples, &serverpb.Sample{SampleTime: &t, SpanStats: sample})


		return nil
	}); err != nil {
		return nil, err
	}
	return &res, nil
}
