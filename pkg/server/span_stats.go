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
)

var _ serverpb.SpanStatsServer = &spanStatsServer{}

type spanStatsServer struct {
	server *Server
}

// GetSpanStatistics implements the SpanStatsServer interface.
func (s *spanStatsServer) GetSpanStatistics(
	ctx context.Context, req *serverpb.GetSpanStatisticsRequest,
) (*serverpb.GetSpanStatisticsResponse, error) {
	if err := s.server.node.stores.VisitStores(func(st *kvserver.Store) error {
		// XXX: make sure this works for multiple stores.
		return st.GetSpanStats(ctx, req.Start, req.End)
	}); err != nil {
		return nil, err
	}
	return &serverpb.GetSpanStatisticsResponse{}, nil
}
