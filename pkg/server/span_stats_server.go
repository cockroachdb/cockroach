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
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvispb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// SpanStatsServer is a concrete implementation of the keyvispb.KeyVisualizerServer interface.
type SpanStatsServer struct {
	server *Server
}

var _ keyvispb.KeyVisualizerServer = &SpanStatsServer{}

func (s *SpanStatsServer) saveBoundaries(
	ctx context.Context,
	boundaries []*roachpb.Span,
) error {

	encoded, err := protoutil.Marshal(&keyvispb.BoundaryUpdate{
		Boundaries: boundaries,
	})

	if err != nil {
		return err
	}

	_, err = s.server.sqlServer.internalExecutor.ExecEx(
		ctx,
		"upsert tenant boundaries",
		nil,
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
		`UPSERT INTO system.span_stats_tenant_boundaries(
			tenant_id,
			boundaries
			) VALUES ($1, $2)
		`,
		roachpb.SystemTenantID.ToUint64(),
		encoded,
	)

	return err
}

func (s *SpanStatsServer) getSamplesFromFanOut(
	ctx context.Context,
) (*keyvispb.FlushSamplesResponse, error) {

	var globalStats []*keyvispb.SpanStats

	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		conn, err := s.server.nodeDialer.Dial(ctx, nodeID, rpc.DefaultClass)
		return keyvispb.NewKeyVisualizerClient(conn), err
	}

	nodeFn := func(ctx context.Context, client interface{}, nodeID roachpb.NodeID) (interface{}, error) {
		stats, err := client.(keyvispb.KeyVisualizerClient).FlushSamples(ctx,
			&keyvispb.FlushSamplesRequest{
			NodeID: nodeID,
		})
		if err != nil {
			return nil, err
		}
		return stats, err
	}

	responseFn := func(nodeID roachpb.NodeID, resp interface{}) {
		nodeResponse := resp.(*keyvispb.FlushSamplesResponse)
		globalStats = append(globalStats, nodeResponse.Samples.SpanStats...)
	}

	errorFn := func(nodeID roachpb.NodeID, err error) {
		log.Errorf(ctx, "could not get span stats sample for node %d: %v", nodeID, err)
	}

	err := s.server.status.iterateNodes(ctx, "iterating nodes for span stats", dialFn, nodeFn, responseFn, errorFn)

	if err != nil {
		return nil, err
	}

	// We set the timestamp here, because for now,
	// collectors are not keeping historical samples.
	timestamp := s.server.clock.Now()
	samples := &keyvispb.Sample{
		SampleTime: &timestamp,
		SpanStats:  cumulativeStats(globalStats),
	}

	res := &keyvispb.FlushSamplesResponse{Samples: samples}
	return res, nil
}

func (s *SpanStatsServer) getLocalSamples() (*keyvispb.FlushSamplesResponse, error) {

	// In the future, setting the timestamp(s) of a sample will be the
	// responsibility of the collector. For now, it's the `getSamplesFromFanOut`
	// function that's responsible for setting the timestamp on the
	// outgoing response. So here, the SampleTime can just be nil.
	sample := &keyvispb.Sample{
		SampleTime: nil,
		SpanStats:  []*keyvispb.SpanStats{},
	}

	err := s.server.node.stores.VisitStores(func(s *kvserver.Store) error {
		spanStats, err := s.GetSpanStatsCollector().FlushSample()
		if err != nil {
			return err
		}
		sample.SpanStats = append(sample.SpanStats, spanStats...)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &keyvispb.FlushSamplesResponse{Samples: sample}, nil
}


// cumulativeStats uniques and accumulates all of a sample's
// keyvispb.SpanStats from across the cluster. Stores collect statistics for
// the same spans, and the caller wants the cumulative statistics for those spans.
func cumulativeStats(stats []*keyvispb.SpanStats) []*keyvispb.SpanStats {
	unique := make(map[string]*keyvispb.SpanStats)
	for _, stat := range stats {
		spanAsString := stat.Span.String()
		if uniqueStat, ok := unique[spanAsString]; ok {
			uniqueStat.Requests += stat.Requests
		} else {
			unique[spanAsString] = &keyvispb.SpanStats{
				Span:     stat.Span,
				Requests: stat.Requests,
			}
		}
	}

	ret := make([]*keyvispb.SpanStats, 0, len(unique))
	for _, stat := range unique {
		ret = append(ret, stat)
	}
	return ret
}

// FlushSamples implements the keyvispb.KeyVisualizerServer interface.
func (s *SpanStatsServer) FlushSamples(
	ctx context.Context, req *keyvispb.FlushSamplesRequest,
) (*keyvispb.FlushSamplesResponse, error) {

	if req.NodeID == 0 {

		if err := s.saveBoundaries(ctx, req.Boundaries); err != nil {
			return nil, err
		}

		return s.getSamplesFromFanOut(ctx)
	}

	return s.getLocalSamples()

}
