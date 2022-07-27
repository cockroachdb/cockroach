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
	"encoding/hex"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvispb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanstats/spanstatspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// SpanStatsServer is a concrete implementation of the keyvispb.KeyVisualizerServer interface.
type SpanStatsServer struct {
	server *Server
}

var _ keyvispb.KeyVisualizerServer = &SpanStatsServer{}

// SaveBoundaries implements the keyvispb.KeyVisualizerServer interface.
func (s *SpanStatsServer) SaveBoundaries(
	ctx context.Context, req *keyvispb.SaveBoundariesRequest,
) (*keyvispb.SaveBoundariesResponse, error) {

	encoded, err := protoutil.Marshal(req)

	if err != nil {
		return nil, err
	}

	stmt := fmt.Sprintf("UPSERT INTO system.span_stats_tenant_boundaries ("+
		"tenant_id, boundaries) VALUES (%d, x'%s')", req.Tenant.ToUint64(),
		hex.EncodeToString(encoded))

	_, err = s.server.sqlServer.internalExecutor.ExecEx(
		ctx,
		"upsert tenant boundaries",
		nil,
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
		stmt)

	if err != nil {
		return nil, err
	}

	return &keyvispb.SaveBoundariesResponse{}, nil
}

func (s *SpanStatsServer) dialNode(
	ctx context.Context, nodeID roachpb.NodeID,
) (keyvispb.KeyVisualizerClient, error) {
	addr, err := s.server.gossip.GetNodeIDAddress(nodeID)
	if err != nil {
		return nil, err
	}
	rpcConnection := s.server.rpcContext.GRPCDialNode(addr.String(), nodeID, rpc.DefaultClass)
	conn, err := rpcConnection.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return keyvispb.NewKeyVisualizerClient(conn), nil
}

func (s *SpanStatsServer) getSamplesFromFanOut(
	ctx context.Context, ten *roachpb.TenantID,
) (*keyvispb.GetSamplesResponse, error) {
	// dial each node and GetSamplesFromNode
	// multiple samples can be returned from each node,
	// but for now, assume that only one sample will be returned.
	responsesByNodeID := make(map[roachpb.NodeID]keyvispb.GetSamplesResponse)
	globalStats := make([]*spanstatspb.SpanStats, 0)

	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		client, err := s.dialNode(ctx, nodeID)
		return client, err
	}

	nodeFn := func(ctx context.Context, client interface{}, nodeID roachpb.NodeID) (interface{}, error) {
		c := client.(keyvispb.KeyVisualizerClient)

		stats, err := c.GetSamples(ctx, &keyvispb.GetSamplesRequest{
			Tenant: ten, NodeID: nodeID,
		})
		if err != nil {
			return nil, err
		}
		return stats, err
	}

	responseFn := func(nodeID roachpb.NodeID, resp interface{}) {
		nodeResponse := resp.(*keyvispb.GetSamplesResponse)
		responsesByNodeID[nodeID] = *nodeResponse
	}

	errorFn := func(nodeID roachpb.NodeID, err error) {
		log.Errorf(ctx, "could not get span stats sample for node %d: %s", nodeID, err.Error())
	}

	err := s.server.status.iterateNodes(ctx, "iterating nodes for span stats", dialFn, nodeFn, responseFn, errorFn)

	if err != nil {
		return nil, err
	}

	for _, samples := range responsesByNodeID {
		for _, spanStat := range samples.Samples[0].SpanStats {
			globalStats = append(globalStats, spanStat)
		}
	}

	// We set the timestamp here, because for now,
	// collectors are not keeping historical samples.
	timestamp := hlc.NewClockWithSystemTimeSource(0).Now()
	samples := []*spanstatspb.Sample{{
		SampleTime: &timestamp,
		SpanStats:  uniqueStats(globalStats),
	}}

	res := &keyvispb.GetSamplesResponse{Samples: samples}
	return res, nil
}

func (s *SpanStatsServer) getLocalSamples(
	ctx context.Context, ten *roachpb.TenantID,
) (*keyvispb.GetSamplesResponse, error) {

	localStats := make([]*spanstatspb.SpanStats, 0)
	err := s.server.node.stores.VisitStores(func(s *kvserver.Store) error {
		samples, err := s.GetSpanStatsCollector().GetSamples(*ten)
		if err != nil {
			return err
		}

		stats := samples[0]
		for _, stat := range stats.SpanStats {
			localStats = append(localStats, stat)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	// TODO(zachlite): Right now,
	// this response assumes the collector is only returning a single sample.
	// Eventually, this function should find unique stats from the corresponding
	// samples.
	// The timestamps should originate from the collector.
	samples := []*spanstatspb.Sample{{
		SampleTime: nil,
		SpanStats:  uniqueStats(localStats),
	}}

	res := &keyvispb.GetSamplesResponse{Samples: samples}
	return res, nil
}

func uniqueStats(stats []*spanstatspb.SpanStats) []*spanstatspb.SpanStats {
	unique := make(map[string]*spanstatspb.SpanStats)

	for _, stat := range stats {
		spanAsString := stat.Span.String()
		if uniqueStat, ok := unique[spanAsString]; ok {
			uniqueStat.Requests += stat.Requests
		} else {
			unique[spanAsString] = &spanstatspb.SpanStats{
				Span:     stat.Span,
				Requests: stat.Requests,
			}
		}
	}

	ret := make([]*spanstatspb.SpanStats, 0)
	for _, stat := range unique {
		ret = append(ret, stat)
	}
	return ret
}

// GetSamples implements the keyvispb.KeyVisualizerServer interface.
func (s *SpanStatsServer) GetSamples(
	ctx context.Context, req *keyvispb.GetSamplesRequest,
) (*keyvispb.GetSamplesResponse, error) {

	if req.NodeID == 0 {
		return s.getSamplesFromFanOut(ctx, req.Tenant)
	}

	return s.getLocalSamples(ctx, req.Tenant)

}
