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

	stmt := fmt.Sprintf("UPSERT INTO system.span_stats_tenant_boundaries ("+
		"tenant_id, boundaries) VALUES (%d, x'%s')",
		roachpb.SystemTenantID.ToUint64(),
		hex.EncodeToString(encoded))

	_, err = s.server.sqlServer.internalExecutor.ExecEx(
		ctx,
		"upsert tenant boundaries",
		nil,
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
		stmt)

	return err
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
	ctx context.Context,
) (*keyvispb.FlushSamplesResponse, error) {
	// dial each node and GetSamplesFromNode
	// multiple samples can be returned from each node,
	// but for now, assume that only one sample will be returned.
	responsesByNodeID := make(map[roachpb.NodeID]keyvispb.FlushSamplesResponse)
	globalStats := make([]*keyvispb.SpanStats, 0)

	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		client, err := s.dialNode(ctx, nodeID)
		return client, err
	}

	nodeFn := func(ctx context.Context, client interface{}, nodeID roachpb.NodeID) (interface{}, error) {
		c := client.(keyvispb.KeyVisualizerClient)

		stats, err := c.FlushSamples(ctx, &keyvispb.FlushSamplesRequest{
			NodeID: nodeID,
		})
		if err != nil {
			return nil, err
		}
		return stats, err
	}

	responseFn := func(nodeID roachpb.NodeID, resp interface{}) {
		nodeResponse := resp.(*keyvispb.FlushSamplesResponse)
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
		for _, spanStat := range samples.Samples.SpanStats {
			globalStats = append(globalStats, spanStat)
		}
	}

	// We set the timestamp here, because for now,
	// collectors are not keeping historical samples.
	timestamp := s.server.clock.Now()
	samples := &keyvispb.Sample{
		SampleTime: &timestamp,
		SpanStats:  uniqueStats(globalStats),
	}

	res := &keyvispb.FlushSamplesResponse{Samples: samples}
	return res, nil
}

func (s *SpanStatsServer) getLocalSamples() (*keyvispb.FlushSamplesResponse, error) {

	localStats := make([]*keyvispb.SpanStats, 0)
	err := s.server.node.stores.VisitStores(func(s *kvserver.Store) error {
		spanStats, err := s.GetSpanStatsCollector().FlushSample()
		if err != nil {
			return err
		}

		for _, stat := range spanStats {
			localStats = append(localStats, stat)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	// the timestamp is set by the node that initiates the fan-out.
	samples := &keyvispb.Sample{
		SampleTime: nil,
		SpanStats:  uniqueStats(localStats),
	}

	res := &keyvispb.FlushSamplesResponse{Samples: samples}
	return res, nil
}

func uniqueStats(stats []*keyvispb.SpanStats) []*keyvispb.SpanStats {
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

	ret := make([]*keyvispb.SpanStats, 0)
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
