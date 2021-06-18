// Copyright 2021 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/idxusage"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// IndexUsageStatistics is the GRPC handler for serving index usage statistics.
// If the NodeID in the request payload is left empty, the handler will issue
// a cluster-wide RPC fanout to aggregate all index usage statistics from all
// the nodes. If the NodeID is specified, then the handler will handle the
// request either locally (if the NodeID matches the current node's NodeID) or
// forward it to the correct node.
func (s *statusServer) IndexUsageStatistics(
	ctx context.Context, req *serverpb.IndexUsageStatisticsRequest,
) (*serverpb.IndexUsageStatisticsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireViewActivityPermission(ctx); err != nil {
		return nil, err
	}

	localReq := &serverpb.IndexUsageStatisticsRequest{
		NodeID: "local",
	}

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if local {
			statsReader := s.sqlServer.pgServer.SQLServer.GetLocalIndexStatistics()
			return indexUsageStatsLocal(statsReader)
		}

		statusClient, err := s.dialNode(ctx, requestedNodeID)
		if err != nil {
			return nil, err
		}

		// We issue a localReq instead of the incoming req to other nodes. This is
		// to instruct other nodes to only return us their node-local stats and
		// do not further propagates the RPC call.
		return statusClient.IndexUsageStatistics(ctx, localReq)
	}

	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		client, err := s.dialNode(ctx, nodeID)
		return client, err
	}

	fetchIndexUsageStats := func(ctx context.Context, client interface{}, _ roachpb.NodeID) (interface{}, error) {
		statusClient := client.(serverpb.StatusClient)
		return statusClient.IndexUsageStatistics(ctx, localReq)
	}

	resp := &serverpb.IndexUsageStatisticsResponse{}
	aggFn := func(_ roachpb.NodeID, nodeResp interface{}) {
		stats := nodeResp.(*serverpb.IndexUsageStatisticsResponse)
		resp.Statistics = append(resp.Statistics, stats.Statistics...)
	}

	var combinedError error
	errFn := func(_ roachpb.NodeID, nodeFnError error) {
		combinedError = errors.CombineErrors(combinedError, nodeFnError)
	}

	// It's unfortunate that we cannot use paginatedIterateNodes here because we
	// need to aggregate all stats before returning. Returning a partial result
	// yields an incorrect result.
	if err := s.iterateNodes(ctx,
		fmt.Sprintf("requesting index usage stats for node %s", req.NodeID),
		dialFn, fetchIndexUsageStats, aggFn, errFn); err != nil {
		return nil, err
	}

	return resp, nil
}

func indexUsageStatsLocal(
	idxUsageStats *idxusage.LocalIndexUsageStats,
) (*serverpb.IndexUsageStatisticsResponse, error) {
	resp := &serverpb.IndexUsageStatisticsResponse{}
	if err := idxUsageStats.ForEach(idxusage.IteratorOptions{}, func(key *roachpb.IndexUsageKey, value *roachpb.IndexUsageStatistics) error {
		resp.Statistics = append(resp.Statistics, roachpb.CollectedIndexUsageStatistics{Key: *key,
			Stats: *value,
		})
		return nil
	}); err != nil {
		return nil, err
	}
	return resp, nil
}
