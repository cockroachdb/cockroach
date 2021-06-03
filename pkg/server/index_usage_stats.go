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
	"github.com/cockroachdb/cockroach/pkg/sql/indexusagestats"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *statusServer) IndexUsageStatistics(
	ctx context.Context, req *serverpb.IndexUsageStatisticsRequest,
) (*serverpb.IndexUsageStatisticsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireViewActivityPermission(ctx); err != nil {
		return nil, err
	}

	localReq := &serverpb.IndexUsageStatisticsRequest{
		NodeID:         "local",
		OrderedTableID: req.OrderedTableID,
		OrderedIndexID: req.OrderedIndexID,
	}

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if local {
			statsReader :=
				s.admin.server.sqlServer.pgServer.SQLServer.GetLocalIndexStatisticsReader()
			return serializeIndexUsageStats(req, statsReader)
		}

		statusServer, err := s.dialNode(ctx, requestedNodeID)
		if err != nil {
			return nil, err
		}

		return statusServer.IndexUsageStatistics(ctx, localReq)
	}

	// Creating a sink to aggregate all the information.
	aggStats := s.sqlServer.pgServer.SQLServer.GetClusterIndexStatisticsProvider()
	aggStats.Clear()

	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		client, err := s.dialNode(ctx, nodeID)
		return client, err
	}

	fetchIndexUsageStats := func(ctx context.Context, client interface{}, _ roachpb.NodeID) (interface{}, error) {
		statusClient := client.(serverpb.StatusClient)
		return statusClient.IndexUsageStatistics(ctx, localReq)
	}

	aggFn := func(_ roachpb.NodeID, resp interface{}) {
		stats := resp.(*serverpb.IndexUsageStatisticsResponse)
		aggStats.IngestStats(stats.Statistics)
	}

	var combinedError error
	errFn := func(_ roachpb.NodeID, nodeFnError error) {
		if nodeFnError != nil {
			combinedError = errors.CombineErrors(combinedError, nodeFnError)
		}
	}

	// It's unfortunate that we cannot use paginatedIterateNodes here because we
	// need to aggregate all stats before returning. Returning partial result
	// yields incorrect result.
	if err := s.iterateNodes(ctx,
		fmt.Sprintf("requesting index usage stats for node %s", req.NodeID),
		dialFn, fetchIndexUsageStats, aggFn, errFn); err != nil {
		return nil, err
	}

	return serializeIndexUsageStats(req, aggStats)
}

func serializeIndexUsageStats(
	req *serverpb.IndexUsageStatisticsRequest, reader indexusagestats.Reader,
) (*serverpb.IndexUsageStatisticsResponse, error) {
	indexStats := make([]roachpb.CollectedIndexUsageStatistics, 0)

	var max *uint64
	if req.GetMax() != nil {
		maxLimit := req.GetMaxLimit()
		max = &maxLimit
	}

	err := reader.IterateIndexUsageStats(indexusagestats.IteratorOptions{
		SortedTableID: req.OrderedTableID,
		SortedIndexID: req.OrderedIndexID,
		Max:           max,
	}, func(key *roachpb.IndexUsageKey, value *roachpb.IndexUsageStatistics) error {
		indexStats = append(indexStats, roachpb.CollectedIndexUsageStatistics{
			Key:   *key,
			Stats: *value,
		})
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &serverpb.IndexUsageStatisticsResponse{
		Statistics: indexStats,
	}, nil
}
