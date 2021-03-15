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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *statusServer) ResetSQLStats(
	ctx context.Context, req *serverpb.ResetSQLStatsRequest,
) (*serverpb.ResetSQLStatsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	response := &serverpb.ResetSQLStatsResponse{}

	localReq := &serverpb.ResetSQLStatsRequest{
		NodeID: "local",
	}

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}
		if local {
			s.admin.server.sqlServer.pgServer.SQLServer.ResetSQLStats(ctx)
			response.NumOfNodesReset = 1
			return response, nil
		}
		status, err := s.dialNode(ctx, requestedNodeID)
		if err != nil {
			return nil, err
		}
		return status.ResetSQLStats(ctx, localReq)
	}

	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		client, err := s.dialNode(ctx, nodeID)
		return client, err
	}

	resetSQLStats := func(ctx context.Context, client interface{}, _ roachpb.NodeID) (interface{}, error) {
		status := client.(serverpb.StatusClient)
		return status.ResetSQLStats(ctx, localReq)
	}

	if err := s.iterateNodes(ctx, fmt.Sprintf("reset SQL statistics for node %s", req.NodeID),
		dialFn,
		resetSQLStats,
		func(nodeID roachpb.NodeID, resp interface{}) {
			response.NumOfNodesReset++
		},
		func(nodeID roachpb.NodeID, nodeFnError error) {
		},
	); err != nil {
		return nil, err
	}
	return response, nil
}
