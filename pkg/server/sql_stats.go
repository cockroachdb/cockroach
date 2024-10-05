// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *statusServer) ResetSQLStats(
	ctx context.Context, req *serverpb.ResetSQLStatsRequest,
) (*serverpb.ResetSQLStatsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireRepairClusterMetadataPermission(ctx); err != nil {
		return nil, err
	}

	response := &serverpb.ResetSQLStatsResponse{}
	controller := s.sqlServer.pgServer.SQLServer.GetSQLStatsController()

	// If we need to reset persisted stats, we delegate to SQLStatsController,
	// which will trigger a system table truncation and RPC fanout under the hood.
	if req.ResetPersistedStats {
		if err := controller.ResetClusterSQLStats(ctx); err != nil {
			return nil, err
		}

		return response, nil
	}

	localReq := &serverpb.ResetSQLStatsRequest{
		NodeID: "local",
		// Only the top level RPC handler handles the reset persisted stats.
		ResetPersistedStats: false,
	}

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}
		if local {
			controller.ResetLocalSQLStats(ctx)
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

	var fanoutError error

	if err := s.iterateNodes(ctx, "reset SQL statistics",
		noTimeout,
		dialFn,
		resetSQLStats,
		func(nodeID roachpb.NodeID, resp interface{}) {
			// Nothing to do here.
		},
		func(nodeID roachpb.NodeID, nodeFnError error) {
			if nodeFnError != nil {
				fanoutError = errors.CombineErrors(fanoutError, nodeFnError)
			}
		},
	); err != nil {
		return nil, err
	}

	return response, fanoutError
}
