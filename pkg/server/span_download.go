// Copyright 2023 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (t *statusServer) DownloadSpan(
	ctx context.Context, req *serverpb.DownloadSpanRequest,
) (*serverpb.DownloadSpanResponse, error) {
	ctx = t.AnnotateCtx(ctx)

	// TODO(adityamaru): Figure out proper privileges, for now we restrict this
	// call to system tenant only in the tenant connector.
	return t.sqlServer.tenantConnect.DownloadSpan(ctx, req)
}

func (s *systemStatusServer) DownloadSpan(
	ctx context.Context, req *serverpb.DownloadSpanRequest,
) (*serverpb.DownloadSpanResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	resp := &serverpb.DownloadSpanResponse{
		ErrorsByNodeID: make(map[roachpb.NodeID]string),
	}
	if len(req.NodeID) > 0 {
		_, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}

		if local {
			return resp, s.localDownloadSpan(ctx, req)
		}

		return nil, errors.AssertionFailedf("requesting download on a specific node is not supported yet")
	}

	// Send DownloadSpan request to all stores on all nodes.
	remoteRequest := serverpb.DownloadSpanRequest{NodeID: "local", Span: req.Span}
	nodeFn := func(ctx context.Context, status serverpb.StatusClient, _ roachpb.NodeID) (*serverpb.DownloadSpanResponse, error) {
		return status.DownloadSpan(ctx, &remoteRequest)
	}
	responseFn := func(nodeID roachpb.NodeID, downloadSpanResp *serverpb.DownloadSpanResponse) {}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		resp.ErrorsByNodeID[nodeID] = err.Error()
	}

	if err := iterateNodes(ctx, s.serverIterator, s.stopper, "download spans",
		noTimeout,
		s.dialNode,
		nodeFn,
		responseFn,
		errorFn,
	); err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	return resp, nil
}

func (s *systemStatusServer) localDownloadSpan(
	ctx context.Context, req *serverpb.DownloadSpanRequest,
) error {
	return s.stores.VisitStores(func(store *kvserver.Store) error {
		return store.TODOEngine().Download(ctx, req.Span)
	})
}
