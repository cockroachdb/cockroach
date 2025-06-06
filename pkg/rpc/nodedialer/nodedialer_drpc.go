// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package nodedialer

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
)

type unaryDRPCBatchServiceToInternalAdapter struct {
	client         kvpb.RPCInternalClient
	drpcStreamPool *rpc.DRPCBatchStreamPool
}

func (a *unaryDRPCBatchServiceToInternalAdapter) Batch(
	ctx context.Context, in *kvpb.BatchRequest,
) (*kvpb.BatchResponse, error) {
	if a.drpcStreamPool != nil {
		return a.drpcStreamPool.Send(ctx, in)
	}
	return a.client.Batch(ctx, in)
}

func (a *unaryDRPCBatchServiceToInternalAdapter) MuxRangeFeed(
	ctx context.Context) (kvpb.RPCInternal_MuxRangeFeedClient, error) {
	return a.client.MuxRangeFeed(ctx)
}
