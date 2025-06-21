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
	useStreamPoolClient bool
	rpc.RestrictedInternalClient
	drpcClient     kvpb.DRPCKVBatchClient
	drpcStreamPool *rpc.DRPCBatchStreamPool
}

func (a *unaryDRPCBatchServiceToInternalAdapter) Batch(
	ctx context.Context, in *kvpb.BatchRequest,
) (*kvpb.BatchResponse, error) {
	if a.useStreamPoolClient && a.drpcStreamPool != nil {
		return a.drpcStreamPool.Send(ctx, in)
	}

	return a.drpcClient.Batch(ctx, in)
}
