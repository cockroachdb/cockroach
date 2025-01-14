// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package nodedialer

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

type unaryDRPCBatchServiceToInternalAdapter struct {
	useStreamPoolClient bool
	rpc.RestrictedInternalClient
	drpcClient     kvpb.DRPCBatchClient
	drpcStreamPool *rpc.DRPCBatchStreamPool
}

func (a *unaryDRPCBatchServiceToInternalAdapter) Batch(
	ctx context.Context, in *kvpb.BatchRequest, opts ...grpc.CallOption,
) (*kvpb.BatchResponse, error) {
	if len(opts) > 0 {
		return nil, errors.New("CallOptions unsupported")
	}
	if a.useStreamPoolClient && a.drpcStreamPool != nil {
		return a.drpcStreamPool.Send(ctx, in)
	}

	return a.drpcClient.Batch(ctx, in)
}
