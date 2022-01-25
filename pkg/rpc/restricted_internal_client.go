// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"google.golang.org/grpc"
)

// RestrictedInternalClient represents the part of the roachpb.InternalClient
// interface used by the DistSender. Besides the auto-generated gRPC client,
// this interface is also implemented by rpc.internalClientAdapter which
// bypasses gRPC to call into the local Node.
type RestrictedInternalClient interface {
	Batch(ctx context.Context, in *roachpb.BatchRequest, opts ...grpc.CallOption) (*roachpb.BatchResponse, error)
	RangeFeed(ctx context.Context, in *roachpb.RangeFeedRequest, opts ...grpc.CallOption) (roachpb.Internal_RangeFeedClient, error)
	MuxRangeFeed(ctx context.Context, opts ...grpc.CallOption) (roachpb.Internal_MuxRangeFeedClient, error)
}
