// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvpb

import (
	"context"

	grpc "google.golang.org/grpc"
)

type grpcInternalToTenantServiceClientAdapter internalClient

// NewGRPCInternalToTenantServiceClientAdapter creates a new
// RPCTenantServiceClient that adapts an InternalClient. This is necessary
// because the response types for streaming methods like TenantSettings are not
// compatible between RPCInternal_TenantSettingsClient (of adapter returned by
// NewGRPCInternalClientAdapter) and RPCTenantService_TenantSettingsClient (of
// RPCTenantServiceClient), even though they are otherwise equivalent.
func NewGRPCInternalToTenantServiceClientAdapter(conn *grpc.ClientConn) RPCTenantServiceClient {
	return (*grpcInternalToTenantServiceClientAdapter)(&internalClient{conn})
}

func (a *grpcInternalToTenantServiceClientAdapter) TenantSettings(
	ctx context.Context, in *TenantSettingsRequest,
) (RPCTenantService_TenantSettingsClient, error) {
	return (*internalClient)(a).TenantSettings(ctx, in)
}

func (a *grpcInternalToTenantServiceClientAdapter) RangeLookup(
	ctx context.Context, in *RangeLookupRequest,
) (*RangeLookupResponse, error) {
	return (*internalClient)(a).RangeLookup(ctx, in)
}

func (a *grpcInternalToTenantServiceClientAdapter) GossipSubscription(
	ctx context.Context, in *GossipSubscriptionRequest,
) (RPCTenantService_GossipSubscriptionClient, error) {
	return (*internalClient)(a).GossipSubscription(ctx, in)
}

func (a *grpcInternalToTenantServiceClientAdapter) GetRangeDescriptors(
	ctx context.Context, in *GetRangeDescriptorsRequest,
) (RPCTenantService_GetRangeDescriptorsClient, error) {
	return (*internalClient)(a).GetRangeDescriptors(ctx, in)
}
