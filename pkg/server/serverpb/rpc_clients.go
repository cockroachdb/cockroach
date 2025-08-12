// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serverpb

import (
	context "context"

	roachpb "github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
)

// DialMigrationClient establishes a DRPC connection if enabled; otherwise,
// it falls back to gRPC. The established connection is used to create a
// RPCMigrationClient.
func DialMigrationClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (RPCMigrationClient, error) {
	conn, err := nd.DRPCDial(ctx, nodeID, class)
	if err != nil {
		return nil, err
	}
	return NewDRPCMigrationClientAdapter(conn), nil
}

// DialStatusClientNoBreaker establishes a DRPC connection if enabled;
// otherwise, it falls back to gRPC. The established connection is used
// to create a StatusClient. This method is same as DialStatusClient, but it
// does not check the breaker before dialing the connection.
func DialStatusClientNoBreaker(
	nd rpcbase.NodeDialerNoBreaker,
	ctx context.Context,
	nodeID roachpb.NodeID,
	class rpcbase.ConnectionClass,
) (RPCStatusClient, error) {
	conn, err := nd.DRPCDialNoBreaker(ctx, nodeID, class)
	if err != nil {
		return nil, err
	}
	return NewDRPCStatusClientAdapter(conn), nil
}

// DialStatusClient establishes a DRPC connection if enabled; otherwise, it
// falls back to gRPC. The established connection is used to create a
// RPCStatusClient.
func DialStatusClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID,
) (RPCStatusClient, error) {
	conn, err := nd.DRPCDial(ctx, nodeID, rpcbase.DefaultClass)
	if err != nil {
		return nil, err
	}
	return NewDRPCStatusClientAdapter(conn), nil
}

// DialAdminClient establishes a DRPC connection if enabled; otherwise, it
// falls back to gRPC. The established connection is used to create a
// RPCAdminClient.
func DialAdminClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID,
) (RPCAdminClient, error) {
	conn, err := nd.DRPCDial(ctx, nodeID, rpcbase.DefaultClass)
	if err != nil {
		return nil, err
	}
	return NewDRPCAdminClientAdapter(conn), nil
}

// DialAdminClientNoBreaker establishes a DRPC connection if enabled;
// otherwise, it falls back to gRPC. The established connection is used to
// create a AdminClient. This method is same as DialAdminClient, but it
// does not check the breaker before dialing the connection.
func DialAdminClientNoBreaker(
	nd rpcbase.NodeDialerNoBreaker, ctx context.Context, nodeID roachpb.NodeID,
) (RPCAdminClient, error) {
	conn, err := nd.DRPCDialNoBreaker(ctx, nodeID, rpcbase.DefaultClass)
	if err != nil {
		return nil, err
	}
	return NewDRPCAdminClientAdapter(conn), nil
}
