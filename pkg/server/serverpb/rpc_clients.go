// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serverpb

import (
	context "context"

	roachpb "github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
)

// DialMigrationClient establishes a DRPC connection if enabled; otherwise,
// it falls back to gRPC. The established connection is used to create a
// RPCMigrationClient.
func DialMigrationClient(
	nd rpcbase.NodeDialer,
	ctx context.Context,
	nodeID roachpb.NodeID,
	class rpcbase.ConnectionClass,
	cs *cluster.Settings,
) (RPCMigrationClient, error) {
	return rpcbase.DialRPCClient(nd, ctx, nodeID, class, NewGRPCMigrationClientAdapter, NewDRPCMigrationClientAdapter, cs)
}

// DialStatusClientNoBreaker establishes a DRPC connection if enabled;
// otherwise, it falls back to gRPC. The established connection is used
// to create a StatusClient. This method is same as DialStatusClient, but it
// does not check the breaker before dialing the connection.
func DialStatusClientNoBreaker(
	nd *nodedialer.Dialer, ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (RPCStatusClient, error) {
	return nodedialer.DialRPCClientNoBreaker(nd, ctx, nodeID, class,
		NewGRPCStatusClientAdapter, NewDRPCStatusClientAdapter)
}

// DialStatusClient establishes a DRPC connection if enabled; otherwise, it
// falls back to gRPC. The established connection is used to create a
// RPCStatusClient.
func DialStatusClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID, cs *cluster.Settings,
) (RPCStatusClient, error) {
	if rpcbase.TODODRPC {
		return rpcbase.DialRPCClient(nd, ctx, nodeID, rpcbase.DefaultClass,
			NewGRPCStatusClientAdapter, NewDRPCStatusClientAdapter, cs)
	}
	conn, err := nd.Dial(ctx, nodeID, rpcbase.DefaultClass)
	if err != nil {
		return nil, err
	}
	return NewGRPCStatusClientAdapter(conn), nil
}

// DialAdminClient establishes a DRPC connection if enabled; otherwise, it
// falls back to gRPC. The established connection is used to create a
// RPCAdminClient.
func DialAdminClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID, cs *cluster.Settings,
) (RPCAdminClient, error) {
	if rpcbase.TODODRPC {
		return rpcbase.DialRPCClient(nd, ctx, nodeID, rpcbase.DefaultClass,
			NewGRPCAdminClientAdapter, NewDRPCAdminClientAdapter, cs)
	}
	conn, err := nd.Dial(ctx, nodeID, rpcbase.DefaultClass)
	if err != nil {
		return nil, err
	}
	return NewGRPCAdminClientAdapter(conn), nil
}

// DialAdminClientNoBreaker establishes a DRPC connection if enabled;
// otherwise, it falls back to gRPC. The established connection is used to
// create a AdminClient. This method is same as DialAdminClient, but it
// does not check the breaker before dialing the connection.
func DialAdminClientNoBreaker(
	nd *nodedialer.Dialer, ctx context.Context, nodeID roachpb.NodeID,
) (RPCAdminClient, error) {
	return nodedialer.DialRPCClientNoBreaker(nd, ctx, nodeID, rpcbase.DefaultClass,
		NewGRPCAdminClientAdapter, NewDRPCAdminClientAdapter)
}

// DialLogInClient establishes a DRPC connection if enabled; otherwise, it
// falls back to gRPC. The established connection is used to create a
// RPCLogInClient.
func DialLogInClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID, cs *cluster.Settings,
) (RPCLogInClient, error) {
	return rpcbase.DialRPCClient(nd, ctx, nodeID, rpcbase.DefaultClass,
		NewGRPCLogInClientAdapter, NewDRPCLogInClientAdapter, cs)
}

// DialLogOutClient establishes a DRPC connection if enabled; otherwise, it
// falls back to gRPC. The established connection is used to create a
// RPCLogOutClient.
func DialLogOutClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID, cs *cluster.Settings,
) (RPCLogOutClient, error) {
	return rpcbase.DialRPCClient(nd, ctx, nodeID, rpcbase.DefaultClass,
		NewGRPCLogOutClientAdapter, NewDRPCLogOutClientAdapter, cs)
}
