// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serverpb

import (
	context "context"

	roachpb "github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"google.golang.org/grpc"
	"storj.io/drpc"
)

// DialMigrationClient establishes a DRPC connection if enabled; otherwise,
// it falls back to gRPC. The established connection is used to create a
// RPCMigrationClient.
func DialMigrationClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (RPCMigrationClient, error) {
	return rpcbase.DialRPCClient(nd, ctx, nodeID, class,
		func(conn *grpc.ClientConn) RPCMigrationClient {
			return NewGRPCMigrationClientAdapter(conn)
		}, func(conn drpc.Conn) RPCMigrationClient {
			return NewDRPCMigrationClientAdapter(conn)
		})
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
	return rpcbase.DialRPCClientNoBreaker(nd, ctx, nodeID, class,
		func(conn *grpc.ClientConn) RPCStatusClient {
			return NewGRPCStatusClientAdapter(conn)
		}, func(conn drpc.Conn) RPCStatusClient {
			return NewDRPCStatusClientAdapter(conn)
		})
}

// DialStatusClient establishes a DRPC connection if enabled; otherwise, it
// falls back to gRPC. The established connection is used to create a
// RPCStatusClient.
func DialStatusClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID,
) (RPCStatusClient, error) {
	return rpcbase.DialRPCClient(nd, ctx, nodeID, rpcbase.DefaultClass,
		func(conn *grpc.ClientConn) RPCStatusClient {
			return NewGRPCStatusClientAdapter(conn)
		}, func(conn drpc.Conn) RPCStatusClient {
			return NewDRPCStatusClientAdapter(conn)
		})
}

// DialAdminClient establishes a DRPC connection if enabled; otherwise, it
// falls back to gRPC. The established connection is used to create a
// RPCAdminClient.
func DialAdminClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID,
) (RPCAdminClient, error) {
	return rpcbase.DialRPCClient(nd, ctx, nodeID, rpcbase.DefaultClass,
		func(conn *grpc.ClientConn) RPCAdminClient {
			return NewGRPCAdminClientAdapter(conn)
		}, func(conn drpc.Conn) RPCAdminClient {
			return NewDRPCAdminClientAdapter(conn)
		})
}

// DialAdminClientNoBreaker establishes a DRPC connection if enabled;
// otherwise, it falls back to gRPC. The established connection is used to
// create a AdminClient. This method is same as DialAdminClient, but it
// does not check the breaker before dialing the connection.
func DialAdminClientNoBreaker(
	nd rpcbase.NodeDialerNoBreaker, ctx context.Context, nodeID roachpb.NodeID,
) (RPCAdminClient, error) {
	return rpcbase.DialRPCClientNoBreaker(nd, ctx, nodeID, rpcbase.DefaultClass,
		func(conn *grpc.ClientConn) RPCAdminClient {
			return NewGRPCAdminClientAdapter(conn)
		}, func(conn drpc.Conn) RPCAdminClient {
			return NewDRPCAdminClientAdapter(conn)
		})
}
