// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execinfrapb

import (
	context "context"

	roachpb "github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"google.golang.org/grpc"
	"storj.io/drpc"
)

// DialDistSQLClient establishes a DRPC connection if enabled; otherwise,
// it falls back to gRPC. The established connection is used to create a
// RPCDistSQLClient.
func DialDistSQLClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (RPCDistSQLClient, error) {
	return rpcbase.DialRPCClient(nd, ctx, nodeID, rpcbase.DefaultClass,
		func(conn *grpc.ClientConn) RPCDistSQLClient {
			return NewGRPCDistSQLClientAdapter(conn)
		}, func(conn drpc.Conn) RPCDistSQLClient {
			return NewDRPCDistSQLClientAdapter(conn)
		})
}

// DialDistSQLClientNoBreaker establishes a DRPC connection if enabled;
// otherwise, it falls back to gRPC. The established connection is used
// to create a RPCDistSQLClient.  This method is same as DialDistSQLClient,
// but it does not check the breaker before dialing the connection.
func DialDistSQLClientNoBreaker(
	nd rpcbase.NodeDialerNoBreaker,
	ctx context.Context,
	nodeID roachpb.NodeID,
	class rpcbase.ConnectionClass,
) (RPCDistSQLClient, error) {
	return rpcbase.DialRPCClientNoBreaker(nd, ctx, nodeID, rpcbase.DefaultClass,
		func(conn *grpc.ClientConn) RPCDistSQLClient {
			return NewGRPCDistSQLClientAdapter(conn)
		}, func(conn drpc.Conn) RPCDistSQLClient {
			return NewDRPCDistSQLClientAdapter(conn)
		})
}
