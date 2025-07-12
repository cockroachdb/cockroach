// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ctpb

import (
	context "context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"google.golang.org/grpc"
	"storj.io/drpc"
)

// DialSideTransportClient establishes a DRPC connection if enabled; otherwise,
// it falls back to gRPC. The established connection is used to create a
// RPCSideTransportClient.
func DialSideTransportClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (RPCSideTransportClient, error) {
	return rpcbase.DialRPCClient(nd, ctx, nodeID, class,
		func(conn *grpc.ClientConn) RPCSideTransportClient {
			return NewGRPCSideTransportClientAdapter(conn)
		}, func(conn drpc.Conn) RPCSideTransportClient {
			return NewDRPCSideTransportClientAdapter(conn)
		})
}
