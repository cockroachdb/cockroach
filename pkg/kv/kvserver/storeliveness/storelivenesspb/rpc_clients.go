// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storelivenesspb

import (
	context "context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"google.golang.org/grpc"
	"storj.io/drpc"
)

// DialStoreLivenessClient establishes a DRPC connection if enabled; otherwise,
// it falls back to gRPC. The established connection is used to create a
// RPCStoreLivenessClient.
func DialStoreLivenessClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (RPCStoreLivenessClient, error) {
	return rpcbase.DialRPCClient(nd, ctx, nodeID, class,
		func(conn *grpc.ClientConn) RPCStoreLivenessClient {
			return NewGRPCStoreLivenessClientAdapter(conn)
		}, func(conn drpc.Conn) RPCStoreLivenessClient {
			return NewDRPCStoreLivenessClientAdapter(conn)
		})
}
