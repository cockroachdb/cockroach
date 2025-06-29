// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package blobspb

import (
	context "context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"google.golang.org/grpc"
	"storj.io/drpc"
)

// DialBlobClient establishes a DRPC connection if enabled; otherwise,
// it falls back to gRPC. The established connection is used to create a
// RPCBlobClient.
func DialBlobClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (RPCBlobClient, error) {
	return rpcbase.DialRPCClient(nd, ctx, nodeID, class,
		func(conn *grpc.ClientConn) RPCBlobClient {
			return NewGRPCBlobClientAdapter(conn)
		}, func(conn drpc.Conn) RPCBlobClient {
			return NewDRPCBlobClientAdapter(conn)
		})
}
