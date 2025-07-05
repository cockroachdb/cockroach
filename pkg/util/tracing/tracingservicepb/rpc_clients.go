// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tracingservicepb

import (
	context "context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"google.golang.org/grpc"
	"storj.io/drpc"
)

// DialTracingClient establishes a DRPC connection if enabled; otherwise,
// it falls back to gRPC. The established connection is used to create a
// RPCTracingClient.
func DialTracingClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (RPCTracingClient, error) {
	return rpcbase.DialRPCClient(nd, ctx, nodeID, rpcbase.DefaultClass,
		func(conn *grpc.ClientConn) RPCTracingClient {
			return NewGRPCTracingClientAdapter(conn)
		}, func(conn drpc.Conn) RPCTracingClient {
			return NewDRPCTracingClientAdapter(conn)
		})
}
