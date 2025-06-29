// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	context "context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"google.golang.org/grpc"
	drpc "storj.io/drpc"
)

// DialMultiRaftClient establishes a DRPC connection if enabled; otherwise,
// it falls back to gRPC. The established connection is used to create a
// RPCMultiRaftClient.
func DialMultiRaftClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (RPCMultiRaftClient, error) {
	return rpcbase.DialRPCClient(nd, ctx, nodeID, class,
		func(conn *grpc.ClientConn) RPCMultiRaftClient {
			return NewGRPCMultiRaftClientAdapter(conn)
		}, func(conn drpc.Conn) RPCMultiRaftClient {
			return NewDRPCMultiRaftClientAdapter(conn)
		})
}

// DialPerReplicaClient establishes a DRPC connection if enabled; otherwise,
// it falls back to gRPC. The established connection is used to create a
// RPCPerReplicaClient.
func DialPerReplicaClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (RPCPerReplicaClient, error) {
	return rpcbase.DialRPCClient(nd, ctx, nodeID, class,
		func(conn *grpc.ClientConn) RPCPerReplicaClient {
			return NewGRPCPerReplicaClientAdapter(conn)
		}, func(conn drpc.Conn) RPCPerReplicaClient {
			return NewDRPCPerReplicaClientAdapter(conn)
		})
}

// DialPerStoreClient establishes a DRPC connection if enabled; otherwise,
// it falls back to gRPC. The established connection is used to create a
// RPCPerStoreClient.
func DialPerStoreClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (RPCPerStoreClient, error) {
	return rpcbase.DialRPCClient(nd, ctx, nodeID, class,
		func(conn *grpc.ClientConn) RPCPerStoreClient {
			return NewGRPCPerStoreClientAdapter(conn)
		}, func(conn drpc.Conn) RPCPerStoreClient {
			return NewDRPCPerStoreClientAdapter(conn)
		})
}
