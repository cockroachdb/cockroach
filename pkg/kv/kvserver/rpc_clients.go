// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	context "context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
)

// DialMultiRaftClient establishes a DRPC connection if enabled; otherwise,
// it falls back to gRPC. The established connection is used to create a
// RPCMultiRaftClient.
func DialMultiRaftClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (RPCMultiRaftClient, error) {
	if !rpcbase.TODODRPC {
		conn, err := nd.Dial(ctx, nodeID, class)
		if err != nil {
			return nil, err
		}
		return NewGRPCMultiRaftClientAdapter(conn), nil
	}
	conn, err := nd.DRPCDial(ctx, nodeID, class)
	if err != nil {
		return nil, err
	}
	return NewDRPCMultiRaftClientAdapter(conn), nil
}

// DialPerReplicaClient establishes a DRPC connection if enabled; otherwise,
// it falls back to gRPC. The established connection is used to create a
// RPCPerReplicaClient.
func DialPerReplicaClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (RPCPerReplicaClient, error) {
	if !rpcbase.TODODRPC {
		conn, err := nd.Dial(ctx, nodeID, class)
		if err != nil {
			return nil, err
		}
		return NewGRPCPerReplicaClientAdapter(conn), nil
	}
	conn, err := nd.DRPCDial(ctx, nodeID, class)
	if err != nil {
		return nil, err
	}
	return NewDRPCPerReplicaClientAdapter(conn), nil
}

// DialPerStoreClient establishes a DRPC connection if enabled; otherwise,
// it falls back to gRPC. The established connection is used to create a
// RPCPerStoreClient.
func DialPerStoreClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (RPCPerStoreClient, error) {
	if !rpcbase.TODODRPC {
		conn, err := nd.Dial(ctx, nodeID, class)
		if err != nil {
			return nil, err
		}
		return NewGRPCPerStoreClientAdapter(conn), nil
	}
	conn, err := nd.DRPCDial(ctx, nodeID, class)
	if err != nil {
		return nil, err
	}
	return NewDRPCPerStoreClientAdapter(conn), nil
}
