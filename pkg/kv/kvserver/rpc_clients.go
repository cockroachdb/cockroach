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
// MultiRaftClient.
func DialMultiRaftClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (MultiRaftClient, error) {
	if !rpcbase.TODODRPC {
		conn, err := nd.Dial(ctx, nodeID, class)
		if err != nil {
			return nil, err
		}
		return NewMultiRaftClient(conn), nil
	}
	return nil, nil
}

// DialPerReplicaClient establishes a DRPC connection if enabled; otherwise,
// it falls back to gRPC. The established connection is used to create a
// PerReplicaClient.
func DialPerReplicaClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (PerReplicaClient, error) {
	if !rpcbase.TODODRPC {
		conn, err := nd.Dial(ctx, nodeID, class)
		if err != nil {
			return nil, err
		}
		return NewPerReplicaClient(conn), nil
	}
	return nil, nil
}

// DialPerStoreClient establishes a DRPC connection if enabled; otherwise,
// it falls back to gRPC. The established connection is used to create a
// PerStoreClient.
func DialPerStoreClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (PerStoreClient, error) {
	if !rpcbase.TODODRPC {
		conn, err := nd.Dial(ctx, nodeID, class)
		if err != nil {
			return nil, err
		}
		return NewPerStoreClient(conn), nil
	}
	return nil, nil
}
