// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package keyvispb

import (
	context "context"

	roachpb "github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
)

// DialKeyVisualizerClient establishes a DRPC connection if enabled; otherwise,
// it falls back to gRPC. The established connection is used to create a
// RPCKeyVisualizerClient.
func DialKeyVisualizerClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (RPCKeyVisualizerClient, error) {
	if !rpcbase.TODODRPC {
		conn, err := nd.Dial(ctx, nodeID, class)
		if err != nil {
			return nil, err
		}
		return NewGRPCKeyVisualizerClientAdapter(conn), nil
	}
	return nil, nil
}
