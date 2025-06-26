// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package blobspb

import (
	context "context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
)

// DialBlobClient establishes a DRPC connection if enabled; otherwise,
// it falls back to gRPC. The established connection is used to create a
// RPCBlobClient.
func DialBlobClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (RPCBlobClient, error) {
	if !rpcbase.TODODRPC && nd.UseDRPC() {
		conn, err := nd.Dial(ctx, nodeID, class)
		if err != nil {
			return nil, err
		}
		return NewGRPCBlobClientAdapter(conn), nil
	}
	conn, err := nd.DRPCDial(ctx, nodeID, class)
	if err != nil {
		return nil, err
	}
	return NewDRPCBlobClientAdapter(conn), nil
}
