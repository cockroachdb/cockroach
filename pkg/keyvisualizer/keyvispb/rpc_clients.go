// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package keyvispb

import (
	context "context"

	roachpb "github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
)

// DialKeyVisualizerClient establishes a DRPC connection if enabled; otherwise,
// it falls back to gRPC. The established connection is used to create a
// RPCKeyVisualizerClient.
func DialKeyVisualizerClient(
	nd *nodedialer.Dialer, ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (RPCKeyVisualizerClient, error) {
	return nodedialer.DialRPCClient(nd, ctx, nodeID, class, NewGRPCKeyVisualizerClientAdapter, NewDRPCKeyVisualizerClientAdapter)
}
