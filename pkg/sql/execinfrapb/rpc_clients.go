// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execinfrapb

import (
	context "context"

	roachpb "github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
)

// DialDistSQLClient establishes a DRPC connection if enabled; otherwise,
// it falls back to gRPC. The established connection is used to create a
// RPCDistSQLClient.
func DialDistSQLClient(
	nd *nodedialer.Dialer, ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (RPCDistSQLClient, error) {
	return nodedialer.DialRPCClient(nd, ctx, nodeID, class, NewGRPCDistSQLClientAdapter, NewDRPCDistSQLClientAdapter)
}

// DialDistSQLClientNoBreaker establishes a DRPC connection if enabled;
// otherwise, it falls back to gRPC. The established connection is used
// to create a RPCDistSQLClient.  This method is same as DialDistSQLClient,
// but it does not check the breaker before dialing the connection.
func DialDistSQLClientNoBreaker(
	nd rpcbase.NodeDialerNoBreaker,
	ctx context.Context,
	nodeID roachpb.NodeID,
	class rpcbase.ConnectionClass,
	cs *cluster.Settings,
) (RPCDistSQLClient, error) {
	return rpcbase.DialRPCClientNoBreaker(nd, ctx, nodeID, class, NewGRPCDistSQLClientAdapter, NewDRPCDistSQLClientAdapter, cs)
}
