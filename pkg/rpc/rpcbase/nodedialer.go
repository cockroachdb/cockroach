// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpcbase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"google.golang.org/grpc"
	"storj.io/drpc"
)

var envExperimentalDRPCEnabled = envutil.EnvOrDefaultBool("COCKROACH_EXPERIMENTAL_DRPC_ENABLED", false)

// ExperimentalDRPCEnabled determines whether a drpc server accepting BatchRequest
// is enabled. This server is experimental and completely unsuitable to production
// usage (for example, does not implement authorization checks).
var ExperimentalDRPCEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"rpc.experimental_drpc.enabled",
	"if true, use drpc to execute Batch RPCs (instead of gRPC)",
	envExperimentalDRPCEnabled)

// NodeDialer interface defines methods for dialing peer nodes using their
// node IDs.
type NodeDialer interface {
	Dial(context.Context, roachpb.NodeID, ConnectionClass) (_ *grpc.ClientConn, err error)
	DRPCDial(context.Context, roachpb.NodeID, ConnectionClass) (_ drpc.Conn, err error)
}

// NodeDialerNoBreaker interface defines methods for dialing peer nodes using their
// node IDs. This interface is similar to NodeDialer but does not check the
// breaker before dialing.
type NodeDialerNoBreaker interface {
	DialNoBreaker(context.Context, roachpb.NodeID, ConnectionClass) (_ *grpc.ClientConn, err error)
	DRPCDialNoBreaker(context.Context, roachpb.NodeID, ConnectionClass) (_ drpc.Conn, err error)
}

// DialRPCClient establishes a connection to a node identified by its ID and
// returns a client for the requested service type. When DRPC is enabled, it
// creates a DRPC client; otherwise, it falls back to a gRPC client.
func DialRPCClient[C any](
	nd NodeDialer,
	ctx context.Context,
	nodeID roachpb.NodeID,
	class ConnectionClass,
	grpcClientFn func(*grpc.ClientConn) C,
	drpcClientFn func(drpc.Conn) C,
	st *cluster.Settings,
) (C, error) {
	var nilC C
	if !DRPCEnabled(ctx, st) {
		conn, err := nd.Dial(ctx, nodeID, class)
		if err != nil {
			return nilC, err
		}
		return grpcClientFn(conn), nil
	}

	conn, err := nd.DRPCDial(ctx, nodeID, class)
	if err != nil {
		return nilC, err
	}
	return drpcClientFn(conn), nil
}

// DialRPCClientNoBreaker is like DialRPCClient, but will not check the
// circuit breaker before trying to connect.
func DialRPCClientNoBreaker[C any](
	nd NodeDialerNoBreaker,
	ctx context.Context,
	nodeID roachpb.NodeID,
	class ConnectionClass,
	grpcClientFn func(*grpc.ClientConn) C,
	drpcClientFn func(drpc.Conn) C,
	st *cluster.Settings,
) (C, error) {
	var nilC C
	if !DRPCEnabled(ctx, st) {
		conn, err := nd.DialNoBreaker(ctx, nodeID, class)
		if err != nil {
			return nilC, err
		}
		return grpcClientFn(conn), nil
	}

	conn, err := nd.DRPCDialNoBreaker(ctx, nodeID, class)
	if err != nil {
		return nilC, err
	}
	return drpcClientFn(conn), nil
}

func DRPCEnabled(ctx context.Context, st *cluster.Settings) bool {
	return st != nil && ExperimentalDRPCEnabled.Get(&st.SV)
}
