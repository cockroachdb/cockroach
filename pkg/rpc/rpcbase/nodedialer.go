// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpcbase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"google.golang.org/grpc"
	"storj.io/drpc"
)

// TODODRPC is a marker to identify each RPC client creation site that needs to
// be updated to support DRPC.
const TODODRPC = false

// NodeDialer interface defines methods for dialing peer nodes using their
// node IDs.
type NodeDialer interface {
	Dial(context.Context, roachpb.NodeID, ConnectionClass) (_ *grpc.ClientConn, err error)
	DRPCDial(context.Context, roachpb.NodeID, ConnectionClass) (_ drpc.Conn, err error)
	DRPCEnabled() bool
}

// NodeDialerNoBreaker interface defines methods for dialing peer nodes using their
// node IDs. This interface is similar to NodeDialer but does not check the
// breaker before dialing.
type NodeDialerNoBreaker interface {
	DialNoBreaker(context.Context, roachpb.NodeID, ConnectionClass) (_ *grpc.ClientConn, err error)
	DRPCDialNoBreaker(context.Context, roachpb.NodeID, ConnectionClass) (_ drpc.Conn, err error)
	DRPCEnabled() bool
}

func DialRPCClient[C any](
	nd NodeDialer,
	ctx context.Context,
	nodeID roachpb.NodeID,
	class ConnectionClass,
	newGRPCClientFn func(*grpc.ClientConn) C,
	newDRPCClientFn func(drpc.Conn) C,
) (C, error) {
	var nilC C
	if !TODODRPC && !nd.DRPCEnabled() {
		conn, err := nd.Dial(ctx, nodeID, class)
		if err != nil {
			return nilC, err
		}
		return newGRPCClientFn(conn), nil
	}
	conn, err := nd.DRPCDial(ctx, nodeID, class)
	if err != nil {
		return nilC, err
	}
	return newDRPCClientFn(conn), nil
}

func DialRPCClientNoBreaker[C any](
	nd NodeDialerNoBreaker,
	ctx context.Context,
	nodeID roachpb.NodeID,
	class ConnectionClass,
	newGRPCClientFn func(*grpc.ClientConn) C,
	newDRPCClientFn func(drpc.Conn) C,
) (C, error) {
	var nilC C
	if !TODODRPC && !nd.DRPCEnabled() {
		conn, err := nd.DialNoBreaker(ctx, nodeID, class)
		if err != nil {
			return nilC, err
		}
		return newGRPCClientFn(conn), nil
	}
	conn, err := nd.DRPCDialNoBreaker(ctx, nodeID, class)
	if err != nil {
		return nilC, err
	}
	return newDRPCClientFn(conn), nil
}
