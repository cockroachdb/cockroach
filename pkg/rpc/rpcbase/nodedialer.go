// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpcbase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"google.golang.org/grpc"
)

// TODODRPC is a marker to identify each RPC client creation site that needs to
// be updated to support DRPC.
const TODODRPC = false

// NodeDialer interface defines methods for dialing peer nodes using their
// node IDs.
type NodeDialer interface {
	Dial(context.Context, roachpb.NodeID, ConnectionClass) (_ *grpc.ClientConn, err error)
}

// NodeDialerNoBreaker interface defines methods for dialing peer nodes using their
// node IDs. This interface is similar to NodeDialer but does not check the
// breaker before dialing.
type NodeDialerNoBreaker interface {
	DialNoBreaker(context.Context, roachpb.NodeID, ConnectionClass) (_ *grpc.ClientConn, err error)
}
