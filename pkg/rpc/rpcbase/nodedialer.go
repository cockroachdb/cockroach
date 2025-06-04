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

const TODODRPC = true

// NodeDialer interface defines methods for dialing peer nodes using their
// node IDs.
type NodeDialer interface {
	Dial(context.Context, roachpb.NodeID, ConnectionClass) (_ *grpc.ClientConn, err error)
	DialDRPC(context.Context, roachpb.NodeID, ConnectionClass) (_ drpc.Conn, err error)
}
