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

// dialOptions to be used when dialing a node.
type dialOptions struct {
	ConnectionClass ConnectionClass
	CheckBreaker    bool
}

// DialOptions customize dialing behavior when connecting to a node.
type DialOption func(*dialOptions)

// WithNoBreaker skips the circuit breaker check before trying to connect.
// This function should only be used when there is good reason to believe
// that the node is reachable.
func WithNoBreaker() DialOption {
	return func(opts *dialOptions) {
		opts.CheckBreaker = false
	}
}

func WithConnectionClass(class ConnectionClass) DialOption {
	return func(opts *dialOptions) {
		opts.ConnectionClass = class
	}
}

func NewDefaultDialOptions() *dialOptions {
	return &dialOptions{
		ConnectionClass: DefaultClass,
		CheckBreaker:    true,
	}
}

// NodeDialer interface defines methods for dialing peer nodes using their
// node IDs.
type NodeDialer interface {
	Dial(context.Context, roachpb.NodeID, ...DialOption) (_ *grpc.ClientConn, err error)
}
