// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpcbase

import (
	"context"
	"net"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"google.golang.org/grpc"
)

// TODODRPC is a marker to identify each RPC client creation site that needs to
// be updated to support DRPC.
const TODODRPC = false

// dialOptions to be used when dialing a node.
type dialOptions struct {
	NodeLocality    roachpb.Locality
	CheckBreaker    bool
	AddressResolver AddressResolver
}

// DialOptions customize dialing behavior when connecting to a node.
type DialOption func(*dialOptions)

// An AddressResolver translates NodeIDs into addresses.
type AddressResolver func(roachpb.NodeID) (net.Addr, roachpb.Locality, error)

// WithNodeLocality sets the locality of the node being dialed.
func WithNodeLocality(nodeLocality roachpb.Locality) DialOption {
	return func(opts *dialOptions) {
		opts.NodeLocality = nodeLocality
	}
}

// WithNoBreaker skips the circuit breaker check before trying to connect.
// This function should only be used when there is good reason to believe
// that the node is reachable.
func WithNoBreaker() DialOption {
	return func(opts *dialOptions) {
		opts.CheckBreaker = false
	}
}

// WithAddressResolver is used to override the default AddressResolver for
// dialing a node. Otherwise, the default address resolver is used.
func WithAddressResolver(addressResolver AddressResolver) DialOption {
	return func(opts *dialOptions) {
		opts.AddressResolver = addressResolver
	}
}

func NewDefaultDialOptions() *dialOptions {
	return &dialOptions{
		NodeLocality: roachpb.Locality{},
		CheckBreaker: true,
	}
}

// NodeDialer interface defines methods for dialing peer nodes using their
// node IDs.
type NodeDialer interface {
	Dial(context.Context, roachpb.NodeID, ConnectionClass, ...DialOption) (_ *grpc.ClientConn, err error)
}
