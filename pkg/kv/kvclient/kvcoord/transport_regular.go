// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !race

package kvcoord

import "github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"

// GRPCTransportFactory is the default TransportFactory, using GRPC.
func GRPCTransportFactory(nodeDialer *nodedialer.Dialer) TransportFactory {
	return func(options SendOptions, slice ReplicaSlice) Transport {
		return grpcTransportFactoryImpl(options, nodeDialer, slice)
	}
}
