// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !race
// +build !race

package kvcoord

import "github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"

// GRPCTransportFactory is the default TransportFactory, using GRPC.
func GRPCTransportFactory(
	opts SendOptions, nodeDialer *nodedialer.Dialer, replicas ReplicaSlice,
) (Transport, error) {
	return grpcTransportFactoryImpl(opts, nodeDialer, replicas)
}
