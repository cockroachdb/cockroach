// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !race

package kvcoord

import "github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"

// GRPCTransportFactory is the default TransportFactory, using GRPC.
func GRPCTransportFactory(
	opts SendOptions, nodeDialer *nodedialer.Dialer, replicas ReplicaSlice,
) (Transport, error) {
	return grpcTransportFactoryImpl(opts, nodeDialer, replicas)
}
