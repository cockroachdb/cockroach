// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build !race
// +build !race

package kvcoord

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
)

// GRPCTransportFactory is the default TransportFactory, using GRPC.
func GRPCTransportFactory(nodeDialer *nodedialer.Dialer) TransportFactory {
	return func(options SendOptions, slice roachpb.ReplicaSet) (Transport, error) {
		return grpcTransportFactoryImpl(options, nodeDialer, slice)
	}
}
