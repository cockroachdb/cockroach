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

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
)

// GRPCTransportFactory is the default TransportFactory, using GRPC.
//
// Notice that a different implementation is used during race builds: we wrap
// this to hold on to and read all obtained requests in a tight loop, exposing
// data races; see transport_race.go.
func GRPCTransportFactory(
	ctx context.Context,
	opts SendOptions,
	curNode *roachpb.NodeDescriptor,
	nodeDescStore NodeDescStore,
	nodeDialer *nodedialer.Dialer,
	latencyFn LatencyFunc,
	desc *roachpb.RangeDescriptor,
	leaseholder roachpb.ReplicaID,
) (Transport, error) {
	return grpcTransportFactoryImpl(
		ctx, opts, curNode, nodeDescStore, nodeDialer, latencyFn, desc, leaseholder)
}
