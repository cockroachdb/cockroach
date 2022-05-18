// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package flowinfra

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// createDummyStream creates the server and client side of a FlowStream stream.
// This can be use by tests to pretend that then have received a FlowStream RPC.
// The stream can be used to send messages (ConsumerSignal's) on it (within a
// gRPC window limit since nobody's reading from the stream), for example
// Handshake messages.
//
// We do this by creating a mock server, dialing into it and capturing the
// server stream. The server-side RPC call will be blocked until the caller
// calls the returned cleanup function.
func createDummyStream() (
	serverStream execinfrapb.DistSQL_FlowStreamServer,
	clientStream execinfrapb.DistSQL_FlowStreamClient,
	cleanup func(),
	err error,
) {
	stopper := stop.NewStopper()
	ctx := context.Background()
	clock := hlc.NewClockWithSystemTimeSource(time.Nanosecond /* maxOffset */)
	storageClusterID, mockServer, addr, err := execinfrapb.StartMockDistSQLServer(ctx, clock, stopper, execinfra.StaticSQLInstanceID)
	if err != nil {
		return nil, nil, nil, err
	}

	rpcContext := rpc.NewInsecureTestingContextWithClusterID(ctx, clock, stopper, storageClusterID)
	conn, err := rpcContext.GRPCDialNode(addr.String(), roachpb.NodeID(execinfra.StaticSQLInstanceID),
		rpc.DefaultClass).Connect(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	client := execinfrapb.NewDistSQLClient(conn)
	clientStream, err = client.FlowStream(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	streamNotification := <-mockServer.InboundStreams
	serverStream = streamNotification.Stream
	cleanup = func() {
		close(streamNotification.Donec)
		stopper.Stop(ctx)
	}
	return serverStream, clientStream, cleanup, nil
}
