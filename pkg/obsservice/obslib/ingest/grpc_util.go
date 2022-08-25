// Copyright 2022 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package ingest

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// connWrapper is a wrapper on top of a grpc client connection providing a
// utility for waiting for the connection to be (re-)established.
type connWrapper struct {
	conn *grpc.ClientConn
}

// dial attempts to grpc.Dial() the address provided and returns a connWrapper.
// The dialing attempt is blocking - i.e. a successful return value means that
// we are able to communicate with the remote server.
//
// ctx can be used to cancel or expire the dialing.
//
// close() needs to be called eventually in order to free up resources.
func grpcDial(ctx context.Context, addr string) (connWrapper, error) {
	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithBlock(), // block until the connection is established
		// Enable keepalive. This is important because this gRPC connection
		// is used under a lease, and we want to detect failed connections
		// in a timely manner so that we can release the respective lease.
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: false,
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return connWrapper{}, err
	}
	return connWrapper{conn: conn}, nil
}

func (c connWrapper) close() {
	_ = c.conn.Close() // nolint:grpcconnclose
}

// connect blocks until the gRPC connection is established, proving that we can
// communicate with the remote server. If a non-zero timeout is provided, the
// wait is bounded.
//
// connect() is expected to be called after the connection has experienced gRPC
// errors. Calling connect() on an already open connection (e.g. one recently
// returned by dial()) is a no-op.
//
// Returns true if the connection succeeds, false if the timeout expires. If no
// timeout is specified, the call will ultimately return true or blocks forever.
func (c connWrapper) connect(ctx context.Context, timeout time.Duration) bool {
	var deadline time.Time
	if timeout != 0 {
		deadline = timeutil.Now().Add(timeout)
	}
	// Loop until either the connection succeeds or the deadline occurs (if any).
	for {
		state := c.conn.GetState()
		switch state {
		case connectivity.Ready:
			return true
		case connectivity.Idle:
			// Ping the gRPC channel to re-initiate a connection attempt.
			c.conn.Connect()
			fallthrough
		case connectivity.Connecting:
			fallthrough
		case connectivity.TransientFailure:
			var waitCtx context.Context
			var cancel func()
			if deadline == (time.Time{}) {
				waitCtx = ctx
				cancel = func() {}
			} else {
				waitCtx, cancel = context.WithDeadline(ctx, deadline) // nolint:context
			}
			ok := c.conn.WaitForStateChange(waitCtx, state)
			cancel()
			if ok {
				// State changed; loop around.
				continue
			}
			return false
		case connectivity.Shutdown:
			panic("connect() called concurrently with close()")
		}
	}
}
