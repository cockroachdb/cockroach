// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

type grpcCloseNotifier struct {
	stopper *stop.Stopper
	conn    *grpc.ClientConn
}

func (g *grpcCloseNotifier) CloseNotify(ctx context.Context) <-chan struct{} {
	ch := make(chan struct{}, 1)

	// Close notify is called after the first heatbeat RPC is successful, so we
	// can assume that the connection should be `Ready`. Any additional state
	// transition indicates that we need to remove it, and we want to do so
	// reactively. Unfortunately, gRPC forces us to spin up a separate goroutine
	// for this purpose even though it internally uses a channel.
	//
	// Note also that the implementation of this in gRPC is clearly racy,
	// so consider this somewhat best-effort.
	_ = g.stopper.RunAsyncTask(ctx, "conn state watcher", func(ctx context.Context) {
		defer close(ch)
		st := connectivity.Ready
		for {
			if !g.conn.WaitForStateChange(ctx, st) {
				return
			}
			st = g.conn.GetState()
			if st == connectivity.TransientFailure || st == connectivity.Shutdown {
				return
			}
		}
	})

	return ch
}

type grpcHeartbeatClient struct {
	c HeartbeatClient
}

func (g *grpcHeartbeatClient) Ping(ctx context.Context, in *PingRequest) (*PingResponse, error) {
	return g.c.Ping(ctx, in)
}

type GRPCConnection = Connection[*grpc.ClientConn]
