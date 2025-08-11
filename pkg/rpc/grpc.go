// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"net"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

type GRPCConnection = Connection[*grpc.ClientConn]

// newGRPCPeerOptions creates peerOptions for gRPC peers.
func newGRPCPeerOptions(
	rpcCtx *Context, k peerKey, locality roachpb.Locality,
) *peerOptions[*grpc.ClientConn] {
	pm, lm := rpcCtx.metrics.acquire(k, locality)
	return &peerOptions[*grpc.ClientConn]{
		locality: locality,
		peers:    &rpcCtx.peers,
		connOptions: &ConnectionOptions[*grpc.ClientConn]{
			dial: func(ctx context.Context, target string, class rpcbase.ConnectionClass) (*grpc.ClientConn, error) {
				additionalDialOpts := []grpc.DialOption{grpc.WithStatsHandler(&statsTracker{lm})}
				additionalDialOpts = append(additionalDialOpts, rpcCtx.testingDialOpts...)
				// onNetworkDial is a callback that is called after we dial a TCP connection.
				// It is not called if we use the loopback dialer.
				// We define it here because we need access to the peer map.
				onNetworkDial := func(conn net.Conn) {
					tcpConn, ok := conn.(*net.TCPConn)
					if !ok {
						return
					}

					rpcCtx.peers.mu.Lock()
					defer rpcCtx.peers.mu.Unlock()
					p := rpcCtx.peers.mu.m[k]

					p.mu.Lock()
					defer p.mu.Unlock()
					p.mu.tcpConn = tcpConn

					log.VEventf(ctx, 2, "gRPC network dial: laddr=%v", tcpConn.LocalAddr())
				}
				return rpcCtx.grpcDialRaw(ctx, target, class, onNetworkDial, additionalDialOpts...)
			},
			connEquals: func(a, b *grpc.ClientConn) bool {
				return a == b
			},
			newBatchStreamClient: func(ctx context.Context, cc *grpc.ClientConn) (BatchStreamClient, error) {
				return kvpb.NewGRPCInternalClientAdapter(cc).BatchStream(ctx)
			},
			newCloseNotifier: func(stopper *stop.Stopper, cc *grpc.ClientConn) closeNotifier {
				return &grpcCloseNotifier{stopper: stopper, conn: cc}
			},
		},
		newHeartbeatClient: func(cc *grpc.ClientConn) RPCHeartbeatClient {
			return NewGRPCHeartbeatClientAdapter(cc)
		},
		pm: pm,
	}
}
