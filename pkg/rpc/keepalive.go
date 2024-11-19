// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"google.golang.org/grpc/keepalive"
)

// serverTimeout is how long the server will wait for a send to be acknowledged
// before automatically closing the connection. This is enforced at two levels:
// by gRPC itself as the timeout for keepalive pings on idle connections, and
// via the OS TCP stack by gRPC setting TCP_USER_TIMEOUT on the network socket.
//
// NetworkTimeout should be sufficient here for TCP_USER_TIMEOUT, but the
// keepalive pings are processed above the Go runtime and are therefore more
// sensitive to node overload (e.g. scheduling latencies). We therefore set it
// to 2x NetworkTimeout to avoid spurious closed connections.
//
// An aggressive timeout is not particularly beneficial here anyway, because the
// client-side timeout (in our case the CRDB RPC heartbeat) is what mostly
// matters for recovery time following network or node outages -- the server
// side doesn't really care if the connection remains open for a bit longer. The
// exception is an asymmetric partition where inbound packets are dropped but
// outbound packets go through -- in that case, an aggressive connection close
// would allow the client to fail and try a different node before waiting out
// the RPC heartbeat timeout. However, this is a rarer failure mode, and does
// not justify the increased stability risk during node overload.
var serverTimeout = envutil.EnvOrDefaultDuration(
	"COCKROACH_RPC_SERVER_TIMEOUT", 2*base.NetworkTimeout)

// serverKeepaliveInterval is the interval between server keepalive pings.
// These are used both to keep the connection alive, and to detect and close
// failed connections from the server-side. Keepalive pings are only sent and
// checked if there is no other activity on the connection.
//
// We set this to 2x PingInterval, since we expect RPC heartbeats to be sent
// regularly (obviating the need for the keepalive ping), and there's no point
// sending keepalive pings until we've waited long enough for the RPC heartbeat
// to show up.
var serverKeepaliveInterval = envutil.EnvOrDefaultDuration(
	"COCKROACH_RPC_SERVER_KEEPALIVE_INTERVAL", 2*base.PingInterval)

// 10 seconds is the minimum keepalive interval permitted by gRPC.
// Setting it to a value lower than this will lead to gRPC adjusting to this
// value and annoyingly logging "Adjusting keepalive ping interval to minimum
// period of 10s". See grpc/grpc-go#2642.
const minimumClientKeepaliveInterval = 10 * time.Second

// To prevent unidirectional network partitions from keeping an unhealthy
// connection alive, we use both client-side and server-side keepalive pings.
var clientKeepalive = keepalive.ClientParameters{
	// Send periodic pings on the connection.
	Time: minimumClientKeepaliveInterval,
	// If the pings don't get a response within the timeout, we might be
	// experiencing a network partition. gRPC will close the transport-level
	// connection and all the pending RPCs (which may not have timeouts) will
	// fail eagerly. gRPC will then reconnect the transport transparently.
	Timeout: minimumClientKeepaliveInterval,
	// Do the pings even when there are no ongoing RPCs.
	PermitWithoutStream: true,
}
var serverKeepalive = keepalive.ServerParameters{
	// Send periodic pings on the connection when there is no other traffic.
	Time: serverKeepaliveInterval,
	// Close the connection if either a keepalive ping doesn't receive a response
	// within the timeout, or a TCP send doesn't receive a TCP ack within the
	// timeout (enforced by the OS via TCP_USER_TIMEOUT).
	Timeout: serverTimeout,
}

// By default, gRPC disconnects clients that send "too many" pings,
// but we don't really care about that, so configure the server to be
// as permissive as possible.
var serverEnforcement = keepalive.EnforcementPolicy{
	MinTime:             time.Nanosecond,
	PermitWithoutStream: true,
}
