// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"google.golang.org/grpc/keepalive"
)

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
	// Send periodic pings on the connection.
	Time: base.NetworkTimeout,
	// If the pings don't get a response within the timeout, we might be
	// experiencing a network partition. gRPC will close the transport-level
	// connection and all the pending RPCs (which may not have timeouts) will
	// fail eagerly.
	Timeout: base.NetworkTimeout,
}

// By default, gRPC disconnects clients that send "too many" pings,
// but we don't really care about that, so configure the server to be
// as permissive as possible.
var serverEnforcement = keepalive.EnforcementPolicy{
	MinTime:             time.Nanosecond,
	PermitWithoutStream: true,
}

var serverTestingKeepalive = keepalive.ServerParameters{
	Time:    200 * time.Millisecond,
	Timeout: 300 * time.Millisecond,
}
