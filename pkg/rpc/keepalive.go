// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package rpc

import (
	"time"

	"google.golang.org/grpc/keepalive"

	"github.com/cockroachdb/cockroach/pkg/base"
)

// To prevent unidirectional network partitions from keeping an unhealthy
// connection alive, we use both client-side and server-side keepalive pings.
var clientKeepalive = keepalive.ClientParameters{
	// Send periodic pings on the connection.
	Time: base.NetworkTimeout,
	// If the pings don't get a response within the timeout, we might be
	// experiencing a network partition. gRPC will close the transport-level
	// connection and all the pending RPCs (which may not have timeouts) will
	// fail eagerly. gRPC will then reconnect the transport transparently.
	Timeout: base.NetworkTimeout,
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

// These aggressively low keepalive timeouts ensure that tests which use
// them don't take too long.
var clientTestingKeepalive = keepalive.ClientParameters{
	Time:                200 * time.Millisecond,
	Timeout:             300 * time.Millisecond,
	PermitWithoutStream: true,
}
var serverTestingKeepalive = keepalive.ServerParameters{
	Time:    200 * time.Millisecond,
	Timeout: 300 * time.Millisecond,
}
