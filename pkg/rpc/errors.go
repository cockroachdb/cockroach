// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// errQuiescing is returned from client interceptors when the server's
// stopper is quiescing. The error is constructed to return true in
// `grpcutil.IsConnectionRejected` which prevents infinite retry loops during
// cluster shutdown, especially in unit testing.
var errQuiescing = status.Error(codes.PermissionDenied, "refusing to dial; node is quiescing")

// ErrNotHeartbeated is returned by ConnHealth or Connection.Health when we have
// not yet performed the first heartbeat. This error will typically only be
// observed when checking the health during the first connection attempt to a
// node, as during subsequent periods of an unhealthy connection the circuit
// breaker error will be returned instead.
var ErrNotHeartbeated = errors.New("not yet heartbeated")

type versionCompatError struct{}

func (versionCompatError) Error() string {
	return "version compatibility check failed on ping response"
}

var VersionCompatError = versionCompatError{}
