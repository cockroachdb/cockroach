// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package grpcutil

import (
	"context"
	"io"
	"strings"

	circuit "github.com/cockroachdb/circuitbreaker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrConnectionInterrupted is returned when a failed connection is
// being reused. We require that new connections be created with
// pkg/rpc.GRPCDial instead.
var ErrConnectionInterrupted = errors.New(errConnectionInterruptedMsg)

const errConnectionInterruptedMsg = "connection interrupted (did the remote node shut down or are there networking issues?)"

type localRequestKey struct{}

// NewLocalRequestContext returns a Context that can be used for local
// (in-process) RPC requests performed by the InternalClientAdapter. The ctx
// carries information about what tenant (if any) is the client of the RPC. The
// auth interceptor uses this information to authorize the tenant.
func NewLocalRequestContext(ctx context.Context, tenantID roachpb.TenantID) context.Context {
	return context.WithValue(ctx, localRequestKey{}, tenantID)
}

// IsLocalRequestContext returns true if this context is marked for local (in-process) use.
func IsLocalRequestContext(ctx context.Context) (roachpb.TenantID, bool) {
	val := ctx.Value(localRequestKey{})
	if val == nil {
		return roachpb.TenantID{}, false
	}
	return val.(roachpb.TenantID), true
}

// IsTimeout returns true if err's Cause is a gRPC timeout, or the request
// was canceled by a context timeout.
func IsTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	err = errors.Cause(err)
	if s, ok := status.FromError(err); ok {
		return s.Code() == codes.DeadlineExceeded
	}
	return false
}

// IsConnectionUnavailable checks if grpc code is codes.Unavailable which is
// set when we are not able to establish connection to remote node.
func IsConnectionUnavailable(err error) bool {
	if s, ok := status.FromError(errors.UnwrapAll(err)); ok {
		return s.Code() == codes.Unavailable
	}
	return false
}

// IsContextCanceled returns true if err's Cause is an error produced by gRPC
// on context cancellation.
func IsContextCanceled(err error) bool {
	if s, ok := status.FromError(errors.UnwrapAll(err)); ok {
		return s.Code() == codes.Canceled && s.Message() == context.Canceled.Error()
	}
	return false
}

// IsClosedConnection returns true if err's Cause is an error produced by gRPC
// on closed connections.
func IsClosedConnection(err error) bool {
	if errors.Is(err, ErrConnectionInterrupted) {
		return true
	}
	err = errors.Cause(err)
	if s, ok := status.FromError(err); ok {
		if s.Code() == codes.Canceled ||
			s.Code() == codes.Unavailable {
			return true
		}
	}
	if errors.Is(err, context.Canceled) ||
		strings.Contains(err.Error(), "is closing") ||
		strings.Contains(err.Error(), "tls: use of closed connection") ||
		strings.Contains(err.Error(), "use of closed network connection") ||
		strings.Contains(err.Error(), io.ErrClosedPipe.Error()) ||
		strings.Contains(err.Error(), io.EOF.Error()) ||
		strings.Contains(err.Error(), "node unavailable") {
		return true
	}
	return netutil.IsClosedConnection(err)
}

// IsConnectionRejected returns true if err's cause is an error produced by
// gRPC due to remote node being unavailable and retrying immediately would
// not fix the problem. It happens when either remote node is decommissioned
// or caller is not authorized to talk to the node.
// This check is helpful if caller doesn't want to distinguish between
// authentication and decommissioning errors in specific ways and just want
// to abort operations.
func IsConnectionRejected(err error) bool {
	if s, ok := status.FromError(errors.UnwrapAll(err)); ok {
		switch s.Code() {
		case codes.Unauthenticated, codes.PermissionDenied, codes.FailedPrecondition:
			return true
		}
	}
	return false
}

// IsAuthError returns true if err's Cause is an error produced by
// gRPC due to an authentication or authorization error for the operation.
// AuthErrors should generally be considered non-retriable. They indicate
// that the operation would not succeed even if directed at another node
// in the cluster.
//
// As a special case, an AuthError (PermissionDenied) is returned on outbound
// dialing when the source node is in the process of terminating (see
// rpc.errDialRejected).
func IsAuthError(err error) bool {
	if s, ok := status.FromError(errors.UnwrapAll(err)); ok {
		switch s.Code() {
		case codes.Unauthenticated, codes.PermissionDenied:
			return true
		}
	}
	return false
}

// RequestDidNotStart returns true if the given error from gRPC
// means that the request definitely could not have started on the
// remote server.
func RequestDidNotStart(err error) bool {
	if errors.HasType(err, (*netutil.InitialHeartbeatFailedError)(nil)) ||
		errors.Is(err, circuit.ErrBreakerOpen) ||
		IsConnectionRejected(err) {
		return true
	}
	_, ok := status.FromError(errors.Cause(err))
	if !ok {
		// This is a non-gRPC error; assume nothing.
		return false
	}
	// This is where you'd hope to treat some gRPC errors as unambiguous.
	// Unfortunately, gRPC provides no good way to distinguish ambiguous from
	// unambiguous failures.
	//
	// https://github.com/grpc/grpc-go/issues/1443
	// https://github.com/cockroachave hdb/cockroach/issues/19708#issuecomment-343891640
	return false
}
