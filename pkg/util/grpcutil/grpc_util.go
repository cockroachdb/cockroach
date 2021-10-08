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
	"fmt"
	"io"
	"strings"

	circuit "github.com/cockroachdb/circuitbreaker"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
)

// ErrCannotReuseClientConn is returned when a failed connection is
// being reused. We require that new connections be created with
// pkg/rpc.GRPCDial instead.
var ErrCannotReuseClientConn = errors.New(errCannotReuseClientConnMsg)

const errCannotReuseClientConnMsg = "cannot reuse client connection"

type localRequestKey struct{}

// NewLocalRequestContext returns a Context that can be used for local (in-process) requests.
func NewLocalRequestContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, localRequestKey{}, struct{}{})
}

// IsLocalRequestContext returns true if this context is marked for local (in-process) use.
func IsLocalRequestContext(ctx context.Context) bool {
	return ctx.Value(localRequestKey{}) != nil
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
	if errors.Is(err, ErrCannotReuseClientConn) {
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
//
// This method currently depends on implementation details, matching
// on the text of an error message that is known to only be used
// in this case in the version of gRPC that we use today. We will
// need to watch for changes here in future versions of gRPC.
// TODO(bdarnell): Replace this with a cleaner mechanism when/if
// https://github.com/grpc/grpc-go/issues/1443 is resolved.
func RequestDidNotStart(err error) bool {
	if errors.HasType(err, connectionNotReadyError{}) ||
		errors.HasType(err, (*netutil.InitialHeartbeatFailedError)(nil)) ||
		errors.Is(err, circuit.ErrBreakerOpen) ||
		IsConnectionRejected(err) {
		return true
	}
	s, ok := status.FromError(errors.Cause(err))
	if !ok {
		// This is a non-gRPC error; assume nothing.
		return false
	}
	// TODO(bdarnell): In gRPC 1.7, we have no good way to distinguish
	// ambiguous from unambiguous failures, so we must assume all gRPC
	// errors are ambiguous.
	// https://github.com/cockroachdb/cockroach/issues/19708#issuecomment-343891640
	if false && s.Code() == codes.Unavailable && s.Message() == "grpc: the connection is unavailable" {
		return true
	}
	return false
}

// ConnectionReady returns nil if the given connection is ready to
// send a request, or an error (which will pass RequestDidNotStart) if
// not.
//
// This is a workaround for the fact that gRPC 1.7 fails to
// distinguish between ambiguous and unambiguous errors.
//
// This is designed for use with connections prepared by
// pkg/rpc.Connection.Connect (which performs an initial heartbeat and
// thereby ensures that we will never see a connection in the
// first-time Connecting state).
func ConnectionReady(conn *grpc.ClientConn) error {
	if s := conn.GetState(); s == connectivity.TransientFailure {
		return connectionNotReadyError{s}
	}
	return nil
}

type connectionNotReadyError struct {
	state connectivity.State
}

func (e connectionNotReadyError) Error() string {
	return fmt.Sprintf("connection not ready: %s", e.state)
}
