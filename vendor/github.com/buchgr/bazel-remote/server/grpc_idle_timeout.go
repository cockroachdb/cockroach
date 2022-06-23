package server

import (
	"context"

	"google.golang.org/grpc"

	"github.com/buchgr/bazel-remote/utils/idle"
)

// GrpcIdleTimer wraps an idle.Timer, and provides gRPC interceptors that
// reset the given idle.Timer at the start of each gRPC request.
type GrpcIdleTimer struct {
	idleTimer *idle.Timer
}

// NewGrpcIdleTimer returns a GrpcIdleTimer that wraps the given idle.Timer.
func NewGrpcIdleTimer(idleTimer *idle.Timer) *GrpcIdleTimer {
	return &GrpcIdleTimer{idleTimer: idleTimer}
}

// StreamServerInterceptor returns a streaming server interceptor that resets
// the idle.Timer at the start of each gRPC request.
func (t *GrpcIdleTimer) StreamServerInterceptor(srv interface{},
	ss grpc.ServerStream, info *grpc.StreamServerInfo,
	handler grpc.StreamHandler) error {

	t.idleTimer.ResetTimer()
	return handler(srv, ss)
}

// UnaryServerInterceptor returns a unary server interceptor that resets the
// idle.Timer at the start of each gRPC request.
func (t *GrpcIdleTimer) UnaryServerInterceptor(ctx context.Context,
	req interface{}, info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler) (interface{}, error) {

	t.idleTimer.ResetTimer()
	return handler(ctx, req)
}
