// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package drpcinterceptor

import (
	"context"
	"storj.io/drpc/drpcmetadata"

	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes" // OTel codes
	"google.golang.org/grpc/status"  // For gRPC status codes
	"storj.io/drpc"
	"storj.io/drpc/drpcserver"
)

// extractSpanMetaFromDRPCCtx retrieves a SpanMeta carried by a dRPC call.
// Note: Actual dRPC metadata extraction would depend on how metadata is
// propagated in the dRPC implementation and would require a carrier
// compatible with tracer.ExtractMetaFrom. For now, using a MapCarrier.
func extractSpanMetaFromDRPCCtx(
	ctx context.Context, tracer *tracing.Tracer,
) (tracing.SpanMeta, error) {
	md, ok := drpcmetadata.Get(ctx)
	if !ok {
		return tracing.SpanMeta{}, nil
	}
	return tracer.ExtractMetaFrom(tracing.MapCarrier{Map: md})
}

// setDRPCErrorTag sets error information on the span.
// It attempts to interpret the error as a gRPC status.
// TODO check if we need to extract error status differently in drpc.
func setDRPCErrorTag(sp *tracing.Span, err error) {
	if err == nil {
		return
	}
	s, _ := status.FromError(err) // If err is not a gRPC status, s will have codes.Unknown
	sp.SetTag("response_code", attribute.IntValue(int(codes.Error)))
	sp.SetOtelStatus(codes.Error, s.Message())
}

// ServerInterceptor returns a drpcserver.ServerInterceptor suitable for use with a dRPC server.
// It instruments dRPC calls with tracing spans.
//
// The interceptor does the following:
// - It checks if the method is excluded from tracing.
// - It attempts to extract a SpanMeta from the context to link with a parent span.
// - It starts a new server span, making it a child of the extracted SpanMeta if found.
// - The new span-aware context is propagated to the handler via a wrapped drpc.Stream.
// - Errors from the handler are recorded on the span.
func ServerInterceptor(tracer *tracing.Tracer) drpcserver.ServerInterceptor {
	return func(
		ctx context.Context,
		methodName string,
		stream drpc.Stream,
		handler drpc.Handler,
	) error {
		if tracing.MethodExcludedFromTracing(methodName) {
			return handler.HandleRPC(stream, methodName)
		}

		// Use the initial context from the stream for extracting metadata.
		spanMeta, err := extractSpanMetaFromDRPCCtx(stream.Context(), tracer)
		if err != nil {
			return err
		}

		// Check if tracing should be included for this server span.
		if !tracing.SpanInclusionFuncForServer(tracer, spanMeta) {
			return handler.HandleRPC(stream, methodName)
		}

		// Start a new span, using the stream's context as the parent context.
		// The new span (serverSpan) will be a child of spanMeta if spanMeta is not empty.
		spanCtx, serverSpan := tracer.StartSpanCtx(
			stream.Context(), // Parent context for the new span
			methodName,
			tracing.WithRemoteParentFromSpanMeta(spanMeta),
			tracing.WithServerSpanKind,
		)
		defer serverSpan.Finish()

		// Wrap the stream to carry the new span-aware context.
		wrappedStream := &tracingDRPCServerStream{
			Stream: stream,
			ctx:    spanCtx,
		}

		// Call the actual handler with the wrapped stream.
		err = handler.HandleRPC(wrappedStream, methodName)
		if err != nil {
			setDRPCErrorTag(serverSpan, err)
			serverSpan.Recordf("error: %s", err)
		}
		return err
	}
}

// tracingDRPCServerStream wraps a drpc.Stream to carry a modified context.
type tracingDRPCServerStream struct {
	drpc.Stream
	ctx context.Context
}

// Context returns the modified context, which includes the tracing span.
func (ss *tracingDRPCServerStream) Context() context.Context {
	return ss.ctx
}
