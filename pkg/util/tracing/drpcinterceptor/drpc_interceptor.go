// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package drpcinterceptor

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/ctxutil"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingutil"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"storj.io/drpc"
	"storj.io/drpc/drpcclient"
	"storj.io/drpc/drpcerr"
	"storj.io/drpc/drpcmux"
)

// drpcRequestKey is a context key type used to mark a request as coming via DRPC.
type drpcRequestKey struct{}

// IsDRPCRequest returns true if the context indicates this request came via DRPC.
func IsDRPCRequest(ctx context.Context) bool {
	return ctx.Value(drpcRequestKey{}) != nil
}

// ExtractSpanMetaFromDRPCCtx retrieves trace context (as a SpanMeta) that has
// been propagated through DRPC metadata in the context.
func ExtractSpanMetaFromDRPCCtx(
	ctx context.Context, tracer *tracing.Tracer,
) (tracing.SpanMeta, error) {
	md, ok := grpcutil.FastFromIncomingContext(ctx)
	if !ok {
		return tracing.SpanMeta{}, nil
	}
	return tracer.ExtractMetaFrom(tracing.MetadataCarrier{MD: md})
}

// ServerInterceptor returns a drpcmux.UnaryServerInterceptor for tracing.
// It starts a server-side span for each incoming unary RPC, extracting a
// parent span from the request metadata if available. The span is finished
// when the handler returns.
func ServerInterceptor(tracer *tracing.Tracer) drpcmux.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		rpc string,
		handler drpcmux.UnaryHandler,
	) (interface{}, error) {
		ctx = context.WithValue(ctx, drpcRequestKey{}, struct{}{})
		if tracingutil.MethodExcludedFromTracing(rpc) {
			return handler(ctx, req)
		}
		spanMeta, err := ExtractSpanMetaFromDRPCCtx(ctx, tracer)
		if err != nil {
			return nil, err
		}
		if !tracing.SpanInclusionFuncForServer(tracer, spanMeta) {
			return handler(ctx, req)
		}

		// Create the server-side span for this RPC.
		ctx, serverSpan := tracer.StartSpanCtx(
			ctx,
			rpc,
			tracing.WithRemoteParentFromSpanMeta(spanMeta),
			tracing.WithServerSpanKind,
		)
		defer serverSpan.Finish()
		resp, err := handler(ctx, req)
		if err != nil {
			setDRPCErrorTag(serverSpan, err)
			serverSpan.Recordf("error: %s", err)
		}
		return resp, err
	}
}

// StreamServerInterceptor returns a drpcmux.StreamServerInterceptor for tracing.
// It creates a single span that covers the entire lifetime of the streaming RPC.
// The span is started before the handler is invoked and finished when the handler
// returns.
func StreamServerInterceptor(tracer *tracing.Tracer) drpcmux.StreamServerInterceptor {
	return func(
		stream drpc.Stream,
		rpc string,
		handler drpcmux.StreamHandler,
	) (interface{}, error) {
		stream = &tracingServerStream{
			Stream: stream,
			ctx:    context.WithValue(stream.Context(), drpcRequestKey{}, struct{}{}),
		}
		if tracingutil.MethodExcludedFromTracing(rpc) {
			return handler(stream)
		}
		spanMeta, err := ExtractSpanMetaFromDRPCCtx(stream.Context(), tracer)
		if err != nil {
			return nil, err
		}
		sp := tracing.SpanFromContext(stream.Context())
		if sp == nil && !tracing.SpanInclusionFuncForServer(tracer, spanMeta) {
			return handler(stream)
		}

		// Create the server-side span for the stream.
		ctx, serverSpan := tracer.StartSpanCtx(
			stream.Context(),
			rpc,
			tracing.WithRemoteParentFromSpanMeta(spanMeta),
			tracing.WithServerSpanKind,
		)
		defer serverSpan.Finish()

		stream = &tracingServerStream{Stream: stream, ctx: ctx}

		resp, err := handler(stream)
		if err != nil {
			setDRPCErrorTag(serverSpan, err)
			serverSpan.Recordf("error: %s", err)
		}
		return resp, err
	}
}

// tracingServerStream wraps a drpc.Stream to inject a context containing a span.
type tracingServerStream struct {
	drpc.Stream
	ctx context.Context
}

// Context overrides the embedded Stream's Context method to return the new
// context that includes the server-side span.
func (ss *tracingServerStream) Context() context.Context { return ss.ctx }

// ClientInterceptor returns a drpcclient.UnaryClientInterceptor for tracing.
// It starts a client-side span for each outgoing unary RPC, injecting the
// span's context into the request metadata for propagation. The `init` function
// can be used to add initial tags to the span.
func ClientInterceptor(
	tracer *tracing.Tracer, init func(*tracing.Span),
) drpcclient.UnaryClientInterceptor {
	if init == nil {
		init = func(*tracing.Span) {}
	}
	return func(
		ctx context.Context,
		rpc string,
		enc drpc.Encoding,
		in,
		out drpc.Message,
		cc *drpcclient.ClientConn,
		invoker drpcclient.UnaryInvoker,
	) error {
		if tracingutil.ShouldSkipClientTracing(ctx) {
			return invoker(ctx, rpc, enc, in, out, cc)
		}

		// Start a new client-side span as a child of the current span in the context.
		parent := tracing.SpanFromContext(ctx)
		clientSpan := tracer.StartSpan(rpc, tracing.WithParent(parent), tracing.WithClientSpanKind)
		init(clientSpan)
		defer clientSpan.Finish()

		if !tracingutil.MethodExcludedFromTracing(rpc) {
			ctx = tracingutil.InjectSpanMeta(ctx, tracer, clientSpan)
		}

		if invoker != nil {
			err := invoker(ctx, rpc, enc, in, out, cc)
			if err != nil {
				// If the call returned an error, record it on the span.
				setDRPCErrorTag(clientSpan, err)
				clientSpan.Recordf("error: %s", err)
				return err
			}
		}
		return nil
	}
}

// StreamClientInterceptor returns a drpcclient.StreamClientInterceptor for tracing.
// It starts a client-side span that covers the entire lifetime of the streaming
// RPC, finishing only when the stream is closed. The `init` function can be used
// to add initial tags to the span.
func StreamClientInterceptor(
	tracer *tracing.Tracer, init func(*tracing.Span),
) drpcclient.StreamClientInterceptor {
	if init == nil {
		init = func(*tracing.Span) {}
	}
	return func(
		ctx context.Context,
		rpc string,
		enc drpc.Encoding,
		cc *drpcclient.ClientConn,
		streamer drpcclient.Streamer,
	) (drpc.Stream, error) {
		if tracingutil.ShouldSkipClientTracing(ctx) {
			return streamer(ctx, rpc, enc, cc)
		}

		// Start a new client-side span as a child of the current span in the context.
		parent := tracing.SpanFromContext(ctx)
		clientSpan := tracer.StartSpan(rpc, tracing.WithParent(parent), tracing.WithClientSpanKind)
		init(clientSpan)

		if !tracingutil.MethodExcludedFromTracing(rpc) {
			ctx = tracingutil.InjectSpanMeta(ctx, tracer, clientSpan)
		}

		str, err := streamer(ctx, rpc, enc, cc)
		if err != nil {
			clientSpan.Recordf("error: %s", err)
			setDRPCErrorTag(clientSpan, err)
			clientSpan.Finish()
			return str, err
		}

		return newTracingClientStream(ctx, str, clientSpan), nil
	}
}

// newTracingClientStream wraps a drpc.Stream to ensure the associated client
// span is finished exactly once when the stream terminates, either through an
// error, a successful close, or context cancellation.
func newTracingClientStream(
	ctx context.Context, s drpc.Stream, clientSpan *tracing.Span,
) drpc.Stream {
	isFinished := new(int32)
	*isFinished = 0
	finishFunc := func(err error) {
		if !atomic.CompareAndSwapInt32(isFinished, 0, 1) {
			return
		}
		defer clientSpan.Finish()
		if err != nil {
			clientSpan.Recordf("error: %s", err)
			setDRPCErrorTag(clientSpan, err)
		}
	}
	// If the context is non-cancellable (ctx.Done() == nil), WhenDone returns
	// false and never invokes the function. Even though it is strange to see
	// non-cancellable context here, it's not exactly broken if we never invoke
	// this function because of context cancellation.  The finishFunc
	// can still be invoked directly.
	_ = ctxutil.WhenDone(ctx, func() {
		// A streaming RPC can be finished by the caller cancelling the ctx. If
		// the ctx is cancelled, the caller doesn't necessarily need to interact
		// with the stream anymore. Thus, we listen for ctx cancellation and
		// finish the span.
		finishFunc(nil /* err */)
	})
	return &tracingClientStream{Stream: s, finishFunc: finishFunc}
}

// tracingClientStream wraps a drpc.Stream and intercepts its methods to detect
// when the stream terminates, so it can finish the associated tracing span.
type tracingClientStream struct {
	drpc.Stream
	finishFunc func(error)
}

func (cs *tracingClientStream) MsgSend(msg drpc.Message, enc drpc.Encoding) error {
	err := cs.Stream.MsgSend(msg, enc)
	if err != nil {
		// io.EOF on send indicates a graceful close by the
		// receiver and not actually an error on the span.
		if err == io.EOF {
			cs.finishFunc(nil)
			return err
		}
		cs.finishFunc(err)
	}
	return err
}

func (cs *tracingClientStream) MsgRecv(msg drpc.Message, enc drpc.Encoding) error {
	err := cs.Stream.MsgRecv(msg, enc)
	if err != nil {
		if err == io.EOF {
			cs.finishFunc(nil)
			return err
		}
		cs.finishFunc(err)
	}
	return err
}

func (cs *tracingClientStream) CloseSend() error {
	err := cs.Stream.CloseSend()
	if err != nil {
		cs.finishFunc(err)
	}
	return err
}

func (cs *tracingClientStream) Close() error {
	// Any error from Close indicates a problem with terminating the stream.
	err := cs.Stream.Close()
	cs.finishFunc(err)
	return err
}

func setDRPCErrorTag(sp *tracing.Span, err error) {
	if err == nil {
		return
	}
	// Extract the DRPC error code from the error.
	code := drpcerr.Code(err)
	if code != 0 {
		sp.SetTag("response_code", attribute.IntValue(int(code)))
		sp.SetOtelStatus(codes.Error, err.Error())
	}
}
