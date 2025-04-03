// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package grpcinterceptor

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/ctxutil"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ExtractSpanMetaFromGRPCCtx retrieves a SpanMeta carried as gRPC metadata by
// an RPC.
func ExtractSpanMetaFromGRPCCtx(
	ctx context.Context, tracer *tracing.Tracer,
) (tracing.SpanMeta, error) {
	md, ok := grpcutil.FastFromIncomingContext(ctx)
	if !ok {
		return tracing.SpanMeta{}, nil
	}
	return tracer.ExtractMetaFrom(tracing.MetadataCarrier{MD: md})
}

// setGRPCErrorTag sets an error tag on the span.
func setGRPCErrorTag(sp *tracing.Span, err error) {
	if err == nil {
		return
	}
	s, _ := status.FromError(err)
	sp.SetTag("response_code", attribute.IntValue(int(codes.Error)))
	sp.SetOtelStatus(codes.Error, s.Message())
}

// BatchMethodName is the method name of Internal.Batch RPC.
const BatchMethodName = "/cockroach.roachpb.Internal/Batch"

// BatchStreamMethodName is the method name of the Internal.BatchStream RPC.
const BatchStreamMethodName = "/cockroach.roachpb.Internal/BatchStream"

// sendKVBatchMethodName is the method name for adminServer.SendKVBatch.
const sendKVBatchMethodName = "/cockroach.server.serverpb.Admin/SendKVBatch"

// SetupFlowMethodName is the method name of DistSQL.SetupFlow RPC.
const SetupFlowMethodName = "/cockroach.sql.distsqlrun.DistSQL/SetupFlow"
const flowStreamMethodName = "/cockroach.sql.distsqlrun.DistSQL/FlowStream"

// methodExcludedFromTracing returns true if a call to the given RPC method does
// not need to propagate tracing info. Some RPCs (Internal.Batch,
// DistSQL.SetupFlow) have dedicated fields for passing along the tracing
// context in the request, which is more efficient than letting the RPC
// interceptors deal with it. Others (DistSQL.FlowStream) are simply exempt from
// tracing because it's not worth it.
func methodExcludedFromTracing(method string) bool {
	return method == BatchMethodName ||
		method == BatchStreamMethodName ||
		method == sendKVBatchMethodName ||
		method == SetupFlowMethodName ||
		method == flowStreamMethodName
}

// ServerInterceptor returns a grpc.UnaryServerInterceptor suitable
// for use in a grpc.NewServer call.
//
// For example:
//
//	s := grpcutil.NewServer(
//	    ...,  // (existing ServerOptions)
//	    grpc.UnaryInterceptor(ServerInterceptor(tracer)))
//
// All gRPC server spans will look for an tracing SpanMeta in the gRPC
// metadata; if found, the server span will act as the ChildOf that RPC
// SpanMeta.
//
// Root or not, the server Span will be embedded in the context.Context for the
// application-specific gRPC handler(s) to access.
func ServerInterceptor(tracer *tracing.Tracer) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		if methodExcludedFromTracing(info.FullMethod) {
			return handler(ctx, req)
		}

		spanMeta, err := ExtractSpanMetaFromGRPCCtx(ctx, tracer)
		if err != nil {
			return nil, err
		}
		if !tracing.SpanInclusionFuncForServer(tracer, spanMeta) {
			return handler(ctx, req)
		}

		ctx, serverSpan := tracer.StartSpanCtx(
			ctx,
			info.FullMethod,
			tracing.WithRemoteParentFromSpanMeta(spanMeta),
			tracing.WithServerSpanKind,
		)
		defer serverSpan.Finish()

		resp, err := handler(ctx, req)
		if err != nil {
			setGRPCErrorTag(serverSpan, err)
			serverSpan.Recordf("error: %s", err)
		}
		return resp, err
	}
}

// StreamServerInterceptor returns a grpc.StreamServerInterceptor suitable
// for use in a grpc.NewServer call. The interceptor instruments streaming RPCs by
// creating a single span to correspond to the lifetime of the RPC's stream.
//
// For example:
//
//	s := grpcutil.NewServer(
//	    ...,  // (existing ServerOptions)
//	    grpc.StreamInterceptor(StreamServerInterceptor(tracer)))
//
// All gRPC server spans will look for a SpanMeta in the gRPC
// metadata; if found, the server span will act as the ChildOf that RPC
// SpanMeta.
//
// Root or not, the server Span will be embedded in the context.Context for the
// application-specific gRPC handler(s) to access.
func StreamServerInterceptor(tracer *tracing.Tracer) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if methodExcludedFromTracing(info.FullMethod) {
			return handler(srv, ss)
		}
		spanMeta, err := ExtractSpanMetaFromGRPCCtx(ss.Context(), tracer)
		if err != nil {
			return err
		}
		sp := tracing.SpanFromContext(ss.Context())
		// NB: when this method is called through the local internal client optimization,
		// we also invoke this interceptor, but don't have spanMeta available. If we then
		// call straight into the handler below without making a child span, we hit various
		// use-after-finish conditions. So we also check whether we have a nontrivial `sp`
		// and if so make sure to go through `StartSpanCtx` below. This can be seen as a
		// workaround for the following issue:
		//
		// https://github.com/cockroachdb/cockroach/issues/135686
		if sp == nil && !tracing.SpanInclusionFuncForServer(tracer, spanMeta) {
			return handler(srv, ss)
		}

		ctx, serverSpan := tracer.StartSpanCtx(
			ss.Context(),
			info.FullMethod,
			tracing.WithRemoteParentFromSpanMeta(spanMeta),
			tracing.WithServerSpanKind,
		)
		defer serverSpan.Finish()
		ss = &tracingServerStream{
			ServerStream: ss,
			ctx:          ctx,
		}
		err = handler(srv, ss)
		if err != nil {
			setGRPCErrorTag(serverSpan, err)
			serverSpan.Recordf("error: %s", err)
		}
		return err
	}
}

type tracingServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (ss *tracingServerStream) Context() context.Context {
	return ss.ctx
}

func injectSpanMeta(
	ctx context.Context, tracer *tracing.Tracer, clientSpan *tracing.Span,
) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(nil)
	} else {
		md = md.Copy()
	}
	tracer.InjectMetaInto(clientSpan.Meta(), tracing.MetadataCarrier{MD: md})
	return metadata.NewOutgoingContext(ctx, md)
}

// ClientInterceptor returns a grpc.UnaryClientInterceptor suitable
// for use in a grpc.Dial call.
//
// For example:
//
//	conn, err := grpc.Dial(
//	    address,
//	    ...,  // (existing DialOptions)
//	    grpc.WithUnaryInterceptor(ClientInterceptor(tracer)))
//
// All gRPC client spans will inject the tracing SpanMeta into the gRPC
// metadata; they will also look in the context.Context for an active
// in-process parent Span and establish a ChildOf relationship if such a parent
// Span could be found.
func ClientInterceptor(
	tracer *tracing.Tracer, init func(*tracing.Span),
) grpc.UnaryClientInterceptor {
	if init == nil {
		init = func(*tracing.Span) {}
	}
	return func(
		ctx context.Context,
		method string,
		req, resp interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		// Local RPCs don't need any special tracing, since the caller's context
		// will be used on the "server".
		_, localRequest := grpcutil.IsLocalRequestContext(ctx)
		if localRequest {
			return invoker(ctx, method, req, resp, cc, opts...)
		}
		parent := tracing.SpanFromContext(ctx)
		if !tracing.SpanInclusionFuncForClient(parent) {
			return invoker(ctx, method, req, resp, cc, opts...)
		}

		clientSpan := tracer.StartSpan(
			method,
			tracing.WithParent(parent),
			tracing.WithClientSpanKind,
		)
		init(clientSpan)
		defer clientSpan.Finish()

		// For most RPCs we pass along tracing info as gRPC metadata. Some select
		// RPCs carry the tracing in the request protos, which is more efficient.
		if !methodExcludedFromTracing(method) {
			ctx = injectSpanMeta(ctx, tracer, clientSpan)
		}
		if invoker != nil {
			err := invoker(ctx, method, req, resp, cc, opts...)
			if err != nil {
				setGRPCErrorTag(clientSpan, err)
				clientSpan.Recordf("error: %s", err)
				return err
			}
		}
		return nil
	}
}

// StreamClientInterceptor returns a grpc.StreamClientInterceptor suitable
// for use in a grpc.Dial call. The interceptor instruments streaming RPCs by creating
// a single span to correspond to the lifetime of the RPC's stream.
//
// For example:
//
//	conn, err := grpc.Dial(
//	    address,
//	    ...,  // (existing DialOptions)
//	    grpc.WithStreamInterceptor(StreamClientInterceptor(tracer)))
//
// All gRPC client spans will inject the tracing SpanMeta into the gRPC
// metadata; they will also look in the context.Context for an active
// in-process parent Span and establish a ChildOf relationship if such a parent
// Span could be found.
func StreamClientInterceptor(
	tracer *tracing.Tracer, init func(*tracing.Span),
) grpc.StreamClientInterceptor {
	if init == nil {
		init = func(*tracing.Span) {}
	}
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		// Local RPCs don't need any special tracing, since the caller's context
		// will be used on the "server".
		_, localRequest := grpcutil.IsLocalRequestContext(ctx)
		if localRequest {
			return streamer(ctx, desc, cc, method, opts...)
		}
		parent := tracing.SpanFromContext(ctx)
		if !tracing.SpanInclusionFuncForClient(parent) {
			return streamer(ctx, desc, cc, method, opts...)
		}

		// Create a span that will live for the life of the stream.
		clientSpan := tracer.StartSpan(
			method,
			tracing.WithParent(parent),
			tracing.WithClientSpanKind,
		)
		init(clientSpan)

		if !methodExcludedFromTracing(method) {
			ctx = injectSpanMeta(ctx, tracer, clientSpan)
		}

		cs, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			clientSpan.Recordf("error: %s", err)
			setGRPCErrorTag(clientSpan, err)
			clientSpan.Finish()
			return cs, err
		}
		return newTracingClientStream(
			ctx, cs, desc,
			// Pass ownership of clientSpan to the stream.
			clientSpan), nil
	}
}

// newTracingClientStream creates and implementation of grpc.ClientStream that
// finishes `clientSpan` when the stream terminates.
func newTracingClientStream(
	ctx context.Context, cs grpc.ClientStream, desc *grpc.StreamDesc, clientSpan *tracing.Span,
) grpc.ClientStream {
	isFinished := new(int32)
	*isFinished = 0
	finishFunc := func(err error) {
		// Since we have multiple code paths that could concurrently call
		// `finishFunc`, we need to add some sort of synchronization to guard
		// against multiple finishing.
		if !atomic.CompareAndSwapInt32(isFinished, 0, 1) {
			return
		}
		defer clientSpan.Finish()
		if err != nil {
			clientSpan.Recordf("error: %s", err)
			setGRPCErrorTag(clientSpan, err)
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
		// with the stream anymore (see [1]), so finishChan might never be
		// signaled). Thus, we listen for ctx cancellation and finish the span.
		//
		// [1] https://pkg.go.dev/google.golang.org/grpc#ClientConn.NewStream
		finishFunc(nil /* err */)
	})

	return &tracingClientStream{
		ClientStream: cs,
		desc:         desc,
		finishFunc:   finishFunc,
	}
}

type tracingClientStream struct {
	grpc.ClientStream
	desc       *grpc.StreamDesc
	finishFunc func(error)
}

func (cs *tracingClientStream) Header() (metadata.MD, error) {
	md, err := cs.ClientStream.Header()
	if err != nil {
		cs.finishFunc(err)
	}
	return md, errors.Wrap(err, "header error")
}

func (cs *tracingClientStream) SendMsg(m interface{}) error {
	err := cs.ClientStream.SendMsg(m)
	if err == io.EOF {
		cs.finishFunc(nil)
		// Do not wrap EOF.
		return err
	} else if err != nil {
		cs.finishFunc(err)
	}
	return errors.Wrap(err, "send msg error")
}

func (cs *tracingClientStream) RecvMsg(m interface{}) error {
	err := cs.ClientStream.RecvMsg(m)
	if err == io.EOF {
		cs.finishFunc(nil)
		// Do not wrap EOF.
		return err
	} else if err != nil {
		cs.finishFunc(err)
	} else if !cs.desc.ServerStreams {
		cs.finishFunc(nil)
	}
	return errors.Wrap(err, "recv msg error")
}

func (cs *tracingClientStream) CloseSend() error {
	err := cs.ClientStream.CloseSend()
	if err != nil {
		cs.finishFunc(err)
	}
	return errors.Wrap(err, "close send error")
}
