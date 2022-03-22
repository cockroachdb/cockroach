// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracing

import (
	"context"
	"io"
	"runtime"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// metadataCarrier is an implementation of the Carrier interface for gRPC
// metadata.
type metadataCarrier struct {
	metadata.MD
}

// Set implements the Carrier interface.
func (w metadataCarrier) Set(key, val string) {
	// The GRPC HPACK implementation rejects any uppercase keys here.
	//
	// As such, since the HTTP_HEADERS format is case-insensitive anyway, we
	// blindly lowercase the key.
	key = strings.ToLower(key)
	w.MD[key] = append(w.MD[key], val)
}

// ForEach implements the Carrier interface.
func (w metadataCarrier) ForEach(fn func(key, val string) error) error {
	for k, vals := range w.MD {
		for _, v := range vals {
			if err := fn(k, v); err != nil {
				return err
			}
		}
	}

	return nil
}

// ExtractSpanMetaFromGRPCCtx retrieves a SpanMeta carried as gRPC metadata by
// an RPC.
func ExtractSpanMetaFromGRPCCtx(ctx context.Context, tracer *Tracer) (SpanMeta, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return SpanMeta{}, nil
	}
	return tracer.ExtractMetaFrom(metadataCarrier{md})
}

// spanInclusionFuncForServer is used as a SpanInclusionFunc for the server-side
// of RPCs, deciding for which operations the gRPC tracing interceptor should
// create a span.
func spanInclusionFuncForServer(t *Tracer, spanMeta SpanMeta) bool {
	// If there is an incoming trace on the RPC (spanMeta) or the tracer is
	// configured to always trace, return true. The second part is particularly
	// useful for calls coming through the HTTP->RPC gateway (i.e. the AdminUI),
	// where client is never tracing.
	return !spanMeta.Empty() || t.AlwaysTrace()
}

// setGRPCErrorTag sets an error tag on the span.
func setGRPCErrorTag(sp *Span, err error) {
	if err == nil {
		return
	}
	s, _ := status.FromError(err)
	sp.SetTag("response_code", attribute.IntValue(int(codes.Error)))
	if sp.i.otelSpan != nil {
		sp.i.otelSpan.SetStatus(codes.Error, s.Message())
	}
}

// BatchMethodName is the method name of Internal.Batch RPC.
const BatchMethodName = "/cockroach.roachpb.Internal/Batch"

// SendKVBatchMethodName is the method name for adminServer.SendKVBatch.
const SendKVBatchMethodName = "/cockroach.server.serverpb.Admin/SendKVBatch"

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
		method == SendKVBatchMethodName ||
		method == SetupFlowMethodName ||
		method == flowStreamMethodName
}

// ServerInterceptor returns a grpc.UnaryServerInterceptor suitable
// for use in a grpc.NewServer call.
//
// For example:
//
//     s := grpcutil.NewServer(
//         ...,  // (existing ServerOptions)
//         grpc.UnaryInterceptor(ServerInterceptor(tracer)))
//
// All gRPC server spans will look for an tracing SpanMeta in the gRPC
// metadata; if found, the server span will act as the ChildOf that RPC
// SpanMeta.
//
// Root or not, the server Span will be embedded in the context.Context for the
// application-specific gRPC handler(s) to access.
func ServerInterceptor(tracer *Tracer) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		if methodExcludedFromTracing(info.FullMethod) {
			return handler(ctx, req)
		}

		spanMeta, err := ExtractSpanMetaFromGRPCCtx(ctx, tracer)
		if err != nil {
			return nil, err
		}
		if !spanInclusionFuncForServer(tracer, spanMeta) {
			return handler(ctx, req)
		}

		ctx, serverSpan := tracer.StartSpanCtx(
			ctx,
			info.FullMethod,
			WithRemoteParentFromSpanMeta(spanMeta),
			WithServerSpanKind,
		)
		defer serverSpan.Finish()

		resp, err = handler(ctx, req)
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
//     s := grpcutil.NewServer(
//         ...,  // (existing ServerOptions)
//         grpc.StreamInterceptor(StreamServerInterceptor(tracer)))
//
// All gRPC server spans will look for a SpanMeta in the gRPC
// metadata; if found, the server span will act as the ChildOf that RPC
// SpanMeta.
//
// Root or not, the server Span will be embedded in the context.Context for the
// application-specific gRPC handler(s) to access.
func StreamServerInterceptor(tracer *Tracer) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if methodExcludedFromTracing(info.FullMethod) {
			return handler(srv, ss)
		}
		spanMeta, err := ExtractSpanMetaFromGRPCCtx(ss.Context(), tracer)
		if err != nil {
			return err
		}
		if !spanInclusionFuncForServer(tracer, spanMeta) {
			return handler(srv, ss)
		}

		ctx, serverSpan := tracer.StartSpanCtx(
			ss.Context(),
			info.FullMethod,
			WithRemoteParentFromSpanMeta(spanMeta),
			WithServerSpanKind,
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

// spanInclusionFuncForClient is used as a SpanInclusionFunc for the client-side
// of RPCs, deciding for which operations the gRPC tracing interceptor should
// create a span.
//
// We use this to circumvent the interceptor's work when tracing is
// disabled. Otherwise, the interceptor causes an increase in the
// number of packets (even with an empty context!).
//
// See #17177.
func spanInclusionFuncForClient(parent *Span) bool {
	return parent != nil && !parent.IsNoop()
}

func injectSpanMeta(ctx context.Context, tracer *Tracer, clientSpan *Span) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(nil)
	} else {
		md = md.Copy()
	}
	tracer.InjectMetaInto(clientSpan.Meta(), metadataCarrier{md})
	return metadata.NewOutgoingContext(ctx, md)
}

// ClientInterceptor returns a grpc.UnaryClientInterceptor suitable
// for use in a grpc.Dial call.
//
// For example:
//
//     conn, err := grpc.Dial(
//         address,
//         ...,  // (existing DialOptions)
//         grpc.WithUnaryInterceptor(ClientInterceptor(tracer)))
//
// All gRPC client spans will inject the tracing SpanMeta into the gRPC
// metadata; they will also look in the context.Context for an active
// in-process parent Span and establish a ChildOf relationship if such a parent
// Span could be found.
//
// compatibilityMode is a callback that will be used to check whether the node
// (still) needs compatibility with 21.2. If it doesn't, then a more performant
// trace propagation mechanism is used. The compatibility check is built as a
// callback rather than directly checking the cluster version because this
// tracing package cannot use cluster settings.
func ClientInterceptor(
	tracer *Tracer, init func(*Span), compatibilityMode func(ctx context.Context) bool,
) grpc.UnaryClientInterceptor {
	if init == nil {
		init = func(*Span) {}
	}
	return func(
		ctx context.Context,
		method string,
		req, resp interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		parent := SpanFromContext(ctx)
		if !spanInclusionFuncForClient(parent) {
			return invoker(ctx, method, req, resp, cc, opts...)
		}

		clientSpan := tracer.StartSpan(
			method,
			WithParent(parent),
			WithClientSpanKind,
		)
		init(clientSpan)
		defer clientSpan.Finish()

		// For most RPCs we pass along tracing info as gRPC metadata. Some select
		// RPCs carry the tracing in the request protos, which is more efficient.
		if compatibilityMode(ctx) || !methodExcludedFromTracing(method) {
			ctx = injectSpanMeta(ctx, tracer, clientSpan)
		}
		var err error
		if invoker != nil {
			err = invoker(ctx, method, req, resp, cc, opts...)
		}
		if err != nil {
			setGRPCErrorTag(clientSpan, err)
			clientSpan.Recordf("error: %s", err)
		}
		return err
	}
}

// StreamClientInterceptor returns a grpc.StreamClientInterceptor suitable
// for use in a grpc.Dial call. The interceptor instruments streaming RPCs by creating
// a single span to correspond to the lifetime of the RPC's stream.
//
// For example:
//
//     conn, err := grpc.Dial(
//         address,
//         ...,  // (existing DialOptions)
//         grpc.WithStreamInterceptor(StreamClientInterceptor(tracer)))
//
// All gRPC client spans will inject the tracing SpanMeta into the gRPC
// metadata; they will also look in the context.Context for an active
// in-process parent Span and establish a ChildOf relationship if such a parent
// Span could be found.
func StreamClientInterceptor(tracer *Tracer, init func(*Span)) grpc.StreamClientInterceptor {
	if init == nil {
		init = func(*Span) {}
	}
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		parent := SpanFromContext(ctx)
		if !spanInclusionFuncForClient(parent) {
			return streamer(ctx, desc, cc, method, opts...)
		}

		clientSpan := tracer.StartSpan(
			method,
			WithParent(parent),
			WithClientSpanKind,
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
		return newTracingClientStream(cs, method, desc, clientSpan), nil
	}
}

func newTracingClientStream(
	cs grpc.ClientStream, method string, desc *grpc.StreamDesc, clientSpan *Span,
) grpc.ClientStream {
	finishChan := make(chan struct{})

	isFinished := new(int32)
	*isFinished = 0
	finishFunc := func(err error) {
		// Since we have multiple code paths that could concurrently call
		// `finishFunc`, we need to add some sort of synchronization to guard
		// against multiple finishing.
		if !atomic.CompareAndSwapInt32(isFinished, 0, 1) {
			return
		}
		close(finishChan)
		defer clientSpan.Finish()
		if err != nil {
			clientSpan.Recordf("error: %s", err)
			setGRPCErrorTag(clientSpan, err)
		}
	}
	go func() {
		select {
		case <-finishChan:
			// The client span is being finished by another code path; hence, no
			// action is necessary.
		case <-cs.Context().Done():
			finishFunc(nil)
		}
	}()
	otcs := &tracingClientStream{
		ClientStream: cs,
		desc:         desc,
		finishFunc:   finishFunc,
	}

	// The `ClientStream` interface allows one to omit calling `Recv` if it's
	// known that the result will be `io.EOF`. See
	// http://stackoverflow.com/q/42915337
	// In such cases, there's nothing that triggers the span to finish. We,
	// therefore, set a finalizer so that the span and the context goroutine will
	// at least be cleaned up when the garbage collector is run.
	runtime.SetFinalizer(otcs, func(otcs *tracingClientStream) {
		otcs.finishFunc(nil)
	})
	return otcs
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
	return md, err
}

func (cs *tracingClientStream) SendMsg(m interface{}) error {
	err := cs.ClientStream.SendMsg(m)
	if err != nil {
		cs.finishFunc(err)
	}
	return err
}

func (cs *tracingClientStream) RecvMsg(m interface{}) error {
	err := cs.ClientStream.RecvMsg(m)
	if err == io.EOF {
		cs.finishFunc(nil)
		return err
	} else if err != nil {
		cs.finishFunc(err)
		return err
	}
	if !cs.desc.ServerStreams {
		cs.finishFunc(nil)
	}
	return err
}

func (cs *tracingClientStream) CloseSend() error {
	err := cs.ClientStream.CloseSend()
	if err != nil {
		cs.finishFunc(err)
	}
	return err
}

// Recording represents a group of RecordedSpans rooted at a fixed root span, as
// returned by GetRecording. Spans are sorted by StartTime.
type Recording []tracingpb.RecordedSpan
