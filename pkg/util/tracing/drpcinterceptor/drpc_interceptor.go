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
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/interceptorutil"
	"github.com/cockroachdb/errors"
	"storj.io/drpc"
	"storj.io/drpc/drpcclient"
)

// ClientInterceptorDrpc returns a drpcclient.UnaryClientInterceptor suitable
// for use with DRPC client connections.
//
// It provides the same tracing functionality as ClientInterceptor but adapted
// for the DRPC protocol. The interceptor will inject the tracing SpanMeta into
// the context metadata and establish a ChildOf relationship with any active
// parent Span in the context.
func ClientInterceptorDrpc(
	tracer *tracing.Tracer, init func(*tracing.Span),
) drpcclient.UnaryClientInterceptor {
	if init == nil {
		init = func(*tracing.Span) {}
	}
	return func(
		ctx context.Context,
		rpc string,
		enc drpc.Encoding,
		in, out drpc.Message,
		cc *drpcclient.ClientConn,
		invoker drpcclient.UnaryInvoker,
	) error {
		ctx, clientSpan, skipTracing := interceptorutil.CreateClientSpan(ctx, rpc, tracer, init)
		if skipTracing {
			return invoker(ctx, rpc, enc, in, out, cc)
		}

		defer clientSpan.Finish()

		if invoker != nil {
			err := invoker(ctx, rpc, enc, in, out, cc)
			if err != nil {
				//TODO check if setting GRPCError tag works for DRPC.
				interceptorutil.SetGRPCErrorTag(clientSpan, err)
				clientSpan.Recordf("error: %s", err)
				return err
			}
		}
		return nil
	}
}

// StreamClientInterceptorDrpc returns a drpcclient.StreamClientInterceptor suitable
// for use with DRPC client connections.
//
// It provides the same tracing functionality as StreamClientInterceptor but adapted
// for the DRPC protocol. The interceptor instruments streaming RPCs by creating
// a single span to correspond to the lifetime of the RPC's stream.
//
// All DRPC client spans will inject the tracing SpanMeta into the context metadata;
// they will also look in the context.Context for an active in-process parent Span
// and establish a ChildOf relationship if such a parent Span could be found.
func StreamClientInterceptorDrpc(
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
		ctx, clientSpan, skipTracing := interceptorutil.CreateStreamClientSpan(ctx, rpc, tracer, init)
		if skipTracing {
			return streamer(ctx, rpc, enc, cc)
		}

		ds, err := streamer(ctx, rpc, enc, cc)
		if err != nil {
			clientSpan.Recordf("error: %s", err)
			//TODO check if setting GRPCError tag works for DRPC.
			interceptorutil.SetGRPCErrorTag(clientSpan, err)
			clientSpan.Finish()
			return ds, err
		}

		return newTracingClientStreamDrpc(
			ctx, ds,
			// Pass ownership of clientSpan to the stream.
			clientSpan), nil
	}
}

// tracingClientStreamDrpc is an implementation of drpc.Stream that
// finishes the clientSpan when the stream terminates.
type tracingClientStreamDrpc struct {
	drpc.Stream
	finishFunc func(error)
}

// MsgSend sends a message and tracks any errors for tracing.
func (cs *tracingClientStreamDrpc) MsgSend(msg drpc.Message, enc drpc.Encoding) error {
	err := cs.Stream.MsgSend(msg, enc)
	if err == io.EOF {
		cs.finishFunc(nil)
		// Do not wrap EOF.
		return err
	} else if err != nil {
		cs.finishFunc(err)
	}
	return errors.Wrap(err, "send msg error")
}

// MsgRecv receives a message and tracks any errors for tracing.
func (cs *tracingClientStreamDrpc) MsgRecv(msg drpc.Message, enc drpc.Encoding) error {
	err := cs.Stream.MsgRecv(msg, enc)
	if err == io.EOF {
		cs.finishFunc(nil)
		// Do not wrap EOF.
		return err
	} else if err != nil {
		cs.finishFunc(err)
	}
	return errors.Wrap(err, "recv msg error")
}

// CloseSend closes the send direction of the stream and tracks any errors for tracing.
func (cs *tracingClientStreamDrpc) CloseSend() error {
	err := cs.Stream.CloseSend()
	if err != nil {
		cs.finishFunc(err)
	}
	return errors.Wrap(err, "close send error")
}

// Close closes the stream and tracks any errors for tracing.
func (cs *tracingClientStreamDrpc) Close() error {
	err := cs.Stream.Close()
	if err != nil {
		cs.finishFunc(err)
	} else {
		// Ensure the span is finished when the stream is closed
		cs.finishFunc(nil)
	}
	return errors.Wrap(err, "close error")
}

// newTracingClientStreamDrpc creates an implementation of drpc.Stream that
// finishes `clientSpan` when the stream terminates.
func newTracingClientStreamDrpc(
	ctx context.Context, ds drpc.Stream, clientSpan *tracing.Span,
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
			interceptorutil.SetGRPCErrorTag(clientSpan, err)
		}
	}
	_ = ctxutil.WhenDone(ctx, func() {
		finishFunc(nil /* err */)
	})

	return &tracingClientStreamDrpc{
		Stream:     ds,
		finishFunc: finishFunc,
	}
}
