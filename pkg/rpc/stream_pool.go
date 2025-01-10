// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"io"
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"storj.io/drpc"
)

// streamClient is a type constraint that is satisfied by a bidirectional gRPC
// client stream.
type streamClient[Req, Resp any] interface {
	Send(Req) error
	Recv() (Resp, error)
}

// streamConstructor creates a new gRPC stream client over the provided client
// connection, using the provided call options.
type streamConstructor[Req, Resp, Conn any] func(
	context.Context, Conn,
) (streamClient[Req, Resp], error)

type result[Resp any] struct {
	resp Resp
	err  error
}

// defaultPooledStreamIdleTimeout is the default duration after which a pooled
// stream is considered idle and is closed. The idle timeout is used to ensure
// that stream pools eventually shrink when the load decreases.
const defaultPooledStreamIdleTimeout = 10 * time.Second

// pooledStream is a wrapper around a grpc.ClientStream that is managed by a
// streamPool. It is responsible for sending a single request and receiving a
// single response on the stream at a time, mimicking the behavior of a gRPC
// unary RPC. However, unlike a unary RPC, the client stream is not discarded
// after a single use. Instead, it is returned to the pool for reuse.
//
// Most of the complexity around this type (e.g. the worker goroutine) comes
// from the need to handle context cancellation while a request is in-flight.
// gRPC streams support context cancellation, but they use the context provided
// to the stream when it was created for its entire lifetime. Meanwhile, we want
// to be able to handle context cancellation on a per-request basis while we
// layer unary RPC semantics on top of a pooled, bidirectional stream. To
// accomplish this, we use a worker goroutine to perform the (blocking) RPC
// function calls (Send and Recv) and let callers in Send wait on the result of
// the RPC call while also listening to their own context for cancellation. If
// the caller's context is canceled, it cancels the stream's context, which in
// turn cancels the RPC call.
//
// A pooledStream is not safe for concurrent use. It is intended to be used by
// only a single caller at a time. Mutual exclusion is coordinated by removing a
// pooledStream from the pool while it is in use.
//
// A pooledStream must only be returned to the pool for reuse after a successful
// Send call. If the Send call fails, the pooledStream must not be reused.
type pooledStream[Req, Resp any, Conn comparable] struct {
	pool         *streamPool[Req, Resp, Conn]
	stream       streamClient[Req, Resp]
	streamCtx    context.Context
	streamCancel context.CancelFunc

	reqC  chan Req
	respC chan result[Resp]
}

func newPooledStream[Req, Resp any, Conn comparable](
	pool *streamPool[Req, Resp, Conn],
	stream streamClient[Req, Resp],
	streamCtx context.Context,
	streamCancel context.CancelFunc,
) *pooledStream[Req, Resp, Conn] {
	return &pooledStream[Req, Resp, Conn]{
		pool:         pool,
		stream:       stream,
		streamCtx:    streamCtx,
		streamCancel: streamCancel,
		reqC:         make(chan Req),
		respC:        make(chan result[Resp], 1),
	}
}

func (s *pooledStream[Req, Resp, Conn]) run(ctx context.Context) {
	defer s.close()
	for s.runOnce(ctx) {
	}
}

func (s *pooledStream[Req, Resp, Conn]) runOnce(ctx context.Context) (loop bool) {
	select {
	case req := <-s.reqC:
		err := s.stream.Send(req)
		if err != nil {
			// From grpc.ClientStream.SendMsg:
			// > On error, SendMsg aborts the stream.
			s.respC <- result[Resp]{err: err}
			return false
		}
		resp, err := s.stream.Recv()
		if err != nil {
			// From grpc.ClientStream.RecvMsg:
			// > It returns io.EOF when the stream completes successfully. On any
			// > other error, the stream is aborted and the error contains the RPC
			// > status.
			if errors.Is(err, io.EOF) {
				log.Errorf(ctx, "stream unexpectedly closed by server: %+v", err)
			}
			s.respC <- result[Resp]{err: err}
			return false
		}
		s.respC <- result[Resp]{resp: resp}
		return true

	case <-time.After(s.pool.idleTimeout):
		// Try to remove ourselves from the pool. If we don't find ourselves in the
		// pool, someone just grabbed us from the pool and we should keep running.
		// If we do find and remove ourselves, we can close the stream and stop
		// running. This ensures that callers never encounter spurious stream
		// closures due to idle timeouts.
		return !s.pool.remove(s)

	case <-ctx.Done():
		return false
	}
}

func (s *pooledStream[Req, Resp, Conn]) close() {
	// Make sure the stream's context is canceled to ensure that we clean up
	// resources in idle timeout case.
	//
	// From grpc.ClientConn.NewStream:
	// > To ensure resources are not leaked due to the stream returned, one of the
	// > following actions must be performed:
	// > ...
	// >  2. Cancel the context provided.
	// > ...
	s.streamCancel()
	// Try to remove ourselves from the pool, now that we're closed. If we don't
	// find ourselves in the pool, someone has already grabbed us from the pool
	// and will check whether we are closed before putting us back.
	s.pool.remove(s)
}

// Send sends a request on the pooled stream and returns the response in a unary
// RPC fashion. Context cancellation is respected.
func (s *pooledStream[Req, Resp, Conn]) Send(ctx context.Context, req Req) (Resp, error) {
	var resp result[Resp]
	select {
	case s.reqC <- req:
		// The request was passed to the stream's worker goroutine, which will
		// invoke the RPC function calls (Send and Recv). Wait for a response.
		select {
		case resp = <-s.respC:
			// Return the response.
		case <-ctx.Done():
			// Cancel the stream and return the request's context error.
			s.streamCancel()
			resp.err = ctx.Err()
		}
	case <-s.streamCtx.Done():
		// The stream was closed before its worker goroutine could accept the
		// request. Return the stream's context error.
		resp.err = s.streamCtx.Err()
	}

	if resp.err != nil {
		// On error, wait until we see the streamCtx.Done() signal, to ensure that
		// the stream has been cleaned up and won't be placed back in the pool by
		// putIfNotClosed.
		<-s.streamCtx.Done()
	}
	return resp.resp, resp.err
}

// streamPool is a pool of grpc.ClientStream objects (wrapped in pooledStream)
// that are used to send requests and receive corresponding responses in a
// manner that mimics unary RPC invocation. Pooling these streams allows for
// reuse of gRPC resources across calls, as opposed to native unary RPCs, which
// create a new stream and throw it away for each request (see grpc.invoke).
type streamPool[Req, Resp any, Conn comparable] struct {
	stopper     *stop.Stopper
	idleTimeout time.Duration
	newStream   streamConstructor[Req, Resp, Conn]

	// cc and ccCtx are set on bind, when the gRPC connection is established.
	cc Conn
	// Derived from rpc.Context.MasterCtx, canceled on stopper quiesce.
	ccCtx context.Context

	streams struct {
		syncutil.Mutex
		s []*pooledStream[Req, Resp, Conn]
	}
}

func makeStreamPool[Req, Resp any, Conn comparable](
	stopper *stop.Stopper, newStream streamConstructor[Req, Resp, Conn],
) streamPool[Req, Resp, Conn] {
	return streamPool[Req, Resp, Conn]{
		stopper:     stopper,
		idleTimeout: defaultPooledStreamIdleTimeout,
		newStream:   newStream,
	}
}

// Bind sets the gRPC connection and context for the streamPool. This must be
// called once before streamPool.Send.
func (p *streamPool[Req, Resp, Conn]) Bind(ctx context.Context, cc Conn) {
	p.cc = cc
	p.ccCtx = ctx
}

// Conn returns the gRPC connection bound to the streamPool.
func (p *streamPool[Req, Resp, Conn]) Conn() Conn {
	return p.cc
}

// Close closes all streams in the pool.
func (p *streamPool[Req, Resp, Conn]) Close() {
	p.streams.Lock()
	defer p.streams.Unlock()
	for _, s := range p.streams.s {
		s.streamCancel()
	}
	p.streams.s = nil
}

func (p *streamPool[Req, Resp, Conn]) get() *pooledStream[Req, Resp, Conn] {
	p.streams.Lock()
	defer p.streams.Unlock()
	if len(p.streams.s) == 0 {
		return nil
	}
	// Pop from the tail to bias towards reusing the same streams repeatedly so
	// that streams at the head of the slice are more likely to be closed due to
	// idle timeouts.
	s := p.streams.s[len(p.streams.s)-1]
	p.streams.s[len(p.streams.s)-1] = nil
	p.streams.s = p.streams.s[:len(p.streams.s)-1]
	return s
}

func (p *streamPool[Req, Resp, Conn]) putIfNotClosed(s *pooledStream[Req, Resp, Conn]) {
	p.streams.Lock()
	defer p.streams.Unlock()
	if s.streamCtx.Err() != nil {
		// The stream is closed, don't put it in the pool. Note that this must be
		// done under lock to avoid racing with pooledStream.close, which attempts
		// to remove a closing stream from the pool.
		return
	}
	p.streams.s = append(p.streams.s, s)
}

func (p *streamPool[Req, Resp, Conn]) remove(s *pooledStream[Req, Resp, Conn]) bool {
	p.streams.Lock()
	defer p.streams.Unlock()
	i := slices.Index(p.streams.s, s)
	if i == -1 {
		return false
	}
	copy(p.streams.s[i:], p.streams.s[i+1:])
	p.streams.s[len(p.streams.s)-1] = nil
	p.streams.s = p.streams.s[:len(p.streams.s)-1]
	return true
}

func (p *streamPool[Req, Resp, Conn]) newPooledStream() (*pooledStream[Req, Resp, Conn], error) {
	var zero Conn
	if p.cc == zero {
		return nil, errors.AssertionFailedf("streamPool not bound to a client conn")
	}

	ctx, cancel := context.WithCancel(p.ccCtx)
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()

	stream, err := p.newStream(ctx, p.cc)
	if err != nil {
		return nil, err
	}

	s := newPooledStream(p, stream, ctx, cancel)
	if err := p.stopper.RunAsyncTask(ctx, "pooled gRPC stream", s.run); err != nil {
		return nil, err
	}
	cancel = nil
	return s, nil
}

// Send sends a request on a pooled stream and returns the response in a unary
// RPC fashion. If no stream is available in the pool, a new stream is created.
func (p *streamPool[Req, Resp, Conn]) Send(ctx context.Context, req Req) (Resp, error) {
	s := p.get()
	if s == nil {
		var err error
		s, err = p.newPooledStream()
		if err != nil {
			var zero Resp
			return zero, err
		}
	}
	defer p.putIfNotClosed(s)
	return s.Send(ctx, req)
}

// BatchStreamPool is a streamPool specialized for BatchStreamClient streams.
type BatchStreamPool = streamPool[*kvpb.BatchRequest, *kvpb.BatchResponse, *grpc.ClientConn]

// BatchStreamClient is a streamClient specialized for the BatchStream RPC.
//
//go:generate mockgen -destination=mocks_generated_test.go --package=. BatchStreamClient
type BatchStreamClient = streamClient[*kvpb.BatchRequest, *kvpb.BatchResponse]

// newBatchStream constructs a BatchStreamClient from a grpc.ClientConn.
func newBatchStream(ctx context.Context, cc *grpc.ClientConn) (BatchStreamClient, error) {
	return kvpb.NewInternalClient(cc).BatchStream(ctx)
}

type DRPCBatchStreamPool = streamPool[*kvpb.BatchRequest, *kvpb.BatchResponse, drpc.Conn]

type DRPCBatchStreamClient = streamClient[*kvpb.BatchRequest, *kvpb.BatchResponse]

// newDRPCBatchStream constructs a BatchStreamClient from a drpc.Conn.
func newDRPCBatchStream(ctx context.Context, dc drpc.Conn) (DRPCBatchStreamClient, error) {
	return kvpb.NewDRPCBatchClient(dc).BatchStream(ctx)
}
