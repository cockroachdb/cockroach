// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/interceptor"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// clientMsgAny is used to denote a wildcard client message type.
var clientMsgAny = pgwirebase.ClientMessageType(0)

// forwarder is used to forward pgwire messages from the client to the server,
// and vice-versa. The forwarder instance should always be constructed through
// the forward function, which also starts the forwarder.
type forwarder struct {
	// ctx is a single context used to control all goroutines spawned by the
	// forwarder.
	ctx       context.Context
	ctxCancel context.CancelFunc

	// connector is an instance of the connector, which will be used to open a
	// new connection to a SQL pod. This connector instance must be associated
	// to the same tenant as the forwarder.
	connector *connector

	// metrics contains various counters reflecting proxy operations. This is
	// the same as the metrics field in the proxyHandler instance.
	metrics *metrics

	// req and res represent the processors used to handle client-to-server
	// and server-to-client messages.
	req *requestProcessor
	res *responseProcessor

	// errCh is a buffered channel that contains the first forwarder error.
	// This channel may receive nil errors. When an error is written to this
	// channel, it is guaranteed that the forwarder and all connections will
	// be closed.
	errCh chan error

	// clientConn (and serverConn) provides a convenient way to read and forward
	// Postgres messages, while minimizing IO reads and memory allocations.
	//
	// clientConn is set once during initialization, and stays the same
	// throughout the lifetime of the forwarder.
	//
	// All reads from these connections must go through the interceptors. It is
	// not safe to call Read directly as the interceptors may have buffered data.
	clientConn *interceptor.BackendConn // client <-> proxy

	// mu contains state protected by the forwarder's mutex. This is necessary
	// since fields will be read and write from different goroutines.
	mu struct {
		syncutil.Mutex

		// isTransferring indicates that a connection migration is in progress.
		isTransferring bool

		// serverConn is only set after the authentication phase for the initial
		// connection. In the context of a connection migration, serverConn is
		// only replaced once the session has successfully been deserialized,
		// and the old connection will be closed. Whenever serverConn gets
		// updated, both clientMessageTypeSent and isServerMsgReadyReceived
		// fields have to reset to their initial values.
		//
		// See clientConn for more information.
		serverConn *interceptor.FrontendConn // proxy <-> server

		// clientMessageTypeSent indicates the message type for the last pgwire
		// message sent to serverConn. This will be initialized to clientMsgAny.
		//
		// Used for connection migration.
		clientMessageTypeSent pgwirebase.ClientMessageType

		// isServerMsgReadyReceived denotes whether a ReadyForQuery message has
		// been received by the response processor *after* a message has been
		// sent to the server through a Write on serverConn, either directly
		// or through ForwardMsg. This will be initialized to true to implicitly
		// denote that the server is ready to accept queries.
		//
		// Used for connection migration.
		isServerMsgReadyReceived bool
	}

	// Knobs used for testing.
	testingKnobs struct {
		isSafeTransferPoint            func() bool
		transferTimeoutDuration        func() time.Duration
		onTransferTimeoutHandlerStart  func()
		onTransferTimeoutHandlerFinish func()
	}
}

// forward returns a new instance of forwarder, and starts forwarding messages
// from clientConn to serverConn. When this is called, it is expected that the
// caller passes ownership of both clientConn and serverConn to the forwarder,
// which implies that the forwarder will clen them up. clientConn and serverConn
// must not be nil in all cases except for testing.
//
// Note that callers MUST call Close in all cases, even if ctx was cancelled.
//
// TODO(jaylim-crl): Convert this to return a Forwarder interface.
func forward(
	ctx context.Context,
	connector *connector,
	metrics *metrics,
	clientConn net.Conn,
	serverConn net.Conn,
) *forwarder {
	ctx, cancelFn := context.WithCancel(ctx)
	f := &forwarder{
		ctx:        ctx,
		ctxCancel:  cancelFn,
		errCh:      make(chan error, 1),
		connector:  connector,
		metrics:    metrics,
		clientConn: interceptor.NewBackendConn(clientConn),
	}
	f.mu.serverConn = interceptor.NewFrontendConn(serverConn)
	f.mu.clientMessageTypeSent = clientMsgAny
	f.mu.isServerMsgReadyReceived = true
	f.req = &requestProcessor{f: f}
	f.res = &responseProcessor{f: f}
	f.resumeProcessors()
	return f
}

// Close closes the forwarder and all connections. This is idempotent.
func (f *forwarder) Close() {
	f.ctxCancel()

	// Whenever Close is called while both of the processors are suspended, the
	// main goroutine will be stuck waiting for a reponse from the forwarder.
	// Send an error to unblock that. If an error has been sent, this error will
	// be ignored.
	//
	// We don't use tryReportError here since that will call Close, leading to
	// a recursive call.
	select {
	case f.errCh <- errors.New("forwarder closed"): /* error reported */
	default: /* the channel already contains an error */
	}

	// Since Close is idempotent, we'll ignore the error from Close calls in
	// case they have already been closed.
	f.clientConn.Close()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mu.serverConn.Close()
}

// RequestTransfer requests that the forwarder performs a best-effort connection
// migration whenever it can. It is best-effort because this will be a no-op if
// the forwarder is not in a state that is eligible for a connection migration.
// If a transfer is already in progress, or has been requested, this is a no-op.
func (f *forwarder) RequestTransfer() {
	go f.runTransfer()
}

// resumeProcessors starts both the request and response processors
// asynchronously. The forwarder will be closed if any of the processors
// return an error while resuming. This is idempotent as Resume() will return
// nil if the processor has already been started.
func (f *forwarder) resumeProcessors() {
	go func() {
		err := f.req.Resume()
		if err != nil {
			f.tryReportError(wrapClientToServerError(err))
		}
	}()
	go func() {
		err := f.res.Resume()
		if err != nil {
			f.tryReportError(wrapServerToClientError(err))
		}
	}()
}

// wrapClientToServerError overrides client to server errors for external
// consumption.
//
// TODO(jaylim-crl): We don't send any of these to the client today,
// unfortunately. At the moment, this is only used for metrics. See TODO in
// proxy_handler about sending safely to avoid corrupted packets. Handle these
// errors in a friendly manner.
func wrapClientToServerError(err error) error {
	if err == nil ||
		errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
		return nil
	}
	return newErrorf(codeClientDisconnected, "copying from client to target server: %v", err)
}

// wrapServerToClientError overrides server to client errors for external
// consumption.
//
// TODO(jaylim-crl): We don't send any of these to the client today,
// unfortunately. At the moment, this is only used for metrics. See TODO in
// proxy_handler about sending safely to avoid corrupted packets. Handle these
// errors in a friendly manner.
func wrapServerToClientError(err error) error {
	if err == nil ||
		errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
		return nil
	}
	return newErrorf(codeBackendDisconnected, "copying from target server to client: %s", err)
}

// tryReportError tries to send err to errChan, and closes the forwarder if
// it succeeds. If an error has already been reported, err will be dropped.
func (f *forwarder) tryReportError(err error) {
	select {
	case f.errCh <- err: /* error reported */
		f.Close()
	default: /* the channel already contains an error */
	}
}

// aLongTimeAgo is a non-zero time, far in the past, used for immediate
// cancellation of dials.
var aLongTimeAgo = time.Unix(1, 0)

type requestProcessor struct {
	f *forwarder

	mu struct {
		syncutil.Mutex
		resumed    bool
		cond       *sync.Cond
		inPeek     bool
		suspendReq bool
	}
	wg sync.WaitGroup
}

func (p *requestProcessor) lock() {
	p.mu.Lock()
	if p.mu.cond == nil {
		p.mu.cond = sync.NewCond(&p.mu)
	}
}

func (p *requestProcessor) unlock() { p.mu.Unlock() }

// Once this returns an error, Resume should not be called again.
func (p *requestProcessor) Resume() error {
	p.wg.Add(1)
	defer p.wg.Done()

	if p.f.ctx.Err() != nil {
		return p.f.ctx.Err()
	}

	p.lock()
	if p.mu.resumed {
		p.unlock()
		return nil
	}
	p.mu.resumed = true
	p.unlock()
	defer func() {
		p.lock()
		defer p.unlock()
		p.mu.resumed = false
	}()

	for p.f.ctx.Err() == nil {
		// inPeek has to be false before this because the resumed field guards
		// against concurrent runs.
		p.lock()
		if p.mu.suspendReq {
			p.mu.suspendReq = false
			p.unlock()
			return nil
		}
		p.mu.inPeek = true
		p.unlock()

		// Always peek the message to ensure that we're blocked on reading the
		// header, rather than when forwarding.
		typ, _, err := p.f.clientConn.PeekMsg()

		p.lock()
		suspend := p.mu.suspendReq
		p.mu.suspendReq = false
		p.mu.inPeek = false
		p.unlock()
		p.mu.cond.Broadcast()

		if err != nil {
			if ne, ok := err.(net.Error); ok && suspend && ne.Timeout() {
				// Do nothing.
				err = nil
			} else {
				err = errors.Wrap(err, "peeking message in client-to-server")
			}
		}
		if err != nil || suspend {
			return err
		}

		p.f.mu.Lock()
		p.f.mu.clientMessageTypeSent = typ
		p.f.mu.isServerMsgReadyReceived = false
		p.f.mu.Unlock()

		// NOTE: No need to obtain lock for serverConn since that can only
		// be updated during a transfer, and when that happens, the request
		// processor must have already been suspended.
		if _, err := p.f.clientConn.ForwardMsg(p.f.mu.serverConn); err != nil {
			return errors.Wrap(err, "forwarding message in client-to-server")
		}
	}
	return p.f.ctx.Err()
}

func (p *requestProcessor) Suspend() {
	p.lock()
	defer p.unlock()
	if !p.mu.resumed {
		return
	}
	if !p.mu.inPeek {
		p.mu.suspendReq = true
		return
	}
	for p.mu.inPeek {
		p.mu.suspendReq = true
		p.f.clientConn.SetReadDeadline(aLongTimeAgo)
		p.mu.cond.Wait()
	}
	p.f.clientConn.SetReadDeadline(time.Time{})
}

func (p *requestProcessor) WaitUntilSuspended() {
	p.wg.Wait()
}

type responseProcessor struct {
	f *forwarder

	mu struct {
		syncutil.Mutex
		resumed    bool
		cond       *sync.Cond
		inPeek     bool
		suspendReq bool
	}
	wg sync.WaitGroup
}

func (p *responseProcessor) lock() {
	p.mu.Lock()
	if p.mu.cond == nil {
		p.mu.cond = sync.NewCond(&p.mu)
	}
}

func (p *responseProcessor) unlock() { p.mu.Unlock() }

// Once this returns an error, Resume should not be called again.
func (p *responseProcessor) Resume() error {
	p.wg.Add(1)
	defer p.wg.Done()

	if p.f.ctx.Err() != nil {
		return p.f.ctx.Err()
	}

	p.lock()
	if p.mu.resumed {
		p.unlock()
		return nil
	}
	p.mu.resumed = true
	p.unlock()
	defer func() {
		p.lock()
		defer p.unlock()
		p.mu.resumed = false
	}()

	// NOTE: No need to obtain lock for serverConn since that can only be
	// updated during a transfer, and when that happens, the request processor
	// must have already been suspended.
	for p.f.ctx.Err() == nil {
		// inPeek has to be false before this because the resumed field guards
		// against concurrent runs.
		p.lock()
		if p.mu.suspendReq {
			p.mu.suspendReq = false
			p.unlock()
			return nil
		}
		p.mu.inPeek = true
		p.unlock()

		// Always peek the message to ensure that we're blocked on reading the
		// header, rather than when forwarding or reading the entire message.
		typ, _, err := p.f.mu.serverConn.PeekMsg()

		p.lock()
		suspend := p.mu.suspendReq
		p.mu.suspendReq = false
		p.mu.inPeek = false
		p.unlock()
		p.mu.cond.Broadcast()

		if err != nil {
			if ne, ok := err.(net.Error); ok && suspend && ne.Timeout() {
				// Do nothing.
				err = nil
			} else {
				err = errors.Wrap(err, "peeking message in server-to-client")
			}
		}
		if err != nil || suspend {
			return err
		}

		p.f.mu.Lock()
		// Did we see a ReadyForQuery message?
		if typ == pgwirebase.ServerMsgReady {
			p.f.mu.isServerMsgReadyReceived = true
		}
		p.f.mu.Unlock()

		if _, err := p.f.mu.serverConn.ForwardMsg(p.f.clientConn); err != nil {
			return errors.Wrap(err, "forwarding message in server-to-client")
		}
	}
	return p.f.ctx.Err()
}

func (p *responseProcessor) Suspend() {
	p.lock()
	defer p.unlock()
	if !p.mu.resumed {
		return
	}
	if !p.mu.inPeek {
		p.mu.suspendReq = true
		return
	}
	// No need to obtain locks for serverConn since the processor is still
	// running, which guarantees that there won't be writes to it.
	for p.mu.inPeek {
		p.mu.suspendReq = true
		p.f.mu.serverConn.SetReadDeadline(aLongTimeAgo)
		p.mu.cond.Wait()
	}
	p.f.mu.serverConn.SetReadDeadline(time.Time{})
}

func (p *responseProcessor) WaitUntilSuspended() {
	p.wg.Wait()
}
