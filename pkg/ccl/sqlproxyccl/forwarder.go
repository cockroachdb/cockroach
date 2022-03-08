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
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// forwarder is used to forward pgwire messages from the client to the server,
// and vice-versa. At the moment, this does a direct proxying, and there is
// no intercepting. Once https://github.com/cockroachdb/cockroach/issues/76000
// has been addressed, we will start intercepting pgwire messages at their
// boundaries here.
//
// The forwarder instance should always be constructed through the forward
// function, which also starts the forwarder.
type forwarder struct {
	// ctx is a single context used to control all goroutines spawned by the
	// forwarder. An exception to this is that if the goroutines are blocked
	// due to IO on clientConn or serverConn, cancelling the context does not
	// unblock them. Due to this, it is important to invoke Close() on the
	// forwarder whenever ctx has been cancelled to prevent leaks.
	ctx       context.Context
	ctxCancel context.CancelFunc

	// clientConn and serverConn provide a convenient way to read and forward
	// Postgres messages, while minimizing IO reads and memory allocations.
	//
	// clientConn is set once during initialization, and stays the same
	// throughout the lifetime of the forwarder.
	//
	// serverConn is only set after the authentication phase for the initial
	// connection. In the context of a connection migration, serverConn is only
	// replaced once the session has successfully been deserialized, and the old
	// connection will be closed. Whenever serverConn gets updated, both
	// clientMessageTypeSent and isServerMsgReadyReceived fields have to reset
	// to their initial values.
	//
	// All reads from these connections must go through the interceptors. It is
	// not safe to call Read directly as the interceptors may have buffered data.
	clientConn *interceptor.BackendConn  // client <-> proxy
	serverConn *interceptor.FrontendConn // proxy <-> server

	// request and response represent the processors used to handle
	// client-to-server and server-to-client messages.
	request  processor
	response processor

	// errCh is a buffered channel that contains the first forwarder error.
	// This channel may receive nil errors. When an error is written to this
	// channel, it is guaranteed that the forwarder and all connections will
	// be closed.
	errCh chan error
}

// forward returns a new instance of forwarder, and starts forwarding messages
// from clientConn to serverConn. When this is called, it is expected that the
// caller passes ownership of both clientConn and serverConn to the forwarder,
// which implies that the forwarder will clean them up. clientConn and
// serverConn must not be nil in all cases except for testing.
//
// Note that callers MUST call Close in all cases, even if ctx was cancelled,
// and callers will need to detect that (for now).
func forward(ctx context.Context, clientConn, serverConn net.Conn) *forwarder {
	ctx, cancelFn := context.WithCancel(ctx)
	f := &forwarder{
		ctx:        ctx,
		ctxCancel:  cancelFn,
		errCh:      make(chan error, 1),
		clientConn: interceptor.NewBackendConn(clientConn),
		serverConn: interceptor.NewFrontendConn(serverConn),
	}
	f.request = &requestProcessor{f: f}
	f.response = &responseProcessor{f: f}
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
	f.serverConn.Close()
}

// resumeProcessors starts both the request and response processors
// asynchronously. The forwarder will be closed if any of the processors
// return an error while resuming. This is idempotent as Resume() will return
// nil if the processor has already been started.
func (f *forwarder) resumeProcessors() {
	go func() {
		if err := f.request.resume(); err != nil {
			f.tryReportError(wrapClientToServerError(err))
		}
	}()
	go func() {
		if err := f.response.resume(); err != nil {
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

// tryReportError tries to send err to errCh, and closes the forwarder if
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
var aLongTimeAgo = timeutil.Unix(1, 0)

var errProcessorResumed = errors.New("processor has already been resumed")

type processor interface {
	// resume starts the processor and blocks if it hasn't been started yet.
	// This returns nil if the processor can be resumed again in the future.
	// If an error (with the exception of errProcessorResumed) was returned,
	// the processor should not be resumed again, and the forwarder should be
	// closed.
	resume() error
	// suspend requests for the processor to be suspended if it is in a safe
	// state. Callers must call waitUntilSuspended to ensure that the processor
	// has been suspended. This is because there's a chance where the processor
	// is not in a state to be suspended (e.g. blocked on forwarding a large
	// message). If the suspend request failed, an error will be returned. The
	// caller is safe to retry again.
	suspend() error
	// waitUntilSuspended blocks until the processor has been suspended. If
	// suspend was not called before, this may block forever.
	waitUntilSuspended()
}

// requestProcessor handles the communication from the client to the server.
type requestProcessor struct {
	f  *forwarder
	mu struct {
		syncutil.Mutex
		cond       *sync.Cond
		resumed    bool
		inPeek     bool
		suspendReq bool // Indicates that a suspend has been requested.
	}
	wg sync.WaitGroup
}

var _ processor = &requestProcessor{}

func (p *requestProcessor) lock() {
	p.mu.Lock()
	if p.mu.cond == nil {
		p.mu.cond = sync.NewCond(&p.mu)
	}
}

func (p *requestProcessor) unlock() { p.mu.Unlock() }

func (p *requestProcessor) resume() error {
	p.wg.Add(1)
	defer p.wg.Done()

	// Has context been cancelled?
	if p.f.ctx.Err() != nil {
		return p.f.ctx.Err()
	}

	p.lock()
	// Already resumed - don't start another one.
	if p.mu.resumed {
		p.unlock()
		return errProcessorResumed
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
		// Suspend has been requested. Suspend now before blocking.
		if p.mu.suspendReq {
			p.mu.suspendReq = false
			p.unlock()
			return nil
		}
		p.mu.inPeek = true
		if dErr := p.f.clientConn.SetReadDeadline(time.Time{}); dErr != nil {
			p.unlock()
			return dErr
		}
		p.unlock()

		// Always peek the message to ensure that we're blocked on reading the
		// header, rather than when forwarding.
		_, _, err := p.f.clientConn.PeekMsg()

		p.lock()
		suspend := p.mu.suspendReq
		p.mu.suspendReq = false
		p.mu.inPeek = false
		if dErr := p.f.clientConn.SetReadDeadline(time.Time{}); dErr != nil {
			p.unlock()
			p.mu.cond.Broadcast()
			return dErr
		}
		p.unlock()
		p.mu.cond.Broadcast()

		// If suspend was requested, there are two cases where we will
		// terminate:
		//   1. err == nil, where we managed to read a header. In that case,
		//      suspension gets priority.
		//   2. err != nil, where the error was due to a timeout. Connection
		//      is likely idle here.
		if err != nil {
			if netErr := (net.Error)(nil); errors.As(err, &netErr) && suspend && netErr.Timeout() {
				// Do nothing.
				err = nil
			} else {
				err = errors.Wrap(err, "peeking message")
			}
		}
		if err != nil || suspend {
			return err
		}

		if _, err := p.f.clientConn.ForwardMsg(p.f.serverConn); err != nil {
			return errors.Wrap(err, "forwarding message")
		}
	}
	return p.f.ctx.Err()
}

func (p *requestProcessor) suspend() error {
	p.lock()
	defer p.unlock()
	// Processor has not been resumed.
	if !p.mu.resumed {
		return nil
	}
	// We're not in PeekMsg, so there's no need to set a deadline. We don't
	// want to unblock ForwardMsg because that would result in a corrupted
	// state.
	if !p.mu.inPeek {
		p.mu.suspendReq = true
		return nil
	}
	for p.mu.inPeek {
		p.mu.suspendReq = true
		if err := p.f.clientConn.SetReadDeadline(aLongTimeAgo); err != nil {
			return err
		}
		p.mu.cond.Wait()
	}
	return nil
}

func (p *requestProcessor) waitUntilSuspended() {
	p.wg.Wait()
}

// responseProcessor handles the communication from the server to the client.
type responseProcessor struct {
	f  *forwarder
	mu struct {
		syncutil.Mutex
		cond       *sync.Cond
		resumed    bool
		inPeek     bool
		suspendReq bool // Indicates that a suspend has been requested.
	}
	wg sync.WaitGroup
}

var _ processor = &responseProcessor{}

func (p *responseProcessor) lock() {
	p.mu.Lock()
	if p.mu.cond == nil {
		p.mu.cond = sync.NewCond(&p.mu)
	}
}

func (p *responseProcessor) unlock() { p.mu.Unlock() }

func (p *responseProcessor) resume() error {
	p.wg.Add(1)
	defer p.wg.Done()

	// Has context been cancelled?
	if p.f.ctx.Err() != nil {
		return p.f.ctx.Err()
	}

	p.lock()
	// Already resumed - don't start another one.
	if p.mu.resumed {
		p.unlock()
		return errProcessorResumed
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
		// Suspend has been requested. Suspend now before blocking.
		if p.mu.suspendReq {
			p.mu.suspendReq = false
			p.unlock()
			return nil
		}
		p.mu.inPeek = true
		if dErr := p.f.serverConn.SetReadDeadline(time.Time{}); dErr != nil {
			p.unlock()
			return dErr
		}
		p.unlock()

		// Always peek the message to ensure that we're blocked on reading the
		// header, rather than when forwarding or reading the entire message.
		_, _, err := p.f.serverConn.PeekMsg()

		p.lock()
		suspend := p.mu.suspendReq
		p.mu.suspendReq = false
		p.mu.inPeek = false
		if dErr := p.f.serverConn.SetReadDeadline(time.Time{}); dErr != nil {
			p.unlock()
			p.mu.cond.Broadcast()
			return dErr
		}
		p.unlock()
		p.mu.cond.Broadcast()

		// If suspend was requested, there are two cases where we will
		// terminate:
		//   1. err == nil, where we managed to read a header. In that case,
		//      suspension gets priority.
		//   2. err != nil, where the error was due to a timeout. Connection
		//      is likely idle here.
		if err != nil {
			if netErr := (net.Error)(nil); errors.As(err, &netErr) && suspend && netErr.Timeout() {
				// Do nothing.
				err = nil
			} else {
				err = errors.Wrap(err, "peeking message")
			}
		}
		if err != nil || suspend {
			return err
		}

		if _, err := p.f.serverConn.ForwardMsg(p.f.clientConn); err != nil {
			return errors.Wrap(err, "forwarding message")
		}
	}
	return p.f.ctx.Err()
}

func (p *responseProcessor) suspend() error {
	p.lock()
	defer p.unlock()
	// Processor has not been resumed.
	if !p.mu.resumed {
		return nil
	}
	// We're not in PeekMsg, so there's no need to set a deadline. We don't
	// want to unblock ForwardMsg because that would result in a corrupted
	// state.
	if !p.mu.inPeek {
		p.mu.suspendReq = true
		return nil
	}
	for p.mu.inPeek {
		p.mu.suspendReq = true
		if err := p.f.serverConn.SetReadDeadline(aLongTimeAgo); err != nil {
			return err
		}
		p.mu.cond.Wait()
	}
	return nil
}

func (p *responseProcessor) waitUntilSuspended() {
	p.wg.Wait()
}
