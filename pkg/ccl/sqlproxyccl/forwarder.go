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

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// ErrForwarderClosed indicates that the forwarder has been closed.
var ErrForwarderClosed = errors.New("forwarder has been closed")

// ErrForwarderStarted indicates that the forwarder has already started.
var ErrForwarderStarted = errors.New("forwarder has already started")

// forwarder is used to forward pgwire messages from the client to the server,
// and vice-versa. At the moment, this does a direct proxying, and there is
// no intercepting. Once https://github.com/cockroachdb/cockroach/issues/76000
// has been addressed, we will start intercepting pgwire messages at their
// boundaries here.
type forwarder struct {
	// ctx is a single context used to control all goroutines spawned by the
	// forwarder.
	ctx       context.Context
	ctxCancel context.CancelFunc

	// crdbConn is only set after the authentication phase for the initial
	// connection. In the context of a connection migration, crdbConn is only
	// replaced once the session has successfully been deserialized, and the
	// old connection will be closed.
	conn     net.Conn // client <-> proxy
	crdbConn net.Conn // proxy <-> client

	// errChan is a buffered channel that contains the first forwarder error.
	// This channel may receive nil errors.
	errChan chan error

	mu struct {
		syncutil.Mutex

		// closed is set to true whenever the forwarder is closed explicitly
		// through Close, or when any of its main goroutines has terminated,
		// whichever that happens first.
		//
		// A new forwarder instance will have to be recreated if one wants to
		// reuse the same pair of connections.
		closed bool

		// started is set to true once Run has been invoked on the forwarder.
		// This is a safeguard to prevent callers from starting the forwarding
		// process twice. This will never be set back to false.
		started bool
	}
}

// newForwarder returns a new instance of forwarder. When this is called, it
// is expected that the caller passes ownerships of conn and crdbConn to the
// forwarder, which implies that these connections may be closed by the
// forwarder if necessary, but this is not guaranteed. The caller should still
// attempt to cleanup the connections.
func newForwarder(ctx context.Context, conn net.Conn, crdbConn net.Conn) *forwarder {
	ctx, cancelFn := context.WithCancel(ctx)

	return &forwarder{
		ctx:       ctx,
		ctxCancel: cancelFn,
		conn:      conn,
		crdbConn:  crdbConn,
		errChan:   make(chan error, 1),
	}
}

// Run starts the forwarding process for the associated connections, and can
// only be called once throughout the lifetime of the forwarder instance.
//
// All goroutines spun up must check on f.ctx to prevent leaks, if possible. If
// there was an error within the goroutines, the forwarder will be closed, and
// the first error can be found in f.errChan.
//
// This returns ErrForwarderClosed if the forwarder has been closed, and
// ErrForwarderStarted if the forwarder has already been started.
func (f *forwarder) Run() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.mu.closed {
		return ErrForwarderClosed
	}
	if f.mu.started {
		return ErrForwarderStarted
	}

	go func() {
		defer f.Close()

		// Block until context is done.
		<-f.ctx.Done()

		// Closing the connection will terminate the goroutine below.
		// This will be here temporarily because the only way to unblock
		// io.Copy is to close one of the ends. Once we replace io.Copy
		// with the interceptors, we could use ctx directly, and no longer
		// need this goroutine.
		f.crdbConn.Close()
	}()

	// Copy all pgwire messages from frontend to backend connection until we
	// encounter an error or shutdown signal.
	go func() {
		defer f.Close()

		err := ConnectionCopy(f.crdbConn, f.conn)
		select {
		case f.errChan <- err: /* error reported */
		default: /* the channel already contains an error */
		}
	}()

	f.mu.started = true
	return nil
}

// Close closes the forwarder, and stops the forwarding process. This is
// idempotent.
func (f *forwarder) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.mu.closed = true
	f.ctxCancel()
}

// IsClosed returns a boolean indicating whether the forwarder is closed.
func (f *forwarder) IsClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.mu.closed
}

// IsStarted returns a boolean indicating whether the forwarder has been
// started during its lifetime. This does not indicate that the forwarder has
// been closed.
func (f *forwarder) IsStarted() bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.mu.started
}
