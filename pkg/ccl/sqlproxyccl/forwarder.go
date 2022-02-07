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

	"github.com/cockroachdb/errors"
)

// ErrForwarderClosed indicates that the forwarder has been closed.
var ErrForwarderClosed = errors.New("forwarder has been closed")

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
	// forwarder.
	ctx       context.Context
	ctxCancel context.CancelFunc

	// serverConn is only set after the authentication phase for the initial
	// connection. In the context of a connection migration, serverConn is only
	// replaced once the session has successfully been deserialized, and the
	// old connection will be closed.
	clientConn net.Conn // client <-> proxy
	serverConn net.Conn // proxy <-> server

	// errChan is a buffered channel that contains the first forwarder error.
	// This channel may receive nil errors.
	errChan chan error
}

// forward returns a new instance of forwarder, and starts forwarding messages
// from clientConn to serverConn. When this is called, it is expected that the
// caller passes ownership of serverConn to the forwarder, which implies that
// the forwarder will clean up serverConn.
//
// All goroutines spun up must check on f.ctx to prevent leaks, if possible. If
// there was an error within the goroutines, the forwarder will be closed, and
// the first error can be found in f.errChan.
//
// clientConn and serverConn must not be nil in all cases except testing.
//
// Note that callers MUST call Close in all cases (regardless of IsStopped)
// since we only check on context cancellation there. There could be a
// possibility where the top-level context was cancelled, but the forwarder
// has not cleaned up.
func forward(ctx context.Context, clientConn, serverConn net.Conn) *forwarder {
	ctx, cancelFn := context.WithCancel(ctx)

	f := &forwarder{
		ctx:        ctx,
		ctxCancel:  cancelFn,
		clientConn: clientConn,
		serverConn: serverConn,
		errChan:    make(chan error, 1),
	}

	go func() {
		// Block until context is done.
		<-f.ctx.Done()

		// Close the forwarder to clean up. This goroutine is temporarily here
		// because the only way to unblock io.Copy is to close one of the ends,
		// which will be done through closing the forwarder. Once we replace
		// io.Copy with the interceptors, we could use f.ctx directly, and no
		// longer need this goroutine.
		//
		// Note that if f.Close was called externally, this will result
		// in two f.Close calls in total, i.e. one externally, and one here
		// once the context gets cancelled. This is fine for now since we'll
		// be removing this soon anyway.
		f.Close()
	}()

	// Copy all pgwire messages from frontend to backend connection until we
	// encounter an error or shutdown signal.
	go func() {
		defer f.Close()

		err := ConnectionCopy(f.serverConn, f.clientConn)
		select {
		case f.errChan <- err: /* error reported */
		default: /* the channel already contains an error */
		}
	}()

	return f
}

// Close closes the forwarder, and stops the forwarding process. This is
// idempotent.
func (f *forwarder) Close() {
	f.ctxCancel()

	// Since Close is idempotent, we'll ignore the error from Close in case it
	// has already been closed.
	f.serverConn.Close()
}

// IsStopped returns a boolean indicating that the forwarder has stopped
// forwarding messages. The forwarder will be stopped when one calls Close
// explicitly, or when any of its main goroutines is terminated, whichever that
// happens first.
//
// A new forwarder instance will have to be recreated if one wants to reuse the
// same pair of connections.
func (f *forwarder) IsStopped() bool {
	return f.ctx.Err() != nil
}
