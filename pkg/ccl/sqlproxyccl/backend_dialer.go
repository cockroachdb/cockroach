// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlproxyccl

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgproto3/v2"
)

// BackendDial is an example backend dialer that does a TCP/IP connection
// to a backend, SSL and forwards the start message. It is defined as a variable
// so it can be redirected for testing.
// TODO(jaylim-crl): Move dialer into connector in the future.
var BackendDial = func(
	ctx context.Context, msg *pgproto3.StartupMessage, serverAddress string, tlsConfig *tls.Config,
) (net.Conn, error) {
	var d net.Dialer

	conn, err := d.DialContext(ctx, "tcp", serverAddress)
	if err != nil {
		return nil, withCode(
			errors.Wrap(err, "unable to reach backend SQL server"),
			codeBackendDialFailed)
	}

	// Try to upgrade the PG connection to use SSL.
	err = func() error {
		// If the context is cancelled during the negotiation process, close the
		// connection. Closing the connection unblocks active reads or writes on
		// the connection.
		removeCancelHook := closeWhenCancelled(ctx, conn)
		defer removeCancelHook()

		if tlsConfig != nil {
			// Send SSLRequest.
			if err := binary.Write(conn, binary.BigEndian, pgSSLRequest); err != nil {
				return errors.Wrap(err, "sending SSLRequest to target server")
			}
			response := make([]byte, 1)
			if _, err = io.ReadFull(conn, response); err != nil {
				return errors.New("reading response to SSLRequest")
			}
			if response[0] != pgAcceptSSLRequest {
				return errors.New("target server refused TLS connection")
			}
			conn = tls.Client(conn, tlsConfig.Clone())
		}

		// Forward startup message to the backend connection.
		buf, err := msg.Encode(nil)
		if err != nil {
			return errors.Wrapf(err, "encoding StartingMessage for target server %v", serverAddress)
		}
		if _, err := conn.Write(buf); err != nil {
			return errors.Wrapf(err, "relaying StartupMessage to target server %v", serverAddress)
		}
		return nil
	}()
	if ctx.Err() != nil {
		// If the context is cancelled, overwrite the error because closing the
		// connection caused the connection to fail at an arbitrary step.
		err = errors.Wrapf(ctx.Err(), "unable to negotiate connection with %s", serverAddress)
	}
	if err != nil {
		_ = conn.Close()
		return nil, withCode(err, codeBackendDialFailed)
	}

	return conn, nil
}

// closeWhenCancelled will close the connection if the context is cancelled
// before the cleanup function is called.
func closeWhenCancelled(ctx context.Context, conn net.Conn) (cleanup func()) {
	// TODO(jeffswenson): when we upgrade to go 1.21 replace the implementation
	// of closeWhenCancelled with context.AfterFunc.
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
			// Do nothing because the cleanup function was called.
		case <-ctx.Done():
			conn.Close()
		}
	}()
	return func() {
		select {
		case done <- struct{}{}:
			// Send a done signal to the closing goroutine. Unbuffered channels
			// guarantee synchronous delivery. We can't use close(done) because the
			// close signal is asynchronous and a cancel that arrives after the
			// cleanup function returns may be processed by the closing goroutine.
		case <-ctx.Done():
			// Do nothing if the context is cancelled, since the go routine will
			// process the close.
		}
	}
}
