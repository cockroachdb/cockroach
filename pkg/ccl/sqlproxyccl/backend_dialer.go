// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

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
			codeBackendDown)
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
				return withCode(
					errors.Wrap(err, "sending SSLRequest to target server"),
					codeBackendDown)
			}
			response := make([]byte, 1)
			if _, err = io.ReadFull(conn, response); err != nil {
				return withCode(
					errors.New("reading response to SSLRequest"),
					codeBackendDown)
			}
			if response[0] != pgAcceptSSLRequest {
				return withCode(
					errors.New("target server refused TLS connection"),
					codeBackendRefusedTLS)
			}
			conn = tls.Client(conn, tlsConfig.Clone())
		}

		// Forward startup message to the backend connection.
		if _, err := conn.Write(msg.Encode(nil)); err != nil {
			return withCode(
				errors.Wrapf(err, "relaying StartupMessage to target server %v", serverAddress),
				codeBackendDown)
		}

		return nil
	}()
	if ctx.Err() != nil {
		// If the context is cancelled, overwrite the error because closing the
		// connection caused the connection to fail at an arbitrary step.
		err = withCode(
			errors.Wrapf(ctx.Err(), "unable to negotiate connection with %s", serverAddress),
			codeBackendDown,
		)
	}
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	return conn, nil
}

// closeWhenCancelled will close the connection if the context is cancelled
// before the cleanup function is called.
func closeWhenCancelled(ctx context.Context, conn net.Conn) (cleanup func()) {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-done:
			// Do nothing because the cleanup function was called.
		}
	}()
	return func() { close(done) }
}
