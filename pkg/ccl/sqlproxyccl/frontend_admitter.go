// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"crypto/tls"
	"net"

	"github.com/jackc/pgproto3/v2"
)

// FrontendAdmit is the default implementation of a frontend admitter. It can
// upgrade to an optional SSL connection, and will handle and verify the startup
// message received from the PG SQL client. The connection returned should never
// be nil in case of error. Depending on whether the error happened before the
// connection was upgraded to TLS or not it will either be the original or the
// TLS connection.
var FrontendAdmit = func(
	conn net.Conn, incomingTLSConfig *tls.Config,
) (net.Conn, *pgproto3.StartupMessage, error) {
	// `conn` could be replaced by `conn` embedded in a `tls.Conn` connection,
	// hence it's important to close `conn` rather than `proxyConn` since closing
	// the latter will not call `Close` method of `tls.Conn`.
	var sniServerName string

	// Read first message from client.
	m, err := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn).ReceiveStartupMessage()
	if err != nil {
		return conn, nil, newErrorf(codeClientReadFailed, "while receiving startup message")
	}

	// CancelRequest is unencrypted and unauthenticated, regardless of whether
	// the server requires TLS connections. For now, ignore the request to cancel,
	// and send back a nil StartupMessage, which will cause the proxy to just
	// close the connection in response.
	if _, ok := m.(*pgproto3.CancelRequest); ok {
		return conn, nil, nil
	}

	// If we have an incoming TLS Config, require that the client initiates with
	// an SSLRequest message.
	if incomingTLSConfig != nil {
		if _, ok := m.(*pgproto3.SSLRequest); !ok {
			code := codeUnexpectedInsecureStartupMessage
			return conn, nil, newErrorf(code, "unsupported startup message: %T", m)
		}

		_, err = conn.Write([]byte{pgAcceptSSLRequest})
		if err != nil {
			return conn, nil, newErrorf(codeClientWriteFailed, "acking SSLRequest: %v", err)
		}

		cfg := incomingTLSConfig.Clone()

		cfg.GetConfigForClient = func(h *tls.ClientHelloInfo) (*tls.Config, error) {
			sniServerName = h.ServerName
			return nil, nil
		}
		conn = tls.Server(conn, cfg)

		// Now that SSL is established, read the encrypted startup message.
		m, err = pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn).ReceiveStartupMessage()
		if err != nil {
			return conn, nil, newErrorf(codeClientReadFailed, "receiving post-TLS startup message: %v", err)
		}
	}

	if startup, ok := m.(*pgproto3.StartupMessage); ok {
		// Add the sniServerName (if used) as parameter
		if sniServerName != "" {
			startup.Parameters["sni-server"] = sniServerName
		}
		// This forwards the remote addr to the backend.
		startup.Parameters[remoteAddrStartupParam] = conn.RemoteAddr().String()
		// The client is blocked from using session revival tokens; only the proxy
		// itself can.
		if _, ok := startup.Parameters[sessionRevivalTokenStartupParam]; ok {
			return conn, nil, newErrorf(
				codeUnexpectedStartupMessage,
				"parameter %s is not allowed",
				sessionRevivalTokenStartupParam,
			)
		}
		return conn, startup, nil
	}

	code := codeUnexpectedStartupMessage
	return conn, nil, newErrorf(code, "unsupported post-TLS startup message: %T", m)
}
