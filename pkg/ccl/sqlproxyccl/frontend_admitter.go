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
	// If we have an incoming TLS Config, require that the client initiates
	// with a TLS connection.
	if incomingTLSConfig != nil {
		m, err := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn).ReceiveStartupMessage()
		if err != nil {
			return conn, nil, newErrorf(codeClientReadFailed, "while receiving startup message")
		}
		switch m.(type) {
		case *pgproto3.SSLRequest:
		case *pgproto3.CancelRequest:
			// Ignore CancelRequest explicitly. We don't need to do this but it
			// makes testing easier by avoiding a call to SendErrToClient on this
			// path (which would confuse assertCtx).
			return conn, nil, nil
		default:
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
	}

	m, err := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn).ReceiveStartupMessage()
	if err != nil {
		return conn, nil, newErrorf(codeClientReadFailed, "receiving post-TLS startup message: %v", err)
	}
	msg, ok := m.(*pgproto3.StartupMessage)
	if !ok {
		return conn, nil, newErrorf(codeUnexpectedStartupMessage, "unsupported post-TLS startup message: %T", m)
	}

	// Add the sniServerName (if used) as parameter
	if sniServerName != "" {
		msg.Parameters["sni-server"] = sniServerName
	}

	return conn, msg, nil
}
