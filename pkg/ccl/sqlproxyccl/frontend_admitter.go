// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlproxyccl

import (
	"crypto/tls"
	"io"
	"net"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgproto3/v2"
)

// FrontendAdmitInfo contains the result of FrontendAdmit call. Fields are
// exported because FrontendAdmit is used by CockroachCloud.
type FrontendAdmitInfo struct {
	// Conn represents a handle to the incoming connection. This will never be
	// nil even in the case of an error.
	Conn net.Conn
	// Msg corresponds to the startup message received from the client.
	Msg *pgproto3.StartupMessage
	// Err represents errors from the FrontendAdmit call.
	Err error
	// SniServerName, if present, would be the SNI server name received from the
	// client.
	SniServerName string
	// CancelRequest corresponds to a cancel request received from the client.
	CancelRequest *proxyCancelRequest
}

// noStartupMessage is an error used to indicate that there were no data packets
// read at all when trying to read a startup message from the connection.
var noStartupMessage = errors.New("no startup message")

// FrontendAdmit is the default implementation of a frontend admitter. It can
// upgrade to an optional SSL connection, and will handle and verify the startup
// message received from the PG SQL client. The connection returned should never
// be nil in case of error. Depending on whether the error happened before the
// connection was upgraded to TLS or not it will either be the original or the
// TLS connection.
var FrontendAdmit = func(
	conn net.Conn, incomingTLSConfig *tls.Config,
) *FrontendAdmitInfo {
	// `conn` could be replaced by `conn` embedded in a `tls.Conn` connection,
	// hence it's important to close `conn` rather than `proxyConn` since closing
	// the latter will not call `Close` method of `tls.Conn`.

	// Read first message from client.
	m, err := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn).ReceiveStartupMessage()
	if err != nil {
		var startupErr error
		// ReceiveStartupMessage returns io.EOF if the first four bytes cannot
		// be read at all. All other read errors will be converted to
		// io.ErrUnexpectedEOF.
		//
		// The io.EOF case usually happens with TCP probes (i.e. an opened
		// connection without any bytes). For this special case, we will return
		// noStartupMessage.
		//
		// See: https://github.com/jackc/pgproto3/blob/0c0f7b03fb4967dfff8de06d07a9fe20baf83449/backend.go#L60-L63
		if errors.Is(err, io.EOF) {
			startupErr = noStartupMessage
		} else {
			startupErr = withCode(
				errors.Wrap(err, "while receiving startup message"), codeClientReadFailed,
			)
		}
		return &FrontendAdmitInfo{Conn: conn, Err: startupErr}
	}

	// CancelRequest is unencrypted and unauthenticated, regardless of whether
	// the server requires TLS connections.
	if c, ok := m.(*pgproto3.CancelRequest); ok {
		// Craft a proxyCancelRequest in case we need to forward the request.
		cr := &proxyCancelRequest{
			ProxyIP:   decodeIP(c.ProcessID),
			SecretKey: c.SecretKey,
			ClientIP:  conn.RemoteAddr().(*net.TCPAddr).IP,
		}
		return &FrontendAdmitInfo{
			Conn:          conn,
			CancelRequest: cr,
		}
	}

	var sniServerName string

	// If we have an incoming TLS Config, require that the client initiates with
	// an SSLRequest message.
	if incomingTLSConfig != nil {
		if _, ok := m.(*pgproto3.SSLRequest); !ok {
			code := codeUnexpectedInsecureStartupMessage
			return &FrontendAdmitInfo{Conn: conn, Err: withCode(
				errors.Newf("unsupported startup message: %T", m), code)}
		}

		_, err = conn.Write([]byte{pgAcceptSSLRequest})
		if err != nil {
			return &FrontendAdmitInfo{Conn: conn, Err: withCode(
				errors.Wrap(err, "acking SSLRequest"), codeClientWriteFailed)}
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
			return &FrontendAdmitInfo{
				Conn: conn,
				Err: withCode(errors.Wrap(err,
					"receiving post-TLS startup message"), codeClientReadFailed),
			}
		}
	}

	// CancelRequest is unencrypted and unauthenticated, regardless of whether
	// the server requires TLS connections.
	// The PostgreSQL protocol definition says that cancel payloads
	// must be sent *prior to upgrading the connection to use TLS*.
	// Yet, we've found clients in the wild that send the cancel
	// after the TLS handshake, for example at
	// https://github.com/cockroachlabs/support/issues/600.
	if c, ok := m.(*pgproto3.CancelRequest); ok {
		// Craft a proxyCancelRequest in case we need to forward the request.
		cr := &proxyCancelRequest{
			ProxyIP:   decodeIP(c.ProcessID),
			SecretKey: c.SecretKey,
			ClientIP:  conn.RemoteAddr().(*net.TCPAddr).IP,
		}
		return &FrontendAdmitInfo{
			Conn:          conn,
			CancelRequest: cr,
		}
	}

	if startup, ok := m.(*pgproto3.StartupMessage); ok {
		// This forwards the remote addr to the backend.
		startup.Parameters[remoteAddrStartupParam] = conn.RemoteAddr().String()
		// The client is blocked from using session revival tokens; only the proxy
		// itself can.
		if _, ok := startup.Parameters[sessionRevivalTokenStartupParam]; ok {
			return &FrontendAdmitInfo{
				Conn: conn,
				Err: withCode(errors.Newf(
					"parameter %s is not allowed",
					sessionRevivalTokenStartupParam),
					codeUnexpectedStartupMessage),
			}
		}
		return &FrontendAdmitInfo{Conn: conn, Msg: startup, SniServerName: sniServerName}
	}

	code := codeUnexpectedStartupMessage
	return &FrontendAdmitInfo{
		Conn: conn,
		Err: withCode(errors.Newf(
			"unsupported post-TLS startup message: %T", m), code),
	}
}
