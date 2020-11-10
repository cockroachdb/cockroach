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
	"encoding/binary"
	"io"
	"net"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgproto3/v2"
)

const pgAcceptSSLRequest = 'S'

// See https://www.postgresql.org/docs/9.1/protocol-message-formats.html.
var pgSSLRequest = []int32{8, 80877103}

// BackendConfig contains the configuration of a backend connection that is
// being proxied.
type BackendConfig struct {
	// The address to which the connection is forwarded.
	OutgoingAddress string
	// TLS settings to use when connecting to OutgoingAddress.
	TLSConf *tls.Config
	// Called after successfully connecting to OutgoingAddr.
	OnConnectionSuccess func()
}

// Options are the options to the Proxy method.
type Options struct {
	IncomingTLSConfig *tls.Config // config used for client -> proxy connection

	// TODO(tbg): this is unimplemented and exists only to check which clients
	// allow use of SNI. Should always return ("", nil).
	BackendConfigFromSNI func(serverName string) (config *BackendConfig, clientErr error)
	// BackendFromParams returns the config to use for the proxy -> backend
	// connection. The TLS config is in it and it must have an appropriate
	// ServerName for the remote backend.
	BackendConfigFromParams func(
		params map[string]string, ipAddress string,
	) (config *BackendConfig, clientErr error)

	// If set, consulted to modify the parameters set by the frontend before
	// forwarding them to the backend during startup.
	ModifyRequestParams func(map[string]string)

	// If set, consulted to decorate an error message to be sent to the client.
	// The error passed to this method will contain no internal information.
	OnSendErrToClient func(code ErrorCode, msg string) string
}

// Proxy takes an incoming client connection and relays it to a backend SQL
// server.
func (s *Server) Proxy(conn net.Conn) error {
	sendErrToClient := func(conn net.Conn, code ErrorCode, msg string) {
		if s.opts.OnSendErrToClient != nil {
			msg = s.opts.OnSendErrToClient(code, msg)
		}
		_, _ = conn.Write((&pgproto3.ErrorResponse{
			Severity: "FATAL",
			Code:     "08004", // rejected connection
			Message:  msg,
		}).Encode(nil))
	}

	{
		m, err := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn).ReceiveStartupMessage()
		if err != nil {
			return NewErrorf(CodeClientReadFailed, "while receiving startup message")
		}
		switch m.(type) {
		case *pgproto3.SSLRequest:
		case *pgproto3.CancelRequest:
			// Ignore CancelRequest explicitly. We don't need to do this but it makes
			// testing easier by avoiding a call to sendErrToClient on this path
			// (which would confuse assertCtx).
			return nil
		default:
			code := CodeUnexpectedInsecureStartupMessage
			sendErrToClient(conn, code, "server requires encryption")
			return NewErrorf(code, "unsupported startup message: %T", m)
		}

		_, err = conn.Write([]byte{pgAcceptSSLRequest})
		if err != nil {
			return NewErrorf(CodeClientWriteFailed, "acking SSLRequest: %v", err)
		}

		cfg := s.opts.IncomingTLSConfig.Clone()
		var sniServerName string
		cfg.GetConfigForClient = func(h *tls.ClientHelloInfo) (*tls.Config, error) {
			sniServerName = h.ServerName
			return nil, nil
		}
		if s.opts.BackendConfigFromSNI != nil {
			cfg, clientErr := s.opts.BackendConfigFromSNI(sniServerName)
			if clientErr != nil {
				code := CodeSNIRoutingFailed
				sendErrToClient(conn, code, clientErr.Error()) // won't actually be shown by most clients
				return NewErrorf(code, "rejected by OutgoingAddrFromSNI")
			}
			if cfg.OutgoingAddress != "" {
				return NewErrorf(CodeSNIRoutingFailed, "BackendConfigFromSNI is unimplemented")
			}
		}
		conn = tls.Server(conn, cfg)
	}

	m, err := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn).ReceiveStartupMessage()
	if err != nil {
		return NewErrorf(CodeClientReadFailed, "receiving post-TLS startup message: %v", err)
	}
	msg, ok := m.(*pgproto3.StartupMessage)
	if !ok {
		return NewErrorf(CodeUnexpectedStartupMessage, "unsupported post-TLS startup message: %T", m)
	}

	var backendConfig *BackendConfig
	{
		ip, _, err := net.SplitHostPort(conn.RemoteAddr().String())
		if err != nil {
			return NewErrorf(
				CodeParamsRoutingFailed, "could not parse address %s: %v",
				conn.RemoteAddr().String(), err)
		}
		var clientErr error
		backendConfig, clientErr = s.opts.BackendConfigFromParams(msg.Parameters, ip)
		if clientErr != nil {
			var codeErr *codeError
			if !errors.As(clientErr, &codeErr) {
				codeErr = &codeError{
					code: CodeParamsRoutingFailed,
					err:  errors.Errorf("rejected by BackendConfigFromParams: %v", clientErr),
				}
			}
			if codeErr.code == CodeProxyRefusedConnection {
				s.metrics.RefusedConnCount.Inc(1)
			} else {
				s.metrics.RoutingErrCount.Inc(1)
			}
			sendErrToClient(conn, codeErr.code, clientErr.Error())
			return codeErr
		}
	}

	crdbConn, err := net.Dial("tcp", backendConfig.OutgoingAddress)
	if err != nil {
		s.metrics.BackendDownCount.Inc(1)
		code := CodeBackendDown
		sendErrToClient(conn, code, "unable to reach backend SQL server")
		return NewErrorf(code, "dialing backend server: %v", err)
	}

	// Send SSLRequest.
	if err := binary.Write(crdbConn, binary.BigEndian, pgSSLRequest); err != nil {
		s.metrics.BackendDownCount.Inc(1)
		return NewErrorf(CodeBackendDown, "sending SSLRequest to target server: %v", err)
	}

	response := make([]byte, 1)
	if _, err = io.ReadFull(crdbConn, response); err != nil {
		s.metrics.BackendDownCount.Inc(1)
		return NewErrorf(CodeBackendDown, "reading response to SSLRequest")
	}

	if response[0] != pgAcceptSSLRequest {
		s.metrics.BackendDownCount.Inc(1)
		return NewErrorf(CodeBackendRefusedTLS, "target server refused TLS connection")
	}

	outCfg := backendConfig.TLSConf.Clone()
	crdbConn = tls.Client(crdbConn, outCfg)

	if s.opts.ModifyRequestParams != nil {
		s.opts.ModifyRequestParams(msg.Parameters)
	}

	if _, err := crdbConn.Write(msg.Encode(nil)); err != nil {
		s.metrics.BackendDownCount.Inc(1)
		return NewErrorf(CodeBackendDown, "relaying StartupMessage to target server %v: %v",
			backendConfig.OutgoingAddress, err)
	}

	if backendConfig.OnConnectionSuccess != nil {
		backendConfig.OnConnectionSuccess()
	}

	// These channels are buffered because we'll only consume one of them.
	errOutgoing := make(chan error, 1)
	errIncoming := make(chan error, 1)

	go func() {
		_, err := io.Copy(crdbConn, conn)
		errOutgoing <- err
	}()
	go func() {
		_, err := io.Copy(conn, crdbConn)
		errIncoming <- err
	}()

	select {
	// NB: when using pgx, we see a nil errIncoming first on clean connection
	// termination. Using psql I see a nil errOutgoing first. I think the PG
	// protocol stipulates sending a message to the server at which point
	// the server closes the connection (errIncoming), but presumably the
	// client gets to close the connection once it's sent that message,
	// meaning either case is possible.
	case err := <-errIncoming:
		if err != nil {
			s.metrics.BackendDisconnectCount.Inc(1)
			return NewErrorf(CodeBackendDisconnected, "copying from target server to client: %s", err)
		}
		return nil
	case err := <-errOutgoing:
		// The incoming connection got closed.
		if err != nil {
			s.metrics.ClientDisconnectCount.Inc(1)
			return NewErrorf(CodeClientDisconnected, "copying from target server to client: %v", err)
		}
		return nil
	}
}
