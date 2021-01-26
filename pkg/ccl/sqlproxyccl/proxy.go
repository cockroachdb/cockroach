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
	"io"
	"net"
	"os"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgproto3/v2"
)

const pgAcceptSSLRequest = 'S'

// See https://www.postgresql.org/docs/9.1/protocol-message-formats.html.
var pgSSLRequest = []int32{8, 80877103}

// BackendConfig contains the configuration of a backend connection that is
// being proxied.
// To be removed once all clients are migrated to use backend dialer.
type BackendConfig struct {
	// The address to which the connection is forwarded.
	OutgoingAddress string
	// TLS settings to use when connecting to OutgoingAddress.
	TLSConf *tls.Config
	// Called after successfully connecting to OutgoingAddr.
	OnConnectionSuccess func()
	// KeepAliveLoop if provided controls the lifetime of the proxy connection.
	// It will be run in its own goroutine when the connection is successfully
	// opened. Returning from `KeepAliveLoop` will close the proxy connection.
	// Note that non-nil error return values will be forwarded to the user and
	// hence should explain the reason for terminating the connection.
	// Most common use of KeepAliveLoop will be as an infinite loop that
	// periodically checks if the connection should still be kept alive. Hence it
	// may block indefinitely so it's prudent to use the provided context and
	// return on context cancellation.
	// See `TestProxyKeepAlive` for example usage.
	KeepAliveLoop func(context.Context) error
}

// Options are the options to the Proxy method.
type Options struct {
	// Deprecated: construct FrontendAdmitter, passing this information in case
	// that SSL is desired.
	IncomingTLSConfig *tls.Config // config used for client -> proxy connection

	// BackendFromParams returns the config to use for the proxy -> backend
	// connection. The TLS config is in it and it must have an appropriate
	// ServerName for the remote backend.
	// Deprecated: processing of the params now happens in the BackendDialer.
	// This is only here to support OnSuccess and KeepAlive.
	BackendConfigFromParams func(
		params map[string]string, incomingConn *Conn,
	) (config *BackendConfig, clientErr error)

	// If set, consulted to modify the parameters set by the frontend before
	// forwarding them to the backend during startup.
	// Deprecated: include the code that modifies the request params
	// in the backend dialer.
	ModifyRequestParams func(map[string]string)

	// If set, consulted to decorate an error message to be sent to the client.
	// The error passed to this method will contain no internal information.
	OnSendErrToClient func(code ErrorCode, msg string) string

	// If set, will be called immediately after a new incoming connection
	// is accepted. It can optionally negotiate SSL, provide admittance control or
	// other types of frontend connection filtering.
	FrontendAdmitter func(incoming net.Conn) (net.Conn, *pgproto3.StartupMessage, error)

	// If set, will be used to establish and return connection to the backend.
	// If not set, the old logic will be used.
	// The argument is the startup message received from the frontend. It
	// contains the protocol version and params sent by the client.
	BackendDialer func(msg *pgproto3.StartupMessage) (net.Conn, error)
}

// Proxy takes an incoming client connection and relays it to a backend SQL
// server.
func (s *Server) Proxy(proxyConn *Conn) error {
	sendErrToClient := func(conn net.Conn, code ErrorCode, msg string) {
		if s.opts.OnSendErrToClient != nil {
			msg = s.opts.OnSendErrToClient(code, msg)
		}

		var pgCode string
		if code == CodeIdleDisconnect {
			pgCode = "57P01" // admin shutdown
		} else {
			pgCode = "08004" // rejected connection
		}
		_, _ = conn.Write((&pgproto3.ErrorResponse{
			Severity: "FATAL",
			Code:     pgCode,
			Message:  msg,
		}).Encode(nil))
	}

	frontendAdmitter := s.opts.FrontendAdmitter
	if frontendAdmitter == nil {
		// Keep this until all clients are switched to provide FrontendAdmitter
		// at what point we can also drop IncomingTLSConfig
		frontendAdmitter = func(incoming net.Conn) (net.Conn, *pgproto3.StartupMessage, error) {
			return FrontendAdmit(incoming, s.opts.IncomingTLSConfig)
		}
	}

	conn, msg, err := frontendAdmitter(proxyConn)
	if err != nil {
		var codeErr *CodeError
		if errors.As(err, &codeErr) && codeErr.code == CodeUnexpectedInsecureStartupMessage {
			sendErrToClient(
				proxyConn, // Do this on the TCP connection as it means denying SSL
				CodeUnexpectedInsecureStartupMessage,
				"server requires encryption",
			)
		}
		return err
	}

	// This currently only happens for CancelRequest type of startup messages
	// that we don't support
	if conn == nil {
		return nil

	}
	defer func() { _ = conn.Close() }()

	backendDialer := s.opts.BackendDialer
	var backendConfig *BackendConfig
	if s.opts.BackendConfigFromParams != nil {
		var clientErr error
		backendConfig, clientErr = s.opts.BackendConfigFromParams(msg.Parameters, proxyConn)
		if clientErr != nil {
			var codeErr *CodeError
			if !errors.As(clientErr, &codeErr) {
				codeErr = &CodeError{
					code: CodeParamsRoutingFailed,
					err:  errors.Errorf("rejected by BackendConfigFromParams: %v", clientErr),
				}
			}
			return codeErr
		}
	}
	if backendDialer == nil {
		// This we need to keep until all the clients are switched to provide BackendDialer.
		// It constructs a backend dialer from the information provided via
		// BackendConfigFromParams function.
		backendDialer = func(msg *pgproto3.StartupMessage) (net.Conn, error) {
			// We should be able to remove this when the all clients switch to
			// backend dialer.
			if s.opts.ModifyRequestParams != nil {
				s.opts.ModifyRequestParams(msg.Parameters)
			}

			crdbConn, err := BackendDial(msg, backendConfig.OutgoingAddress, backendConfig.TLSConf)
			if err != nil {
				return nil, err
			}

			return crdbConn, nil
		}
	}

	crdbConn, err := backendDialer(msg)
	if err != nil {
		s.metrics.BackendDownCount.Inc(1)
		var codeErr *CodeError
		if !errors.As(err, &codeErr) {
			codeErr = &CodeError{
				code: CodeBackendDown,
				err:  errors.Errorf("unable to reach backend SQL server: %v", err),
			}
		}
		if codeErr.code == CodeProxyRefusedConnection {
			s.metrics.RefusedConnCount.Inc(1)
		} else if codeErr.code == CodeParamsRoutingFailed {
			s.metrics.RoutingErrCount.Inc(1)
		}
		sendErrToClient(conn, codeErr.code, codeErr.Error())
		return codeErr
	}
	defer func() { _ = crdbConn.Close() }()

	if err := authenticate(conn, crdbConn); err != nil {
		s.metrics.AuthFailedCount.Inc(1)
		if codeErr := (*CodeError)(nil); errors.As(err, &codeErr) {
			sendErrToClient(conn, codeErr.code, codeErr.Error())
			return err
		}
		return errors.AssertionFailedf("unrecognized auth failure")
	}

	s.metrics.SuccessfulConnCount.Inc(1)

	// These channels are buffered because we'll only consume one of them.
	errOutgoing := make(chan error, 1)
	errIncoming := make(chan error, 1)
	errExpired := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if backendConfig != nil {
		if backendConfig.OnConnectionSuccess != nil {
			backendConfig.OnConnectionSuccess()
		}
		if backendConfig.KeepAliveLoop != nil {
			go func() {
				errExpired <- backendConfig.KeepAliveLoop(ctx)
			}()
		}
	}

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
		if err == nil {
			return nil
		} else if codeErr := (*CodeError)(nil); errors.As(err, &codeErr) &&
			codeErr.code == CodeExpiredClientConnection {
			s.metrics.ExpiredClientConnCount.Inc(1)
			sendErrToClient(conn, codeErr.code, codeErr.Error())
			return codeErr
		} else if errors.Is(err, os.ErrDeadlineExceeded) {
			s.metrics.IdleDisconnectCount.Inc(1)
			sendErrToClient(conn, CodeIdleDisconnect, "terminating connection due to idle timeout")
			return NewErrorf(CodeIdleDisconnect, "terminating connection due to idle timeout: %v", err)
		} else {
			s.metrics.BackendDisconnectCount.Inc(1)
			return NewErrorf(CodeBackendDisconnected, "copying from target server to client: %s", err)
		}
	case err := <-errOutgoing:
		// The incoming connection got closed.
		if err != nil {
			s.metrics.ClientDisconnectCount.Inc(1)
			return NewErrorf(CodeClientDisconnected, "copying from target server to client: %v", err)
		}
		return nil
	case err := <-errExpired:
		if err != nil {
			// The client connection expired.
			s.metrics.ExpiredClientConnCount.Inc(1)
			code := CodeExpiredClientConnection
			sendErrToClient(conn, code, err.Error())
			return NewErrorf(code, "expired client conn: %v", err)
		}
		return nil
	}
}
