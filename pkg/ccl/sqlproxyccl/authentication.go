// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlproxyccl

import (
	"net"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/interceptor"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/throttler"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/errors"
	pgproto3 "github.com/jackc/pgproto3/v2"
)

// These errors occur after successful auth but are sent back as a result of
// the auth request. Receiving such error shouldn't trigger throttle.
var pgPostAuthErrorCodes = []string{
	pgcode.TooManyConnections.String(),
}

// authenticate handles the startup of the pgwire protocol to the point where
// the connections is considered authenticated. If that doesn't happen, it
// returns an error.
var authenticate = func(
	clientConn, crdbConn net.Conn,
	proxyBackendKeyData *pgproto3.BackendKeyData,
	throttleHook func(throttler.AttemptStatus) error,
) (crdbBackendKeyData *pgproto3.BackendKeyData, _ error) {
	fe := pgproto3.NewBackend(pgproto3.NewChunkReader(clientConn), clientConn)
	be := pgproto3.NewFrontend(pgproto3.NewChunkReader(crdbConn), crdbConn)

	feSend := func(msg pgproto3.BackendMessage) error {
		err := fe.Send(msg)
		if err != nil {
			return withCode(
				errors.Wrapf(err, "unable to send message %v to client", msg),
				codeClientWriteFailed)
		}
		return nil
	}

	// The auth step should require only a few back and forths so 20 iterations
	// should be enough.
	var i int
	for ; i < 20; i++ {
		// Read the server response and forward it to the client.
		// TODO(spaskob): in verbose mode, log these messages.
		backendMsg, err := be.Receive()
		if err != nil {
			return nil, withCode(
				errors.Wrap(err, "unable to receive message from backend"),
				codeBackendReadFailed)
		}

		// The cases in this switch are roughly sorted in the order the server will send them.
		switch tp := backendMsg.(type) {

		// The backend is requesting the user to authenticate.
		// Read the client response and forward it to server.
		case
			*pgproto3.AuthenticationCleartextPassword,
			*pgproto3.AuthenticationMD5Password,
			*pgproto3.AuthenticationSASLContinue,
			*pgproto3.AuthenticationSASLFinal,
			*pgproto3.AuthenticationSASL:
			if err = feSend(backendMsg); err != nil {
				return nil, err
			}
			switch backendMsg.(type) {
			case *pgproto3.AuthenticationCleartextPassword:
				_ = fe.SetAuthType(pgproto3.AuthTypeCleartextPassword)
			case *pgproto3.AuthenticationMD5Password:
				_ = fe.SetAuthType(pgproto3.AuthTypeMD5Password)
			case *pgproto3.AuthenticationSASLContinue:
				_ = fe.SetAuthType(pgproto3.AuthTypeSASLContinue)
			case *pgproto3.AuthenticationSASL:
				_ = fe.SetAuthType(pgproto3.AuthTypeSASL)
			case *pgproto3.AuthenticationSASLFinal:
				// Final SCRAM message. Nothing more to expect from the
				// client: the next message will be from the server and be
				// AuthenticationOk or AuthenticationFail.
				continue
			}
			fntMsg, err := fe.Receive()
			if err != nil {
				return nil, withCode(
					errors.Wrap(err, "unable to receive message from client"),
					codeClientReadFailed)
			}
			err = be.Send(fntMsg)
			if err != nil {
				return nil, withCode(
					errors.Wrapf(err, "unable to send message %v to backend", fntMsg),
					codeBackendWriteFailed)
			}

		// Server has authenticated the connection; keep reading messages until
		// `pgproto3.ReadyForQuery` is encountered which signifies that server
		// is ready to serve queries.
		case *pgproto3.AuthenticationOk:
			throttleError := throttleHook(throttler.AttemptOK)
			if throttleError != nil {
				// Send a user-facing error.
				if err = feSend(toPgError(authThrottledError)); err != nil {
					return nil, err
				}
				return nil, throttleError
			}
			if err = feSend(backendMsg); err != nil {
				return nil, err
			}

		// Server has rejected the authentication response from the client and
		// has closed the connection.
		case *pgproto3.ErrorResponse:
			// The error may be in response of auth message but may not indicate
			// unsuccessful authentication. Clear throttle if this is the case.
			var throttleError error
			if slices.Contains(pgPostAuthErrorCodes, tp.Code) {
				throttleError = throttleHook(throttler.AttemptOK)
			} else {
				throttleError = throttleHook(throttler.AttemptInvalidCredentials)
			}
			if throttleError != nil {
				// Send a user-facing error.
				if err = feSend(toPgError(authThrottledError)); err != nil {
					return nil, err
				}
				return nil, throttleError
			}

			if err = feSend(backendMsg); err != nil {
				return nil, err
			}
			return nil, withCode(
				errors.Newf("authentication failed: %s", tp.Message),
				codeAuthFailed)

		// Information provided by the server to the client before the connection is ready
		// to accept queries. These are typically returned after AuthenticationOk and before
		// ReadyForQuery.
		case *pgproto3.ParameterStatus:
			if err = feSend(backendMsg); err != nil {
				return nil, err
			}

		// BackendKeyData is part of the Postgres query cancellation protocol.
		// sqlproxy saves it and returns a different one to the client.
		case *pgproto3.BackendKeyData:
			crdbBackendKeyData = tp
			if err = feSend(proxyBackendKeyData); err != nil {
				return nil, err
			}

		// Server has authenticated the connection successfully and is ready to
		// serve queries.
		case *pgproto3.ReadyForQuery:
			if err = feSend(backendMsg); err != nil {
				return nil, err
			}
			return crdbBackendKeyData, nil

		default:
			return nil, withCode(
				errors.Newf("received unexpected backend message type: %v", tp),
				codeBackendDisconnected)
		}
	}
	return nil, withCode(
		errors.Newf("authentication took more than %d iterations", i),
		codeBackendDisconnected)
}

// readTokenAuthResult reads the result for the token-based authentication, and
// assumes that the connection credentials have already been transmitted to the
// server (as part of the startup message). If authentication fails, this will
// return an error.
//
// NOTE: For now, this also reads the initial connection data
// (i.e. ParameterStatus and BackendKeyData) until we see a ReadyForQuery
// message. All messages will be discarded, and this is fine because we only
// call this during connection migration, and the proxy is the client. Once we
// address the TODO below, we could generalize it such that the client here is
// a no-op client.
//
// TODO(jaylim-crl): We should extract out the initial connection data stuff
// into a readInitialConnData function that `authenticate` can also use. It was
// a mistake to split reader (interceptor) and writer (net.Conn), and I think
// we should merge them back in the future. Instead of having the writer as the
// other end, the writer should be the same connection. That way, a
// sqlproxyccl.Conn can be used to read-from, or write-to the same component.
var readTokenAuthResult = func(conn net.Conn) (*pgproto3.BackendKeyData, error) {
	// This interceptor is discarded once this function returns. Just like
	// pgproto3.NewFrontend, this serverConn object has an internal buffer.
	// Discarding the buffer is fine since there won't be any other messages
	// from the server once we receive the ReadyForQuery message because the
	// caller (i.e. proxy) does not forward client messages until then.
	serverConn := interceptor.NewFrontendConn(conn)

	var backendKeyData *pgproto3.BackendKeyData
	// The auth step should require only a few back and forths so 20 iterations
	// should be enough.
	var i int
	for ; i < 20; i++ {
		backendMsg, err := serverConn.ReadMsg()
		if err != nil {
			return nil, withCode(
				errors.Wrap(err, "unable to receive message from backend"),
				codeBackendReadFailed)
		}

		switch tp := backendMsg.(type) {
		case *pgproto3.AuthenticationOk, *pgproto3.ParameterStatus:
		// Do nothing.
		case *pgproto3.BackendKeyData:
			backendKeyData = tp

		case *pgproto3.ErrorResponse:
			return nil, withCode(
				errors.Newf("authentication failed: %s", tp.Message),
				codeAuthFailed)

		case *pgproto3.ReadyForQuery:
			return backendKeyData, nil

		default:
			return nil, withCode(
				errors.Newf("received unexpected backend message type: %v", tp),
				codeBackendDisconnected)
		}
	}

	return nil, withCode(
		errors.Newf("authentication took more than %d iterations", i),
		codeBackendDisconnected)
}
