// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"net"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/throttler"
	"github.com/jackc/pgproto3/v2"
)

// authenticate handles the startup of the pgwire protocol to the point where
// the connections is considered authenticated. If that doesn't happen, it
// returns an error.
var authenticate = func(clientConn, crdbConn net.Conn, throttleHook func(throttler.AttemptStatus) *pgproto3.ErrorResponse) error {
	fe := pgproto3.NewBackend(pgproto3.NewChunkReader(clientConn), clientConn)
	be := pgproto3.NewFrontend(pgproto3.NewChunkReader(crdbConn), crdbConn)

	feSend := func(msg pgproto3.BackendMessage) error {
		err := fe.Send(msg)
		if err != nil {
			return newErrorf(codeClientWriteFailed, "unable to send message %v to client: %v", msg, err)
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
			return newErrorf(codeBackendReadFailed, "unable to receive message from backend: %v", err)
		}

		// The cases in this switch are roughly sorted in the order the server will send them.
		switch tp := backendMsg.(type) {

		// The backend is requesting the user to authenticate.
		// Read the client response and forward it to server.
		case
			*pgproto3.AuthenticationCleartextPassword,
			*pgproto3.AuthenticationMD5Password,
			*pgproto3.AuthenticationSASL:
			if err = feSend(backendMsg); err != nil {
				return err
			}
			fntMsg, err := fe.Receive()
			if err != nil {
				return newErrorf(codeClientReadFailed, "unable to receive message from client: %v", err)
			}
			err = be.Send(fntMsg)
			if err != nil {
				return newErrorf(
					codeBackendWriteFailed, "unable to send message %v to backend: %v", fntMsg, err,
				)
			}

		// Server has authenticated the connection; keep reading messages until
		// `pgproto3.ReadyForQuery` is encountered which signifies that server
		// is ready to serve queries.
		case *pgproto3.AuthenticationOk:
			throttleError := throttleHook(throttler.AttemptOK)
			if throttleError != nil {
				if err = feSend(throttleError); err != nil {
					return err
				}
				return newErrorf(codeProxyRefusedConnection, "connection attempt throttled")
			}
			if err = feSend(backendMsg); err != nil {
				return err
			}

		// Server has rejected the authentication response from the client and
		// has closed the connection.
		case *pgproto3.ErrorResponse:
			throttleError := throttleHook(throttler.AttemptInvalidCredentials)
			if throttleError != nil {
				if err = feSend(throttleError); err != nil {
					return err
				}
				return newErrorf(codeProxyRefusedConnection, "connection attempt throttled")
			}
			if err = feSend(backendMsg); err != nil {
				return err
			}
			return newErrorf(codeAuthFailed, "authentication failed: %s", tp.Message)

		// Information provided by the server to the client before the connection is ready
		// to accept queries. These are typically returned after AuthenticationOk and before
		// ReadyForQuery.
		case *pgproto3.ParameterStatus, *pgproto3.BackendKeyData:
			if err = feSend(backendMsg); err != nil {
				return err
			}

		// Server has authenticated the connection successfully and is ready to
		// serve queries.
		case *pgproto3.ReadyForQuery:
			if err = feSend(backendMsg); err != nil {
				return err
			}
			return nil

		default:
			return newErrorf(codeBackendDisconnected, "received unexpected backend message type: %v", tp)
		}
	}
	return newErrorf(codeBackendDisconnected, "authentication took more than %d iterations", i)
}
