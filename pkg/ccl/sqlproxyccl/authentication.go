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

	"github.com/jackc/pgproto3/v2"
)

// authenticate handles the startup of the pgwire protocol to the point where
// the connections is considered authenticated. If that doesn't happen, it
// returns an error.
var authenticate = func(clientConn, crdbConn net.Conn) error {
	fe := pgproto3.NewBackend(pgproto3.NewChunkReader(clientConn), clientConn)
	be := pgproto3.NewFrontend(pgproto3.NewChunkReader(crdbConn), crdbConn)

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

		err = fe.Send(backendMsg)
		if err != nil {
			return newErrorf(
				codeClientWriteFailed, "unable to send message %v to client: %v", backendMsg, err,
			)
		}

		// Decide what to do based on the type of the server response.
		switch tp := backendMsg.(type) {
		case *pgproto3.ReadyForQuery:
			// Server has authenticated the connection successfully and is ready to
			// serve queries.
			return nil
		case *pgproto3.AuthenticationOk:
			// Server has authenticated the connection; keep reading messages until
			// `pgproto3.ReadyForQuery` is encountered which signifies that server
			// is ready to serve queries.
		case *pgproto3.ParameterStatus:
			// Server sent status message; keep reading messages until
			// `pgproto3.ReadyForQuery` is encountered.
		case *pgproto3.BackendKeyData:
		// Server sent backend key data; keep reading messages until
		// `pgproto3.ReadyForQuery` is encountered.
		case *pgproto3.ErrorResponse:
			// Server has rejected the authentication response from the client and
			// has closed the connection.
			return newErrorf(codeAuthFailed, "authentication failed: %s", tp.Message)
		case
			*pgproto3.AuthenticationCleartextPassword,
			*pgproto3.AuthenticationMD5Password,
			*pgproto3.AuthenticationSASL:
			// The backend is requesting the user to authenticate.
			// Read the client response and forward it to server.
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
		default:
			return newErrorf(codeBackendDisconnected, "received unexpected backend message type: %v", tp)
		}
	}
	return newErrorf(codeBackendDisconnected, "authentication took more than %d iterations", i)
}
