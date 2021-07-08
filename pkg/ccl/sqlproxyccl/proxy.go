// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"io"
	"net"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgproto3/v2"
)

const pgAcceptSSLRequest = 'S'

// See https://www.postgresql.org/docs/9.1/protocol-message-formats.html.
var pgSSLRequest = []int32{8, 80877103}

// sendErrToClientAndUpdateMetrics simply combines the update of the metrics and
// the transmission of the err back to the client.
func updateMetricsAndSendErrToClient(err error, conn net.Conn, metrics *metrics) {
	metrics.updateForError(err)
	SendErrToClient(conn, err)
}

// SendErrToClient will encode and pass back to the SQL client an error message.
// It can be called by the implementors of proxyHandler to give more
// information to the end user in case of a problem.
var SendErrToClient = func(conn net.Conn, err error) {
	if err == nil || conn == nil {
		return
	}
	codeErr := (*codeError)(nil)
	if errors.As(err, &codeErr) {
		var msg string
		switch codeErr.code {
		// These are send as is.
		case codeExpiredClientConnection,
			codeBackendDown,
			codeParamsRoutingFailed,
			codeClientDisconnected,
			codeBackendDisconnected,
			codeAuthFailed,
			codeProxyRefusedConnection,
			codeIdleDisconnect:
			msg = codeErr.Error()
		// The rest - the message sent back is sanitized.
		case codeUnexpectedInsecureStartupMessage:
			msg = "server requires encryption"
		}

		var pgCode string
		if codeErr.code == codeIdleDisconnect {
			pgCode = "57P01" // admin shutdown
		} else {
			pgCode = "08004" // rejected connection
		}
		_, _ = conn.Write((&pgproto3.ErrorResponse{
			Severity: "FATAL",
			Code:     pgCode,
			Message:  msg,
		}).Encode(nil))
	} else {
		// Return a generic "internal server error" message.
		_, _ = conn.Write((&pgproto3.ErrorResponse{
			Severity: "FATAL",
			Code:     "08004", // rejected connection
			Message:  "internal server error",
		}).Encode(nil))
	}
}

// ConnectionCopy does a bi-directional copy between the backend and frontend
// connections. It terminates when one of connections terminate.
func ConnectionCopy(crdbConn, conn net.Conn) error {
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
	// protocol stipulates sending a message to the server at which point the
	// server closes the connection (errIncoming), but presumably the client
	// gets to close the connection once it's sent that message, meaning either
	// case is possible.
	case err := <-errIncoming:
		if err == nil {
			return nil
		} else if codeErr := (*codeError)(nil); errors.As(err, &codeErr) &&
			codeErr.code == codeExpiredClientConnection {
			return codeErr
		} else if ne := (net.Error)(nil); errors.As(err, &ne) && ne.Timeout() {
			return newErrorf(codeIdleDisconnect, "terminating connection due to idle timeout: %v", err)
		} else {
			return newErrorf(codeBackendDisconnected, "copying from target server to client: %s", err)
		}
	case err := <-errOutgoing:
		// The incoming connection got closed.
		if err != nil {
			return newErrorf(codeClientDisconnected, "copying from target server to client: %v", err)
		}
		return nil
	}
}
