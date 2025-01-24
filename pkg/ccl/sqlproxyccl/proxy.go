// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package sqlproxyccl implements a server to proxy SQL connections.
package sqlproxyccl

import (
	"net"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
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

func toPgError(err error) *pgproto3.ErrorResponse {
	if getErrorCode(err) != codeNone {
		var msg string
		switch getErrorCode(err) {
		// These are send as is.
		case codeExpiredClientConnection,
			codeBackendDialFailed,
			codeParamsRoutingFailed,
			codeClientDisconnected,
			codeBackendDisconnected,
			codeAuthFailed,
			codeProxyRefusedConnection,
			codeUnavailable:
			msg = err.Error()
		// The rest - the message sent back is sanitized.
		case codeUnexpectedInsecureStartupMessage:
			msg = "server requires encryption"
		}

		return &pgproto3.ErrorResponse{
			Severity: "FATAL",
			Code:     pgcode.ProxyConnectionError.String(),
			Message:  msg,
			Hint:     errors.FlattenHints(err),
		}
	}
	// Return a generic "internal server error" message.
	return &pgproto3.ErrorResponse{
		Severity: "FATAL",
		Code:     pgcode.ProxyConnectionError.String(),
		Message:  "internal server error",
	}
}

// SendErrToClient will encode and pass back to the SQL client an error message.
// It can be called by the implementors of proxyHandler to give more
// information to the end user in case of a problem.
var SendErrToClient = func(conn net.Conn, err error) {
	if err == nil || conn == nil {
		return
	}
	buf, err := toPgError(err).Encode(nil)
	if err != nil {
		return // void function - eat the error
	}
	_, _ = conn.Write(buf)
}
