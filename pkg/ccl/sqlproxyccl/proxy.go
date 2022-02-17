// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

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
			pgCode = pgcode.AdminShutdown.String()
		} else {
			pgCode = pgcode.SQLserverRejectedEstablishmentOfSQLconnection.String()
		}

		return &pgproto3.ErrorResponse{
			Severity: "FATAL",
			Code:     pgCode,
			Message:  msg,
			Hint:     errors.FlattenHints(err),
		}
	}
	// Return a generic "internal server error" message.
	return &pgproto3.ErrorResponse{
		Severity: "FATAL",
		Code:     pgcode.SQLserverRejectedEstablishmentOfSQLconnection.String(),
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
	_, _ = conn.Write(toPgError(err).Encode(nil))
}
