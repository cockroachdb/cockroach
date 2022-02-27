// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/interceptor"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/errors"
	pgproto3 "github.com/jackc/pgproto3/v2"
)

// runShowTransferState sends a SHOW TRANSFER STATE query with the input
// transferKey to the given writer. The transferKey will be used to uniquely
// identify the request when parsing the response messages in
// waitForShowTransferState.
//
// Unlike runAndWaitForDeserializeSession, we split the SHOW TRANSFER STATE
// operation into `run` and `wait` since they both will be invoked in different
// goroutines. If we combined them, we'll have to wait for at least one of the
// goroutines to pause, which can introduce a latency of about 1-2s per transfer
// while waiting for Read in readTimeoutConn to be unblocked.
func runShowTransferState(w io.Writer, transferKey string) error {
	return writeQuery(w, "SHOW TRANSFER STATE WITH '%s'", transferKey)
}

// waitForShowTransferState retrieves the transfer state from the SQL pod
// through SHOW TRANSFER STATE WITH 'key'. It is assumed that the last message
// from the server was ReadyForQuery, so the server is ready to accept a query.
// Since ReadyForQuery may be for a previous pipelined query, this handles the
// forwarding of messages back to the client in case we don't see our state yet.
//
// WARNING: When using this, we assume that no other goroutines are using both
// serverConn and clientConn, as well as their respective interceptors. In the
// context of a transfer, the client-to-server processor must be blocked.
var waitForShowTransferState = func(
	ctx context.Context,
	serverInterceptor *interceptor.FrontendInterceptor,
	clientConn io.Writer,
	transferKey string,
) (transferErr string, state string, revivalToken string, retErr error) {
	// Wait for a response that looks like the following:
	//
	//   error | session_state_base64 | session_revival_token_base64 | transfer_key
	// --------+----------------------+------------------------------+---------------
	//   NULL  | .................... | ............................ | <transferKey>
	// (1 row)
	//
	// Postgres messages always come in the following order for the
	// SHOW TRANSFER STATE WITH '<transferKey>' query:
	//   1. RowDescription
	//   2. DataRow
	//   3. CommandComplete
	//   4. ReadyForQuery

	// 1. Wait for the relevant RowDescription.
	if err := waitForSmallRowDescription(
		ctx,
		serverInterceptor,
		clientConn,
		func(msg *pgproto3.RowDescription) bool {
			// Do we have the right number of columns?
			if len(msg.Fields) != 4 {
				return false
			}
			// Do the names of the columns match?
			var transferStateCols = []string{
				"error",
				"session_state_base64",
				"session_revival_token_base64",
				"transfer_key",
			}
			for i, col := range transferStateCols {
				if string(msg.Fields[i].Name) != col {
					return false
				}
			}
			return true
		},
	); err != nil {
		return "", "", "", errors.Wrap(err, "waiting for RowDescription")
	}

	// 2. Read DataRow.
	if err := expectDataRow(ctx, serverInterceptor, func(msg *pgproto3.DataRow) bool {
		// This has to be 4 since we validated RowDescription earlier.
		if len(msg.Values) != 4 {
			return false
		}

		// Validate transfer key. It is possible that the end-user uses the SHOW
		// TRANSFER STATE WITH 'transfer_key' statement, but that isn't designed
		// for external usage, so it is fine to just terminate here if the
		// transfer key does not match.
		if string(msg.Values[3]) != transferKey {
			return false
		}

		// NOTE: We have to cast to string and copy here since the slice
		// referenced in msg will no longer be valid once we read the next pgwire
		// message.
		transferErr, state, revivalToken = string(msg.Values[0]), string(msg.Values[1]), string(msg.Values[2])
		return true
	}); err != nil {
		return "", "", "", errors.Wrap(err, "expecting DataRow")
	}

	// 3. Read CommandComplete.
	if err := expectCommandComplete(ctx, serverInterceptor, "SHOW TRANSFER STATE 1"); err != nil {
		return "", "", "", errors.Wrap(err, "expecting CommandComplete")
	}

	// 4. Read ReadyForQuery.
	if err := expectReadyForQuery(ctx, serverInterceptor); err != nil {
		return "", "", "", errors.Wrap(err, "expecting ReadyForQuery")
	}

	return transferErr, state, revivalToken, nil
}

// runAndWaitForDeserializeSession deserializes state into the SQL pod through
// crdb_internal.deserialize_session. It is assumed that the last message from
// the server was ReadyForQuery, so the server is ready to accept a query.
//
// This is meant to be used with a new connection, and nothing needs to be
// forwarded back to the client.
//
// WARNING: When using this, we assume that no other goroutines are using both
// serverConn and clientConn, and their respective interceptors.
var runAndWaitForDeserializeSession = func(
	ctx context.Context,
	serverConn io.Writer,
	serverInterceptor *interceptor.FrontendInterceptor,
	state string,
) error {
	// Send deserialization query.
	if err := writeQuery(serverConn,
		"SELECT crdb_internal.deserialize_session(decode('%s', 'base64'))", state); err != nil {
		return err
	}

	// Wait for a response that looks like the following:
	//
	//   crdb_internal.deserialize_session
	// -------------------------------------
	//                 true
	// (1 row)
	//
	// Postgres messages always come in the following order for the
	// deserialize_session query:
	//   1. RowDescription
	//   2. DataRow
	//   3. CommandComplete
	//   4. ReadyForQuery

	// 1. Read RowDescription. We reuse waitFor here for convenience when we are
	//    really expecting instead. This is fine because we only deserialize a
	//    session for a new connection which hasn't been handed off to the user,
	//    so we can guarantee that there won't be pipelined queries.
	if err := waitForSmallRowDescription(
		ctx,
		serverInterceptor,
		&errWriter{},
		func(msg *pgproto3.RowDescription) bool {
			return len(msg.Fields) == 1 &&
				string(msg.Fields[0].Name) == "crdb_internal.deserialize_session"
		},
	); err != nil {
		return errors.Wrap(err, "expecting RowDescription")
	}

	// 2. Read DataRow.
	if err := expectDataRow(ctx, serverInterceptor, func(msg *pgproto3.DataRow) bool {
		return len(msg.Values) == 1 && string(msg.Values[0]) == "t"
	}); err != nil {
		return errors.Wrap(err, "expecting DataRow")
	}

	// 3. Read CommandComplete.
	if err := expectCommandComplete(ctx, serverInterceptor, "SELECT 1"); err != nil {
		return errors.Wrap(err, "expecting CommandComplete")
	}

	// 4. Read ReadyForQuery.
	if err := expectReadyForQuery(ctx, serverInterceptor); err != nil {
		return errors.Wrap(err, "expecting ReadyForQuery")
	}

	return nil
}

// writeQuery writes a SimpleQuery to the given writer w.
func writeQuery(w io.Writer, format string, a ...interface{}) error {
	query := &pgproto3.Query{String: fmt.Sprintf(format, a...)}
	_, err := w.Write(query.Encode(nil))
	return err
}

// waitForSmallRowDescription waits until the next message from the interceptor
// is a *small* RowDescription message (i.e. within 4K bytes), and one that
// passes matchFn. When that happens, this returns nil.
//
// For all other messages (i.e. non RowDescription or large messages), they will
// be forwarded to conn. One exception to this would be the ErrorResponse
// message, which will result in an error since we're in an ambiguous state.
// The ErrorResponse message may be for a pipelined query, or the RowDescription
// message that we're waiting.
func waitForSmallRowDescription(
	ctx context.Context,
	interceptor *interceptor.FrontendInterceptor,
	conn io.Writer,
	matchFn func(*pgproto3.RowDescription) bool,
) error {
	// Since we're waiting for the first message that matches the given
	// condition, we're going to loop here until we find one.
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		typ, size, err := interceptor.PeekMsg()
		if err != nil {
			return errors.Wrap(err, "peeking message")
		}

		// We don't know if the ErrorResponse is for the expected RowDescription
		// or a previous pipelined query, so return an error.
		if typ == pgwirebase.ServerMsgErrorResponse {
			// Error messages are small, so read for debugging purposes.
			msg, err := interceptor.ReadMsg()
			if err != nil {
				return errors.Wrap(err, "ambiguous ErrorResponse")
			}
			return errors.Newf("ambiguous ErrorResponse: %v", jsonOrRaw(msg))
		}

		// Messages are intended for the client in two cases:
		//   1. We have not seen a RowDescription message yet
		//   2. Message was too large. This function only expects a few columns.
		//
		// This is mostly an optimization, and there's no point reading such
		// messages into memory, so we'll just forward them back to the client
		// right away.
		const maxSmallMsgSize = 1 << 12 // 4KB
		if typ != pgwirebase.ServerMsgRowDescription || size > maxSmallMsgSize {
			if _, err := interceptor.ForwardMsg(conn); err != nil {
				return errors.Wrap(err, "forwarding message")
			}
			continue
		}

		msg, err := interceptor.ReadMsg()
		if err != nil {
			return errors.Wrap(err, "reading RowDescription")
		}

		pgMsg, ok := msg.(*pgproto3.RowDescription)
		if !ok {
			// This case will not occur since have validated the type earlier.
			return errors.Newf("unexpected message: %v", jsonOrRaw(msg))
		}

		// We have found our desired RowDescription.
		if matchFn(pgMsg) {
			return nil
		}

		// Matching fails, so forward the message back to the client, and
		// continue searching.
		if _, err := conn.Write(msg.Encode(nil)); err != nil {
			return errors.Wrap(err, "writing message")
		}
	}
}

// expectDataRow expects that the next message from the interceptor is a DataRow
// message. If the next message is a DataRow message, validateFn will be called
// to validate the contents. This function will return an error if we don't see
// a DataRow message or the validation failed.
//
// WARNING: Use this with care since this reads the entire message into memory.
// Unlike the other expectX methods, DataRow messages may be large, and this
// does not check for that. We are currently only using this for the SHOW
// TRANSFER and crdb_internal.deserialize_session() statements, and they both
// have been vetted. The former's size will be guarded behind a cluster setting,
// whereas for the latter, the response is expected to be small.
func expectDataRow(
	ctx context.Context,
	interceptor *interceptor.FrontendInterceptor,
	validateFn func(*pgproto3.DataRow) bool,
) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	msg, err := interceptor.ReadMsg()
	if err != nil {
		return errors.Wrap(err, "reading message")
	}
	pgMsg, ok := msg.(*pgproto3.DataRow)
	if !ok {
		return errors.Newf("unexpected message: %v", jsonOrRaw(msg))
	}
	if !validateFn(pgMsg) {
		return errors.Newf("validation failed for message: %v", jsonOrRaw(msg))
	}
	return nil
}

// expectCommandComplete expects that the next message from the interceptor is
// a CommandComplete message with the input tag, and returns an error if it
// isn't.
func expectCommandComplete(
	ctx context.Context, interceptor *interceptor.FrontendInterceptor, tag string,
) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	msg, err := interceptor.ReadMsg()
	if err != nil {
		return errors.Wrap(err, "reading message")
	}
	pgMsg, ok := msg.(*pgproto3.CommandComplete)
	if !ok || string(pgMsg.CommandTag) != tag {
		return errors.Newf("unexpected message: %v", jsonOrRaw(msg))
	}
	return nil
}

// expectReadyForQuery expects that the next message from the interceptor is a
// ReadyForQuery message, and returns an error if it isn't.
func expectReadyForQuery(ctx context.Context, interceptor *interceptor.FrontendInterceptor) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	msg, err := interceptor.ReadMsg()
	if err != nil {
		return errors.Wrap(err, "reading message")
	}
	_, ok := msg.(*pgproto3.ReadyForQuery)
	if !ok {
		return errors.Newf("unexpected message: %v", jsonOrRaw(msg))
	}
	return nil
}

// jsonOrRaw returns msg in a json string representation if it can be marshaled
// into one, or in a raw struct string representation otherwise. Only used for
// displaying better error messages.
func jsonOrRaw(msg pgproto3.BackendMessage) string {
	m, err := json.Marshal(msg)
	if err != nil {
		return fmt.Sprintf("%v", msg)
	}
	return string(m)
}

var _ io.Writer = &errWriter{}

// errWriter is an io.Writer that fails whenever a Write call is made.
type errWriter struct{}

// Write implements the io.Writer interface.
func (w *errWriter) Write(p []byte) (int, error) {
	return 0, errors.AssertionFailedf("unexpected Write call")
}
