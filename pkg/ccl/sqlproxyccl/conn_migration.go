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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/interceptor"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
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
//
// Since ReadyForQuery may be for a previous pipelined query, this handles the
// forwarding of messages back to the client in case we don't see our state yet.
//
// This also does not support transferring messages with a large state (> 4K
// bytes). This may occur if a user sets their cluster settings to large values.
// We can potentially add a TenantReadOnly cluster setting that restricts the
// maximum length of session variables for a better migration experience. For
// now, we'll just close the connection.
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

	// 1. Read RowDescription. Loop here since there could be pipelined queries
	//    that were sent before.
	for {
		if ctx.Err() != nil {
			return "", "", "", ctx.Err()
		}

		// We skip reads here so we could call ForwardMsg for messages that are
		// not our concern.
		_, err := expectNextServerMessage(
			ctx, serverInterceptor, pgwirebase.ServerMsgRowDescription, true /* skipRead */)

		// We don't know if the ErrorResponse is for the client or proxy, so we
		// will just close the connection.
		if isErrorResponseError(err) {
			return "", "", "", errors.Wrap(err, "ambiguous ErrorResponse")
		}

		// Messages are intended for the client in two cases:
		// 1. We have not seen a RowDescription message yet
		// 2. Message was too lage. Connection migration doesn't care about
		//    large messages since we expected our header for SHOW TRANSFER
		//    STATE to fit 4K bytes.
		if isTypeMismatchError(err) || isLargeMessageError(err) {
			if _, err := serverInterceptor.ForwardMsg(clientConn); err != nil {
				return "", "", "", errors.Wrap(err, "forwarding message")
			}
			continue
		}

		if err != nil {
			return "", "", "", errors.Wrap(err, "waiting for RowDescription")
		}

		msg, err := serverInterceptor.ReadMsg()
		if err != nil {
			return "", "", "", errors.Wrap(err, "reading RowDescription")
		}

		// If pgMsg is nil, isValidStartTransferStateResponse will handle that.
		pgMsg, _ := msg.(*pgproto3.RowDescription)

		// We found our intended header, so start expecting a DataRow.
		if isValidStartTransferStateResponse(pgMsg) {
			break
		}

		// Column names do not match, so forward the message back to the client,
		// and continue waiting.
		if _, err := clientConn.Write(msg.Encode(nil)); err != nil {
			return "", "", "", errors.Wrap(err, "writing RowDescription")
		}
	}

	// 2. Read DataRow.
	{
		msg, err := expectNextServerMessage(
			ctx, serverInterceptor, pgwirebase.ServerMsgDataRow, false /* skipRead */)
		if err != nil {
			return "", "", "", errors.Wrap(err, "waiting for DataRow")
		}

		// If pgMsg is nil, parseTransferStateResponse will handle that.
		pgMsg, _ := msg.(*pgproto3.DataRow)
		transferErr, state, revivalToken, err = parseTransferStateResponse(pgMsg, transferKey)
		if err != nil {
			return "", "", "", errors.Wrapf(err, "invalid DataRow: %v", jsonOrRaw(msg))
		}
	}

	// 3. Read CommandComplete.
	{
		msg, err := expectNextServerMessage(
			ctx, serverInterceptor, pgwirebase.ServerMsgCommandComplete, false /* skipRead */)
		if err != nil {
			return "", "", "", errors.Wrap(err, "waiting for CommandComplete")
		}

		// If pgMsg is nil, isValidEndTransferStateResponse will handle that.
		pgMsg, _ := msg.(*pgproto3.CommandComplete)
		if !isValidEndTransferStateResponse(pgMsg) {
			return "", "", "", errors.Newf("invalid CommandComplete: %v", jsonOrRaw(msg))
		}
	}

	// 4. Read ReadyForQuery.
	if _, err := expectNextServerMessage(
		ctx, serverInterceptor, pgwirebase.ServerMsgReady, false /* skipRead */); err != nil {
		return "", "", "", errors.Wrap(err, "waiting for ReadyForQuery")
	}

	return transferErr, state, revivalToken, nil
}

// isValidStartTransferStateResponse returns true if m represents a valid
// column header for the SHOW TRANSFER STATE statement, or false otherwise.
func isValidStartTransferStateResponse(m *pgproto3.RowDescription) bool {
	if m == nil {
		return false
	}
	// Do we have the right number of columns?
	if len(m.Fields) != 4 {
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
		// Prevent an allocation when converting byte slice to string.
		if encoding.UnsafeString(m.Fields[i].Name) != col {
			return false
		}
	}
	return true
}

// isValidEndTransferStateResponse returns true if this is a valid
// CommandComplete message that denotes the end of a transfer state response
// message, or false otherwise.
func isValidEndTransferStateResponse(m *pgproto3.CommandComplete) bool {
	if m == nil {
		return false
	}
	// We only expect 1 response row.
	return encoding.UnsafeString(m.CommandTag) == "SHOW TRANSFER STATE 1"
}

// parseTransferStateResponse parses the state in the DataRow message, and
// extracts the fields for the SHOW TRANSFER STATE query. If err != nil, then
// all other returned fields will be empty strings.
//
// If the input transferKey does not match the result for the transfer_key
// column within the DataRow message, this will return an error.
func parseTransferStateResponse(
	m *pgproto3.DataRow, transferKey string,
) (transferErr string, state string, revivalToken string, err error) {
	if m == nil {
		return "", "", "", errors.New("DataRow message is nil")
	}

	// Do we have the right number of columns? This has to be 4 since we have
	// validated RowDescription earlier.
	if len(m.Values) != 4 {
		return "", "", "", errors.Newf(
			"unexpected %d columns in DataRow", len(m.Values))
	}

	// Validate transfer key. It is possible that the end-user uses the SHOW
	// TRANSFER STATE WITH 'transfer_key' statement, but that isn't designed for
	// external usage, so it is fine to just terminate here if the transfer key
	// does not match.
	keyVal := encoding.UnsafeString(m.Values[3])
	if keyVal != transferKey {
		return "", "", "", errors.Newf(
			"expected '%s' as transfer key, found '%s'", transferKey, keyVal)
	}

	// NOTE: We have to cast to string and copy here since the slice referenced
	// in m will no longer be valid once we read the next pgwire message.
	return string(m.Values[0]), string(m.Values[1]), string(m.Values[2]), nil
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
	const skipRead = false

	// 1. Read RowDescription.
	{
		msg, err := expectNextServerMessage(
			ctx, serverInterceptor, pgwirebase.ServerMsgRowDescription, skipRead)
		if err != nil {
			return errors.Wrap(err, "waiting for RowDescription")
		}
		pgMsg, ok := msg.(*pgproto3.RowDescription)
		if !ok || len(pgMsg.Fields) != 1 ||
			encoding.UnsafeString(pgMsg.Fields[0].Name) != "crdb_internal.deserialize_session" {
			return errors.Newf("invalid RowDescription: %v", jsonOrRaw(msg))
		}
	}

	// 2. Read DataRow.
	{
		msg, err := expectNextServerMessage(
			ctx, serverInterceptor, pgwirebase.ServerMsgDataRow, skipRead)
		if err != nil {
			return errors.Wrap(err, "waiting for DataRow")
		}
		// Expect just 1 column with "true" as value.
		pgMsg, ok := msg.(*pgproto3.DataRow)
		if !ok || len(pgMsg.Values) != 1 ||
			encoding.UnsafeString(pgMsg.Values[0]) != "t" {
			return errors.Newf("invalid DataRow: %v", jsonOrRaw(msg))
		}
	}

	// 3. Read CommandComplete.
	{
		msg, err := expectNextServerMessage(
			ctx, serverInterceptor, pgwirebase.ServerMsgCommandComplete, skipRead)
		if err != nil {
			return errors.Wrap(err, "waiting for CommandComplete")
		}
		pgMsg, ok := msg.(*pgproto3.CommandComplete)
		if !ok || encoding.UnsafeString(pgMsg.CommandTag) != "SELECT 1" {
			return errors.Newf("invalid CommandComplete: %v", jsonOrRaw(msg))
		}
	}

	// 4. Read ReadyForQuery.
	if _, err := expectNextServerMessage(
		ctx, serverInterceptor, pgwirebase.ServerMsgReady, skipRead); err != nil {
		return errors.Wrap(err, "waiting for ReadyForQuery")
	}

	return nil
}

// writeQuery writes a SimpleQuery to the given writer w.
func writeQuery(w io.Writer, format string, a ...interface{}) error {
	query := &pgproto3.Query{String: fmt.Sprintf(format, a...)}
	_, err := w.Write(query.Encode(nil))
	return err
}

// expectNextServerMessage expects that the next message in the server's
// interceptor will match the input message type. This will block until one
// message can be peeked. On return, this reads the next message into memory,
// and returns that. To avoid this read behavior, set skipRead to true, so the
// caller can decide what to do with the next message (i.e. if skipRead=true,
// retMsg=nil).
//
// retMsg != nil if there is a type mismatch, or the message cannot fit within
// 4K bytes. Use isTypeMismatchError or isLargeMessageError to detect such
// errors.
func expectNextServerMessage(
	ctx context.Context,
	interceptor *interceptor.FrontendInterceptor,
	msgType pgwirebase.ServerMessageType,
	skipRead bool,
) (retMsg pgproto3.BackendMessage, retErr error) {
	// Limit messages for connection transfers to 4K bytes. If we decide that
	// we need more, we could lift this restriction.
	const maxBodySize = 1 << 12 // 4K

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	typ, size, err := interceptor.PeekMsg()
	if err != nil {
		return nil, errors.Wrap(err, "peeking message")
	}

	if msgType != typ {
		return nil, errors.Newf("type mismatch: expected '%c', but found '%c'", msgType, typ)
	}

	if size > maxBodySize {
		return nil, errors.Newf("too many bytes: expected <= %d, but found %d", maxBodySize, size)
	}

	if skipRead {
		return nil, nil
	}

	msg, err := interceptor.ReadMsg()
	if err != nil {
		return nil, errors.Wrap(err, "reading message")
	}
	return msg, nil
}

// isErrorResponseError returns true if the error is a type mismatch due to
// matching an ErrorResponse pgwire message, and false otherwise. err must come
// from expectNextServerMessage.
func isErrorResponseError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "type mismatch") &&
		strings.Contains(err.Error(), fmt.Sprintf("found '%c'", pgwirebase.ServerMsgErrorResponse))
}

// isTypeMismatchError returns true if the error represents a type mismatch
// error, and false otherwise. err must come from expectNextServerMessage.
func isTypeMismatchError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "type mismatch")
}

// isLargeMessageError returns true if the error stems from "too many bytes",
// and false otherwise. This error will be returned if the message has more
// than 4K bytes. Connection migration does not care about such large messages.
// err must come from expectNextServerMessage.
func isLargeMessageError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "too many bytes")
}

// jsonOrRaw returns msg in a json string representation if it can be marshaled
// into one, or in a raw struct string representation otherwise. Only used for
// displaying better error messages.
func jsonOrRaw(msg pgproto3.BackendMessage) string {
	m, err := json.Marshal(msg)
	if err != nil {
		return fmt.Sprintf("%v", msg)
	}
	return encoding.UnsafeString(m)
}
