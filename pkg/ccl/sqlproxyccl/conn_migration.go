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
	"fmt"
	"io"
	"net"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/interceptor"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/errors"
	pgproto3 "github.com/jackc/pgproto3/v2"
)

// awaitTransferStateResponse intercepts messages from the server to the client,
// and attempts to look for a transfer state response message, which is defined
// by the following sequence of pgwire messages:
//
//   1. RowDescription
//   2. DataRow
//   3. CommandComplete
//   4. ReadyForQuery
//
// The RowDescription message that we are looking for has to contain the column
// names that we expect in order for it to be denoted as the start of a transfer
// response message. All other messages will be forwarded to the client until
// we find (1). Once we see (1), subsequent messages have to abide to 2-4 in
// order, or else an error will be returned.
//
// This function will block until the response message has been received. At
// any point in time, we may receive and ErrorResponse message, and when that
// happens, this will return an error.
//
// If err != nil, the forwarder has to be closed by the caller to prevent any
// data corruption, with one exception: the caller may choose to abort the
// transfer process and continue with the fowarding if and only if the error
// has been marked with errConnRecoverableSentinel, which can be verified with
// isConnRecoverableError().
//
// NOTE: This should only be called during the transfer process.
func (f *forwarder) awaitTransferStateResponse(
	ctx context.Context, expectedServerMsgType pgwirebase.ServerMessageType,
) (state string, revivalToken string, err error) {
	if f.testingKnobs.awaitTransferStateResponse != nil {
		return f.testingKnobs.awaitTransferStateResponse(ctx, expectedServerMsgType)
	}

	if ctx.Err() != nil {
		return "", "", ctx.Err()
	}

	typ, size, err := f.serverInterceptor.PeekMsg()
	if err != nil {
		// Read failure: either the forwarder has been closed explicitly, or
		// the server terminated.
		return "", "", errors.Wrap(err, "peeking message during transfer")
	}

	// Return an error because there is ambiguity in detecting whether this
	// response is for the client or the proxy.
	if typ == pgwirebase.ServerMsgErrorResponse {
		return "", "", errors.New("ambiguous ErrorResponse message")
	}

	// NOTE: These may need to be updated if we end up adding more fields to the
	// SHOW TRANSFER STATE query.
	const (
		// Denotes the size of the candidate message. We know that the header
		// row for the transfer response message will be ~200 bytes, so there's
		// no point attempting to read large messages into memory.
		startResponseMaxSize = 2 << 9 // 512 bytes

		// Limit transfers to sessions with state smaller than this size. We may
		// exceed this size if a user sets their session's state to large values
		// (e.g. application_name with a long length).
		bodyResponseMaxSize = 2 << 13 // 8K
	)

	// If the expected server message type is "any", this means we're still
	// looking for the RowDescription message that indicates the start of the
	// transfer state response.
	if expectedServerMsgType == serverMsgAny {
		// Not a possible candidate, so forward current message and search again.
		if typ != pgwirebase.ServerMsgRowDescription || size > startResponseMaxSize {
			if _, err := f.serverInterceptor.ForwardMsg(f.clientConn); err != nil {
				return "", "", errors.Wrap(err, "forwarding message during transfer")
			}
			return f.awaitTransferStateResponse(ctx, serverMsgAny)
		}
		// We found RowDescription, and the message is small enough to be a
		// possible candidate of the header that we're looking for.
	} else if expectedServerMsgType != typ {
		// Ensure that the expected server message type matches. This happens
		// when the server returns a message in an order that we do not expect,
		// which should not be possible.
		return "", "", errors.Wrapf(errTransferProtocol,
			"expected message with type '%c', but found '%c'", expectedServerMsgType, typ)
	} else if size > bodyResponseMaxSize {
		// Don't even bother transferring. This is a non-recoverable error
		// because the request has already been sent to the server. In theory,
		// we could "slurp" all the unnecessary messages, and resume the
		// connection. But to do that, we would have to parse the message to
		// validate the transfer key, which is what we didn't want to do in the
		// first place to avoid filling the memory.
		//
		// TODO(jaylim-crl): We could potentially add a TenantReadOnly cluster
		// setting that restricts the maximum length of session variables to
		// avoid handling this case. That will allow us to ensure that users
		// cannot fill up the proxy's memory by abusing this feature.
		return "", "", errors.New("transfer rejected due to large state")
	}

	// Read message into memory.
	msg, err := f.serverInterceptor.ReadMsg()
	if err != nil {
		return "", "", errors.Wrap(err, "reading message during transfer")
	}

	switch pgMsg := msg.(type) {
	case *pgproto3.RowDescription:
		validateFn := isValidStartTransferStateResponse
		if f.testingKnobs.isValidStartTransferStateResponse != nil {
			validateFn = f.testingKnobs.isValidStartTransferStateResponse
		}
		if !validateFn(pgMsg) {
			// We are safe to forward the message to the client.
			if _, err := f.clientConn.Write(msg.Encode(nil)); err != nil {
				return "", "", errors.Wrap(err, "writing message to client during transfer")
			}
			return f.awaitTransferStateResponse(ctx, serverMsgAny)
		}
		// The message was valid. Move on to (2).
		return f.awaitTransferStateResponse(ctx, pgwirebase.ServerMsgDataRow)

	case *pgproto3.DataRow:
		f.mu.Lock()
		key := f.mu.transferKey
		f.mu.Unlock()

		parseFn := parseTransferStateResponse
		if f.testingKnobs.parseTransferStateResponse != nil {
			parseFn = f.testingKnobs.parseTransferStateResponse
		}

		transferError, state, revivalToken, err := parseFn(pgMsg, key)
		if err != nil {
			return "", "", errors.Mark(
				errors.Wrap(err, "parsing transfer response"),
				errTransferProtocol,
			)
		}

		// Transfer key matches, so we'll move on to (3).
		_, _, err = f.awaitTransferStateResponse(ctx, pgwirebase.ServerMsgCommandComplete)
		if err != nil {
			return "", "", err
		}

		// If we managed to consume until ReadyForQuery without errors, but the
		// transfer state response returns an error, we could still continue
		// with the connection, but the transfer process will need to be
		// aborted.
		//
		// This case may happen pretty frequently (e.g. open transactions,
		// temporary tables, etc.).
		if transferError != "" {
			return "", "", markAsConnRecoverableError(errors.Newf("%s", transferError))
		}
		return state, revivalToken, nil

	case *pgproto3.CommandComplete:
		validateFn := isValidEndTransferStateResponse
		if f.testingKnobs.isValidEndTransferStateResponse != nil {
			validateFn = f.testingKnobs.isValidEndTransferStateResponse
		}
		if !validateFn(pgMsg) {
			return "", "", errors.Wrap(errTransferProtocol, "validating end transfer response")
		}
		// Finally, move on to (4).
		return f.awaitTransferStateResponse(ctx, pgwirebase.ServerMsgReady)

	case *pgproto3.ReadyForQuery:
		// We are done.
		// It is unnecessary to check TxStatus in the ReadyForQuery message.
		return "", "", nil

	default:
		// This is not possible. msg must be one of the following above since we
		// only recurse on those, with the exception of RowDescription, which
		// is checked when the expected server message type is "any".
		return "", "", errors.Wrapf(errTransferProtocol, "unknown type '%T'", pgMsg)
	}
}

// writeTransferStateRequest sends a SHOW TRANSFER STATE query with the input
// transferKey to the given writer. The input transferKey will be used to
// uniquely identify the request when parsing the response messages.
func writeTransferStateRequest(w io.Writer, transferKey string) error {
	query := &pgproto3.Query{
		String: fmt.Sprintf("SHOW TRANSFER STATE WITH '%s'", transferKey),
	}
	_, err := w.Write(query.Encode(nil))
	return err
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
		if *((*string)(unsafe.Pointer(&m.Fields[i].Name))) != col {
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
	str := *((*string)(unsafe.Pointer(&m.CommandTag)))
	return str == "SHOW TRANSFER STATE 1"
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
	keyVal := *((*string)(unsafe.Pointer(&(m.Values[3]))))
	if keyVal != transferKey {
		return "", "", "", errors.Newf(
			"expected '%s' as transfer key, found '%s'", transferKey, keyVal)
	}

	// NOTE: We have to cast to string and copy here since the slice referenced
	// in m will no longer be valid once we read the next pgwire message.
	return string(m.Values[0]), string(m.Values[1]), string(m.Values[2]), nil
}

// deserializeSession deserializes the session's state into serverConn through
// crdb_internal.deserialize_session. serverConn has to be an authenticated
// connection with matching user as the connection that the state originated
// from.
func deserializeSession(
	ctx context.Context,
	serverConn net.Conn,
	serverInterceptor *interceptor.FrontendInterceptor,
	state string,
) error {
	// TODO(jaylim-crl): Implement this to ensure that session gets deserialized.
	// This involves sending crdb_internal.deserialize_session to serverConn,
	// followed by parsing a response until a ReadyForQuery message.
	return nil
}
