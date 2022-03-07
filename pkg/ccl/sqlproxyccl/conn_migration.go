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
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/interceptor"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	pgproto3 "github.com/jackc/pgproto3/v2"
)

// defaultTransferTimeout corresponds to the timeout period for the connection
// migration process. If the timeout gets triggered, and we're in a non
// recoverable state, the connection will be closed.
const defaultTransferTimeout = 15 * time.Second

func (f *forwarder) runTransfer() (retErr error) {
	// There should not be concurrent transfers for the same forwarder.
	f.mu.Lock()
	if f.mu.isTransferring {
		f.mu.Unlock()
		return nil
	}
	f.mu.isTransferring = true
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.mu.isTransferring = false
	}()

	// Prepare the transfer:
	// - recoverableConn indicates whether the connection is recoverable.
	// - closerCh will be closed whenever the timeout handler returns.
	var mu struct {
		syncutil.Mutex
		recoverableConn bool
	}
	mu.recoverableConn = true
	closerCh := make(chan struct{})

	// Start the timeout handler the moment we attempt the transfer.
	f.metrics.ConnMigrationAttemptedCount.Inc(1)
	timeout := defaultTransferTimeout
	if f.testingKnobs.transferTimeoutDuration != nil {
		timeout = f.testingKnobs.transferTimeoutDuration()
	}
	transferCtx, cancel := context.WithTimeout(f.ctx, timeout) // nolint:context

	// Use a separate context for logging because f.ctx will be closed whenever
	// the connection is non-recoverable.
	logCtx := logtags.WithTags(context.Background(), logtags.FromContext(f.ctx))
	defer func() {
		// Block until timeout goroutine has terminated.
		<-closerCh

		// Non-recoverable.
		if f.ctx.Err() != nil {
			log.Infof(logCtx, "transfer failed: connection closed, err=%v", retErr)
			f.metrics.ConnMigrationErrorFatalCount.Inc(1)
		} else {
			// Transfer was successful.
			if retErr == nil {
				log.Infof(logCtx, "transfer successful")
				f.metrics.ConnMigrationSuccessCount.Inc(1)
			} else {
				log.Infof(logCtx, "transfer failed: connection recovered, err=%v", retErr)
				f.metrics.ConnMigrationErrorRecoverableCount.Inc(1)
			}
			f.resumeProcessors()
		}
	}()
	defer cancel()

	// Use a goroutine to check whether the connection is recoverable when
	// transferCtx is done (either through a timeout, or when runTransfer
	// returns).
	go func() {
		<-transferCtx.Done()
		mu.Lock()
		defer mu.Unlock()
		if !mu.recoverableConn {
			f.Close()
		}
		close(closerCh)
	}()

	// Suspend request processor.
	f.req.Suspend()
	f.req.WaitUntilSuspended()

	// Context was cancelled.
	if transferCtx.Err() != nil {
		return transferCtx.Err()
	}

	// Can we perform the transfer?
	if !f.isSafeTransferPoint() {
		return errors.New("transfer is unsafe")
	}

	// Suspend the response processor first before sending the transfer request.
	// If we don't do this, transfer responses may be sent to the client, which
	// is incorrect.
	f.res.Suspend()

	transferKey := uuid.MakeV4().String()

	// Send the SHOW TRANSFER STATE statement, and mark connection as
	// non-recoverable.
	mu.Lock()
	mu.recoverableConn = false
	mu.Unlock()
	if err := runShowTransferState(f.mu.serverConn, transferKey); err != nil {
		return errors.Wrap(err, "sending transfer request")
	}

	// Wait for response processor to terminate.
	f.res.WaitUntilSuspended()

	if transferCtx.Err() != nil {
		return transferCtx.Err()
	}

	// Process the transfer.
	transferErr, state, revivalToken, err := waitForShowTransferState(
		transferCtx, f.mu.serverConn, f.clientConn, transferKey)
	if err != nil {
		return errors.Wrap(err, "waiting for transfer state")
	}

	// Updating the state also means that failures after this point are
	// recoverable (i.e. connections should not be terminated).
	mu.Lock()
	mu.recoverableConn = true
	mu.Unlock()

	// If we managed to consume until ReadyForQuery without errors, but the
	// transfer state response returns an error, we could still continue with
	// the connection, but the transfer process will need to be aborted.
	//
	// This case may happen pretty frequently (e.g. open transactions, temporary
	// tables, etc.).
	if transferErr != "" {
		return errors.Newf("%s", transferErr)
	}

	// Connect to a new SQL pod.
	//
	// TODO(jaylim-crl): There is a possibility where the same pod will get
	// selected. Some ideas to solve this: pass in the remote address of
	// serverConn to avoid choosing that pod, or maybe a filter callback?
	// We can also consider adding a target pod as an argument to RequestTransfer.
	// That way a central component gets to choose where the connections go.
	netConn, err := f.connector.OpenTenantConnWithToken(transferCtx, revivalToken)
	if err != nil {
		return errors.Wrap(err, "opening connection")
	}
	defer func() {
		if retErr != nil {
			netConn.Close()
		}
	}()
	newServerConn := interceptor.NewFrontendConn(netConn)

	// Deserialize session state within the new SQL pod.
	if err := runAndWaitForDeserializeSession(transferCtx, newServerConn, state); err != nil {
		return errors.Wrap(err, "deserializing session")
	}

	// Transfer was successful - use the new server connections.
	f.mu.Lock()
	f.mu.serverConn.Close()
	f.mu.serverConn = newServerConn
	f.mu.clientMessageTypeSent = clientMsgAny
	f.mu.isServerMsgReadyReceived = true
	f.mu.Unlock()
	return nil
}

// isSafeTransferPoint returns true if we're at a point where we're safe to
// transfer, and false otherwise. This should only be called during the
// transferRequested state.
func (f *forwarder) isSafeTransferPoint() bool {
	if f.testingKnobs.isSafeTransferPoint != nil {
		return f.testingKnobs.isSafeTransferPoint()
	}
	// Three conditions when evaluating a safe transfer point:
	//   1. The last message sent to the SQL pod was a Sync(S) or
	//      SimpleQuery(Q), and a ReadyForQuery(Z) has already been
	//      received at the time of evaluation.
	//   2. The last message sent to the SQL pod was a CopyDone(c), and
	//      a ReadyForQuery(Z) has already been received at the time of
	//      evaluation.
	//   3. The last message sent to the SQL pod was a CopyFail(f), and
	//      a ReadyForQuery(Z) has already been received at the time of
	//      evaluation.
	//
	// NOTE: clientMessageTypeSent does not require a mutex because it is only
	// set in the transferInProgress state, and this method should only be
	// called in the transferRequested state.
	f.mu.Lock()
	defer f.mu.Unlock()
	switch f.mu.clientMessageTypeSent {
	case clientMsgAny,
		pgwirebase.ClientMsgSync,
		pgwirebase.ClientMsgSimpleQuery,
		pgwirebase.ClientMsgCopyDone,
		pgwirebase.ClientMsgCopyFail:
		return f.mu.isServerMsgReadyReceived
	default:
		return false
	}
}

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
	serverConn *interceptor.FrontendConn,
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
		serverConn,
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
	if err := expectDataRow(ctx, serverConn, func(msg *pgproto3.DataRow) bool {
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
	if err := expectCommandComplete(ctx, serverConn, "SHOW TRANSFER STATE 1"); err != nil {
		return "", "", "", errors.Wrap(err, "expecting CommandComplete")
	}

	// 4. Read ReadyForQuery.
	if err := expectReadyForQuery(ctx, serverConn); err != nil {
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
	ctx context.Context, serverConn *interceptor.FrontendConn, state string,
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
		serverConn,
		&errWriter{},
		func(msg *pgproto3.RowDescription) bool {
			return len(msg.Fields) == 1 &&
				string(msg.Fields[0].Name) == "crdb_internal.deserialize_session"
		},
	); err != nil {
		return errors.Wrap(err, "expecting RowDescription")
	}

	// 2. Read DataRow.
	if err := expectDataRow(ctx, serverConn, func(msg *pgproto3.DataRow) bool {
		return len(msg.Values) == 1 && string(msg.Values[0]) == "t"
	}); err != nil {
		return errors.Wrap(err, "expecting DataRow")
	}

	// 3. Read CommandComplete.
	if err := expectCommandComplete(ctx, serverConn, "SELECT 1"); err != nil {
		return errors.Wrap(err, "expecting CommandComplete")
	}

	// 4. Read ReadyForQuery.
	if err := expectReadyForQuery(ctx, serverConn); err != nil {
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
	serverConn *interceptor.FrontendConn,
	clientConn io.Writer,
	matchFn func(*pgproto3.RowDescription) bool,
) error {
	// Since we're waiting for the first message that matches the given
	// condition, we're going to loop here until we find one.
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		typ, size, err := serverConn.PeekMsg()
		if err != nil {
			return errors.Wrap(err, "peeking message")
		}

		// We don't know if the ErrorResponse is for the expected RowDescription
		// or a previous pipelined query, so return an error.
		if typ == pgwirebase.ServerMsgErrorResponse {
			// Error messages are small, so read for debugging purposes.
			msg, err := serverConn.ReadMsg()
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
			if _, err := serverConn.ForwardMsg(clientConn); err != nil {
				return errors.Wrap(err, "forwarding message")
			}
			continue
		}

		msg, err := serverConn.ReadMsg()
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
		if _, err := clientConn.Write(msg.Encode(nil)); err != nil {
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
	serverConn *interceptor.FrontendConn,
	validateFn func(*pgproto3.DataRow) bool,
) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	msg, err := serverConn.ReadMsg()
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
	ctx context.Context, serverConn *interceptor.FrontendConn, tag string,
) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	msg, err := serverConn.ReadMsg()
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
func expectReadyForQuery(ctx context.Context, serverConn *interceptor.FrontendConn) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	msg, err := serverConn.ReadMsg()
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
