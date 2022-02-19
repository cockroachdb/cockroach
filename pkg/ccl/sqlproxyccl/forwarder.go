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
	"net"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/interceptor"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	pgproto3 "github.com/jackc/pgproto3/v2"
)

const (
	// stateReady represents the state where the forwarder is ready to forward
	// packets from the client to the server (and vice-versa).
	stateReady int32 = iota
	// stateTransferRequested represents the state where a session transfer was
	// requested.
	stateTransferRequested
	// stateTransferInProgress represents the state where the session transfer
	// is in-progress, and all incoming pgwire messages are buffered in the
	// kernel's socket buffer.
	stateTransferInProgress
)

// transferTimeout corresponds to the timeout while waiting for the transfer
// state response. If this gets triggered, the transfer is aborted, and the
// connection will be terminated.
const defaultTransferTimeout = 15 * time.Second

// clientMsgAny and serverMsgAny are used to denote a wildcard message type.
var (
	clientMsgAny = pgwirebase.ClientMessageType(0)
	serverMsgAny = pgwirebase.ServerMessageType(0)
)

var (
	// readAbortedDueToTransferErr is returned whenever a Read call exits due to
	// a session transfer.
	readAbortedDueToTransferErr = errors.New("read aborted due to transfer")

	// transferTimeoutErr denotes that a transfer process has timed out where
	// the forwarder wasn't able to locate the right transfer state response in
	// time
	transferTimeoutErr = errors.New("transfer timeout")

	// largeTransferStateResponseErr denotes that a transfer has been rejected
	// because the transfer state response message was too large.
	largeTransferStateResponseErr = errors.New("transfer rejected due to large state")

	// ambiguousErrorResponseErr denotes that we received an ErrorResponse
	// message during the transfer process, and there's no way to know whether
	// the message is for the client or proxy.
	ambiguousErrorResponseErr = errors.New("ambiguous ErrorResponse message")

	// transferProtocolErr indicates that an invariant has failed.
	transferProtocolErr = errors.New("transfer protocol error")
)

// forwarder is used to forward pgwire messages from the client to the server,
// and vice-versa. The forwarder instance should always be constructed through
// the forward function, which also starts the forwarder.
//
// The forwarder always starts with the ready state, which means that all
// messages from the client are forwarded to the server, and vice-versa. When
// a connection migration is requested through RequestTransfer, the forwarder
// transitions to the transferRequested state. If we are safe to transfer,
// the forwarder will transition to the transferInProgress state. If we are not,
// the forwarder aborts the transfer and transitions back to the ready state.
// Once the transfer process completes, the forwarder goes back to the ready
// state. At any point during the transfer process, we may also transition back
// to the ready state if the connection is deemed recoverable.
type forwarder struct {
	// ctx is a single context used to control all goroutines spawned by the
	// forwarder.
	ctx       context.Context
	ctxCancel context.CancelFunc

	// connect is an instance of the connector, which will be used to open a
	// new connection to a SQL pod. This connector instance must be associated
	// to the same tenant as the forwarder.
	connect *connector

	// serverConn is only set after the authentication phase for the initial
	// connection. In the context of a connection migration, serverConn is only
	// replaced once the session has successfully been deserialized, and the
	// old connection will be closed. Whenever serverConn gets updated, both
	// clientMessageTypeSent and isServerMsgReadyReceived fields have to reset
	// to their initial values.
	//
	// All reads from these connections must go through the interceptors. It is
	// not safe to read from these directly as the interceptors may have
	// buffered data.
	clientConn net.Conn // client <-> proxy
	serverConn net.Conn // proxy <-> server

	// clientInterceptor and serverInterceptor provides a convenient way to
	// read and forward Postgres messages, while minimizing IO reads and memory
	// allocations.
	//
	// These interceptors have to match clientConn and serverConn. See comment
	// above on when those fields will be updated.
	clientInterceptor *interceptor.BackendInterceptor  // clientConn's reader
	serverInterceptor *interceptor.FrontendInterceptor // serverConn's reader

	// errCh is a buffered channel that contains the first forwarder error.
	// This channel may receive nil errors.
	errCh chan error

	// atomics indicates fields that need to be atomically accessed. This is
	// necessary since they will be read and write from different goroutines
	// (i.e. request and response processors).
	atomics struct {
		// state represents the forwarder's state. Most of the time, this will
		// be stateReady.
		state int32

		// isServerMsgReadyReceived denotes whether a ReadyForQuery message has
		// been received by the response processor *after* a message has been
		// sent to the server through a Write on serverConn, either directly
		// or through ForwardMsg. This field will be set to 1 if true, and 0
		// otherwise.
		//
		// This will be initialized to 1 to implicitly denote that the server
		// is ready to accept queries.
		isServerMsgReadyReceived int32
	}

	// ------------------------------------------------------------------------
	// The following fields are used for connection migration.
	//
	// For details on how connection migration works, read the following RFC:
	// https://github.com/cockroachdb/cockroach/pull/75707.
	// ------------------------------------------------------------------------

	// transferKey is a unique string used to identify the transfer request, and
	// will be passed into the SHOW TRANSFER STATE statement. This will be set
	// to a randomly generated UUID whenever the transfer is requested through
	// the RequestTransfer API, and back to an empty string whenever the
	// transfer completes successfully or with a recoverable error.
	transferKey string

	// transferCh is a channel that must be set **before** transitioning to
	// the stateTransferRequested state, and this must be closed whenever the
	// forwarder transitions back to the stateReady state. Closing this will
	// unblock the request processor, which will resume the forwarding process
	// from client to server.
	transferCh chan struct{}

	// disableClientInterrupts denotes that clientConn should not be interrupted
	// by the custom readTimeoutConn that is wrapping the original clientConn.
	// This is false by default.
	disableClientInterrupts bool

	// clientMessageTypeSent indicates the message type for the last pgwire
	// message sent to serverConn. This is used to determine a safe transfer
	// point.
	//
	// If no message has been sent to serverConn by this forwarder, this will be
	// clientMsgAny.
	clientMessageTypeSent pgwirebase.ClientMessageType
}

// forward returns a new instance of forwarder, and starts forwarding messages
// from clientConn to serverConn. When this is called, it is expected that the
// caller passes ownership of serverConn to the forwarder, which implies that
// the forwarder will clean up serverConn. clientConn and serverConn must not
// be nil in all cases except for testing.
//
// Note that callers MUST call Close in all cases, even if ctx was cancelled.
//
// TODO(jaylim-crl): Convert this to return a Forwarder interface.
func forward(ctx context.Context, connect *connector, clientConn, serverConn net.Conn) *forwarder {
	ctx, cancelFn := context.WithCancel(ctx)

	// The forwarder starts with a state where connections migration can occur.
	f := &forwarder{
		ctx:       ctx,
		ctxCancel: cancelFn,
		errCh:     make(chan error, 1),
		connect:   connect,
	}

	// The net.Conn object for the client is switched to a net.Conn that
	// unblocks Read every second on idle to check for exit conditions. This is
	// mainly used to unblock the request processor whenever the forwarder has
	// stopped, or a transfer has been requested.
	clientConn = pgwire.NewReadTimeoutConn(clientConn, func() error {
		// Context was cancelled.
		if f.ctx.Err() != nil {
			return f.ctx.Err()
		}

		// Client interrupts are disabled.
		if f.disableClientInterrupts {
			return nil
		}

		// We want to unblock idle clients whenever a transfer has been
		// requested. This allows the request processor to be freed up to start
		// the transfer.
		if state := atomic.LoadInt32(&f.atomics.state); state != stateReady {
			return readAbortedDueToTransferErr
		}
		return nil
	})

	// Pass nil as interceptors here for automatic initialization.
	f.setClientConn(clientConn)
	f.setServerConn(serverConn)

	// Start request (client to server) and response (server to client)
	// processors. We will copy all pgwire messages/ from client to server
	// (and vice-versa) until we encounter an error or a shutdown signal
	// (i.e. context cancellation).
	go func() {
		defer f.Close()

		err := wrapClientToServerError(f.handleClientToServer())
		select {
		case f.errCh <- err: /* error reported */
		default: /* the channel already contains an error */
		}
	}()
	go func() {
		defer f.Close()

		err := wrapServerToClientError(f.handleServerToClient())
		select {
		case f.errCh <- err: /* error reported */
		default: /* the channel already contains an error */
		}
	}()

	return f
}

// Close closes the forwarder, and stops the forwarding process. This is
// idempotent.
func (f *forwarder) Close() {
	f.ctxCancel()

	// Since Close is idempotent, we'll ignore the error from Close in case it
	// has already been closed.
	f.serverConn.Close()
}

// RequestTransfer requests that the forwarder performs a best-effort connection
// migration whenever it can. It is best-effort because this will be a no-op if
// the forwarder is not in a state that is eligible for a connection migration.
// If a transfer is already in progress, or has been requested, this is a no-op.
func (f *forwarder) RequestTransfer() {
	// Transfer is already in progress.
	if atomic.LoadInt32(&f.atomics.state) != stateReady {
		return
	}
	f.prepareTransfer()
}

// prepareTransfer sets up the transfer process by moving the forwarder into the
// transferRequested state, and generating a unique transfer key for the
// forwarder.
//
// NOTE: This should only be called when the forwarder is in a ready state.
func (f *forwarder) prepareTransfer() {
	f.transferKey = uuid.MakeV4().String()
	// Initialize transferCh before updating the state so we don't write to a
	// nil channel.
	f.transferCh = make(chan struct{})
	atomic.StoreInt32(&f.atomics.state, stateTransferRequested)
}

// finishTransfer moves the forwarder back to the ready state, and unblocks
// the request processor.
//
// NOTE: This should only be called during a transfer, and if the connection is
// safe to continue. In the event of a timeout that is unsafe, we should just
// call Close().
func (f *forwarder) finishTransfer() {
	// It is important to reset the state first before closing the transfer
	// channel because the request processor expects that we're in a ready
	// state when it gets unblocked. However, there's a chance than another
	// transfer will be requested again right after updating the state, so we
	// will reset the state first before doing that.
	f.transferKey = ""
	oldTransferCh := f.transferCh
	f.transferCh = nil

	atomic.StoreInt32(&f.atomics.state, stateReady)

	// Unblock request processor.
	if oldTransferCh != nil {
		close(oldTransferCh)
	}
}

// handleClientToServer handles the communication from the client to the server.
// This returns a context cancellation error whenever the forwarder's context
// is cancelled, or whenever forwarding fails.
func (f *forwarder) handleClientToServer() error {
requestProcessor:
	for f.ctx.Err() == nil {
		typ, _, err := f.clientInterceptor.PeekMsg()
		if err != nil && !errors.Is(err, readAbortedDueToTransferErr) {
			return err
		}

		switch atomic.LoadInt32(&f.atomics.state) {
		case stateReady:
			// If we exit PeekMsg due to a transfer, the state must be in
			// stateTransferRequested unless there's a bug. Be defensive here
			// and peek again so that we don't end up blocking on the peek
			// call within ForwardMsg because client interrupts will be
			// disabled.
			if errors.Is(err, readAbortedDueToTransferErr) {
				continue
			}

			if forwardErr := func() error {
				// We may be blocked waiting for more packets when reading the
				// message's body. If a transfer was requested, there's no point
				// interrupting Reads since we're not at a message boundary, and
				// we cannot start a transfer, so don't interrupt at all.
				f.disableClientInterrupts = true
				defer func() { f.disableClientInterrupts = false }()

				f.clientMessageTypeSent = typ
				atomic.StoreInt32(&f.atomics.isServerMsgReadyReceived, 0)

				// When ForwardMsg gets blocked on Read, we will unblock that
				// through our custom readTimeoutConn wrapper.
				_, err := f.clientInterceptor.ForwardMsg(f.serverConn)
				return err
			}(); forwardErr != nil {
				return forwardErr
			}

		case stateTransferRequested:
			// Are we at a safe transfer point?
			//
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
			var isSafeTransferPoint bool
			switch f.clientMessageTypeSent {
			case clientMsgAny,
				pgwirebase.ClientMsgSync,
				pgwirebase.ClientMsgSimpleQuery,
				pgwirebase.ClientMsgCopyDone,
				pgwirebase.ClientMsgCopyFail:
				if atomic.LoadInt32(&f.atomics.isServerMsgReadyReceived) == 1 {
					isSafeTransferPoint = true
				}
				fallthrough
			default:
				if !isSafeTransferPoint {
					// We have to abort the transfer.
					f.finishTransfer()
					continue requestProcessor
				}
			}

			// Update the state first so that the response processor could start
			// processing. If we update the state after sending the request, we
			// may miss response messages.
			atomic.StoreInt32(&f.atomics.state, stateTransferInProgress)

			if err := f.sendTransferStateRequest(); err != nil {
				return err
			}

			// Wait until transfer is completed.
			select {
			case <-f.ctx.Done():
				return f.ctx.Err()
			case <-f.transferCh:
				// Channel is closed whenever transfer completes, so we are done.
			}

		case stateTransferInProgress:
			// This cannot happen unless there is a bug. While the transfer is
			// in progress, the request processor has to be blocked, and the
			// only way to transition into this state is to go through the
			// stateTransferRequested state.
			//
			// Return an error to close the connection, rather than letting it
			// continue silently.
			return transferProtocolErr
		}
	}
	return f.ctx.Err()
}

// handleServerToClient handles the communication from the server to the client.
// This returns a context cancellation error whenever the forwarder's context
// is cancelled, or whenever forwarding fails.
func (f *forwarder) handleServerToClient() error {
	for f.ctx.Err() == nil {
		typ, _, err := f.serverInterceptor.PeekMsg()
		if err != nil {
			return err
		}

		switch atomic.LoadInt32(&f.atomics.state) {
		case stateReady, stateTransferRequested:
			// Have we seen a ReadyForQuery message?
			if typ == pgwirebase.ServerMsgReady {
				atomic.StoreInt32(&f.atomics.isServerMsgReadyReceived, 1)
			}

			// When ForwardMsg gets blocked on Read, we will unblock that by
			// closing serverConn through f.Close().
			if _, err := f.serverInterceptor.ForwardMsg(f.clientConn); err != nil {
				return err
			}

		case stateTransferInProgress:
			if err := f.runTransferWithTimeout(
				defaultTransferTimeout,
				func(transferCtx context.Context) (retErr error) {
					// Pass ServerMsgAny because we do not know what message to expect yet.
					state, revivalToken, err := f.awaitTransferStateResponse(
						transferCtx, serverMsgAny,
					)
					if err != nil {
						// Some errors may be recoverable, but that is handled
						// in awaitTransferStateResponse.
						return err
					}

					// Connect to a new SQL pod.
					//
					// TODO(jaylim-crl): One enhancement that could be done is
					// to pass in the remote address of serverConn to ensure
					// that the same pod does not get picked (or maybe a filter
					// callback).
					newServerConn, err := f.connect.OpenTenantConnWithToken(
						transferCtx, revivalToken,
					)
					if err != nil {
						return markAsConnRecoverableError(err)
					}
					defer func() {
						if retErr != nil {
							newServerConn.Close()
						}
					}()
					newServerInterceptor := interceptor.NewFrontendInterceptor(newServerConn)

					// Deserialize session state within the new SQL pod.
					if err := deserializeSession(
						transferCtx, newServerConn, newServerInterceptor, state,
					); err != nil {
						return markAsConnRecoverableError(err)
					}

					// Finish the transfer process.
					f.serverConn.Close()
					f.setServerConnAndInterceptor(newServerConn, newServerInterceptor)
					f.finishTransfer()
					return nil
				},
			); err != nil {
				if isConnRecoverableError(err) {
					log.Infof(f.ctx, "recoverable transfer failed: %s", err)
					f.finishTransfer()
					continue
				}
				return err
			}
		}
	}
	return f.ctx.Err()
}

// runTransferWithTimeout runs a transfer function with a timeout of waitTimeout.
// This blocks until one of the following events occur:
//   - the transfer function returns a response or error, or
//   - a timeout of waitTimeout has been exceeded.
//
// If the timeout of maxAwaitDuration has been exceeded, the forwarder will be
// closed as well, and a transferTimeoutErr error will be returned.
//
// If err != nil, the forwarder has to be closed by the caller to prevent any
// data corruption, with one exception: the caller may choose to abort the
// transfer process and continue with the fowarding if and only if the error
// has been marked with errConnRecoverableSentinel, which can be verified with
// isConnRecoverableError().
func (f *forwarder) runTransferWithTimeout(
	waitTimeout time.Duration, transferFn func(transferCtx context.Context) (retErr error),
) error {
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()

	errCh := make(chan error, 1)

	go func() {
		select {
		case <-ctx.Done():
			// Do nothing. Either we returned from this function, or top-level
			// context gets cancelled.
		case <-time.After(waitTimeout):
			// If we're waiting for a message through the server's interceptor,
			// this will unblock that call with a closed pipe. If we're busy
			// processing other messages, the context will eventually be
			// cancelled, and the wait loop below will exit.
			//
			// We send a message to errCh first before closing the forwarder
			// to ensure that we don't get a context cancellation in errCh
			// when we unblock the server interceptor.
			select {
			case errCh <- transferTimeoutErr:
			default:
			}
			f.Close()
		}
	}()

	err := transferFn(ctx)
	select {
	case errCh <- err:
	default:
	}

	// It is guaranteed that there will be something in errCh. We only want
	// the first error that occurred.
	err = <-errCh
	return err
}

// sendTransferStateRequest sends a SHOW TRANSFER STATE query to the active
// server connection. Once the query is sent to the server, the forwarder should
// not send any further messages to the server. Since requests/responses are
// always in a FIFO order, we can guarantee that the server will no longer
// return messages intended for the client once we receive responses for the
// given SHOW TRANSFER STATE query. When that happens, we are safe to terminate
// the connection to the server.
//
// In order to uniquely identify the transfer state response, we will pass the
// transfer key that was generated earlier along with the request. This key
// will be emitted back as part of the response, and the forwarder could look
// for that. The transfer state response should include all the necessary
// states needed to migration the current connection.
func (f *forwarder) sendTransferStateRequest() error {
	query := &pgproto3.Query{
		String: fmt.Sprintf("SHOW TRANSFER STATE WITH '%s'", f.transferKey),
	}
	_, err := f.serverConn.Write(query.Encode(nil))
	return err
}

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
// we find (1). Once we see (1), subsequent messages has to abide to 2-4 in
// order, or else a transferProtocolErr error will be returned.
//
// This function will block until the response message has been received. At
// any point in time, we may receive and ErrorResponse message, and when that
// happens, this will return an ambiguousErrorResponseErr error.
//
// Both transferProtocolErr and ambiguousErrorResponseErr errors are
// non-recoverable, and the caller will have to terminate the forwarder.
func (f *forwarder) awaitTransferStateResponse(
	ctx context.Context, expectedServerMsgType pgwirebase.ServerMessageType,
) (state string, revivalToken string, err error) {
	if ctx.Err() != nil {
		return "", "", ctx.Err()
	}

	typ, size, err := f.serverInterceptor.PeekMsg()
	if err != nil {
		// Read failure: either the forwarder has been closed explicitly, or
		// the server terminated.
		return "", "", err
	}

	// Return an error because there is ambiguity in detecting whether this
	// response is for the client or the proxy.
	if typ == pgwirebase.ServerMsgErrorResponse {
		return "", "", ambiguousErrorResponseErr
	}

	// If the expected server message type is "any", this means we're still
	// looking for the RowDescription message that indicates the start of the
	// transfer state response.
	if expectedServerMsgType == serverMsgAny {
		// Forward current message and search again. We know that the header row
		// for the transfer response message will be ~200 bytes, so there's no
		// point attempting to read large messages into memory.
		if typ != pgwirebase.ServerMsgRowDescription || size > (2<<9 /* 512KB */) {
			// TODO(jaylim-crl): The above needs to be extracted out as a constant.
			if _, err := f.serverInterceptor.ForwardMsg(f.clientConn); err != nil {
				return "", "", err
			}
			return f.awaitTransferStateResponse(ctx, serverMsgAny)
		}
		// We found RowDescription, and the message is small enough to be a
		// possible candidate of the header that we're looking for.
	} else if expectedServerMsgType != typ {
		// Ensure that the expected server message type matches. This happens
		// when the server returns a message in an order that we do not expect,
		// which should not be possible.
		return "", "", transferProtocolErr
	} else if size > (2 << 13 /* 8KB */) {
		// TODO(jaylim-crl): The above needs to be extracted out as a constant.
		//
		// Don't even bother transferring. This case may happen if a user sets
		// their session's state to large values (e.g. application_name with a
		// long length). This is a non-recoverable error because the request
		// has already been sent to the server. Theoretically we could "slurp"
		// all the unnecessary messages, and resume the connection. But to do
		// that, we would have to parse the message to validate the transfer
		// key, which is what we didn't want to do in the first place to avoid
		// filling the memory.
		//
		// TODO(jaylim-crl): We could potentially add a TenantReadOnly cluster
		// setting that restricts the maximum length of session variables to
		// avoid handling this case. That will allow us to ensure that users
		// cannot fill up the proxy's memory by abusing this feature.
		return "", "", largeTransferStateResponseErr
	}

	// Read message into memory.
	msg, err := f.serverInterceptor.ReadMsg()
	if err != nil {
		return "", "", err
	}

	switch pgMsg := msg.(type) {
	case *pgproto3.RowDescription:
		if !isValidStartTransferStateResponse(pgMsg) {
			// We are safe to forward the message to the client.
			if _, err := f.clientConn.Write(msg.Encode(nil)); err != nil {
				return "", "", err
			}
			return f.awaitTransferStateResponse(ctx, serverMsgAny)
		}
		// The message was valid. Move on to (2).
		return f.awaitTransferStateResponse(ctx, pgwirebase.ServerMsgDataRow)

	case *pgproto3.DataRow:
		var transferError string
		transferError, state, revivalToken, err = parseAndValidateTransferStateData(pgMsg, f.transferKey)
		if err != nil {
			return "", "", err
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
			return "", "", markAsConnRecoverableError(errors.New(transferError))
		}
		return state, revivalToken, nil

	case *pgproto3.CommandComplete:
		if err := validateFinishTransferStateResponse(pgMsg); err != nil {
			return "", "", errors.Mark(err, transferProtocolErr)
		}
		// Finally, move on to (4).
		return f.awaitTransferStateResponse(ctx, pgwirebase.ServerMsgReady)

	case *pgproto3.ReadyForQuery:
		// We are done. It is unnecessary to check TxStatus.
		return "", "", nil

	default:
		// This is not possible. msg must be one of the following above since we
		// only recurse on those, with the exception of RowDescription, which
		// is checked when the expected server message type is "any".
		return "", "", transferProtocolErr
	}
}

// isValidStartTransferStateResponse returns true if m represents a valid
// column header for the SHOW TRANSFER STATE statement, and false otherwise.
func isValidStartTransferStateResponse(m *pgproto3.RowDescription) bool {
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

// parseAndValidateTransferStateData parses the DataRow message, and extracts
// the fields for the SHOW TRANSFER STATE query. If the transferKey input does
// not match the transfer_key column within the DataRow message, this will
// return a transferProtocolErr error.
func parseAndValidateTransferStateData(
	m *pgproto3.DataRow, transferKey string,
) (transferErr string, state string, revivalToken string, err error) {
	// Do we have the right number of columns? This has to be 4 since we have
	// validated RowDescription earlier.
	if len(m.Values) != 4 {
		return "", "", "", errors.Wrapf(transferProtocolErr,
			"unexpected %d columns in DataRow", len(m.Values))
	}

	// Validate transfer key. It is possible that the end-user uses the SHOW
	// TRANSFER STATE WITH 'transfer_key' statement, but that isn't designed for
	// external usage, so it is fine to just terminate here if the transfer key
	// does not match.
	keyVal := *((*string)(unsafe.Pointer(&(m.Values[3]))))
	if keyVal != transferKey {
		return "", "", "", errors.Wrapf(transferProtocolErr,
			"expected '%s' as transfer key, found '%s'", transferKey, keyVal)
	}

	// NOTE: We have to cast to string and copy here since the slice referenced
	// in m will no longer be valid once we read the next pgwire message.
	return string(m.Values[0]), string(m.Values[1]), string(m.Values[2]), nil
}

// validateFinishTransferStateResponse returns true if this is a valid
// CommandComplete message that denotes the end of a transfer state response
// message, or false otherwise.
func validateFinishTransferStateResponse(m *pgproto3.CommandComplete) error {
	// We only expect 1 response row.
	str := *((*string)(unsafe.Pointer(&m.CommandTag)))
	if str != "SHOW TRANSFER STATE 1" {
		return errors.Wrapf(transferProtocolErr,
			"invalid CommandComplete message, found '%s'", str)
	}
	return nil
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

// setClientConn is a convenient helper to update clientConn, and will also
// create a matching interceptor for the given connection. It is the caller's
// responsibility to close the old connection before calling this, or there
// may be a leak.
//
// It is the responsibility of the caller to know when this is safe to call.
func (f *forwarder) setClientConn(clientConn net.Conn) {
	f.clientConn = clientConn
	f.clientInterceptor = interceptor.NewBackendInterceptor(f.clientConn)
}

// setServerConn is a convenient helper to update serverConn, and will also
// create a matching interceptor for the given connection. It is the caller's
// responsibility to close the old connection before calling this, or there
// may be a leak.
//
// It is the responsibility of the caller to know when this is safe to call.
func (f *forwarder) setServerConn(serverConn net.Conn) {
	f.setServerConnAndInterceptor(serverConn, nil /* serverInterceptor */)
}

// setServerConnAndInterceptor, is similar to setServerConn, but takes in a
// serverInterceptor as well. That way, an existing interceptor can be used.
// If serverInterceptor=nil, an interceptor will be created for the given
// serverConn.
//
// See setServerConn for more information.
func (f *forwarder) setServerConnAndInterceptor(
	serverConn net.Conn, serverInterceptor *interceptor.FrontendInterceptor,
) {
	f.serverConn = serverConn
	if serverInterceptor == nil {
		f.serverInterceptor = interceptor.NewFrontendInterceptor(f.serverConn)
	} else {
		f.serverInterceptor = serverInterceptor
	}
	f.clientMessageTypeSent = clientMsgAny

	// Atomic is not needed since we will only call this during initialization
	// or when switching backends in the response processor, which in that case,
	// the request processor will be blocked, so there are no possible reads.
	f.atomics.isServerMsgReadyReceived = 1
}

// wrapClientToServerError overrides client to server errors for external
// consumption.
//
// TODO(jaylim-crl): We don't send any of these to the client today,
// unfortunately. At the moment, this is only used for metrics. See TODO in
// proxy_handler about sending safely to avoid corrupted packets. Handle these
// errors in a friendly manner.
func wrapClientToServerError(err error) error {
	if err == nil ||
		errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
		return nil
	}
	return newErrorf(codeClientDisconnected, "copying from client to target server: %v", err)
}

// wrapServerToClientError overrides server to client errors for external
// consumption.
//
// TODO(jaylim-crl): We don't send any of these to the client today,
// unfortunately. At the moment, this is only used for metrics. See TODO in
// proxy_handler about sending safely to avoid corrupted packets. Handle these
// errors in a friendly manner.
func wrapServerToClientError(err error) error {
	if err == nil ||
		errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
		return nil
	}
	return newErrorf(codeBackendDisconnected, "copying from target server to client: %s", err)
}

// errConnRecoverableSentinel exists as a sentinel value to denote that errors
// should not terminate the connection.
var errConnRecoverableSentinel = errors.New("connection recoverable error")

// markAsConnRecoverableError marks the given error with errConnRecoverableSentinel
// to denote that the connection can continue despite having an error.
func markAsConnRecoverableError(err error) error {
	return errors.Mark(err, errConnRecoverableSentinel)
}

// isConnRecoverableError checks whether a given error denotes that a connection
// is recoverable. If this is true, the caller should try to recover the
// connection (e.g. continue the forwarding process instead of terminating the
// forwarder).
func isConnRecoverableError(err error) bool {
	return errors.Is(err, errConnRecoverableSentinel)
}
