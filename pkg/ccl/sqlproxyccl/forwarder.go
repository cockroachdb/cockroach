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
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/interceptor"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	pgproto3 "github.com/jackc/pgproto3/v2"
)

const (
	// stateReady represents the state where the forwarder is ready to forward
	// packets from the client to the server (and vice-versa).
	stateReady int = iota
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
	// errReadAbortedDueToTransfer is returned whenever a Read call exits due to
	// a session transfer.
	errReadAbortedDueToTransfer = errors.New("read aborted due to transfer")

	// errTransferTimeout denotes that a transfer process has timed out where
	// the forwarder wasn't able to locate the right transfer state response in
	// time
	errTransferTimeout = errors.New("transfer timeout")

	// errTransferProtocol indicates that an invariant has failed.
	errTransferProtocol = errors.New("transfer protocol error")
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

	// mu contains state protected by the forwarder's mutex. This is necessary
	// since fields will be read and write from different goroutines.
	mu struct {
		syncutil.Mutex

		// state represents the forwarder's state. Most of the time, this will
		// be stateReady.
		state int

		// isServerMsgReadyReceived denotes whether a ReadyForQuery message has
		// been received by the server-to-client processor *after* a message has
		// been sent to the server through a Write on serverConn, either directly
		// or through ForwardMsg.
		//
		// This will be initialized to true to implicitly denote that the server
		// is ready to accept queries.
		isServerMsgReadyReceived bool

		// transferKey is a unique string used to identify the transfer request,
		// and will be passed into the SHOW TRANSFER STATE statement. This will
		// be set to a randomly generated UUID whenever the transfer is
		// requested through the RequestTransfer API, and back to an empty
		// string whenever the transfer completes successfully or with a
		// recoverable error.
		transferKey string

		// transferCloserCh is a channel that must be set **before**
		// transitioning to the transferRequested state, and this must be closed
		// whenever the forwarder transitions back to the ready state, which
		// signifies that the transfer process has completed successfully.
		// Closing this will unblock the client-to-server processor and stop the
		// timeout handler.
		transferCloserCh chan struct{}

		// transferCtx has to be derived from ctx, and is created by the
		// timeout handler. All transfer related operations will use this so
		// that they can react to the timeout handler when that gets triggered.
		transferCtx context.Context

		// transferConnRecoverable denotes whether the connection is recoverable
		// during the transfer phase. This will be used by the timeout handler
		// to determine whether it should close the forwarder.
		transferConnRecoverable bool
	}

	// ------------------------------------------------------------------------
	// The following fields are used for connection migration.
	//
	// For details on how connection migration works, read the following RFC:
	// https://github.com/cockroachdb/cockroach/pull/75707.
	// ------------------------------------------------------------------------

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

	// Knobs used for testing.
	testingKnobs struct {
		onTransferTimeoutHandlerStart  func()
		onTransferTimeoutHandlerFinish func()

		awaitTransferStateResponse func(
			context.Context, pgwirebase.ServerMessageType,
		) (string, string, error)
		isValidStartTransferStateResponse func(*pgproto3.RowDescription) bool
		isValidEndTransferStateResponse   func(*pgproto3.CommandComplete) bool
		parseTransferStateResponse        func(*pgproto3.DataRow, string) (string, string, string, error)

		deserializeSession func(
			context.Context, net.Conn, *interceptor.FrontendInterceptor, string,
		) error

		transferTimeoutDuration func() time.Duration
	}
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
	// mainly used to unblock the client-to-server processor whenever the
	// forwarder has stopped, or a transfer has been requested.
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
		// requested. This allows the client-to-server processor to be freed up
		// to start the transfer.
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.mu.state != stateReady {
			return errReadAbortedDueToTransfer
		}
		return nil
	})

	f.setClientConn(clientConn)
	f.setServerConn(serverConn)

	// Start client-to-server and server-to-client processors. We will copy all
	// pgwire messages from client to server (and vice-versa) until we encounter
	// an error, or a shutdown signal (i.e. context cancellation).
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
	// We'll get an error if the forwarder is already in one of the transfer
	// states. In that case, just ignore it since we want RequestTransfer to
	// be idempotent.
	_ = f.prepareTransfer()
}

// handleClientToServer handles the communication from the client to the server.
// This returns a context cancellation error whenever the forwarder's context
// is cancelled, or whenever forwarding fails.
func (f *forwarder) handleClientToServer() error {
	for f.ctx.Err() == nil {
		// Always peek the message to ensure that we're blocked on reading the
		// header, rather than when forwarding.
		typ, _, err := f.clientInterceptor.PeekMsg()
		if err != nil && !errors.Is(err, errReadAbortedDueToTransfer) {
			return errors.Wrap(err, "peeking message in client-to-server")
		}

		// Note that if state changes the moment we unlock mu, that's fine.
		// The fact that we got here signifies that there was already a message
		// in the interceptor's buffer, which is valid for the state that was
		// stale. Since this can only happen for the ready->transferRequested
		// case, it follows that when a transfer gets requested the moment the
		// message was read, we'll finish forwarding that last message before
		// starting the transfer in the next iteration.
		f.mu.Lock()
		localState := f.mu.state
		f.mu.Unlock()

		switch localState {
		case stateReady:
			// If we exit PeekMsg due to a transfer, the state must be in
			// stateTransferRequested unless there's a bug. Be defensive here
			// and peek again so that we don't end up blocking on the peek
			// call within ForwardMsg because client interrupts will be
			// disabled.
			if errors.Is(err, errReadAbortedDueToTransfer) {
				log.Error(f.ctx, "read aborted in client-to-server, but state is ready")
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

				f.mu.Lock()
				f.mu.isServerMsgReadyReceived = false
				f.mu.Unlock()

				// When ForwardMsg gets blocked on Read, we will unblock that
				// through our custom readTimeoutConn wrapper.
				_, err := f.clientInterceptor.ForwardMsg(f.serverConn)
				return err
			}(); forwardErr != nil {
				return errors.Wrap(forwardErr, "forwarding message in server-to-client")
			}

		case stateTransferRequested:
			// Can we perform the transfer?
			if !f.isSafeTransferPoint() {
				// Abort the transfer safely.
				if err := f.finishTransfer(); err != nil {
					return errors.Wrap(errTransferProtocol,
						"aborting transfer due to unsafe transfer point")
				}
				continue
			}

			// Update the state first so that the server-to-client processor
			// could start processing. If we update the state after sending the
			// request, we may miss response messages.
			f.mu.Lock()
			f.mu.state = stateTransferInProgress
			key, closer := f.mu.transferKey, f.mu.transferCloserCh
			f.mu.Unlock()

			// Timeout handler begins when we send a transfer state request
			// message to the server.
			timeout := defaultTransferTimeout
			if f.testingKnobs.transferTimeoutDuration != nil {
				timeout = f.testingKnobs.transferTimeoutDuration()
			}
			f.runTransferTimeoutHandler(timeout)

			// Once we send the request, the forwarder should not send any
			// further messages to the server. Since requests and responses
			// are in a FIFO order, we can guarantee that the server will no
			// longer return messages intended for the client once we receive
			// responses for the SHOW TRANSFER STATE query.
			if err := writeTransferStateRequest(f.serverConn, key); err != nil {
				return errors.Wrap(err, "writing transfer state request")
			}

			// Wait until transfer is completed. Client-to-server processor is
			// blocked to ensure that we don't send more client messagess to
			// the server.
			select {
			case <-f.ctx.Done():
				return f.ctx.Err()
			case <-closer:
				// Channel is closed whenever transfer completes, so we are done.
			}

		case stateTransferInProgress:
			// This cannot happen unless there is a bug. While the transfer is
			// in progress, the client-to-server processor has to be blocked,
			// and the only way to transition into this state is to go through
			// the stateTransferRequested state.
			//
			// Return an error to close the connection, rather than letting it
			// continue silently.
			return errors.Wrap(errTransferProtocol,
				"transferInProgress state in client-to-server processor")
		}
	}
	return f.ctx.Err()
}

// handleServerToClient handles the communication from the server to the client.
// This returns an error whenever the forwarder's context is cancelled, or the
// connection can no longer be used due to the state of the server (e.g. failed
// forwarding, or non-recoverable transfers).
func (f *forwarder) handleServerToClient() error {
	for f.ctx.Err() == nil {
		// Always peek the message to ensure that we're blocked on reading the
		// header, rather than when forwarding or reading the entire message.
		typ, _, err := f.serverInterceptor.PeekMsg()
		if err != nil {
			return errors.Wrap(err, "peeking message in server-to-client")
		}

		// When we unlock mu, localState may be stale when transitioning from
		// ready->transferRequested, or transferRequested->transferInProgress.
		// This is fine because the moment we got here, we know that there must
		// be a message in the interceptor's buffer, and that is valid for the
		// previous state, so finish up the current message first.
		localState := func() int {
			f.mu.Lock()
			defer f.mu.Unlock()

			// Have we seen a ReadyForQuery message?
			//
			// It doesn't matter which state we're in. Even if the transfer
			// message has already been sent, the first message that we're going
			// to be looking for isn't ReadyForQuery. This is only used to
			// determine a safe transfer point.
			if typ == pgwirebase.ServerMsgReady {
				f.mu.isServerMsgReadyReceived = true
			}

			return f.mu.state
		}()

		switch localState {
		case stateReady, stateTransferRequested:
			// When ForwardMsg gets blocked on Read, we will unblock that by
			// closing serverConn through f.Close().
			if _, err := f.serverInterceptor.ForwardMsg(f.clientConn); err != nil {
				return errors.Wrap(err, "forwarding message in server-to-client")
			}

		case stateTransferInProgress:
			if err := f.processTransfer(); err != nil {
				// Connection is not recoverable; terminate it right away.
				if !isConnRecoverableError(err) {
					return errors.Wrap(err,
						"terminating due to non-recoverable connection during transfer")
				}
				log.Infof(f.ctx, "transfer failed, but connection is recoverable: %s", err)
			} else {
				log.Infof(f.ctx, "transfer successful")
			}
			if err := f.finishTransfer(); err != nil {
				return errors.Wrap(errTransferProtocol, "wrapping up transfer process")
			}
		}
	}
	return f.ctx.Err()
}

// isSafeTransferPoint returns true if we're at a point where we're safe to
// transfer, and false otherwise. This should only be called during the
// transferRequested state.
func (f *forwarder) isSafeTransferPoint() bool {
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
	switch f.clientMessageTypeSent {
	case clientMsgAny,
		pgwirebase.ClientMsgSync,
		pgwirebase.ClientMsgSimpleQuery,
		pgwirebase.ClientMsgCopyDone,
		pgwirebase.ClientMsgCopyFail:
		f.mu.Lock()
		defer f.mu.Unlock()

		return f.mu.isServerMsgReadyReceived
	default:
		return false
	}
}

// prepareTransfer sets up the transfer metadata. This moves the forwarder into
// the transferRequested state, and generates a unique transfer key for the
// forwarder. If the forwarder's state is not ready, this will return an error.
func (f *forwarder) prepareTransfer() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.mu.state != stateReady {
		return errors.New("transfer is already in-progress")
	}
	f.mu.transferKey = uuid.MakeV4().String()
	f.mu.transferCloserCh = make(chan struct{})
	f.mu.state = stateTransferRequested
	f.mu.transferCtx = nil
	f.mu.transferConnRecoverable = false
	return nil
}

// finishTransfer moves the forwarder back to the ready state, and closes the
// transferCloser channel (which unblocks the client-to-server processor). This
// returns an error if it is called during the steady state.
//
// NOTE: This should only be called if the connection is safe to continue
// because this unblocks the client-to-server processor, which may result in
// more packets being sent to the server. If the connection is unsafe to
// proceed, we should just call Close().
func (f *forwarder) finishTransfer() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.mu.state == stateReady {
		return errors.New("no transfer in-progress")
	}
	f.mu.transferKey = ""
	// This nil case should not happen, but we'll check to avoid closing nil
	// channels, which causes panics.
	if f.mu.transferCloserCh != nil {
		close(f.mu.transferCloserCh)
	}
	f.mu.transferCloserCh = nil
	f.mu.state = stateReady
	f.mu.transferCtx = nil
	f.mu.transferConnRecoverable = false
	return nil
}

// processTransfer attempts to perform the connection migration, and blocks until
// the connection has been migrated, or an error has occurred. If the connection
// has been migrated successfully, retErr == nil.
//
// If retErr != nil, the forwarder has to be closed by the caller to prevent any
// data corruption, with one exception: the caller may choose to abort the
// transfer process and continue with the fowarding if and only if the error
// has been marked with errConnRecoverableSentinel, which can be verified with
// isConnRecoverableError().
//
// NOTE: f.mu.transferCtx has to be set before calling this. We use transferCtx
// instead of ctx here to ensure that we can recover if we fail to connect, or
// deserialize the session. Binding to ctx means the only way to abort is to
// close the forwarder.
func (f *forwarder) processTransfer() (retErr error) {
	f.mu.Lock()
	transferCtx := f.mu.transferCtx
	f.mu.Unlock()

	if transferCtx == nil {
		return errors.Wrap(errTransferProtocol, "transferCtx is nil")
	}

	// Pass serverMsgAny because we do not know what message to expect yet.
	state, revivalToken, err := f.awaitTransferStateResponse(transferCtx, serverMsgAny)
	if err != nil {
		// Some errors may be recoverable, but those are handled in
		// awaitTransferStateResponse, and marked accordingly.
		return err
	}

	f.mu.Lock()
	f.mu.transferConnRecoverable = true
	f.mu.Unlock()

	// Connect to a new SQL pod.
	//
	// TODO(jaylim-crl): There is a possibility where the same pod will get
	// selected. Some ideas to solve this: pass in the remote address of
	// serverConn to avoid choosing that pod, or maybe a filter callback?
	// Will handle this later.
	newServerConn, err := f.connect.OpenTenantConnWithToken(transferCtx, revivalToken)
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
	deserializeFn := deserializeSession
	if f.testingKnobs.deserializeSession != nil {
		deserializeFn = f.testingKnobs.deserializeSession
	}
	err = deserializeFn(transferCtx, newServerConn, newServerInterceptor, state)
	if err != nil {
		return markAsConnRecoverableError(err)
	}

	// Transfer was successful - use the new server connections.
	f.serverConn.Close()
	f.setServerConnAndInterceptor(newServerConn, newServerInterceptor)
	return nil
}

// runTransferTimeoutHandler starts a timeout handler in the background for a
// duration of waitTimeout until the transfer completes; this happens whenever
// the transferCloserCh channel is closed. If the transfer doesn't complete by
// the given duration, the forwarder will be closed if we're in a non-recoverable
// state.
//
// NOTE: This should only be called during a transfer process. We assume that
// transferCloserCh has already been initialized.
func (f *forwarder) runTransferTimeoutHandler(waitTimeout time.Duration) {
	f.mu.Lock()
	closer := f.mu.transferCloserCh
	// This lint rule is intended; transferCtx isn't used here.
	transferCtx, cancel := context.WithTimeout(f.ctx, waitTimeout) // nolint:context
	f.mu.transferCtx = transferCtx
	f.mu.Unlock()

	// We use a goroutine instead of the return value in the processors to
	// allow us to unblock writeTransferStateRequest if the write to the server
	// took a long time.
	go func() {
		defer cancel()

		if f.testingKnobs.onTransferTimeoutHandlerStart != nil {
			f.testingKnobs.onTransferTimeoutHandlerStart()
		}
		select {
		case <-f.ctx.Done():
			// Forwarder's context was cancelled. Do nothing.
		case <-closer:
			// Transfer has completed.
		case <-transferCtx.Done():
			f.mu.Lock()
			recoverable := f.mu.transferConnRecoverable
			f.mu.Unlock()

			// Connection is recoverable, don't close the connection. Context
			// cancellation will be propagated up accordingly.
			if recoverable {
				break
			}

			// If we're waiting for a message through the server's interceptor,
			// this will unblock that call with a closed pipe. If we're busy
			// processing other messages, the cancelled context will eventually
			// be read.
			//
			// We send a message to f.errCh first before closing the forwarder
			// to ensure that we don't get a context cancellation in errCh
			// when we unblock the server interceptor.
			select {
			case f.errCh <- errTransferTimeout: /* error reported */
			default: /* the channel already contains an error */
			}
			f.Close()
		}
		if f.testingKnobs.onTransferTimeoutHandlerFinish != nil {
			f.testingKnobs.onTransferTimeoutHandlerFinish()
		}
	}()
}

// setClientConn is a convenient helper to update clientConn, and will also
// create a matching interceptor for the given connection. It is the caller's
// responsibility to close the old connection before calling this, or there
// may be a leak.
//
// It is the responsibility of the caller to know when this is safe to call
// since this updates clientConn and clientInterceptor, and is not thread-safe.
func (f *forwarder) setClientConn(clientConn net.Conn) {
	f.clientConn = clientConn
	f.clientInterceptor = interceptor.NewBackendInterceptor(f.clientConn)
}

// setServerConn is a convenient helper to update serverConn, and will also
// create a matching interceptor for the given connection. It is the caller's
// responsibility to close the old connection before calling this, or there
// may be a leak.
//
// It is the responsibility of the caller to know when this is safe to call
// since this updates serverConn and serverInterceptor, and is not thread-safe.
func (f *forwarder) setServerConn(serverConn net.Conn) {
	f.setServerConnAndInterceptor(serverConn, nil /* serverInterceptor */)
}

// setServerConnAndInterceptor, is similar to setServerConn, but takes in a
// serverInterceptor as well. That way, an existing interceptor can be used.
// If serverInterceptor is nil, an interceptor will be created for the given
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

	// This method will only be called during initialization, or whenever the
	// transfer is being processed, which in this case, there are no reads on
	// this variable, so there won't be a race.
	f.mu.isServerMsgReadyReceived = true
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
