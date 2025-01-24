// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlproxyccl

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/balancer"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/interceptor"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	pgproto3 "github.com/jackc/pgproto3/v2"
)

// defaultTransferTimeout corresponds to the timeout period for the connection
// migration process. If the timeout gets triggered, and we're in a non
// recoverable state, the connection will be closed.
//
// This is a variable instead of a constant to support testing hooks.
var defaultTransferTimeout = 15 * time.Second

// Used in testing.
var transferConnectionConnectorTestHook func(
	context.Context, balancer.ConnectionHandle, string,
) (net.Conn, error) = nil

type transferContext struct {
	context.Context
	mu struct {
		syncutil.Mutex
		recoverableConn bool
	}
}

func newTransferContext(backgroundCtx context.Context) (*transferContext, context.CancelFunc) {
	transferCtx, cancel := context.WithTimeout(backgroundCtx, defaultTransferTimeout) // nolint:context
	ctx := &transferContext{
		Context: transferCtx,
	}
	ctx.mu.recoverableConn = true
	return ctx, cancel
}

func (t *transferContext) markRecoverable(r bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.recoverableConn = r
}

func (t *transferContext) isRecoverable() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.mu.recoverableConn
}

// tryBeginTransfer returns true if the transfer can be started, and false
// otherwise. If the transfer can be started, it updates the state of the
// forwarder to indicate that a transfer is in progress, and a cleanup function
// will be returned.
func (f *forwarder) tryBeginTransfer() (started bool, cleanupFn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Forwarder hasn't been initialized.
	if !f.mu.isInitialized {
		return false, nil
	}

	// Transfer is already in progress. No concurrent transfers are allowed.
	if f.mu.isTransferring {
		return false, nil
	}

	request, response := f.mu.request, f.mu.response
	request.mu.Lock()
	response.mu.Lock()
	defer request.mu.Unlock()
	defer response.mu.Unlock()

	if !isSafeTransferPointLocked(request, response) {
		return false, nil
	}

	// Once we mark the forwarder as transferring, attempt to suspend right
	// away before unlocking, but without blocking. This ensures that no other
	// messages are forwarded.
	f.mu.isTransferring = true
	request.mu.suspendReq = true
	response.mu.suspendReq = true

	return true, func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.mu.isTransferring = false
	}
}

// errTransferCannotStart is an error that indicates that the transfer cannot be
// started (e.g. transfer already in progress, or we're not at a safe transfer
// point). The caller should retry the transfer again if necessary.
var errTransferCannotStart = errors.New("transfer cannot be started")

// TransferConnection attempts a best-effort connection migration to an
// available SQL pod based on the load-balancing algorithm. If a transfer has
// already been started, or the forwarder has been closed, this returns an
// error. This is a best-effort process because there could be a situation
// where the forwarder is not in a state that is eligible for a connection
// migration.
//
// NOTE: If the forwarder hasn't been closed, TransferConnection has an invariant
// where the processors have been resumed prior to calling this method. When
// TransferConnection returns, it is guaranteed that processors will either be
// re-resumed, or the forwarder will be closed (in the case of a non-recoverable
// error).
//
// TransferConnection implements the balancer.ConnectionHandle interface.
func (f *forwarder) TransferConnection() (retErr error) {
	// A previous non-recoverable transfer would have closed the forwarder, so
	// return right away.
	if f.ctx.Err() != nil {
		return f.ctx.Err()
	}

	started, cleanupFn := f.tryBeginTransfer()
	if !started {
		return errTransferCannotStart
	}
	defer cleanupFn()

	f.metrics.ConnMigrationAttemptedCount.Inc(1)

	// Create a transfer context, and timeout handler which gets triggered
	// whenever the context expires. We have to close the forwarder because
	// the transfer may be blocked on I/O, and the only way for now is to close
	// the connections. This then allow TransferConnection to return and cleanup.
	ctx, cancel := newTransferContext(f.ctx)
	defer cancel()

	// Use a separate handler for timeouts. This is the only way to handle
	// blocked I/Os as described above.
	go func() {
		<-ctx.Done()
		// This Close call here in addition to the one in the defer callback
		// below is on purpose. This would help unblock situations where we're
		// blocked on sending/reading messages from connections that couldn't
		// be handled with context.Context.
		if !ctx.isRecoverable() {
			f.Close()
		}
	}()

	// Use a separate context for logging because f.ctx will be closed whenever
	// the connection is non-recoverable.
	//
	// TODO(jaylim-crl): There's a possible "use of Span after Finish" issue
	// where proxy_handler.handle returns before this function returns because
	// we're calling f.Close() in the timeout goroutine. When handle returns,
	// the context (with the span) gets cleaned up. Some ideas to fix this:
	// (1) errgroup (?), (2) use the stopper instead of the go keyword - that
	// should fork a new span, and avoid this issue.
	tBegin := timeutil.Now()
	logCtx := logtags.WithTags(context.Background(), logtags.FromContext(f.ctx))
	defer func() {
		latencyDur := timeutil.Since(tBegin)
		f.metrics.ConnMigrationAttemptedLatency.RecordValue(latencyDur.Nanoseconds())

		// When TransferConnection returns, it's either the forwarder has been
		// closed, or the procesors have been resumed.
		if !ctx.isRecoverable() {
			log.Infof(logCtx, "transfer failed: connection closed, latency=%v, err=%v", latencyDur, retErr)
			f.metrics.ConnMigrationErrorFatalCount.Inc(1)
			f.Close()
		} else {
			// Transfer was successful.
			if retErr == nil {
				log.Infof(logCtx, "transfer successful, latency=%v", latencyDur)
				f.metrics.ConnMigrationSuccessCount.Inc(1)
			} else {
				log.Infof(logCtx, "transfer failed: connection recovered, latency=%v, err=%v", latencyDur, retErr)
				f.metrics.ConnMigrationErrorRecoverableCount.Inc(1)
			}
			if err := f.resumeProcessors(); err != nil {
				log.Infof(logCtx, "unable to resume processors: %v", err)
				f.Close()
			}
		}
	}()

	// Suspend both processors before starting the transfer.
	request, response := f.getProcessors()
	if err := request.suspend(ctx); err != nil {
		return errors.Wrap(err, "suspending request processor")
	}
	if err := response.suspend(ctx); err != nil {
		return errors.Wrap(err, "suspending response processor")
	}

	// Transfer the connection.
	clientConn, serverConn := f.getConns()
	newServerConn, err := transferConnection(ctx, f, f.connector, f.metrics, clientConn, serverConn)
	if err != nil {
		return errors.Wrap(err, "transferring connection")
	}

	// Transfer was successful.
	f.replaceServerConn(newServerConn)
	return nil
}

// transferConnection performs the transfer operation for the current server
// connection, and returns the a new connection to the server that the
// connection got transferred to.
func transferConnection(
	ctx *transferContext,
	requester balancer.ConnectionHandle,
	connector *connector,
	metrics *metrics,
	clientConn, serverConn *interceptor.PGConn,
) (_ *interceptor.PGConn, retErr error) {
	ctx.markRecoverable(true)

	// Context was cancelled.
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	transferKey := uuid.MakeV4().String()

	// Send the SHOW TRANSFER STATE statement. At this point, connection is
	// non-recoverable because the message has already been sent to the server.
	ctx.markRecoverable(false)
	if err := runShowTransferState(serverConn, transferKey); err != nil {
		return nil, errors.Wrap(err, "sending transfer request")
	}

	transferErr, state, revivalToken, err := waitForShowTransferState(
		ctx, serverConn.ToFrontendConn(), clientConn, transferKey, metrics)
	if err != nil {
		return nil, errors.Wrap(err, "waiting for transfer state")
	}

	// Failures after this point are recoverable, and connections should not be
	// terminated.
	ctx.markRecoverable(true)

	// If we consumed until ReadyForQuery without errors, but the transfer state
	// response returns an error, we could still resume the connection, but the
	// transfer process will need to be aborted.
	//
	// This case may happen pretty frequently (e.g. open transactions, temporary
	// tables, etc.).
	if transferErr != "" {
		return nil, errors.Newf("%s", transferErr)
	}

	// Connect to a new SQL pod.
	connectFn := connector.OpenTenantConnWithToken
	if transferConnectionConnectorTestHook != nil {
		connectFn = transferConnectionConnectorTestHook
	}
	netConn, err := connectFn(ctx, requester, revivalToken)
	if err != nil {
		return nil, errors.Wrap(err, "opening connection")
	}
	defer func() {
		if retErr != nil {
			netConn.Close()
		}
	}()
	newServerConn := interceptor.NewPGConn(netConn)

	// Deserialize session state within the new SQL pod.
	if err := runAndWaitForDeserializeSession(
		ctx, newServerConn.ToFrontendConn(), state,
	); err != nil {
		return nil, errors.Wrap(err, "deserializing session")
	}

	return newServerConn, nil
}

// isSafeTransferPointLocked returns true if we're at a point where we're safe
// to transfer, and false otherwise.
var isSafeTransferPointLocked = func(request *processor, response *processor) bool {
	// Three conditions when evaluating a safe transfer point:
	//   1. The last message sent to the SQL pod was a Sync(S) or SimpleQuery(Q),
	//      and a ReadyForQuery(Z) has been received after.
	//   2. The last message sent to the SQL pod was a CopyDone(c), and a
	//      ReadyForQuery(Z) has been received after.
	//   3. The last message sent to the SQL pod was a CopyFail(f), and a
	//      ReadyForQuery(Z) has been received after.

	// The conditions above are not possible if this is true. They cannot be
	// equal since the same logical clock is used (except during initialization).
	if request.mu.lastMessageTransferredAt > response.mu.lastMessageTransferredAt {
		return false
	}

	// We need to check zero values here to handle the initialization case
	// since we would still want to be able to transfer connections which have
	// not made any queries to the server.
	switch pgwirebase.ClientMessageType(request.mu.lastMessageType) {
	case pgwirebase.ClientMessageType(0),
		pgwirebase.ClientMsgSync,
		pgwirebase.ClientMsgSimpleQuery,
		pgwirebase.ClientMsgCopyDone,
		pgwirebase.ClientMsgCopyFail:

		serverMsg := pgwirebase.ServerMessageType(response.mu.lastMessageType)
		return serverMsg == pgwirebase.ServerMsgReady || serverMsg == pgwirebase.ServerMessageType(0)
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
// operation into `run` and `wait` since doing so allows us to send the query
// ahead of time.
var runShowTransferState = func(w io.Writer, transferKey string) error {
	return writeQuery(w, "SHOW TRANSFER STATE WITH '%s'", transferKey)
}

// waitForShowTransferState retrieves the transfer state from the SQL pod
// through SHOW TRANSFER STATE WITH 'key'. It is assumed that the last message
// from the server was ReadyForQuery, so the server is ready to accept a query.
// Since ReadyForQuery may be for a previous pipelined query, this handles the
// forwarding of messages back to the client in case we don't see our state yet.
//
// metrics is optional, and if not nil, it will be used to record the transfer
// response message size in ConnMigrationTransferResponseMessageSize.
//
// WARNING: When using this, we assume that no other goroutines are using both
// serverConn and clientConn. In the context of a transfer, the response
// processor must be blocked to avoid concurrent reads from serverConn.
var waitForShowTransferState = func(
	ctx context.Context,
	serverConn *interceptor.FrontendConn,
	clientConn io.Writer,
	transferKey string,
	metrics *metrics,
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
	if err := expectDataRow(ctx, serverConn, func(msg *pgproto3.DataRow, size int) (bool, error) {
		// This has to be 4 since we validated RowDescription earlier.
		if len(msg.Values) != 4 {
			return false, nil
		}

		// Validate transfer key. It is possible that the end-user uses the SHOW
		// TRANSFER STATE WITH 'transfer_key' statement, but that isn't designed
		// for external usage, so it is fine to just terminate here if the
		// transfer key does not match.
		if string(msg.Values[3]) != transferKey {
			return false, nil
		}

		// NOTE: We have to cast to string and copy here since the slice
		// referenced in msg will no longer be valid once we read the next pgwire
		// message.
		transferErr, state, revivalToken = string(msg.Values[0]), string(msg.Values[1]), string(msg.Values[2])

		// Since the DataRow is valid, record response message size.
		if metrics != nil {
			metrics.ConnMigrationTransferResponseMessageSize.RecordValue(int64(size))
		}
		return true, nil
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
// serverConn and clientConn.
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
	if err := expectDataRow(ctx, serverConn, func(msg *pgproto3.DataRow, _ int) (bool, error) {
		return len(msg.Values) == 1 && string(msg.Values[0]) == "t", nil
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
	buf, err := query.Encode(nil)
	if err != nil {
		return errors.Wrap(err, "encoding SimpleQuery")
	}
	_, err = w.Write(buf)
	return err
}

// waitForSmallRowDescription waits until the next message from serverConn
// is a *small* RowDescription message (i.e. within 4K bytes), and one that
// passes matchFn. When that happens, this returns nil.
//
// For all other messages (i.e. non RowDescription or large messages), they will
// be forwarded to clientConn. One exception to this would be the ErrorResponse
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
		buf, err := msg.Encode(nil)
		if err != nil {
			return errors.Wrap(err, "encoding message")
		}
		if _, err := clientConn.Write(buf); err != nil {
			return errors.Wrap(err, "writing message")
		}
	}
}

// expectDataRow expects that the next message from serverConn is a DataRow
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
	validateFn func(*pgproto3.DataRow, int) (bool, error),
) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	_, size, err := serverConn.PeekMsg()
	if err != nil {
		return errors.Wrap(err, "peeking message")
	}
	msg, err := serverConn.ReadMsg()
	if err != nil {
		return errors.Wrap(err, "reading message")
	}
	pgMsg, ok := msg.(*pgproto3.DataRow)
	if !ok {
		return errors.Newf("unexpected message: %v", jsonOrRaw(msg))
	}
	if valid, err := validateFn(pgMsg, size); err != nil {
		return errors.Wrap(err, "validation failure")
	} else if !valid {
		return errors.Newf("validation failed for message: %v", jsonOrRaw(msg))
	}
	return nil
}

// expectCommandComplete expects that the next message from serverConn is a
// CommandComplete message with the input tag, and returns an error if it isn't.
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

// expectReadyForQuery expects that the next message from serverConn is a
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
