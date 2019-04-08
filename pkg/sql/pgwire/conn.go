// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package pgwire

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtags"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/lib/pq/oid"
	"github.com/pkg/errors"
)

const (
	authOK                int32 = 0
	authCleartextPassword int32 = 3
)

// conn implements a pgwire network connection (version 3 of the protocol,
// implemented by Postgres v7.4 and later). conn.serve() reads protocol
// messages, transforms them into commands that it pushes onto a StmtBuf (where
// they'll be picked up and executed by the connExecutor).
// The connExecutor produces results for the commands, which are delivered to
// the client through the sql.ClientComm interface, implemented by this conn
// (code is in command_result.go).
type conn struct {
	conn net.Conn

	sessionArgs sql.SessionArgs
	metrics     *ServerMetrics

	// rd is a buffered reader consuming conn. All reads from conn go through
	// this.
	rd bufio.Reader

	// parser is used to avoid allocating a parser each time.
	parser parser.Parser

	// stmtBuf is populated with commands queued for execution by this conn.
	stmtBuf sql.StmtBuf

	// err is an error, accessed atomically. It represents any error encountered
	// while accessing the underlying network connection. This can read via
	// GetErr() by anybody. If it is found to be != nil, the conn is no longer to
	// be used.
	err atomic.Value

	// writerState groups together all aspects of the write-side state of the
	// connection.
	writerState struct {
		fi flushInfo
		// buf contains command results (rows, etc.) until they're flushed to the
		// network connection.
		buf    bytes.Buffer
		tagBuf [64]byte
	}

	readBuf    pgwirebase.ReadBuffer
	msgBuilder writeBuffer

	sv *settings.Values
}

type authOptions struct {
	skipAuth bool                            // test-only
	authHook func(ctx context.Context) error // test-only
	insecure bool
	auth     *hba.Conf
	ie       *sql.InternalExecutor
}

// serveConn creates a conn that will serve the netConn. It returns once the
// network connection is closed.
//
// Internally, a connExecutor will be created to execute commands. Commands read
// from the network are buffered in a stmtBuf which is consumed by the
// connExecutor. The connExecutor produces results which are buffered and
// sometimes synchronously flushed to the network.
//
// The reader goroutine (this one) outlives the connExecutor's goroutine (the
// "processor goroutine").
// However, they can both signal each other to stop. Here's how the different
// cases work:
// 1) The reader receives a ClientMsgTerminate protocol packet: the reader
// closes the stmtBuf and also cancels the command processing context. These
// actions will prompt the command processor to finish.
// 2) The reader gets a read error from the network connection: like above, the
// reader closes the command processor.
// 3) The reader's context is canceled (happens when the server is draining but
// the connection was busy and hasn't quit yet): the reader notices the canceled
// context and, like above, closes the processor.
// 4) The processor encouters an error. This error can come from various fatal
// conditions encoutered internally by the processor, or from a network
// communication error encountered while flushing results to the network.
// The processor will cancel the reader's context and terminate.
// Note that query processing errors are different; they don't cause the
// termination of the connection.
//
// Draining notes:
//
// The reader notices that the server is draining by polling the draining()
// closure passed to serveConn. At that point, the reader delegates the
// responsibility of closing the connection to the statement processor: it will
// push a DrainRequest to the stmtBuf which signal the processor to quit ASAP.
// The processor will quit immediately upon seeing that command if it's not
// currently in a transaction. If it is in a transaction, it will wait until the
// first time a Sync command is processed outside of a transaction - the logic
// being that we want to stop when we're both outside transactions and outside
// batches.
func serveConn(
	ctx context.Context,
	netConn net.Conn,
	sArgs sql.SessionArgs,
	metrics *ServerMetrics,
	reserved mon.BoundAccount,
	sqlServer *sql.Server,
	draining func() bool,
	authOpt authOptions,
	stopper *stop.Stopper,
) {
	sArgs.RemoteAddr = netConn.RemoteAddr()

	if log.V(2) {
		log.Infof(ctx, "new connection with options: %+v", sArgs)
	}

	c := newConn(netConn, sArgs, metrics, &sqlServer.GetExecutorConfig().Settings.SV)

	// Do the reading of commands from the network.
	c.serveImpl(ctx, draining, sqlServer, reserved, authOpt, stopper)
}

func newConn(
	netConn net.Conn, sArgs sql.SessionArgs, metrics *ServerMetrics, sv *settings.Values,
) *conn {
	c := &conn{
		conn:        netConn,
		sessionArgs: sArgs,
		metrics:     metrics,
		rd:          *bufio.NewReader(netConn),
		sv:          sv,
	}
	c.stmtBuf.Init()
	c.writerState.fi.buf = &c.writerState.buf
	c.writerState.fi.lastFlushed = -1
	c.writerState.fi.cmdStarts = make(map[sql.CmdPos]int)
	c.msgBuilder.init(metrics.BytesOutCount)

	return c
}

func (c *conn) setErr(err error) {
	c.err.Store(err)
}

func (c *conn) GetErr() error {
	err := c.err.Load()
	if err != nil {
		return err.(error)
	}
	return nil
}

// serveImpl continuously reads from the network connection and pushes execution
// instructions into a sql.StmtBuf, from where they'll be processed by a command
// "processor" goroutine (a connExecutor).
// The method returns when the pgwire termination message is received, when
// network communication fails, when the server is draining or when ctx is
// canceled (which also happens when draining (but not from the get-go), and
// when the processor encounters a fatal error).
//
// serveImpl always closes the network connection before returning.
//
// sqlServer is used to create the command processor. As a special facility for
// tests, sqlServer can be nil, in which case the command processor and the
// write-side of the connection will not be created.
func (c *conn) serveImpl(
	ctx context.Context,
	draining func() bool,
	sqlServer *sql.Server,
	reserved mon.BoundAccount,
	authOpt authOptions,
	stopper *stop.Stopper,
) {
	defer func() { _ = c.conn.Close() }()

	ctx = logtags.AddTag(ctx, "user", c.sessionArgs.User)

	// NOTE: We're going to write a few messages to the connection in this method,
	// for the handshake. After that, all writes are done async, in the
	// startWriter() goroutine.

	ctx, cancelConn := context.WithCancel(ctx)
	defer cancelConn() // This calms the linter that wants these callbacks to always be called.

	var sentDrainSignal bool
	// The net.Conn is switched to a conn that exits if the ctx is canceled.
	c.conn = newReadTimeoutConn(c.conn, func() error {
		// If the context was canceled, it's time to stop reading. Either a
		// higher-level server or the command processor have canceled us.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// If the server is draining, we'll let the processor know by pushing a
		// DrainRequest. This will make the processor quit whenever it finds a good
		// time.
		if !sentDrainSignal && draining() {
			_ /* err */ = c.stmtBuf.Push(ctx, sql.DrainRequest{})
			sentDrainSignal = true
		}
		return nil
	})
	c.rd = *bufio.NewReader(c.conn)

	// We'll build an authPipe to communicate with the authentication process.
	authPipe := newAuthPipe(c)
	var authenticator authenticator = authPipe

	// procCh is the channel on which we'll receive the termination signal from
	// the command processor.
	var procCh <-chan error

	if sqlServer != nil {
		// Spawn the command processing goroutine, which also handles connection
		// authentication). It will notify us when it's done through procCh, and
		// we'll also interact with the authentication process through ac.
		var ac AuthConn = authPipe
		procCh = c.processCommandsAsync(ctx, authOpt, ac, sqlServer, reserved, cancelConn)
	} else {
		// sqlServer == nil means we are in a local test. In this case
		// we only need the minimum to make pgx happy.
		var err error
		for param, value := range testingStatusReportParams {
			if err := c.sendStatusParam(param, value); err != nil {
				break
			}
		}
		if err != nil {
			reserved.Close(ctx)
			return
		}
		var ac AuthConn = authPipe
		// Simulate auth succeeding.
		ac.AuthOK(fixedIntSizer{size: types.Int})
		dummyCh := make(chan error)
		close(dummyCh)
		procCh = dummyCh
		// An initial readyForQuery message is part of the handshake.
		c.msgBuilder.initMsg(pgwirebase.ServerMsgReady)
		c.msgBuilder.writeByte(byte(sql.IdleTxnBlock))
		if err := c.msgBuilder.finishMsg(c.conn); err != nil {
			reserved.Close(ctx)
			return
		}
	}

	var err error
	var terminateSeen bool
	var doingExtendedQueryMessage bool

	// We need an intSizer, which we're ultimately going to get from the
	// authenticator once authentication succeeds (because it will actually be a
	// ConnectionHandler). Until then, we unfortunately still need some intSizer
	// because we technically might enqueue parsed statements in the statement
	// buffer even before authentication succeeds (because we need this go routine
	// to keep reading from the network connection while authentication is in
	// progress in order to react to the connection closing).
	var intSizer unqualifiedIntSizer = fixedIntSizer{size: types.Int}
	var authDone bool
Loop:
	for {
		var typ pgwirebase.ClientMessageType
		var n int
		typ, n, err = c.readBuf.ReadTypedMsg(&c.rd)
		c.metrics.BytesInCount.Inc(int64(n))
		if err != nil {
			break Loop
		}
		timeReceived := timeutil.Now()
		log.VEventf(ctx, 2, "pgwire: processing %s", typ)

		if !authDone {
			if typ == pgwirebase.ClientMsgPassword {
				var pwd []byte
				if pwd, err = c.readBuf.GetBytes(n - 4); err != nil {
					break Loop
				}
				// Pass the data to the authenticator. This hopefully causes it to finish
				// authentication in the background and give us an intSizer when we loop
				// around.
				if err = authenticator.sendPwdData(pwd); err != nil {
					break Loop
				}
				continue
			}
			// Wait for the auth result.
			intSizer, err = authenticator.authResult()
			if err != nil {
				// The error has already been sent to the client.
				break Loop
			} else {
				authDone = true
			}
		}

		switch typ {
		case pgwirebase.ClientMsgPassword:
			// This messages are only acceptable during the auth phase, handled above.
			err = pgwirebase.NewProtocolViolationErrorf("unexpected authentication data")
			_ /* err */ = writeErr(
				ctx, &sqlServer.GetExecutorConfig().Settings.SV, err,
				&c.msgBuilder, &c.writerState.buf)
			break Loop
		case pgwirebase.ClientMsgSimpleQuery:
			if doingExtendedQueryMessage {
				if err = c.stmtBuf.Push(
					ctx,
					sql.SendError{
						Err: pgwirebase.NewProtocolViolationErrorf(
							"SimpleQuery not allowed while in extended protocol mode"),
					},
				); err != nil {
					break
				}
			}
			if err = c.handleSimpleQuery(
				ctx, &c.readBuf, timeReceived, intSizer.GetUnqualifiedIntSize(),
			); err != nil {
				break
			}
			err = c.stmtBuf.Push(ctx, sql.Sync{})

		case pgwirebase.ClientMsgExecute:
			doingExtendedQueryMessage = true
			err = c.handleExecute(ctx, &c.readBuf, timeReceived)

		case pgwirebase.ClientMsgParse:
			doingExtendedQueryMessage = true
			err = c.handleParse(ctx, &c.readBuf, intSizer.GetUnqualifiedIntSize())

		case pgwirebase.ClientMsgDescribe:
			doingExtendedQueryMessage = true
			err = c.handleDescribe(ctx, &c.readBuf)

		case pgwirebase.ClientMsgBind:
			doingExtendedQueryMessage = true
			err = c.handleBind(ctx, &c.readBuf)

		case pgwirebase.ClientMsgClose:
			doingExtendedQueryMessage = true
			err = c.handleClose(ctx, &c.readBuf)

		case pgwirebase.ClientMsgTerminate:
			terminateSeen = true
			break Loop

		case pgwirebase.ClientMsgSync:
			doingExtendedQueryMessage = false
			// We're starting a batch here. If the client continues using the extended
			// protocol and encounters an error, everything until the next sync
			// message has to be skipped. See:
			// https://www.postgresql.org/docs/current/10/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY

			err = c.stmtBuf.Push(ctx, sql.Sync{})

		case pgwirebase.ClientMsgFlush:
			doingExtendedQueryMessage = true
			err = c.handleFlush(ctx)

		case pgwirebase.ClientMsgCopyData, pgwirebase.ClientMsgCopyDone, pgwirebase.ClientMsgCopyFail:
			// We're supposed to ignore these messages, per the protocol spec. This
			// state will happen when an error occurs on the server-side during a copy
			// operation: the server will send an error and a ready message back to
			// the client, and must then ignore further copy messages. See:
			// https://github.com/postgres/postgres/blob/6e1dd2773eb60a6ab87b27b8d9391b756e904ac3/src/backend/tcop/postgres.c#L4295

		default:
			err = c.stmtBuf.Push(
				ctx,
				sql.SendError{Err: pgwirebase.NewUnrecognizedMsgTypeErr(typ)})
		}
		if err != nil {
			break Loop
		}
	}

	// We're done reading data from the client, so make the communication
	// goroutine stop. Depending on what that goroutine is currently doing (or
	// blocked on), we cancel and close all the possible channels to make sure we
	// tickle it in the right way.

	// Signal command processing to stop. It might be the case that the processor
	// canceled our context and that's how we got here; in that case, this will
	// be a no-op.
	c.stmtBuf.Close()
	// Cancel the processor's context.
	cancelConn()
	// In case the authenticator is blocked on waiting for data from the client,
	// tell it that there's no more data coming. This is a no-op if authentication
	// was completed already.
	authenticator.noMorePwdData()

	// Wait for the processor goroutine to finish, if it hasn't already. We're
	// ignoring the error we get from it, as we have no use for it. It might be a
	// connection error, or a context cancelation error case this goroutine is the
	// one that triggered the execution to stop.
	<-procCh

	if terminateSeen {
		return
	}
	// If we're draining, let the client know by piling on an AdminShutdownError
	// and flushing the buffer.
	if draining() {
		// TODO(andrei): I think sending this extra error to the client if we also
		// sent another error for the last query (like a context canceled) is a bad
		// idead; see #22630. I think we should find a way to return the
		// AdminShutdown error as the only result of the query.
		_ /* err */ = writeErr(ctx, &sqlServer.GetExecutorConfig().Settings.SV,
			newAdminShutdownErr(ErrDrainingExistingConn), &c.msgBuilder, &c.writerState.buf)
		_ /* n */, _ /* err */ = c.writerState.buf.WriteTo(c.conn)
	}
}

// unqualifiedIntSizer is used by a conn to get the SQL session's current int size
// setting.
//
// It's a restriction on the ConnectionHandler type.
type unqualifiedIntSizer interface {
	// GetUnqualifiedIntSize returns the size that the parser should consider for an
	// unqualified INT.
	GetUnqualifiedIntSize() *types.T
}

type fixedIntSizer struct {
	size *types.T
}

func (f fixedIntSizer) GetUnqualifiedIntSize() *types.T {
	return f.size
}

// processCommandsAsync spawns a goroutine that authenticates the connection and
// then processes commands from c.stmtBuf.
//
// It returns a channel that will be signaled when this goroutine is done.
// Whatever error is returned on that channel has already been written to the
// client connection, if applicable.
//
// If authentication fails, this goroutine finishes and, as always, cancelConn
// is called.
//
// Args:
// ac: An interface used by the authentication process to receive password data
//   and to ultimately declare the authentication successful.
// reserved: Reserved memory. This method takes ownership.
// cancelConn: A function to be called when this goroutine exits. Its goal is to
//   cancel the connection's context, thus stopping the connection's goroutine.
//   The returned channel is also closed before this goroutine dies, but the
//   connection's goroutine is not expected to be reading from that channel
//   (instead, it's expected to always be monitoring the network connection).
func (c *conn) processCommandsAsync(
	ctx context.Context,
	authOpt authOptions,
	ac AuthConn,
	sqlServer *sql.Server,
	reserved mon.BoundAccount,
	cancelConn func(),
) <-chan error {
	// reservedOwned is true while we own reserved, false when we pass ownership
	// away.
	reservedOwned := true
	retCh := make(chan error, 1)
	go func() {
		var retErr error
		var connHandler sql.ConnectionHandler
		var authOK bool
		defer func() {
			// Release resources, if we still own them.
			if reservedOwned {
				reserved.Close(ctx)
			}
			// Notify the connection's goroutine that we're terminating. The
			// connection might know already, as it might have triggered this
			// goroutine's finish, but it also might be us that we're triggering the
			// connection's death. This context cancelation serves to interrupt a
			// network read on the connection's goroutine.
			cancelConn()

			pgwireKnobs := sqlServer.GetExecutorConfig().PGWireTestingKnobs
			if pgwireKnobs != nil && pgwireKnobs.CatchPanics {
				if r := recover(); r != nil {
					// Catch the panic and return it to the client as an error.
					pge := pgerror.NewErrorf(pgerror.CodeCrashShutdownError, "caught fatal error: %v", r)
					pge.Detail = string(debug.Stack())
					retErr = pge
					_ = writeErr(
						ctx, &sqlServer.GetExecutorConfig().Settings.SV, retErr,
						&c.msgBuilder, &c.writerState.buf)
					_ /* n */, _ /* err */ = c.writerState.buf.WriteTo(c.conn)
					c.stmtBuf.Close()
					// Send a ready for query to make sure the client can react.
					// TODO(andrei, jordan): Why are we sending this exactly?
					c.bufferReadyForQuery('I')
				}
			}
			if !authOK {
				ac.AuthFail(retErr)
			}
			// Inform the connection goroutine of success or failure.
			retCh <- retErr
		}()

		// Authenticate the connection.
		if !authOpt.skipAuth {
			if authOpt.authHook != nil {
				if retErr = authOpt.authHook(ctx); retErr != nil {
					return
				}
			} else {
				if retErr = c.handleAuthentication(
					ctx, ac, authOpt.insecure, authOpt.ie, authOpt.auth,
					sqlServer.GetExecutorConfig(),
				); retErr != nil {
					return
				}
			}
		}

		connHandler, retErr = c.sendInitialConnData(ctx, sqlServer)
		if retErr != nil {
			return
		}
		ac.AuthOK(connHandler)
		authOK = true

		// Now actually process commands.
		reservedOwned = false // We're about to pass ownership away.
		retErr = sqlServer.ServeConn(ctx, connHandler, reserved, cancelConn)
	}()
	return retCh
}

func (c *conn) sendStatusParam(param, value string) error {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgParameterStatus)
	c.msgBuilder.writeTerminatedString(param)
	c.msgBuilder.writeTerminatedString(value)
	return c.msgBuilder.finishMsg(c.conn)
}

func (c *conn) sendInitialConnData(
	ctx context.Context, sqlServer *sql.Server,
) (sql.ConnectionHandler, error) {
	connHandler, err := sqlServer.SetupConn(
		ctx, c.sessionArgs, &c.stmtBuf, c, c.metrics.SQLMemMetrics)
	if err != nil {
		_ /* err */ = writeErr(
			ctx, &sqlServer.GetExecutorConfig().Settings.SV, err, &c.msgBuilder, c.conn)
		return sql.ConnectionHandler{}, err
	}

	// Send the initial "status parameters" to the client.  This
	// overlaps partially with session variables. The client wants to
	// see the values that result from the combination of server-side
	// defaults with client-provided values.
	// For details see: https://www.postgresql.org/docs/10/static/libpq-status.html
	for _, param := range statusReportParams {
		value := connHandler.GetStatusParam(ctx, param)
		if err := c.sendStatusParam(param, value); err != nil {
			return sql.ConnectionHandler{}, err
		}
	}
	// The two following status parameters have no equivalent session
	// variable.
	if err := c.sendStatusParam("session_authorization", c.sessionArgs.User); err != nil {
		return sql.ConnectionHandler{}, err
	}

	// TODO(knz): this should retrieve the admin status during
	// authentication using the roles table, instead of using a
	// simple/naive username match.
	isSuperUser := c.sessionArgs.User == security.RootUser
	superUserVal := "off"
	if isSuperUser {
		superUserVal = "on"
	}
	if err := c.sendStatusParam("is_superuser", superUserVal); err != nil {
		return sql.ConnectionHandler{}, err
	}

	// An initial readyForQuery message is part of the handshake.
	c.msgBuilder.initMsg(pgwirebase.ServerMsgReady)
	c.msgBuilder.writeByte(byte(sql.IdleTxnBlock))
	if err := c.msgBuilder.finishMsg(c.conn); err != nil {
		return sql.ConnectionHandler{}, err
	}
	return connHandler, nil
}

// An error is returned iff the statement buffer has been closed. In that case,
// the connection should be considered toast.
func (c *conn) handleSimpleQuery(
	ctx context.Context,
	buf *pgwirebase.ReadBuffer,
	timeReceived time.Time,
	unqualifiedIntSize *types.T,
) error {
	query, err := buf.GetString()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}

	tracing.AnnotateTrace()

	startParse := timeutil.Now()
	stmts, err := c.parser.ParseWithInt(query, unqualifiedIntSize)
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	endParse := timeutil.Now()

	if len(stmts) == 0 {
		return c.stmtBuf.Push(
			ctx, sql.ExecStmt{
				Statement:    parser.Statement{},
				TimeReceived: timeReceived,
				ParseStart:   startParse,
				ParseEnd:     endParse,
			})
	}

	for i := range stmts {
		// The CopyFrom statement is special. We need to detect it so we can hand
		// control of the connection, through the stmtBuf, to a copyMachine, and
		// block this network routine until control is passed back.
		if cp, ok := stmts[i].AST.(*tree.CopyFrom); ok {
			if len(stmts) != 1 {
				// NOTE(andrei): I don't know if Postgres supports receiving a COPY
				// together with other statements in the "simple" protocol, but I'd
				// rather not worry about it since execution of COPY is special - it
				// takes control over the connection.
				return c.stmtBuf.Push(
					ctx,
					sql.SendError{
						Err: pgwirebase.NewProtocolViolationErrorf(
							"COPY together with other statements in a query string is not supported"),
					})
			}
			copyDone := sync.WaitGroup{}
			copyDone.Add(1)
			if err := c.stmtBuf.Push(ctx, sql.CopyIn{Conn: c, Stmt: cp, CopyDone: &copyDone}); err != nil {
				return err
			}
			copyDone.Wait()
			return nil
		}

		if err := c.stmtBuf.Push(
			ctx,
			sql.ExecStmt{
				Statement:    stmts[i],
				TimeReceived: timeReceived,
				ParseStart:   startParse,
				ParseEnd:     endParse,
			}); err != nil {
			return err
		}
	}
	return nil
}

// An error is returned iff the statement buffer has been closed. In that case,
// the connection should be considered toast.
func (c *conn) handleParse(
	ctx context.Context, buf *pgwirebase.ReadBuffer, nakedIntSize *types.T,
) error {
	// protocolErr is set if a protocol error has to be sent to the client. A
	// stanza at the bottom of the function pushes instructions for sending this
	// error.
	var protocolErr *pgerror.Error

	name, err := buf.GetString()
	if protocolErr != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	query, err := buf.GetString()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	// The client may provide type information for (some of) the placeholders.
	numQArgTypes, err := buf.GetUint16()
	if err != nil {
		return err
	}
	inTypeHints := make([]oid.Oid, numQArgTypes)
	for i := range inTypeHints {
		typ, err := buf.GetUint32()
		if err != nil {
			return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
		}
		inTypeHints[i] = oid.Oid(typ)
	}

	startParse := timeutil.Now()
	stmts, err := c.parser.ParseWithInt(query, nakedIntSize)
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	if len(stmts) > 1 {
		err := pgerror.NewWrongNumberOfPreparedStatements(len(stmts))
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	var stmt parser.Statement
	if len(stmts) == 1 {
		stmt = stmts[0]
	}
	// len(stmts) == 0 results in a nil (empty) statement.

	if len(inTypeHints) > stmt.NumPlaceholders {
		err := pgwirebase.NewProtocolViolationErrorf(
			"received too many type hints: %d vs %d placeholders in query",
			len(inTypeHints), stmt.NumPlaceholders,
		)
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}

	var sqlTypeHints tree.PlaceholderTypes
	if len(inTypeHints) > 0 {
		// Prepare the mapping of SQL placeholder names to types. Pre-populate it with
		// the type hints received from the client, if any.
		sqlTypeHints = make(tree.PlaceholderTypes, stmt.NumPlaceholders)
		for i, t := range inTypeHints {
			if t == 0 {
				continue
			}
			v, ok := types.OidToType[t]
			if !ok {
				err := pgwirebase.NewProtocolViolationErrorf("unknown oid type: %v", t)
				return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
			}
			sqlTypeHints[i] = v
		}
	}

	endParse := timeutil.Now()

	if _, ok := stmt.AST.(*tree.CopyFrom); ok {
		// We don't support COPY in extended protocol because it'd be complicated:
		// it wouldn't be the preparing, but the execution that would need to
		// execute the copyMachine.
		// Also note that COPY FROM in extended mode seems to be quite broken in
		// Postgres too:
		// https://www.postgresql.org/message-id/flat/CAMsr%2BYGvp2wRx9pPSxaKFdaObxX8DzWse%2BOkWk2xpXSvT0rq-g%40mail.gmail.com#CAMsr+YGvp2wRx9pPSxaKFdaObxX8DzWse+OkWk2xpXSvT0rq-g@mail.gmail.com
		return c.stmtBuf.Push(ctx, sql.SendError{Err: fmt.Errorf("CopyFrom not supported in extended protocol mode")})
	}

	return c.stmtBuf.Push(
		ctx,
		sql.PrepareStmt{
			Name:         name,
			Statement:    stmt,
			TypeHints:    sqlTypeHints,
			RawTypeHints: inTypeHints,
			ParseStart:   startParse,
			ParseEnd:     endParse,
		})
}

// An error is returned iff the statement buffer has been closed. In that case,
// the connection should be considered toast.
func (c *conn) handleDescribe(ctx context.Context, buf *pgwirebase.ReadBuffer) error {
	typ, err := buf.GetPrepareType()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	name, err := buf.GetString()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	return c.stmtBuf.Push(
		ctx,
		sql.DescribeStmt{
			Name: name,
			Type: typ,
		})
}

// An error is returned iff the statement buffer has been closed. In that case,
// the connection should be considered toast.
func (c *conn) handleClose(ctx context.Context, buf *pgwirebase.ReadBuffer) error {
	typ, err := buf.GetPrepareType()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	name, err := buf.GetString()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	return c.stmtBuf.Push(
		ctx,
		sql.DeletePreparedStmt{
			Name: name,
			Type: typ,
		})
}

// handleBind queues instructions for creating a portal from a prepared
// statement.
// An error is returned iff the statement buffer has been closed. In that case,
// the connection should be considered toast.
func (c *conn) handleBind(ctx context.Context, buf *pgwirebase.ReadBuffer) error {
	portalName, err := buf.GetString()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	statementName, err := buf.GetString()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}

	// From the docs on number of argument format codes to bind:
	// This can be zero to indicate that there are no arguments or that the
	// arguments all use the default format (text); or one, in which case the
	// specified format code is applied to all arguments; or it can equal the
	// actual number of arguments.
	// http://www.postgresql.org/docs/current/static/protocol-message-formats.html
	numQArgFormatCodes, err := buf.GetUint16()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	lenCodes := numQArgFormatCodes
	if lenCodes == 0 {
		lenCodes = 1
	}
	qArgFormatCodes := make([]pgwirebase.FormatCode, lenCodes)
	switch numQArgFormatCodes {
	case 0:
		// No format codes means all arguments are passed as text.
		qArgFormatCodes[0] = pgwirebase.FormatText
	case 1:
		// `1` means read one code and apply it to every argument.
		ch, err := buf.GetUint16()
		if err != nil {
			return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
		}
		fmtCode := pgwirebase.FormatCode(ch)
		qArgFormatCodes[0] = fmtCode
	default:
		// Read one format code for each argument and apply it to that argument.
		for i := range qArgFormatCodes {
			code, err := buf.GetUint16()
			if err != nil {
				return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
			}
			qArgFormatCodes[i] = pgwirebase.FormatCode(code)
		}
	}

	numValues, err := buf.GetUint16()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	qargs := make([][]byte, numValues)
	for i := 0; i < int(numValues); i++ {
		plen, err := buf.GetUint32()
		if err != nil {
			return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
		}
		if int32(plen) == -1 {
			// The argument is a NULL value.
			qargs[i] = nil
			continue
		}
		b, err := buf.GetBytes(int(plen))
		if err != nil {
			return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
		}
		qargs[i] = b
	}

	// From the docs on number of result-column format codes to bind:
	// This can be zero to indicate that there are no result columns or that
	// the result columns should all use the default format (text); or one, in
	// which case the specified format code is applied to all result columns
	// (if any); or it can equal the actual number of result columns of the
	// query.
	// http://www.postgresql.org/docs/current/static/protocol-message-formats.html
	numColumnFormatCodes, err := buf.GetUint16()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	var columnFormatCodes []pgwirebase.FormatCode
	switch numColumnFormatCodes {
	case 0:
		// All columns will use the text format.
		columnFormatCodes = make([]pgwirebase.FormatCode, 1)
		columnFormatCodes[0] = pgwirebase.FormatText
	case 1:
		// All columns will use the one specficied format.
		ch, err := buf.GetUint16()
		if err != nil {
			return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
		}
		fmtCode := pgwirebase.FormatCode(ch)
		columnFormatCodes = make([]pgwirebase.FormatCode, 1)
		columnFormatCodes[0] = fmtCode
	default:
		columnFormatCodes = make([]pgwirebase.FormatCode, numColumnFormatCodes)
		// Read one format code for each column and apply it to that column.
		for i := range columnFormatCodes {
			ch, err := buf.GetUint16()
			if err != nil {
				return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
			}
			columnFormatCodes[i] = pgwirebase.FormatCode(ch)
		}
	}
	return c.stmtBuf.Push(
		ctx,
		sql.BindStmt{
			PreparedStatementName: statementName,
			PortalName:            portalName,
			Args:                  qargs,
			ArgFormatCodes:        qArgFormatCodes,
			OutFormats:            columnFormatCodes,
		})
}

// An error is returned iff the statement buffer has been closed. In that case,
// the connection should be considered toast.
func (c *conn) handleExecute(
	ctx context.Context, buf *pgwirebase.ReadBuffer, timeReceived time.Time,
) error {
	portalName, err := buf.GetString()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	limit, err := buf.GetUint32()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	return c.stmtBuf.Push(ctx, sql.ExecPortal{
		Name:         portalName,
		TimeReceived: timeReceived,
		Limit:        int(limit),
	})
}

func (c *conn) handleFlush(ctx context.Context) error {
	return c.stmtBuf.Push(ctx, sql.Flush{})
}

// BeginCopyIn is part of the pgwirebase.Conn interface.
func (c *conn) BeginCopyIn(ctx context.Context, columns []sqlbase.ResultColumn) error {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgCopyInResponse)
	c.msgBuilder.writeByte(byte(pgwirebase.FormatText))
	c.msgBuilder.putInt16(int16(len(columns)))
	for range columns {
		c.msgBuilder.putInt16(int16(pgwirebase.FormatText))
	}
	return c.msgBuilder.finishMsg(c.conn)
}

// SendCommandComplete is part of the pgwirebase.Conn interface.
func (c *conn) SendCommandComplete(tag []byte) error {
	c.bufferCommandComplete(tag)
	return nil
}

// Rd is part of the pgwirebase.Conn interface.
func (c *conn) Rd() pgwirebase.BufferedReader {
	return &pgwireReader{conn: c}
}

// flushInfo encapsulates information about what results have been flushed to
// the network.
type flushInfo struct {
	// buf is a reference to writerState.buf.
	buf *bytes.Buffer
	// lastFlushed indicates the highest command for which results have been
	// flushed. The command may have further results in the buffer that haven't
	// been flushed.
	lastFlushed sql.CmdPos
	// map from CmdPos to the index of the buffer where the results for the
	// respective result begins.
	cmdStarts map[sql.CmdPos]int
}

// registerCmd updates cmdStarts when the first result for a new command is
// received.
func (fi *flushInfo) registerCmd(pos sql.CmdPos) {
	if _, ok := fi.cmdStarts[pos]; ok {
		return
	}
	fi.cmdStarts[pos] = fi.buf.Len()
}

// convertToErrWithPGCode recognizes errs that should have SQL error codes to be
// reported to the client and converts err to them. If this doesn't apply, err
// is returned.
// Note that this returns a new error, and details from the original error are
// not preserved in any way (except possibly the message).
//
// TODO(andrei): sqlbase.ConvertBatchError() seems to serve similar purposes, but
// it's called from more specialized contexts. Consider unifying the two.
func convertToErrWithPGCode(err error) error {
	if err == nil {
		return nil
	}

	// If the error was wrapped, get to the cause. Otherwise the cast
	// below will not see what's really happening.
	wrappedErr := errors.Cause(err)

	switch wrappedErr.(type) {
	case *roachpb.TransactionRetryWithProtoRefreshError:
		return sqlbase.NewRetryError(err)
	case *roachpb.AmbiguousResultError:
		// TODO(andrei): Once DistSQL starts executing writes, we'll need a
		// different mechanism to marshal AmbiguousResultErrors from the executing
		// nodes.
		return sqlbase.NewStatementCompletionUnknownError(err)
	default:
		return err
	}
}

func cookTag(tagStr string, buf []byte, stmtType tree.StatementType, rowsAffected int) []byte {
	if tagStr == "INSERT" {
		// From the postgres docs (49.5. Message Formats):
		// `INSERT oid rows`... oid is the object ID of the inserted row if
		//	rows is 1 and the target table has OIDs; otherwise oid is 0.
		tagStr = "INSERT 0"
	}
	tag := append(buf, tagStr...)

	switch stmtType {
	case tree.RowsAffected:
		tag = append(tag, ' ')
		tag = strconv.AppendInt(tag, int64(rowsAffected), 10)

	case tree.Rows:
		tag = append(tag, ' ')
		tag = strconv.AppendUint(tag, uint64(rowsAffected), 10)

	case tree.Ack, tree.DDL:
		if tagStr == "SELECT" {
			tag = append(tag, ' ')
			tag = strconv.AppendInt(tag, int64(rowsAffected), 10)
		}

	case tree.CopyIn:
		// Nothing to do. The CommandComplete message has been sent elsewhere.
		panic(fmt.Sprintf("CopyIn statements should have been handled elsewhere " +
			"and not produce results"))
	default:
		panic(fmt.Sprintf("unexpected result type %v", stmtType))
	}

	return tag
}

// bufferRow serializes a row and adds it to the buffer.
//
// formatCodes describes the desired encoding for each column. It can be nil, in
// which case all columns are encoded using the text encoding. Otherwise, it
// needs to contain an entry for every column.
func (c *conn) bufferRow(
	ctx context.Context,
	row tree.Datums,
	formatCodes []pgwirebase.FormatCode,
	conv sessiondata.DataConversionConfig,
	oids []oid.Oid,
) {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgDataRow)
	c.msgBuilder.putInt16(int16(len(row)))
	for i, col := range row {
		fmtCode := pgwirebase.FormatText
		if formatCodes != nil {
			fmtCode = formatCodes[i]
		}
		switch fmtCode {
		case pgwirebase.FormatText:
			c.msgBuilder.writeTextDatum(ctx, col, conv)
		case pgwirebase.FormatBinary:
			c.msgBuilder.writeBinaryDatum(ctx, col, conv.Location, oids[i])
		default:
			c.msgBuilder.setError(errors.Errorf("unsupported format code %s", fmtCode))
		}
	}
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(fmt.Sprintf("unexpected err from buffer: %s", err))
	}
}

func (c *conn) bufferReadyForQuery(txnStatus byte) {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgReady)
	c.msgBuilder.writeByte(txnStatus)
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(fmt.Sprintf("unexpected err from buffer: %s", err))
	}
}

func (c *conn) bufferParseComplete() {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgParseComplete)
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(fmt.Sprintf("unexpected err from buffer: %s", err))
	}
}

func (c *conn) bufferBindComplete() {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgBindComplete)
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(fmt.Sprintf("unexpected err from buffer: %s", err))
	}
}

func (c *conn) bufferCloseComplete() {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgCloseComplete)
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(fmt.Sprintf("unexpected err from buffer: %s", err))
	}
}

func (c *conn) bufferCommandComplete(tag []byte) {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgCommandComplete)
	c.msgBuilder.write(tag)
	c.msgBuilder.nullTerminate()
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(fmt.Sprintf("unexpected err from buffer: %s", err))
	}
}

func (c *conn) bufferErr(err error) {
	// TODO(andrei,knz): This would benefit from a context with the
	// current connection log tags.
	if err := writeErr(context.Background(), c.sv,
		err, &c.msgBuilder, &c.writerState.buf); err != nil {
		panic(fmt.Sprintf("unexpected err from buffer: %s", err))
	}
}

func (c *conn) bufferEmptyQueryResponse() {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgEmptyQuery)
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(fmt.Sprintf("unexpected err from buffer: %s", err))
	}
}

func writeErr(
	ctx context.Context, sv *settings.Values, err error, msgBuilder *writeBuffer, w io.Writer,
) error {
	sqltelemetry.RecordError(ctx, err, sv)
	msgBuilder.initMsg(pgwirebase.ServerMsgErrorResponse)

	msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFieldSeverity)
	msgBuilder.writeTerminatedString("ERROR")

	pgErr, ok := pgerror.GetPGCause(err)
	var code string
	if ok {
		code = pgErr.Code
	} else {
		// The error was not decorated as an pgerror.Error. We don't know
		// its code, in fact we don't know pretty much anything about it.
		// We're going to let it flow to the user as a XXUUU error.
		// We don't use CodeInternalError here (XX000) because internal
		// errors have gain special status "please tell us about it
		// ASAP" in CockroachDB.
		code = pgerror.CodeUncategorizedError
		// However, we'll keep track of the number of occurrences in
		// telemetry. Over time, we'll want this count to go down
		// (i.e. more errors becoming qualified).
		telemetry.Inc(sqltelemetry.UncategorizedErrorCounter)
	}

	msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFieldSQLState)
	msgBuilder.writeTerminatedString(code)

	if ok && pgErr.Detail != "" {
		msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFileldDetail)
		msgBuilder.writeTerminatedString(pgErr.Detail)
	}

	if ok && pgErr.Hint != "" {
		msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFileldHint)
		msgBuilder.writeTerminatedString(pgErr.Hint)
	}

	if ok && pgErr.Source != nil {
		errCtx := pgErr.Source
		if errCtx.File != "" {
			msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFieldSrcFile)
			msgBuilder.writeTerminatedString(errCtx.File)
		}

		if errCtx.Line > 0 {
			msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFieldSrcLine)
			msgBuilder.writeTerminatedString(strconv.Itoa(int(errCtx.Line)))
		}

		if errCtx.Function != "" {
			msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFieldSrcFunction)
			msgBuilder.writeTerminatedString(errCtx.Function)
		}
	}

	msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFieldMsgPrimary)
	msgBuilder.writeTerminatedString(err.Error())

	msgBuilder.nullTerminate()
	return msgBuilder.finishMsg(w)
}

func (c *conn) bufferParamDesc(types []oid.Oid) {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgParameterDescription)
	c.msgBuilder.putInt16(int16(len(types)))
	for _, t := range types {
		c.msgBuilder.putInt32(int32(t))
	}
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(fmt.Sprintf("unexpected err from buffer: %s", err))
	}
}

func (c *conn) bufferNoDataMsg() {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgNoData)
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(fmt.Sprintf("unexpected err from buffer: %s", err))
	}
}

// writeRowDescription writes a row description to the given writer.
//
// formatCodes specifies the format for each column. It can be nil, in which
// case all columns will use FormatText.
//
// If an error is returned, it has also been saved on c.err.
func (c *conn) writeRowDescription(
	ctx context.Context,
	columns []sqlbase.ResultColumn,
	formatCodes []pgwirebase.FormatCode,
	w io.Writer,
) error {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgRowDescription)
	c.msgBuilder.putInt16(int16(len(columns)))
	for i, column := range columns {
		if log.V(2) {
			log.Infof(ctx, "pgwire: writing column %s of type: %T", column.Name, column.Typ)
		}
		c.msgBuilder.writeTerminatedString(column.Name)
		typ := pgTypeForParserType(column.Typ)
		c.msgBuilder.putInt32(0) // Table OID (optional).
		c.msgBuilder.putInt16(0) // Column attribute ID (optional).
		c.msgBuilder.putInt32(int32(mapResultOid(typ.oid)))
		c.msgBuilder.putInt16(int16(typ.size))
		// The type modifier (atttypmod) is used to include various extra information
		// about the type being sent. -1 is used for values which don't make use of
		// atttypmod and is generally an acceptable catch-all for those that do.
		// See https://www.postgresql.org/docs/9.6/static/catalog-pg-attribute.html
		// for information on atttypmod. In theory we differ from Postgres by never
		// giving the scale/precision, and by not including the length of a VARCHAR,
		// but it's not clear if any drivers/ORMs depend on this.
		//
		// TODO(justin): It would be good to include this information when possible.
		c.msgBuilder.putInt32(-1)
		if formatCodes == nil {
			c.msgBuilder.putInt16(int16(pgwirebase.FormatText))
		} else {
			c.msgBuilder.putInt16(int16(formatCodes[i]))
		}
	}
	if err := c.msgBuilder.finishMsg(w); err != nil {
		c.setErr(err)
		return err
	}
	return nil
}

// Flush is part of the ClientComm interface.
//
// In case conn.err is set, this is a no-op - the previous err is returned.
func (c *conn) Flush(pos sql.CmdPos) error {
	// Check that there were no previous network errors. If there were, we'd
	// probably also fail the write below, but this check is here to make
	// absolutely sure that we don't send some results after we previously had
	// failed to send others.
	if err := c.GetErr(); err != nil {
		return err
	}

	c.writerState.fi.lastFlushed = pos
	c.writerState.fi.cmdStarts = make(map[sql.CmdPos]int)

	_ /* n */, err := c.writerState.buf.WriteTo(c.conn)
	if err != nil {
		c.setErr(err)
		return err
	}
	return nil
}

// maybeFlush flushes the buffer to the network connection if it exceeded
// sessionArgs.ConnResultsBufferSize.
func (c *conn) maybeFlush(pos sql.CmdPos) (bool, error) {
	if int64(c.writerState.buf.Len()) <= c.sessionArgs.ConnResultsBufferSize {
		return false, nil
	}
	return true, c.Flush(pos)
}

// LockCommunication is part of the ClientComm interface.
//
// The current implementation of conn writes results to the network
// synchronously, as they are produced (modulo buffering). Therefore, there's
// nothing to "lock" - communication is naturally blocked as the command
// processor won't write any more results.
func (c *conn) LockCommunication() sql.ClientLock {
	return &clientConnLock{flushInfo: &c.writerState.fi}
}

// clientConnLock is the connection's implementation of sql.ClientLock. It lets
// the sql module lock the flushing of results and find out what has already
// been flushed.
type clientConnLock struct {
	*flushInfo
}

var _ sql.ClientLock = &clientConnLock{}

// Close is part of the sql.ClientLock interface.
func (cl *clientConnLock) Close() {
	// Nothing to do. See LockCommunication note.
}

// ClientPos is part of the sql.ClientLock interface.
func (cl *clientConnLock) ClientPos() sql.CmdPos {
	return cl.lastFlushed
}

// RTrim is part of the sql.ClientLock interface.
func (cl *clientConnLock) RTrim(ctx context.Context, pos sql.CmdPos) {
	if pos <= cl.lastFlushed {
		panic(fmt.Sprintf("asked to trim to pos: %d, below the last flush: %d", pos, cl.lastFlushed))
	}
	idx, ok := cl.cmdStarts[pos]
	if !ok {
		// If we don't have a start index for pos yet, it must be that no results
		// for it yet have been produced yet.
		idx = cl.buf.Len()
	}
	// Remove everything from the buffer after idx.
	cl.buf.Truncate(idx)
	// Update cmdStarts: delete commands that were trimmed.
	for p := range cl.cmdStarts {
		if p >= pos {
			delete(cl.cmdStarts, p)
		}
	}
}

// CreateStatementResult is part of the sql.ClientComm interface.
func (c *conn) CreateStatementResult(
	stmt tree.Statement,
	descOpt sql.RowDescOpt,
	pos sql.CmdPos,
	formatCodes []pgwirebase.FormatCode,
	conv sessiondata.DataConversionConfig,
) sql.CommandResult {
	res := c.makeCommandResult(descOpt, pos, stmt, formatCodes, conv)
	return &res
}

// CreateSyncResult is part of the sql.ClientComm interface.
func (c *conn) CreateSyncResult(pos sql.CmdPos) sql.SyncResult {
	res := c.makeMiscResult(pos, readyForQuery)
	return &res
}

// CreateFlushResult is part of the sql.ClientComm interface.
func (c *conn) CreateFlushResult(pos sql.CmdPos) sql.FlushResult {
	res := c.makeMiscResult(pos, flush)
	return &res
}

// CreateDrainResult is part of the sql.ClientComm interface.
func (c *conn) CreateDrainResult(pos sql.CmdPos) sql.DrainResult {
	res := c.makeMiscResult(pos, noCompletionMsg)
	return &res
}

// CreateBindResult is part of the sql.ClientComm interface.
func (c *conn) CreateBindResult(pos sql.CmdPos) sql.BindResult {
	res := c.makeMiscResult(pos, bindComplete)
	return &res
}

// CreatePrepareResult is part of the sql.ClientComm interface.
func (c *conn) CreatePrepareResult(pos sql.CmdPos) sql.ParseResult {
	res := c.makeMiscResult(pos, parseComplete)
	return &res
}

// CreateDescribeResult is part of the sql.ClientComm interface.
func (c *conn) CreateDescribeResult(pos sql.CmdPos) sql.DescribeResult {
	res := c.makeMiscResult(pos, noCompletionMsg)
	return &res
}

// CreateEmptyQueryResult is part of the sql.ClientComm interface.
func (c *conn) CreateEmptyQueryResult(pos sql.CmdPos) sql.EmptyQueryResult {
	res := c.makeMiscResult(pos, emptyQueryResponse)
	return &res
}

// CreateDeleteResult is part of the sql.ClientComm interface.
func (c *conn) CreateDeleteResult(pos sql.CmdPos) sql.DeleteResult {
	res := c.makeMiscResult(pos, closeComplete)
	return &res
}

// CreateErrorResult is part of the sql.ClientComm interface.
func (c *conn) CreateErrorResult(pos sql.CmdPos) sql.ErrorResult {
	res := c.makeMiscResult(pos, noCompletionMsg)
	res.errExpected = true
	return &res
}

// CreateCopyInResult is part of the sql.ClientComm interface.
func (c *conn) CreateCopyInResult(pos sql.CmdPos) sql.CopyInResult {
	res := c.makeMiscResult(pos, noCompletionMsg)
	return &res
}

// pgwireReader is an io.Reader that wraps a conn, maintaining its metrics as
// it is consumed.
type pgwireReader struct {
	conn *conn
}

// pgwireReader implements the pgwirebase.BufferedReader interface.
var _ pgwirebase.BufferedReader = &pgwireReader{}

// Read is part of the pgwirebase.BufferedReader interface.
func (r *pgwireReader) Read(p []byte) (int, error) {
	n, err := r.conn.rd.Read(p)
	r.conn.metrics.BytesInCount.Inc(int64(n))
	return n, err
}

// ReadString is part of the pgwirebase.BufferedReader interface.
func (r *pgwireReader) ReadString(delim byte) (string, error) {
	s, err := r.conn.rd.ReadString(delim)
	r.conn.metrics.BytesInCount.Inc(int64(len(s)))
	return s, err
}

// ReadByte is part of the pgwirebase.BufferedReader interface.
func (r *pgwireReader) ReadByte() (byte, error) {
	b, err := r.conn.rd.ReadByte()
	if err == nil {
		r.conn.metrics.BytesInCount.Inc(1)
	}
	return b, err
}

// handleAuthentication checks the connection's user. Errors are sent to the
// client and also returned.
//
// TODO(knz): handleAuthentication should discuss with the client to arrange
// authentication and update c.sessionArgs with the authenticated user's name,
// if different from the one given initially.
func (c *conn) handleAuthentication(
	ctx context.Context,
	ac AuthConn,
	insecure bool,
	ie *sql.InternalExecutor,
	auth *hba.Conf,
	execCfg *sql.ExecutorConfig,
) error {
	sendError := func(err error) error {
		_ /* err */ = writeErr(ctx, &execCfg.Settings.SV, err, &c.msgBuilder, c.conn)
		return err
	}

	// Check that the requested user exists and retrieve the hashed
	// password in case password authentication is needed.
	exists, hashedPassword, err := sql.GetUserHashedPassword(
		ctx, ie, &c.metrics.SQLMemMetrics, c.sessionArgs.User,
	)
	if err != nil {
		return sendError(err)
	}
	if !exists {
		return sendError(errors.Errorf(security.ErrPasswordUserAuthFailed, c.sessionArgs.User))
	}

	if tlsConn, ok := c.conn.(*readTimeoutConn).Conn.(*tls.Conn); ok {
		tlsState := tlsConn.ConnectionState()
		var methodFn AuthMethod
		var hbaEntry *hba.Entry

		if auth == nil {
			methodFn = authCertPassword
		} else if c.sessionArgs.User == security.RootUser {
			// If a hba.conf file is specified, hard code the root user to always use
			// cert auth. This prevents users from shooting themselves in the foot and
			// making root not able to login, thus disallowing anyone from fixing the
			// hba.conf file.
			methodFn = authCert
		} else {
			addr, _, err := net.SplitHostPort(c.conn.RemoteAddr().String())
			if err != nil {
				return sendError(err)
			}
			ip := net.ParseIP(addr)
			for _, entry := range auth.Entries {
				switch a := entry.Address.(type) {
				case *net.IPNet:
					if !a.Contains(ip) {
						continue
					}
				case hba.String:
					if !a.IsSpecial("all") {
						return sendError(errors.Errorf("unexpected %s address: %q", serverHBAConfSetting, a.Value))
					}
				default:
					return sendError(errors.Errorf("unexpected address type %T", a))
				}
				match := false
				for _, u := range entry.User {
					if u.IsSpecial("all") {
						match = true
						break
					}
					if u.Value == c.sessionArgs.User {
						match = true
						break
					}
				}
				if !match {
					continue
				}
				methodFn = hbaAuthMethods[entry.Method]
				if methodFn == nil {
					return sendError(errors.Errorf("unknown auth method %s", entry.Method))
				}
				hbaEntry = &entry
				break
			}
			if methodFn == nil {
				return sendError(errors.Errorf("no %s entry for host %q, user %q", serverHBAConfSetting, addr, c.sessionArgs.User))
			}
		}

		authenticationHook, err := methodFn(ac, tlsState, insecure, hashedPassword, execCfg, hbaEntry)
		if err != nil {
			return sendError(err)
		}
		if err := authenticationHook(c.sessionArgs.User, true /* public */); err != nil {
			return sendError(err)
		}
	}

	c.msgBuilder.initMsg(pgwirebase.ServerMsgAuth)
	c.msgBuilder.putInt32(authOK)
	return c.msgBuilder.finishMsg(c.conn)
}

const serverHBAConfSetting = "server.host_based_authentication.configuration"

var connAuthConf = settings.RegisterValidatedStringSetting(
	serverHBAConfSetting,
	"host-based authentication configuration to use during connection authentication",
	"",
	func(values *settings.Values, s string) error {
		if s == "" {
			return nil
		}
		conf, err := hba.Parse(s)
		if err != nil {
			return err
		}
		for _, entry := range conf.Entries {
			for _, db := range entry.Database {
				if !db.IsSpecial("all") {
					return errors.New("database must be specified as all")
				}
			}
			if addr, ok := entry.Address.(hba.String); ok && !addr.IsSpecial("all") {
				return errors.New("host addresses not supported")
			}
			if hbaAuthMethods[entry.Method] == nil {
				return errors.Errorf("unknown auth method %q", entry.Method)
			}
			if check := hbaCheckHBAEntries[entry.Method]; check != nil {
				if err := check(entry); err != nil {
					return err
				}
			}
		}
		return nil
	},
)

// authenticator is the interface used by the connection to pass password data
// to the authenticator and expect an authentication decision from it.
type authenticator interface {
	// sendPwdData is used to push authentication data into the authenticator.
	// This call is blocking; authenticators are supposed to consume data hastily
	// once they've requested it.
	sendPwdData(data []byte) error
	// noMorePwdData is used to inform the authenticator that the client is not
	// sending any more authentication data. This method can be called multiple
	// times.
	noMorePwdData()
	// authResult blocks for an authentication decision. This call also informs
	// the authenticator that no more auth data is coming from the client;
	// noMorePwdData() is called internally.
	//
	// The auth result is either an unqualifiedIntSizer (in case the auth
	// succeeded) or an auth error.
	authResult() (unqualifiedIntSizer, error)
}

// AuthConn is the interface used by the authenticator for interacting with the
// pgwire connection.
type AuthConn interface {
	// SendAuthRequest send a request for authentication information. After
	// calling this, the authenticator needs to call GetPwdData() quickly, as the
	// connection's goroutine will be blocked on providing us the requested data.
	SendAuthRequest(authType int32, data []byte) error
	// GetPwdData returns authentication info that was previously requested with
	// SendAuthRequest. The call blocks until such data is available.
	// An error is returned if the client connection dropped or if the client
	// didn't respect the protocol. After an error has been returned, GetPwdData()
	// cannot be called any more.
	GetPwdData() ([]byte, error)
	// AuthOK declares that authentication succeeded and provides a
	// unqualifiedIntSizer, to be returned by authenticator.authResult(). Future
	// authenticator.sendPwdData() calls fail.
	AuthOK(unqualifiedIntSizer)
	// AuthFail declares that authentication has failed and provides an error to
	// be returned by authenticator.authResult(). Future
	// authenticator.sendPwdData() calls fail. The error has already been written
	// to the client connection.
	AuthFail(err error)
}

// authPipe is the implementation for the authenticator and AuthConn interfaces.
// A single authPipe will serve as both an AuthConn and an authenticator; the
// two represent the two "ends" of the pipe and we'll pass data between them.
type authPipe struct {
	c *conn // Only used for writing, not for reading.

	ch chan []byte
	// writerDone is a channel closed by noMorePwdData().
	// Nil if noMorePwdData().
	writerDone chan struct{}
	readerDone chan authRes
}

type authRes struct {
	intSizer unqualifiedIntSizer
	err      error
}

func newAuthPipe(c *conn) *authPipe {
	ap := &authPipe{
		c:          c,
		ch:         make(chan []byte),
		writerDone: make(chan struct{}),
		readerDone: make(chan authRes, 1),
	}
	return ap
}

var _ authenticator = &authPipe{}
var _ AuthConn = &authPipe{}

func (p *authPipe) sendPwdData(data []byte) error {
	select {
	case p.ch <- data:
		return nil
	case <-p.readerDone:
		return pgwirebase.NewProtocolViolationErrorf("unexpected auth data")
	}
}

func (p *authPipe) noMorePwdData() {
	if p.writerDone == nil {
		return
	}
	// A reader blocked in GetPwdData() gets unblocked with an error.
	close(p.writerDone)
	p.writerDone = nil
}

// GetPwdData is part of the AuthConn interface.
func (p *authPipe) GetPwdData() ([]byte, error) {
	select {
	case data := <-p.ch:
		return data, nil
	case <-p.writerDone:
		return nil, pgwirebase.NewProtocolViolationErrorf("client didn't send required auth data")
	}
}

// AuthOK is part of the AuthConn interface.
func (p *authPipe) AuthOK(intSizer unqualifiedIntSizer) {
	p.readerDone <- authRes{intSizer: intSizer}
}

func (p *authPipe) AuthFail(err error) {
	p.readerDone <- authRes{err: err}
}

// authResult is part of the authenticator interface.
func (p *authPipe) authResult() (unqualifiedIntSizer, error) {
	p.noMorePwdData()
	res := <-p.readerDone
	return res.intSizer, res.err
}

// SendAuthRequest is part of the AuthConn interface.
func (p *authPipe) SendAuthRequest(authType int32, data []byte) error {
	c := p.c
	c.msgBuilder.initMsg(pgwirebase.ServerMsgAuth)
	c.msgBuilder.putInt32(authType)
	c.msgBuilder.write(data)
	return c.msgBuilder.finishMsg(c.conn)
}

type (
	// AuthMethod defines a method for authentication of a connection.
	AuthMethod func(c AuthConn, tlsState tls.ConnectionState, insecure bool, hashedPassword []byte, execCfg *sql.ExecutorConfig, entry *hba.Entry) (security.UserAuthHook, error)

	// CheckHBAEntry defines a method for error checking an hba Entry.
	CheckHBAEntry func(hba.Entry) error
)

var (
	hbaAuthMethods     = map[string]AuthMethod{}
	hbaCheckHBAEntries = map[string]CheckHBAEntry{}
)

// RegisterAuthMethod registers an AuthMethod for pgwire authentication.
func RegisterAuthMethod(method string, fn AuthMethod, checkEntry CheckHBAEntry) {
	hbaAuthMethods[method] = fn
	if checkEntry != nil {
		hbaCheckHBAEntries[method] = checkEntry
	}
}

func passwordString(pwdData []byte) (string, error) {
	// Make a string out of the byte array.
	if bytes.IndexByte(pwdData, 0) != len(pwdData)-1 {
		return "", fmt.Errorf("expected 0-terminated byte array")
	}
	return string(pwdData[:len(pwdData)-1]), nil
}

func authPassword(
	c AuthConn,
	tlsState tls.ConnectionState,
	insecure bool,
	hashedPassword []byte,
	execCfg *sql.ExecutorConfig,
	entry *hba.Entry,
) (security.UserAuthHook, error) {
	if err := c.SendAuthRequest(authCleartextPassword, nil /* data */); err != nil {
		return nil, err
	}
	pwdData, err := c.GetPwdData()
	if err != nil {
		return nil, err
	}
	password, err := passwordString(pwdData)
	if err != nil {
		return nil, err
	}
	return security.UserAuthPasswordHook(
		insecure, password, hashedPassword,
	), nil
}

func authCert(
	_ AuthConn,
	tlsState tls.ConnectionState,
	insecure bool,
	hashedPassword []byte,
	execCfg *sql.ExecutorConfig,
	entry *hba.Entry,
) (security.UserAuthHook, error) {
	if len(tlsState.PeerCertificates) == 0 {
		return nil, errors.New("no TLS peer certificates, but required for auth")
	}
	// Normalize the username contained in the certificate.
	tlsState.PeerCertificates[0].Subject.CommonName = tree.Name(
		tlsState.PeerCertificates[0].Subject.CommonName,
	).Normalize()
	return security.UserAuthCertHook(insecure, &tlsState)
}

func authCertPassword(
	c AuthConn,
	tlsState tls.ConnectionState,
	insecure bool,
	hashedPassword []byte,
	execCfg *sql.ExecutorConfig,
	entry *hba.Entry,
) (security.UserAuthHook, error) {
	var fn AuthMethod
	if len(tlsState.PeerCertificates) == 0 {
		fn = authPassword
	} else {
		fn = authCert
	}
	return fn(c, tlsState, insecure, hashedPassword, execCfg, entry)
}

func init() {
	RegisterAuthMethod("password", authPassword, nil)
	RegisterAuthMethod("cert", authCert, nil)
	RegisterAuthMethod("cert-password", authCertPassword, nil)
}

// statusReportParams is a list of session variables that are also
// reported as server run-time parameters in the pgwire connection
// initialization.
//
// The standard PostgreSQL status vars are listed here:
// https://www.postgresql.org/docs/10/static/libpq-status.html
var statusReportParams = []string{
	"server_version",
	"server_encoding",
	"client_encoding",
	"application_name",
	// Note: is_superuser and session_authorization are handled
	// specially in serveImpl().
	"DateStyle",
	"IntervalStyle",
	"TimeZone",
	"integer_datetimes",
	"standard_conforming_strings",
	"crdb_version", // CockroachDB extension.
}

// testingStatusReportParams is the minimum set of status parameters
// needed to make pgx tests in the local package happy.
var testingStatusReportParams = map[string]string{
	"client_encoding":             "UTF8",
	"standard_conforming_strings": "on",
}

// readTimeoutConn overloads net.Conn.Read by periodically calling
// checkExitConds() and aborting the read if an error is returned.
type readTimeoutConn struct {
	net.Conn
	// checkExitConds is called periodically by Read(). If it returns an error,
	// the Read() returns that error. Future calls to Read() are allowed, in which
	// case checkExitConds() will be called again.
	checkExitConds func() error
}

func newReadTimeoutConn(c net.Conn, checkExitConds func() error) net.Conn {
	// net.Pipe does not support setting deadlines. See
	// https://github.com/golang/go/blob/go1.7.4/src/net/pipe.go#L57-L67
	//
	// TODO(andrei): starting with Go 1.10, pipes are supposed to support
	// timeouts, so this should go away when we upgrade the compiler.
	if c.LocalAddr().Network() == "pipe" {
		return c
	}
	return &readTimeoutConn{
		Conn:           c,
		checkExitConds: checkExitConds,
	}
}

func (c *readTimeoutConn) Read(b []byte) (int, error) {
	// readTimeout is the amount of time ReadTimeoutConn should wait on a
	// read before checking for exit conditions. The tradeoff is between the
	// time it takes to react to session context cancellation and the overhead
	// of waking up and checking for exit conditions.
	const readTimeout = 1 * time.Second

	// Remove the read deadline when returning from this function to avoid
	// unexpected behavior.
	defer func() { _ = c.SetReadDeadline(time.Time{}) }()
	for {
		if err := c.checkExitConds(); err != nil {
			return 0, err
		}
		if err := c.SetReadDeadline(timeutil.Now().Add(readTimeout)); err != nil {
			return 0, err
		}
		n, err := c.Conn.Read(b)
		// Continue if the error is due to timing out.
		if err, ok := err.(net.Error); ok && err.Timeout() {
			continue
		}
		return n, err
	}
}
