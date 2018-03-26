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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lib/pq/oid"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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
	execCfg     *sql.ExecutorConfig
	metrics     *ServerMetrics

	// rd is a buffered reader consuming conn. All reads from conn go through
	// this.
	rd bufio.Reader

	// stmtBuf is populated with commands queued for execution by this conn.
	stmtBuf *sql.StmtBuf

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
	msgBuilder *writeBuffer
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
	execCfg *sql.ExecutorConfig,
	stopper *stop.Stopper,
	insecure bool,
) error {
	sArgs.RemoteAddr = netConn.RemoteAddr()

	if log.V(2) {
		log.Infof(ctx, "new connection with options: %+v", sArgs)
	}

	c := newConn(netConn, sArgs, metrics, execCfg)

	if err := c.handleAuthentication(ctx, insecure); err != nil {
		_ = c.conn.Close()
		reserved.Close(ctx)
		return err
	}

	// Do the reading of commands from the network.
	readingErr := c.serveImpl(ctx, draining, sqlServer, reserved, stopper)
	return readingErr
}

func newConn(
	netConn net.Conn, sArgs sql.SessionArgs, metrics *ServerMetrics, execCfg *sql.ExecutorConfig,
) *conn {
	c := &conn{
		conn:        netConn,
		stmtBuf:     sql.NewStmtBuf(),
		sessionArgs: sArgs,
		msgBuilder:  newWriteBuffer(metrics.BytesOutCount),
		metrics:     metrics,
		rd:          *bufio.NewReader(netConn),
		execCfg:     execCfg,
	}
	c.writerState.fi.buf = &c.writerState.buf
	c.writerState.fi.lastFlushed = -1
	c.writerState.fi.cmdStarts = make(map[sql.CmdPos]int)

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
// instructions into a sql.StmtBuf.
// The method returns when the pgwire termination message is received, when
// network communication fails, when the server is draining or when ctx is
// canceled (which also happens when draining, but not from the get-go).
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
	stopper *stop.Stopper,
) error {
	defer func() { _ = c.conn.Close() }()

	// NOTE: We're going to write a few messages to the connection in this method,
	// for the handshake. After that, all writes are done async, in the
	// startWriter() goroutine.

	for key, value := range statusReportParams {
		c.msgBuilder.initMsg(pgwirebase.ServerMsgParameterStatus)
		c.msgBuilder.writeTerminatedString(key)
		c.msgBuilder.writeTerminatedString(value)
		if err := c.msgBuilder.finishMsg(c.conn); err != nil {
			return err
		}
	}

	// An initial readyForQuery message is part of the handshake.
	c.msgBuilder.initMsg(pgwirebase.ServerMsgReady)
	c.msgBuilder.writeByte(byte(sql.IdleTxnBlock))
	if err := c.msgBuilder.finishMsg(c.conn); err != nil {
		return err
	}

	ctx = log.WithLogTagStr(ctx, "user", c.sessionArgs.User)
	ctx, stopReader := context.WithCancel(ctx)
	defer stopReader() // This calms the linter that wants these callbacks to always be called.
	var ctxCanceled bool

	// Once a session has been set up, the underlying net.Conn is switched to
	// a conn that exits if the session's context is canceled.
	c.conn = newReadTimeoutConn(c.conn, func() error {
		// If the context was closed, it's time to bail. Either a higher-level
		// server or the command processor have canceled us.
		if ctx.Err() != nil {
			ctxCanceled = true
			return ctx.Err()
		}
		// If the server is draining, we'll let the processor know by pushing a
		// DrainRequest. This will make the processor quit whenever it finds a good
		// time.
		if draining() {
			_ /* err */ = c.stmtBuf.Push(ctx, sql.DrainRequest{})
		}
		return nil
	})
	c.rd = *bufio.NewReader(c.conn)

	var wg sync.WaitGroup
	var writerErr error
	processorCtx, stopProcessor := context.WithCancel(ctx)
	if sqlServer != nil {
		wg.Add(1)
		go func() {
			writerErr = sqlServer.ServeConn(
				processorCtx, c.sessionArgs, c.stmtBuf, c, reserved, &c.metrics.SQLMemMetrics, stopProcessor)
			// TODO(andrei): Should we sometimes transmit the writerErr's to the
			// client?
			wg.Done()
			stopReader()
		}()
	}

	var err error
	var terminateSeen bool
	var doingExtendedQueryMessage bool

Loop:
	for {
		var typ pgwirebase.ClientMessageType
		var n int
		typ, n, err = c.readBuf.ReadTypedMsg(&c.rd)
		c.metrics.BytesInCount.Inc(int64(n))
		if err != nil {
			break Loop
		}
		if log.V(2) {
			log.Infof(ctx, "pgwire: processing %s", typ)
		}
		timeReceived := timeutil.Now()
		switch typ {
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
			if err = c.handleSimpleQuery(ctx, &c.readBuf, timeReceived); err != nil {
				break
			}
			err = c.stmtBuf.Push(ctx, sql.Sync{})

		case pgwirebase.ClientMsgExecute:
			doingExtendedQueryMessage = true
			err = c.handleExecute(ctx, &c.readBuf, timeReceived)

		case pgwirebase.ClientMsgParse:
			doingExtendedQueryMessage = true
			err = c.handleParse(ctx, &c.readBuf)

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

	// Signal command processing to stop. It might be the case that the processor
	// canceled our context and that's how we got here; in that case, this will
	// be a no-op.
	c.stmtBuf.Close()
	stopProcessor()
	wg.Wait()

	if terminateSeen {
		return nil
	}
	// If we're draining, let the client know by piling on an AdminShutdownError
	// and flushing the buffer.
	if ctxCanceled || draining() {
		_ /* err */ = writeErr(
			newAdminShutdownErr(err), c.msgBuilder, &c.writerState.buf)
		_ /* n */, _ /* err */ = c.writerState.buf.WriteTo(c.conn)

		// Swallow whatever error we might have gotten from the writer. If we're
		// draining, it's probably a canceled context error.
		return nil
	}
	if writerErr != nil {
		return writerErr
	}
	return nil
}

// An error is returned iff the statement buffer has been closed. In that case,
// the connection should be considered toast.
func (c *conn) handleSimpleQuery(
	ctx context.Context, buf *pgwirebase.ReadBuffer, timeReceived time.Time,
) error {
	query, err := buf.GetString()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}

	tracing.AnnotateTrace()

	startParse := timeutil.Now()
	stmts, err := parser.Parse(query)
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	endParse := timeutil.Now()

	if len(stmts) == 0 {
		return c.stmtBuf.Push(
			ctx, sql.ExecStmt{
				Stmt:         nil,
				TimeReceived: timeReceived,
				ParseStart:   startParse,
				ParseEnd:     endParse,
			})
	}

	for _, stmt := range stmts {
		// The CopyFrom statement is special. We need to detect it so we can hand
		// control of the connection, through the stmtBuf, to a copyMachine, and
		// block this network routine until control is passed back.
		if cp, ok := stmt.(*tree.CopyFrom); ok {
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
				Stmt:         stmt,
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
func (c *conn) handleParse(ctx context.Context, buf *pgwirebase.ReadBuffer) error {
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
	// Prepare the mapping of SQL placeholder names to types. Pre-populate it with
	// the type hints received from the client, if any.
	sqlTypeHints := make(tree.PlaceholderTypes)
	for i, t := range inTypeHints {
		if t == 0 {
			continue
		}
		v, ok := types.OidToType[t]
		if !ok {
			err := pgwirebase.NewProtocolViolationErrorf("unknown oid type: %v", t)
			return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
		}
		sqlTypeHints[strconv.Itoa(i+1)] = v
	}

	startParse := timeutil.Now()
	var stmt tree.Statement
	stmts, err := parser.Parse(query)
	if len(stmts) > 1 {
		err = pgerror.NewWrongNumberOfPreparedStatements(len(stmts))
	} else if len(stmts) == 1 {
		stmt = stmts[0]
	}
	// len(stmts) == 0 results in a nil (empty) statement.

	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	endParse := timeutil.Now()

	if _, ok := stmt.(*tree.CopyFrom); ok {
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
			Stmt:         stmt,
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
	if err := c.msgBuilder.finishMsg(c.conn); err != nil {
		return sql.NewWireFailureError(err)
	}
	return nil
}

// SendCommandComplete is part of the pgwirebase.Conn interface.
func (c *conn) SendCommandComplete(tag []byte) error {
	c.bufferCommandComplete(tag)
	return nil
}

// Rd is part of the pgwirebase.Conn interface.
func (c *conn) Rd() pgwirebase.BufferedReader {
	return &pgwireReader2{conn: c}
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
	switch tErr := err.(type) {
	case *roachpb.HandledRetryableTxnError:
		return sqlbase.NewRetryError(err)
	case *roachpb.AmbiguousResultError:
		// TODO(andrei): Once DistSQL starts executing writes, we'll need a
		// different mechanism to marshal AmbiguousResultErrors from the executing
		// nodes.
		return sqlbase.NewStatementCompletionUnknownError(tErr)
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
	ctx context.Context, row tree.Datums, formatCodes []pgwirebase.FormatCode, loc *time.Location,
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
			c.msgBuilder.writeTextDatum(ctx, col, loc)
		case pgwirebase.FormatBinary:
			c.msgBuilder.writeBinaryDatum(ctx, col, loc)
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
	if err := writeErr(err, c.msgBuilder, &c.writerState.buf); err != nil {
		panic(fmt.Sprintf("unexpected err from buffer: %s", err))
	}
}
func (c *conn) bufferEmptyQueryResponse() {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgEmptyQuery)
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(fmt.Sprintf("unexpected err from buffer: %s", err))
	}
}

func writeErr(err error, msgBuilder *writeBuffer, w io.Writer) error {
	msgBuilder.initMsg(pgwirebase.ServerMsgErrorResponse)

	msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFieldSeverity)
	msgBuilder.writeTerminatedString("ERROR")

	pgErr, ok := pgerror.GetPGCause(err)
	var code string
	if ok {
		code = pgErr.Code
	} else {
		code = pgerror.CodeInternalError
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
		c.msgBuilder.putInt32(int32(typ.oid))
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
// connResultsBufferSizeBytes.
func (c *conn) maybeFlush(pos sql.CmdPos) (bool, error) {
	if c.writerState.buf.Len() <= c.execCfg.ConnResultsBufferBytes {
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
	loc *time.Location,
) sql.CommandResult {
	res := c.makeCommandResult(descOpt, pos, stmt, formatCodes, loc)
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

// pgwireReader2 is an io.Reader that wraps a conn, maintaining its metrics as
// it is consumed.
//
// TODO(andrei): rename when pgwireReader goes away.
type pgwireReader2 struct {
	conn *conn
}

// pgwireReader2 implements the pgwirebase.BufferedReader interface.
var _ pgwirebase.BufferedReader = &pgwireReader2{}

// Read is part of the pgwirebase.BufferedReader interface.
func (r *pgwireReader2) Read(p []byte) (int, error) {
	n, err := r.conn.rd.Read(p)
	r.conn.metrics.BytesInCount.Inc(int64(n))
	return n, err
}

// ReadString is part of the pgwirebase.BufferedReader interface.
func (r *pgwireReader2) ReadString(delim byte) (string, error) {
	s, err := r.conn.rd.ReadString(delim)
	r.conn.metrics.BytesInCount.Inc(int64(len(s)))
	return s, err
}

// ReadByte is part of the pgwirebase.BufferedReader interface.
func (r *pgwireReader2) ReadByte() (byte, error) {
	b, err := r.conn.rd.ReadByte()
	if err == nil {
		r.conn.metrics.BytesInCount.Inc(1)
	}
	return b, err
}

// handleAuthentication should discuss with the client to arrange
// authentication and update c.sessionArgs with the authenticated user's
// name, if different from the one given initially. Note: at this
// point the sql.Session does not exist yet! If need exists to access the
// database to look up authentication data, use the internal executor.
func (c *conn) handleAuthentication(ctx context.Context, insecure bool) error {

	sendError := func(err error) error {
		_ /* err */ = writeErr(err, c.msgBuilder, c.conn)
		return err
	}

	// Check that the requested user exists and retrieve the hashed
	// password in case password authentication is needed.
	exists, hashedPassword, err := sql.GetUserHashedPassword(
		ctx, c.execCfg, c.metrics.internalMemMetrics, c.sessionArgs.User,
	)
	if err != nil {
		return sendError(err)
	}
	if !exists {
		return sendError(errors.Errorf("user %s does not exist", c.sessionArgs.User))
	}

	if tlsConn, ok := c.conn.(*tls.Conn); ok {
		var authenticationHook security.UserAuthHook

		tlsState := tlsConn.ConnectionState()
		// If no certificates are provided, default to password
		// authentication.
		if len(tlsState.PeerCertificates) == 0 {
			password, err := c.sendAuthPasswordRequest()
			if err != nil {
				return sendError(err)
			}
			authenticationHook = security.UserAuthPasswordHook(
				insecure, password, hashedPassword,
			)
		} else {
			// Normalize the username contained in the certificate.
			tlsState.PeerCertificates[0].Subject.CommonName = tree.Name(
				tlsState.PeerCertificates[0].Subject.CommonName,
			).Normalize()
			var err error
			authenticationHook, err = security.UserAuthCertHook(insecure, &tlsState)
			if err != nil {
				return sendError(err)
			}
		}

		if err := authenticationHook(c.sessionArgs.User, true /* public */); err != nil {
			return sendError(err)
		}
	}

	c.msgBuilder.initMsg(pgwirebase.ServerMsgAuth)
	c.msgBuilder.putInt32(authOK)
	return c.msgBuilder.finishMsg(c.conn)
}

// sendAuthPasswordRequest requests a cleartext password from the client and
// returns it.
func (c *conn) sendAuthPasswordRequest() (string, error) {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgAuth)
	c.msgBuilder.putInt32(authCleartextPassword)
	if err := c.msgBuilder.finishMsg(c.conn); err != nil {
		return "", err
	}

	typ, n, err := c.readBuf.ReadTypedMsg(&c.rd)
	c.metrics.BytesInCount.Inc(int64(n))
	if err != nil {
		return "", err
	}

	if typ != pgwirebase.ClientMsgPassword {
		return "", errors.Errorf("invalid response to authentication request: %s", typ)
	}

	return c.readBuf.GetString()
}
