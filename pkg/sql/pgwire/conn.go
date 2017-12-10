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
	"context"
	"io"
	"net"
	"strconv"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/lib/pq/oid"
)

// conn implements a pgwire network connection (version 3 of the protocol,
// implemented by Postgres v7.4 and later). conn.serve() read protocol messages,
// transforms them into commands that it pushes onto a StmtBuf, to be picked up
// and executed by the connExecutor. Results are then produced and sent through
// the conn back to the client.
// TODO(andrei): Detail results production path.
type conn struct {
	conn        net.Conn
	rd          *bufio.Reader
	wr          *bufio.Writer
	stmtBuf     *sql.StmtBuf
	readBuf     pgwirebase.ReadBuffer
	writeBuf    *writeBuffer
	sessionArgs sql.SessionArgs

	// The logic governing these guys is hairy, and is not sufficiently
	// specified in documentation. Consult the sources before you modify:
	// https://github.com/postgres/postgres/blob/master/src/backend/tcop/postgres.c
	doingExtendedQueryMessage bool

	metrics *ServerMetrics
}

func newConn(netConn net.Conn, metrics *ServerMetrics) *conn {
	wb := newWriteBuffer()
	wb.bytecount = metrics.BytesOutCount
	c := conn{
		conn:     netConn,
		rd:       bufio.NewReader(netConn),
		wr:       bufio.NewWriter(netConn),
		writeBuf: wb,
		metrics:  metrics,
	}
	c.setupSession()
	return &c
}

func (c *conn) setupSession() {
	// WIP(andrei)
	// c.session = sql.NewSession(
	//   ctx, c.sessionArgs, c.executor, c.conn.RemoteAddr(), &c.metrics.SQLMemMetrics, c,
	// )
	// c.session.StartMonitor(c.sqlMemoryPool, reserved)

	c.stmtBuf = sql.NewStmtBuf()

	// WIP(andrei): create the connExecutor consuming the buffer and pass it the
	// annotated ctx.
}

// serve continuously reads from the network connection and pushes execution
// instructions into a sql.StmtBuf.
// The method when the client has sent the pgwire termination message or when
// network communication has failed.
//
// serve always closes the network connection before returning.
func (c *conn) serve(ctx context.Context, draining func() bool) error {
	// TODO(andrei): figure out who's in charge of closing the stmtBuf and how to
	// send the cancelation signal to queries that might be executing when this
	// returns.
	defer func() { _ = c.conn.Close() }()

	for key, value := range statusReportParams {
		c.writeBuf.initMsg(pgwirebase.ServerMsgParameterStatus)
		c.writeBuf.writeTerminatedString(key)
		c.writeBuf.writeTerminatedString(value)
		if err := c.writeBuf.finishMsg(c.wr); err != nil {
			return err
		}
	}
	if err := c.wr.Flush(); err != nil {
		return err
	}

	ctx = log.WithLogTagStr(ctx, "user", c.sessionArgs.User)

	// Once a session has been set up, the underlying net.Conn is switched to
	// a conn that exits if the session's context is canceled or if the server
	// is draining and the session does not have an ongoing transaction.
	c.conn = newReadTimeoutConn(c.conn, func() error {
		// WIP(andrei): integrate with draining.
		// return newAdminShutdownErr(err)
		return nil
	})
	c.rd = bufio.NewReader(c.conn)

	// An initial readyForQuery messge is part of the handshake.
	c.writeBuf.initMsg(pgwirebase.ServerMsgReady)
	c.writeBuf.writeByte('I') // transaction status: no txn
	if err := c.writeBuf.finishMsg(c.wr); err != nil {
		return err
	}
	if err := c.wr.Flush(); err != nil {
		return err
	}

	for {
		typ, n, err := c.readBuf.ReadTypedMsg(c.rd)
		c.metrics.BytesInCount.Inc(int64(n))
		if err != nil {
			return err
		}
		if log.V(2) {
			log.Infof(ctx, "pgwire: processing %s", typ)
		}
		switch typ {
		case pgwirebase.ClientMsgSync:
			c.doingExtendedQueryMessage = false
			// We're starting a batch here. If the client continues using the extended
			// protocol and encounters an error, everything until the next sync
			// message has to be skipped. See:
			// https://www.postgresql.org/docs/current/10/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
			c.stmtBuf.StartBatch()

		case pgwirebase.ClientMsgSimpleQuery:
			if c.doingExtendedQueryMessage {
				return c.stmtBuf.Push(
					ctx,
					sql.SendError{
						Err: pgwirebase.NewProtocolViolationErrorf(
							"SimpleQuery not allowed while in extended protocol mode"),
					},
				)
			}
			// We're starting a batch here. When the client is using the simple
			// protocol and an error is encountered, all queries part of the query
			// string in the current message (i.e. in the current semicolon-separated
			// string) have to be skipped. See:
			// https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
			c.stmtBuf.StartBatch()
			err = c.handleSimpleQuery(ctx, &c.readBuf)

		case pgwirebase.ClientMsgTerminate:
			c.stmtBuf.Close()
			return nil

		case pgwirebase.ClientMsgParse:
			c.doingExtendedQueryMessage = true
			err = c.handleParse(ctx, &c.readBuf)

		case pgwirebase.ClientMsgDescribe:
			c.doingExtendedQueryMessage = true
			err = c.handleDescribe(ctx, &c.readBuf)

		case pgwirebase.ClientMsgClose:
			c.doingExtendedQueryMessage = true
			err = c.handleClose(ctx, &c.readBuf)

		case pgwirebase.ClientMsgBind:
			c.doingExtendedQueryMessage = true
			err = c.handleBind(ctx, &c.readBuf)

		case pgwirebase.ClientMsgExecute:
			c.doingExtendedQueryMessage = true
			err = c.handleExecute(ctx, &c.readBuf)

		case pgwirebase.ClientMsgFlush:
			c.doingExtendedQueryMessage = true
			// WIP(andrei): flush something

		case pgwirebase.ClientMsgCopyData, pgwirebase.ClientMsgCopyDone, pgwirebase.ClientMsgCopyFail:
			// We're supposed to ignore these messages, per the protocol spec. This
			// state will happen when an error occurs on the server-side during a copy
			// operation executed in simple query mode: the server will send an error
			// and a ready message back to the client, and must then ignore further
			// copy messages. See:
			// https://github.com/postgres/postgres/blob/6e1dd2773eb60a6ab87b27b8d9391b756e904ac3/src/backend/tcop/postgres.c#L4295
			//
			// Note that this should not occur in extended query mode. There, the
			// copyMachine is supposed to have consumed everything until a sync
			// message (which exits extended mode).
			//
			// WIP(andrei): implement this draining in copyMachine.

		default:
			err = c.stmtBuf.Push(
				ctx,
				sql.SendError{Err: pgwirebase.NewUnrecognizedMsgTypeErr(typ)})
		}
		if err != nil {
			return err
		}
	}
}

// An error is returned iff the statement buffer has been closed. In that case,
// the connection should be considered toast.
func (c *conn) handleSimpleQuery(ctx context.Context, buf *pgwirebase.ReadBuffer) error {
	query, err := buf.GetString()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}

	tracing.AnnotateTrace()

	startParse := timeutil.Now()
	sl, err := parser.Parse(query)
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	endParse := timeutil.Now()

	for _, stmt := range sl {
		// The CopyFrom statement is special. We need to detect it so we can hand
		// control of the connection, through the stmtBuf, to a copyMachine, and
		// block this network routine until control is passed back.
		if cp, ok := stmt.(*tree.CopyFrom); ok {
			if len(sl) != 1 {
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
			if err := c.stmtBuf.Push(ctx, sql.CopyIn{Stmt: cp, CopyDone: &copyDone}); err != nil {
				return err
			}
			copyDone.Wait()
			// The copyMachine always exits the extended protocol mode, if we were in
			// it.
			c.doingExtendedQueryMessage = false
			return nil
		}

		if err := c.stmtBuf.Push(
			ctx,
			sql.ExecStmt{
				Stmt: stmt, ParseStart: startParse, ParseEnd: endParse,
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
			err := pgerror.NewErrorf(
				pgerror.CodeProtocolViolationError, "unknown oid type: %v", t)
			return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
		}
		sqlTypeHints[strconv.Itoa(i+1)] = v
	}

	startParse := timeutil.Now()
	stmt, err := parser.ParseOne(query)
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	endParse := timeutil.Now()

	// The CopyFrom statement is special. We need to detect it so we can hand
	// control of the connection, through the stmtBuf, to a copyMachine, and block
	// this network routine until control is passed back.
	// See https://www.postgresql.org/docs/10/static/protocol-flow.html#PROTOCOL-COPY
	if cp, ok := stmt.(*tree.CopyFrom); ok {
		copyDone := sync.WaitGroup{}
		copyDone.Add(1)
		if err := c.stmtBuf.Push(ctx, sql.CopyIn{Stmt: cp, Conn: c, CopyDone: &copyDone}); err != nil {
			return err
		}
		copyDone.Wait()
		// The copyMachine always exits the extended protocol mode, if we were in
		// it.
		c.doingExtendedQueryMessage = false
		return nil
	}

	return c.stmtBuf.Push(
		ctx,
		sql.PrepareStmt{
			Name:       name,
			Stmt:       stmt,
			TypeHints:  sqlTypeHints,
			ParseStart: startParse,
			ParseEnd:   endParse,
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
	columnFormatCodes := make([]pgwirebase.FormatCode, numColumnFormatCodes)
	switch numColumnFormatCodes {
	case 0:
	case 1:
		// Read one code and apply it to every column.
		ch, err := buf.GetUint16()
		if err != nil {
			return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
		}
		fmtCode := pgwirebase.FormatCode(ch)
		for i := range columnFormatCodes {
			columnFormatCodes[i] = fmtCode
		}
	default:
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
			ProtocolMeta:          preparedPortalMeta{outFormats: columnFormatCodes},
		})
}

func (c *conn) handleExecute(ctx context.Context, buf *pgwirebase.ReadBuffer) error {
	portalName, err := buf.GetString()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	limit, err := buf.GetUint32()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	return c.stmtBuf.Push(ctx, sql.ExecPortal{Name: portalName, Limit: int(limit)})
}

// BeginCopyIn is part of the pgwirebase.Conn interface.
func (c *conn) BeginCopyIn(ctx context.Context, columns []sqlbase.ResultColumn) error {
	c.writeBuf.initMsg(pgwirebase.ServerMsgCopyInResponse)
	c.writeBuf.writeByte(byte(pgwirebase.FormatText))
	c.writeBuf.putInt16(int16(len(columns)))
	for range columns {
		c.writeBuf.putInt16(int16(pgwirebase.FormatText))
	}
	if err := c.writeBuf.finishMsg(c.wr); err != nil {
		return sql.NewWireFailureError(err)
	}
	if err := c.wr.Flush(); err != nil {
		return sql.NewWireFailureError(err)
	}
	return nil
}

// SendError is part of te pgwirebase.Conn interface.
func (c *conn) SendError(err error) error {
	// WIP(andrei)
	return nil
}

// SendCommandComplete is part of the pgwirebase.Conn interface.
func (c *conn) SendCommandComplete(tag []byte) error {
	// WIP(andrei)
	return nil
}

// Rd is part of the pgwirebase.Conn interface.
func (c *conn) Rd() pgwirebase.BufferedReader {
	return &pgwireReader2{conn: c}
}

// writeRowDescription writes a row description to the given writer.
//
// formatCodes specifies the format for each column. It can be nil, in which
// case all columns will use FormatText.
func (c *conn) sendRowDescription(
	ctx context.Context,
	columns []sqlbase.ResultColumn,
	formatCodes []pgwirebase.FormatCode,
	w io.Writer,
) error {
	c.writeBuf.initMsg(pgwirebase.ServerMsgRowDescription)
	c.writeBuf.putInt16(int16(len(columns)))
	for i, column := range columns {
		if log.V(2) {
			log.Infof(ctx, "pgwire: writing column %s of type: %T", column.Name, column.Typ)
		}
		c.writeBuf.writeTerminatedString(column.Name)

		typ := pgTypeForParserType(column.Typ)
		c.writeBuf.putInt32(0) // Table OID (optional).
		c.writeBuf.putInt16(0) // Column attribute ID (optional).
		c.writeBuf.putInt32(int32(typ.oid))
		c.writeBuf.putInt16(int16(typ.size))
		// The type modifier (atttypmod) is used to include various extra information
		// about the type being sent. -1 is used for values which don't make use of
		// atttypmod and is generally an acceptable catch-all for those that do.
		// See https://www.postgresql.org/docs/9.6/static/catalog-pg-attribute.html
		// for information on atttypmod. In theory we differ from Postgres by never
		// giving the scale/precision, and by not including the length of a VARCHAR,
		// but it's not clear if any drivers/ORMs depend on this.
		//
		// TODO(justin): It would be good to include this information when possible.
		c.writeBuf.putInt32(-1)
		if formatCodes == nil {
			c.writeBuf.putInt16(int16(pgwirebase.FormatText))
		} else {
			c.writeBuf.putInt16(int16(formatCodes[i]))
		}
	}
	return c.writeBuf.finishMsg(w)
}

// pgwireReader2 is an io.Reader that wrapps a v3Conn, maintaining its metrics as
// it is consumed.
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
