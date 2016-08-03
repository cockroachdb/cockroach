// Copyright 2015 The Cockroach Authors.
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
//
// Author: Ben Darnell
// Author: Tamir Duberstein (tamird@gmail.com)

package pgwire

import (
	"bufio"
	"fmt"
	"net"
	"reflect"
	"strconv"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/tracing"
	"github.com/cockroachdb/pq/oid"
	"github.com/pkg/errors"
)

//go:generate stringer -type=clientMessageType
type clientMessageType byte

//go:generate stringer -type=serverMessageType
type serverMessageType byte

// http://www.postgresql.org/docs/9.4/static/protocol-message-formats.html
const (
	clientMsgBind        clientMessageType = 'B'
	clientMsgClose       clientMessageType = 'C'
	clientMsgDescribe    clientMessageType = 'D'
	clientMsgExecute     clientMessageType = 'E'
	clientMsgFlush       clientMessageType = 'H'
	clientMsgParse       clientMessageType = 'P'
	clientMsgSimpleQuery clientMessageType = 'Q'
	clientMsgSync        clientMessageType = 'S'
	clientMsgTerminate   clientMessageType = 'X'

	serverMsgAuth                 serverMessageType = 'R'
	serverMsgBindComplete         serverMessageType = '2'
	serverMsgCommandComplete      serverMessageType = 'C'
	serverMsgCloseComplete        serverMessageType = '3'
	serverMsgDataRow              serverMessageType = 'D'
	serverMsgEmptyQuery           serverMessageType = 'I'
	serverMsgErrorResponse        serverMessageType = 'E'
	serverMsgNoData               serverMessageType = 'n'
	serverMsgParameterDescription serverMessageType = 't'
	serverMsgParameterStatus      serverMessageType = 'S'
	serverMsgParseComplete        serverMessageType = '1'
	serverMsgReady                serverMessageType = 'Z'
	serverMsgRowDescription       serverMessageType = 'T'
)

//go:generate stringer -type=serverErrFieldType
type serverErrFieldType byte

// http://www.postgresql.org/docs/current/static/protocol-error-fields.html
const (
	serverErrFieldSeverity    serverErrFieldType = 'S'
	serverErrFieldSQLState    serverErrFieldType = 'C'
	serverErrFieldMsgPrimary  serverErrFieldType = 'M'
	serverErrFieldSrcFile     serverErrFieldType = 'F'
	serverErrFieldSrcLine     serverErrFieldType = 'L'
	serverErrFieldSrcFunction serverErrFieldType = 'R'
)

//go:generate stringer -type=prepareType
type prepareType byte

const (
	prepareStatement prepareType = 'S'
	preparePortal    prepareType = 'P'
)

const (
	authOK int32 = 0
)

// preparedStatementMeta is pgwire-specific metadata which is attached to each
// sql.PreparedStatement on a v3Conn's sql.Session.
type preparedStatementMeta struct {
	inTypes []oid.Oid
}

// preparedPortalMeta is pgwire-specific metadata which is attached to each
// sql.PreparedPortal on a v3Conn's sql.Session.
type preparedPortalMeta struct {
	outFormats []formatCode
}

type v3Conn struct {
	conn     net.Conn
	rd       *bufio.Reader
	wr       *bufio.Writer
	executor *sql.Executor
	readBuf  readBuffer
	writeBuf writeBuffer
	tagBuf   [64]byte
	session  *sql.Session

	// The logic governing these guys is hairy, and is not sufficiently
	// specified in documentation. Consult the sources before you modify:
	// https://github.com/postgres/postgres/blob/master/src/backend/tcop/postgres.c
	doingExtendedQueryMessage, ignoreTillSync bool

	metrics *serverMetrics
}

func makeV3Conn(
	conn net.Conn, executor *sql.Executor, metrics *serverMetrics, sessionArgs sql.SessionArgs,
) v3Conn {
	return v3Conn{
		conn:     conn,
		rd:       bufio.NewReader(conn),
		wr:       bufio.NewWriter(conn),
		executor: executor,
		writeBuf: writeBuffer{bytecount: metrics.bytesOutCount},
		metrics:  metrics,
		session:  sql.NewSession(executor.Ctx(), sessionArgs, executor, conn.RemoteAddr()),
	}
}

func (c *v3Conn) finish() {
	// This is better than always flushing on error.
	if err := c.wr.Flush(); err != nil {
		log.Error(context.TODO(), err)
	}
	_ = c.conn.Close()
	c.session.Finish()
}

func parseOptions(data []byte) (sql.SessionArgs, error) {
	args := sql.SessionArgs{}
	buf := readBuffer{msg: data}
	for {
		key, err := buf.getString()
		if err != nil {
			return sql.SessionArgs{}, errors.Errorf("error reading option key: %s", err)
		}
		if len(key) == 0 {
			break
		}
		value, err := buf.getString()
		if err != nil {
			return sql.SessionArgs{}, errors.Errorf("error reading option value: %s", err)
		}
		switch key {
		case "database":
			args.Database = value
		case "user":
			args.User = value
		default:
			if log.V(1) {
				log.Warningf(context.TODO(), "unrecognized configuration parameter %q", key)
			}
		}
	}
	return args, nil
}

// statusReportParams is a static mapping from run-time parameters to their respective
// hard-coded values, each of which is to be returned as part of the status report
// during connection initialization.
var statusReportParams = map[string]string{
	"client_encoding": "UTF8",
	"DateStyle":       "ISO",
	// The latest version of the docs that was consulted during the development
	// of this package. We specify this version to avoid having to support old
	// code paths which various client tools fall back to if they can't
	// determine that the server is new enough.
	"server_version": "9.5.0",
}

func (c *v3Conn) serve(authenticationHook func(string, bool) error) error {
	ctx := c.session.Ctx()

	if authenticationHook != nil {
		if err := authenticationHook(c.session.User, true /* public */); err != nil {
			return c.sendInternalError(err.Error())
		}
	}
	c.writeBuf.initMsg(serverMsgAuth)
	c.writeBuf.putInt32(authOK)
	if err := c.writeBuf.finishMsg(c.wr); err != nil {
		return err
	}
	for key, value := range statusReportParams {
		c.writeBuf.initMsg(serverMsgParameterStatus)
		c.writeBuf.writeTerminatedString(key)
		c.writeBuf.writeTerminatedString(value)
		if err := c.writeBuf.finishMsg(c.wr); err != nil {
			return err
		}
	}
	if err := c.wr.Flush(); err != nil {
		return err
	}

	for {
		if !c.doingExtendedQueryMessage {
			c.writeBuf.initMsg(serverMsgReady)
			var txnStatus byte
			switch c.session.TxnState.State {
			case sql.Aborted, sql.RestartWait:
				// We send status "InFailedTransaction" also for state RestartWait
				// because GO's lib/pq freaks out if we invent a new status.
				txnStatus = 'E'
			case sql.Open:
				txnStatus = 'T'
			case sql.NoTxn:
				// We're not in a txn (i.e. the last txn was committed).
				txnStatus = 'I'
			case sql.CommitWait:
				// We need to lie to pgwire and claim that we're still
				// in a txn. Otherwise drivers freak out.
				// This state is not part of the Postgres protocol.
				txnStatus = 'T'
			}

			if log.V(2) {
				log.Infof(context.TODO(), "pgwire: %s: %q", serverMsgReady, txnStatus)
			}
			c.writeBuf.writeByte(txnStatus)
			if err := c.writeBuf.finishMsg(c.wr); err != nil {
				return err
			}
			// We only flush on every message if not doing an extended query.
			// If we are, wait for an explicit Flush message. See:
			// http://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY.
			if err := c.wr.Flush(); err != nil {
				return err
			}
		}
		typ, n, err := c.readBuf.readTypedMsg(c.rd)
		c.metrics.bytesInCount.Inc(int64(n))
		if err != nil {
			return err
		}
		// When an error occurs handling an extended query message, we have to ignore
		// any messages until we get a sync.
		if c.ignoreTillSync && typ != clientMsgSync {
			if log.V(2) {
				log.Infof(context.TODO(), "pgwire: ignoring %s till sync", typ)
			}
			continue
		}
		if log.V(2) {
			log.Infof(context.TODO(), "pgwire: processing %s", typ)
		}
		switch typ {
		case clientMsgSync:
			c.doingExtendedQueryMessage = false
			c.ignoreTillSync = false

		case clientMsgSimpleQuery:
			c.doingExtendedQueryMessage = false
			err = c.handleSimpleQuery(ctx, &c.readBuf)

		case clientMsgTerminate:
			return nil

		case clientMsgParse:
			c.doingExtendedQueryMessage = true
			err = c.handleParse(ctx, &c.readBuf)

		case clientMsgDescribe:
			c.doingExtendedQueryMessage = true
			err = c.handleDescribe(&c.readBuf)

		case clientMsgClose:
			c.doingExtendedQueryMessage = true
			err = c.handleClose(&c.readBuf)

		case clientMsgBind:
			c.doingExtendedQueryMessage = true
			err = c.handleBind(&c.readBuf)

		case clientMsgExecute:
			c.doingExtendedQueryMessage = true
			err = c.handleExecute(ctx, &c.readBuf)

		case clientMsgFlush:
			c.doingExtendedQueryMessage = true
			err = c.wr.Flush()

		default:
			err = c.sendErrorWithCode(pgerror.CodeProtocolViolationError, sqlbase.MakeSrcCtx(0),
				fmt.Sprintf("unrecognized client message type %s", typ))
		}
		if err != nil {
			return err
		}
	}
}

func (c *v3Conn) handleSimpleQuery(ctx context.Context, buf *readBuffer) error {
	query, err := buf.getString()
	if err != nil {
		return err
	}

	return c.executeStatements(ctx, query, nil, nil, true, 0)
}

func (c *v3Conn) handleParse(ctx context.Context, buf *readBuffer) error {
	name, err := buf.getString()
	if err != nil {
		return err
	}
	// The unnamed prepared statement can be freely overwritten.
	if name != "" {
		if c.session.PreparedStatements.Exists(name) {
			return c.sendInternalError(fmt.Sprintf("prepared statement %q already exists", name))
		}
	}
	query, err := buf.getString()
	if err != nil {
		return err
	}
	// The client may provide type information for (some of) the
	// placeholders. Read this first.
	numQArgTypes, err := buf.getUint16()
	if err != nil {
		return err
	}
	inTypeHints := make([]oid.Oid, numQArgTypes)
	for i := range inTypeHints {
		typ, err := buf.getUint32()
		if err != nil {
			return err
		}
		inTypeHints[i] = oid.Oid(typ)
	}
	// Prepare the mapping of SQL placeholder names to
	// types. Pre-populate it with the type hints received from the
	// client, if any.
	sqlTypeHints := make(parser.PlaceholderTypes)
	for i, t := range inTypeHints {
		if t == 0 {
			continue
		}
		v, ok := oidToDatum[t]
		if !ok {
			return c.sendInternalError(fmt.Sprintf("unknown oid type: %v", t))
		}
		sqlTypeHints[fmt.Sprint(i+1)] = v
	}
	// Create the new PreparedStatement in the connection's Session.
	stmt, err := c.session.PreparedStatements.New(ctx, c.executor, name, query, sqlTypeHints)
	if err != nil {
		return c.sendError(err)
	}
	// Convert the inferred SQL types back to an array of pgwire Oids.
	inTypes := make([]oid.Oid, 0, len(stmt.SQLTypes))
	for k, t := range stmt.SQLTypes {
		i, err := strconv.Atoi(k)
		if err != nil || i < 1 {
			return c.sendInternalError(fmt.Sprintf("invalid placeholder name: $%s", k))
		}
		// Placeholder names are 1-indexed; the arrays in the protocol are
		// 0-indexed.
		i--
		// Grow inTypes to be at least as large as i. Prepopulate all
		// slots with the hints provided, if any.
		for j := len(inTypes); j <= i; j++ {
			inTypes = append(inTypes, 0)
			if j < len(inTypeHints) {
				inTypes[j] = inTypeHints[j]
			}
		}
		// OID to Datum is not a 1-1 mapping (for example, int4 and int8
		// both map to TypeInt), so we need to maintain the types sent by
		// the client.
		if inTypes[i] != 0 {
			continue
		}
		id, ok := datumToOid[reflect.TypeOf(t)]
		if !ok {
			return c.sendInternalError(fmt.Sprintf("unknown datum type: %s", t.Type()))
		}
		inTypes[i] = id
	}
	for i, t := range inTypes {
		if t == 0 {
			return c.sendInternalError(
				fmt.Sprintf("could not determine data type of placeholder $%d", i+1))
		}
	}
	// Attach pgwire-specific metadata to the PreparedStatement.
	stmt.ProtocolMeta = preparedStatementMeta{inTypes: inTypes}
	c.writeBuf.initMsg(serverMsgParseComplete)
	return c.writeBuf.finishMsg(c.wr)
}

func (c *v3Conn) handleDescribe(buf *readBuffer) error {
	typ, err := buf.getPrepareType()
	if err != nil {
		return c.sendInternalError(err.Error())
	}
	name, err := buf.getString()
	if err != nil {
		return err
	}
	switch typ {
	case prepareStatement:
		stmt, ok := c.session.PreparedStatements.Get(name)
		if !ok {
			return c.sendInternalError(fmt.Sprintf("unknown prepared statement %q", name))
		}

		stmtMeta := stmt.ProtocolMeta.(preparedStatementMeta)
		c.writeBuf.initMsg(serverMsgParameterDescription)
		c.writeBuf.putInt16(int16(len(stmtMeta.inTypes)))
		for _, t := range stmtMeta.inTypes {
			c.writeBuf.putInt32(int32(t))
		}
		if err := c.writeBuf.finishMsg(c.wr); err != nil {
			return err
		}

		return c.sendRowDescription(stmt.Columns, nil)
	case preparePortal:
		portal, ok := c.session.PreparedPortals.Get(name)
		if !ok {
			return c.sendInternalError(fmt.Sprintf("unknown portal %q", name))
		}

		portalMeta := portal.ProtocolMeta.(preparedPortalMeta)
		return c.sendRowDescription(portal.Stmt.Columns, portalMeta.outFormats)
	default:
		return errors.Errorf("unknown describe type: %s", typ)
	}
}

func (c *v3Conn) handleClose(buf *readBuffer) error {
	typ, err := buf.getPrepareType()
	if err != nil {
		return c.sendInternalError(err.Error())
	}
	name, err := buf.getString()
	if err != nil {
		return err
	}
	switch typ {
	case prepareStatement:
		c.session.PreparedStatements.Delete(name)
	case preparePortal:
		c.session.PreparedPortals.Delete(name)
	default:
		return errors.Errorf("unknown close type: %s", typ)
	}
	c.writeBuf.initMsg(serverMsgCloseComplete)
	return c.writeBuf.finishMsg(c.wr)
}

func (c *v3Conn) handleBind(buf *readBuffer) error {
	portalName, err := buf.getString()
	if err != nil {
		return err
	}
	// The unnamed portal can be freely overwritten.
	if portalName != "" {
		if c.session.PreparedPortals.Exists(portalName) {
			return c.sendInternalError(fmt.Sprintf("portal %q already exists", portalName))
		}
	}
	statementName, err := buf.getString()
	if err != nil {
		return err
	}
	stmt, ok := c.session.PreparedStatements.Get(statementName)
	if !ok {
		return c.sendInternalError(fmt.Sprintf("unknown prepared statement %q", statementName))
	}

	stmtMeta := stmt.ProtocolMeta.(preparedStatementMeta)
	numQArgs := uint16(len(stmtMeta.inTypes))
	qArgFormatCodes := make([]formatCode, numQArgs)
	// From the docs on number of argument format codes to bind:
	// This can be zero to indicate that there are no arguments or that the
	// arguments all use the default format (text); or one, in which case the
	// specified format code is applied to all arguments; or it can equal the
	// actual number of arguments.
	// http://www.postgresql.org/docs/current/static/protocol-message-formats.html
	numQArgFormatCodes, err := buf.getUint16()
	if err != nil {
		return err
	}
	switch numQArgFormatCodes {
	case 0:
	case 1:
		// `1` means read one code and apply it to every argument.
		c, err := buf.getUint16()
		if err != nil {
			return err
		}
		fmtCode := formatCode(c)
		for i := range qArgFormatCodes {
			qArgFormatCodes[i] = fmtCode
		}
	case numQArgs:
		// Read one format code for each argument and apply it to that argument.
		for i := range qArgFormatCodes {
			c, err := buf.getUint16()
			if err != nil {
				return err
			}
			qArgFormatCodes[i] = formatCode(c)
		}
	default:
		return c.sendInternalError(fmt.Sprintf("wrong number of format codes specified: %d for %d arguments", numQArgFormatCodes, numQArgs))
	}

	numValues, err := buf.getUint16()
	if err != nil {
		return err
	}
	if numValues != numQArgs {
		return c.sendInternalError(fmt.Sprintf("expected %d arguments, got %d", numQArgs, numValues))
	}
	qargs := parser.QueryArguments{}
	for i, t := range stmtMeta.inTypes {
		plen, err := buf.getUint32()
		if err != nil {
			return err
		}
		k := fmt.Sprint(i + 1)
		if int32(plen) == -1 {
			// The argument is a NULL value.
			qargs[k] = parser.DNull
			continue
		}
		b, err := buf.getBytes(int(plen))
		if err != nil {
			return err
		}
		d, err := decodeOidDatum(t, qArgFormatCodes[i], b)
		if err != nil {
			return c.sendInternalError(fmt.Sprintf("error in argument for $%d: %s", i+1, err))
		}
		qargs[k] = d
	}

	numColumns := uint16(len(stmt.Columns))
	columnFormatCodes := make([]formatCode, numColumns)

	// From the docs on number of result-column format codes to bind:
	// This can be zero to indicate that there are no result columns or that
	// the result columns should all use the default format (text); or one, in
	// which case the specified format code is applied to all result columns
	// (if any); or it can equal the actual number of result columns of the
	// query.
	// http://www.postgresql.org/docs/current/static/protocol-message-formats.html
	numColumnFormatCodes, err := buf.getUint16()
	if err != nil {
		return err
	}
	switch numColumnFormatCodes {
	case 0:
	case 1:
		// Read one code and apply it to every column.
		c, err := buf.getUint16()
		if err != nil {
			return err
		}
		fmtCode := formatCode(c)
		for i := range columnFormatCodes {
			columnFormatCodes[i] = fmtCode
		}
	case numColumns:
		// Read one format code for each column and apply it to that column.
		for i := range columnFormatCodes {
			c, err := buf.getUint16()
			if err != nil {
				return err
			}
			columnFormatCodes[i] = formatCode(c)
		}
	default:
		return c.sendInternalError(fmt.Sprintf("expected 0, 1, or %d for number of format codes, got %d", numColumns, numColumnFormatCodes))
	}
	// Create the new PreparedPortal in the connection's Session.
	portal := c.session.PreparedPortals.New(portalName, stmt, qargs)
	// Attach pgwire-specific metadata to the PreparedPortal.
	portal.ProtocolMeta = preparedPortalMeta{outFormats: columnFormatCodes}
	c.writeBuf.initMsg(serverMsgBindComplete)
	return c.writeBuf.finishMsg(c.wr)
}

func (c *v3Conn) handleExecute(ctx context.Context, buf *readBuffer) error {
	portalName, err := buf.getString()
	if err != nil {
		return err
	}
	portal, ok := c.session.PreparedPortals.Get(portalName)
	if !ok {
		return c.sendInternalError(fmt.Sprintf("unknown portal %q", portalName))
	}
	limit, err := buf.getUint32()
	if err != nil {
		return err
	}

	stmt := portal.Stmt
	portalMeta := portal.ProtocolMeta.(preparedPortalMeta)
	pinfo := parser.PlaceholderInfo{
		Types:  stmt.SQLTypes,
		Values: portal.Qargs,
	}

	return c.executeStatements(ctx, stmt.Query, &pinfo, portalMeta.outFormats, false, int(limit))
}

func (c *v3Conn) executeStatements(
	ctx context.Context,
	stmts string,
	pinfo *parser.PlaceholderInfo,
	formatCodes []formatCode,
	sendDescription bool,
	limit int,
) error {
	tracing.AnnotateTrace()
	results := c.executor.ExecuteStatements(c.session, stmts, pinfo)

	tracing.AnnotateTrace()
	if results.Empty {
		// Skip executor and just send EmptyQueryResponse.
		c.writeBuf.initMsg(serverMsgEmptyQuery)
		return c.writeBuf.finishMsg(c.wr)
	}
	return c.sendResponse(results.ResultList, formatCodes, sendDescription, limit)
}

func (c *v3Conn) sendCommandComplete(tag []byte) error {
	c.writeBuf.initMsg(serverMsgCommandComplete)
	c.writeBuf.write(tag)
	c.writeBuf.nullTerminate()
	return c.writeBuf.finishMsg(c.wr)
}

func (c *v3Conn) sendError(err error) error {
	if sqlErr, ok := err.(sqlbase.ErrorWithPGCode); ok {
		return c.sendErrorWithCode(sqlErr.Code(), sqlErr.SrcContext(), err.Error())
	}
	return c.sendInternalError(err.Error())
}

// TODO(andrei): Figure out the correct codes to send for all the errors
// in this file and remove this function.
func (c *v3Conn) sendInternalError(errToSend string) error {
	return c.sendErrorWithCode(pgerror.CodeInternalError, sqlbase.MakeSrcCtx(1), errToSend)
}

// errCode is a postgres error code, plus our extensions.
// See http://www.postgresql.org/docs/9.5/static/errcodes-appendix.html
func (c *v3Conn) sendErrorWithCode(errCode string, errCtx sqlbase.SrcCtx, errToSend string) error {
	if c.doingExtendedQueryMessage {
		c.ignoreTillSync = true
	}

	c.writeBuf.initMsg(serverMsgErrorResponse)

	c.writeBuf.putErrFieldMsg(serverErrFieldSeverity)
	c.writeBuf.writeTerminatedString("ERROR")

	c.writeBuf.putErrFieldMsg(serverErrFieldSQLState)
	c.writeBuf.writeTerminatedString(errCode)

	c.writeBuf.putErrFieldMsg(serverErrFieldMsgPrimary)
	c.writeBuf.writeTerminatedString(errToSend)

	if errCtx.File != "" {
		c.writeBuf.putErrFieldMsg(serverErrFieldSrcFile)
		c.writeBuf.writeTerminatedString(errCtx.File)
	}

	if errCtx.Line > 0 {
		c.writeBuf.putErrFieldMsg(serverErrFieldSrcLine)
		c.writeBuf.writeTerminatedString(strconv.Itoa(errCtx.Line))
	}

	if errCtx.Function != "" {
		c.writeBuf.putErrFieldMsg(serverErrFieldSrcFunction)
		c.writeBuf.writeTerminatedString(errCtx.Function)
	}

	c.writeBuf.nullTerminate()
	if err := c.writeBuf.finishMsg(c.wr); err != nil {
		return err
	}
	return c.wr.Flush()
}

func (c *v3Conn) sendResponse(results sql.ResultList, formatCodes []formatCode, sendDescription bool, limit int) error {
	if len(results) == 0 {
		return c.sendCommandComplete(nil)
	}
	for _, result := range results {
		if result.Err != nil {
			if err := c.sendError(result.Err); err != nil {
				return err
			}
			break
		}
		if limit != 0 && len(result.Rows) > limit {
			if err := c.sendInternalError(fmt.Sprintf("execute row count limits not supported: %d of %d", limit, len(result.Rows))); err != nil {
				return err
			}
			break
		}

		if result.PGTag == "INSERT" {
			// From the postgres docs (49.5. Message Formats):
			// `INSERT oid rows`... oid is the object ID of the inserted row if
			//	rows is 1 and the target table has OIDs; otherwise oid is 0.
			result.PGTag = "INSERT 0"
		}
		tag := append(c.tagBuf[:0], result.PGTag...)

		switch result.Type {
		case parser.RowsAffected:
			// Send CommandComplete.
			tag = append(tag, ' ')
			tag = strconv.AppendInt(tag, int64(result.RowsAffected), 10)
			if err := c.sendCommandComplete(tag); err != nil {
				return err
			}

		case parser.Rows:
			if sendDescription {
				if err := c.sendRowDescription(result.Columns, formatCodes); err != nil {
					return err
				}
			}

			// Send DataRows.
			for _, row := range result.Rows {
				c.writeBuf.initMsg(serverMsgDataRow)
				c.writeBuf.putInt16(int16(len(row.Values)))
				for i, col := range row.Values {
					fmtCode := formatText
					if formatCodes != nil {
						fmtCode = formatCodes[i]
					}
					switch fmtCode {
					case formatText:
						c.writeBuf.writeTextDatum(col, c.session.Location)
					case formatBinary:
						c.writeBuf.writeBinaryDatum(col)
					default:
						c.writeBuf.setError(errors.Errorf("unsupported format code %s", fmtCode))
					}
				}
				if err := c.writeBuf.finishMsg(c.wr); err != nil {
					return err
				}
			}

			// Send CommandComplete.
			tag = append(tag, ' ')
			tag = strconv.AppendUint(tag, uint64(len(result.Rows)), 10)
			if err := c.sendCommandComplete(tag); err != nil {
				return err
			}

		case parser.Ack, parser.DDL:
			if err := c.sendCommandComplete(tag); err != nil {
				return err
			}

		default:
			panic(fmt.Sprintf("unexpected result type %v", result.Type))
		}
	}

	return nil
}

func (c *v3Conn) sendRowDescription(columns []sql.ResultColumn, formatCodes []formatCode) error {
	if len(columns) == 0 {
		c.writeBuf.initMsg(serverMsgNoData)
		return c.writeBuf.finishMsg(c.wr)
	}

	c.writeBuf.initMsg(serverMsgRowDescription)
	c.writeBuf.putInt16(int16(len(columns)))
	for i, column := range columns {
		if log.V(2) {
			log.Infof(context.TODO(), "pgwire writing column %s of type: %T", column.Name, column.Typ)
		}
		c.writeBuf.writeTerminatedString(column.Name)

		typ := typeForDatum(column.Typ)
		c.writeBuf.putInt32(0) // Table OID (optional).
		c.writeBuf.putInt16(0) // Column attribute ID (optional).
		c.writeBuf.putInt32(int32(typ.oid))
		c.writeBuf.putInt16(int16(typ.size))
		c.writeBuf.putInt32(0) // Type modifier (none of our supported types have modifiers).
		if formatCodes == nil {
			c.writeBuf.putInt16(int16(formatText))
		} else {
			c.writeBuf.putInt16(int16(formatCodes[i]))
		}
	}
	return c.writeBuf.finishMsg(c.wr)
}
